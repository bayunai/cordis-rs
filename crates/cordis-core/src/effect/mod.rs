//! [`EffectScope`] / [`EffectHandle`]：可撤销资源的所有权边界。
//!
//! 子模块：`resources` 存放资源集合与受控工作记录；`dispose` 负责清理与等待顺序。

mod dispose;
mod resources;

use crate::CoreError;
use std::{
    future::Future,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;

use dispose::DisposeCompletion;
use resources::{BoxFuture, ManagedWork, Resources};

static NEXT_EFFECT_ID: AtomicU64 = AtomicU64::new(1);

/// 共享不可变祖先链：子 Scope 只挂一层 `Arc`，不按深度拷贝 ID 数组。
#[derive(Clone)]
struct AncestorNode {
    id: u64,
    parent: Option<Arc<AncestorNode>>,
}

fn lineage_contains(lineage: &Option<Arc<AncestorNode>>, id: u64) -> bool {
    let mut current = lineage.as_ref();
    while let Some(node) = current {
        if node.id == id {
            return true;
        }
        current = node.parent.as_ref();
    }
    false
}

pub(super) struct EffectScopeInner {
    pub(super) id: u64,
    pub(super) name: String,
    pub(super) handle: Handle,
    pub(super) cancellation: CancellationToken,
    pub(super) disposed: AtomicBool,
    pub(super) parent: Mutex<Option<Weak<EffectScopeInner>>>,
    /// 创建时冻结的逻辑祖先链。`detach_from_parent` 只摘除父子弱引用，不清本链。
    lineage: Option<Arc<AncestorNode>>,
    resources: Mutex<Resources>,
    /// `completion` 是受控释放的唯一线性化状态：Some 表示已开始释放，且所有
    /// 并发调用者必须等待同一轮结果。先写入它再置 `disposed`，避免父 Scope
    /// 观察到“已释放但尚未有 Completion”的中间状态。
    completion: Mutex<Option<Arc<DisposeCompletion>>>,
    registry: Mutex<Weak<crate::registry::Registry>>,
    executions: Mutex<usize>,
    execution_finished: tokio::sync::Notify,
}

/// 一个可撤销的资源所有权边界。
#[derive(Clone)]
pub(crate) struct EffectScope {
    pub(super) inner: Arc<EffectScopeInner>,
}

impl EffectScope {
    pub(crate) fn root(handle: Handle) -> Self {
        Self::new("root", handle, CancellationToken::new(), None)
    }

    fn new(
        name: impl Into<String>,
        handle: Handle,
        cancellation: CancellationToken,
        lineage: Option<Arc<AncestorNode>>,
    ) -> Self {
        Self {
            inner: Arc::new(EffectScopeInner {
                id: NEXT_EFFECT_ID.fetch_add(1, Ordering::Relaxed),
                name: name.into(),
                handle,
                cancellation,
                disposed: AtomicBool::new(false),
                parent: Mutex::new(None),
                lineage,
                resources: Mutex::new(Resources::default()),
                completion: Mutex::new(None),
                registry: Mutex::new(Weak::new()),
                executions: Mutex::new(0),
                execution_finished: tokio::sync::Notify::new(),
            }),
        }
    }

    pub(crate) fn child_named(&self, name: impl Into<String>) -> Self {
        let lineage = Some(Arc::new(AncestorNode {
            id: self.inner.id,
            parent: self.inner.lineage.clone(),
        }));
        let child = Self::new(
            name,
            self.inner.handle.clone(),
            CancellationToken::new(),
            lineage,
        );
        *child.inner.registry.lock().expect("scope registry") =
            self.inner.registry.lock().expect("scope registry").clone();
        if let Ok(mut parent) = child.inner.parent.lock() {
            *parent = Some(Arc::downgrade(&self.inner));
        }
        let Ok(mut resources) = self.inner.resources.lock() else {
            child.dispose();
            return child;
        };
        if self.inner.disposed.load(Ordering::Acquire) {
            drop(resources);
            child.dispose();
            return child;
        }
        resources.children.push(child.clone());
        child
    }

    pub(crate) fn attach_registry(&self, registry: &Arc<crate::registry::Registry>) {
        *self.inner.registry.lock().expect("scope registry") = Arc::downgrade(registry);
    }

    pub(crate) fn registry(&self) -> Option<Arc<crate::registry::Registry>> {
        self.inner
            .registry
            .lock()
            .expect("scope registry")
            .upgrade()
    }

    pub(crate) fn contains(&self, other: &Self) -> bool {
        self.ptr_eq(other) || lineage_contains(&other.inner.lineage, self.id())
    }

    pub(crate) fn begin_execution(&self) -> Result<ExecutionGuard, CoreError> {
        let mut count = self.inner.executions.lock().expect("executions");
        if self.is_disposed() {
            return Err(CoreError::ContextDisposed);
        }
        *count += 1;
        Ok(ExecutionGuard(self.clone()))
    }

    pub(crate) async fn wait_executions(&self) {
        loop {
            let notified = self.inner.execution_finished.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if *self.inner.executions.lock().expect("executions") == 0 {
                return;
            }
            notified.await;
        }
    }

    pub(crate) fn runtime_handle(&self) -> Handle {
        self.inner.handle.clone()
    }

    /// 两 Scope 是否属于同一 Effect 树（自身或互为祖先；不依赖 live parent 弱引用）。
    pub(crate) fn is_same_tree_as(&self, other: &Self) -> bool {
        if self.ptr_eq(other) {
            return true;
        }
        lineage_contains(&self.inner.lineage, other.inner.id)
            || lineage_contains(&other.inner.lineage, self.inner.id)
    }

    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    pub(crate) fn id(&self) -> u64 {
        self.inner.id
    }

    pub(crate) fn name(&self) -> &str {
        &self.inner.name
    }

    pub(crate) fn parent_id(&self) -> Option<u64> {
        self.inner
            .parent
            .lock()
            .ok()
            .and_then(|slot| slot.as_ref().and_then(Weak::upgrade))
            .map(|parent| parent.id)
    }

    pub(crate) fn is_disposed(&self) -> bool {
        self.inner.disposed.load(Ordering::Acquire)
            || self
                .parent_inner()
                .is_some_and(|parent| EffectScope { inner: parent }.is_disposed())
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.inner.cancellation.is_cancelled()
    }

    pub(crate) fn cancellation(&self) -> CancellationToken {
        self.inner.cancellation.clone()
    }

    pub(crate) fn child_scope_count(&self) -> usize {
        self.inner
            .resources
            .lock()
            .map(|resources| {
                resources
                    .children
                    .iter()
                    .filter(|child| !child.is_disposed())
                    .count()
            })
            .unwrap_or(0)
    }

    pub(crate) fn task_count(&self) -> usize {
        self.inner
            .resources
            .lock()
            .map(|resources| {
                resources
                    .work
                    .iter()
                    .filter(|item| matches!(item, ManagedWork::Task(_)))
                    .count()
            })
            .unwrap_or(0)
    }

    pub(crate) fn cleanup_count(&self) -> usize {
        self.inner
            .resources
            .lock()
            .map(|resources| resources.cleanups.len())
            .unwrap_or(0)
    }

    /// Linearize registration and parent ownership against parent resource extraction.
    pub(crate) fn register_owned<T>(
        &self,
        register: impl FnOnce() -> Result<T, CoreError>,
        cleanup: impl FnOnce() + Send + 'static,
    ) -> Result<T, CoreError> {
        let mut resources = self
            .inner
            .resources
            .lock()
            .map_err(|_| CoreError::ContextDisposed)?;
        if self.is_disposed() {
            return Err(CoreError::ContextDisposed);
        }
        let value = register()?;
        resources.cleanups.push(Box::new(cleanup));
        Ok(value)
    }

    pub(crate) fn on_dispose(&self, cleanup: impl FnOnce() + Send + 'static) {
        let Ok(mut resources) = self.inner.resources.lock() else {
            cleanup();
            return;
        };
        if self.is_disposed() && *self.inner.executions.lock().expect("executions") == 0 {
            drop(resources);
            cleanup();
            return;
        }
        resources.cleanups.push(Box::new(cleanup));
    }

    pub(crate) fn on_dispose_async<F, Fut>(&self, disposer: F) -> Result<(), CoreError>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), CoreError>> + Send + 'static,
    {
        let Ok(mut resources) = self.inner.resources.lock() else {
            return Err(CoreError::ContextDisposed);
        };
        if self.is_disposed() && *self.inner.executions.lock().expect("executions") == 0 {
            return Err(CoreError::ContextDisposed);
        }
        resources
            .async_disposers
            .push(Box::new(move || Box::pin(disposer()) as BoxFuture));
        Ok(())
    }

    /// Start work only after it has atomically joined this Scope's resource set.
    pub(crate) fn spawn_task(
        &self,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), CoreError> {
        let mut resources = self
            .inner
            .resources
            .lock()
            .map_err(|_| CoreError::ContextDisposed)?;
        if self.is_disposed() {
            return Err(CoreError::ContextDisposed);
        }
        let task = self.inner.handle.spawn(future);
        resources.work.push(ManagedWork::Task(task));
        Ok(())
    }
}

pub(crate) struct ExecutionGuard(EffectScope);

impl Drop for ExecutionGuard {
    fn drop(&mut self) {
        *self.0.inner.executions.lock().expect("executions") -= 1;
        self.0.inner.execution_finished.notify_waiters();
    }
}

/// 具名 Effect 的公开句柄（诊断与生命周期观察）。
#[derive(Clone)]
pub struct EffectHandle {
    scope: EffectScope,
}

impl EffectHandle {
    pub(crate) fn from_scope(scope: EffectScope) -> Self {
        Self { scope }
    }

    pub fn id(&self) -> u64 {
        self.scope.id()
    }

    pub fn name(&self) -> &str {
        self.scope.name()
    }

    pub fn parent_id(&self) -> Option<u64> {
        self.scope.parent_id()
    }

    pub fn is_disposed(&self) -> bool {
        self.scope.is_disposed()
    }

    pub fn is_cancelled(&self) -> bool {
        self.scope.is_cancelled()
    }

    pub fn child_count(&self) -> usize {
        self.scope.child_scope_count()
    }

    pub fn task_count(&self) -> usize {
        self.scope.task_count()
    }

    pub fn cleanup_count(&self) -> usize {
        self.scope.cleanup_count()
    }
}

#[cfg(test)]
mod lineage_tests {
    use super::*;

    #[test]
    fn shared_lineage_survives_detach_and_siblings_are_distinct() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let handle = runtime.handle().clone();
        std::mem::forget(runtime);

        let root = EffectScope::root(handle);
        let left = root.child_named("left");
        let right = root.child_named("right");
        let nested = left.child_named("nested");

        assert!(left.is_same_tree_as(&nested));
        assert!(nested.is_same_tree_as(&left));
        assert!(root.is_same_tree_as(&nested));
        assert!(!left.is_same_tree_as(&right));
        assert!(!nested.is_same_tree_as(&right));

        nested.detach_from_parent();
        assert!(nested.parent_id().is_none());
        // detach 只摘 live parent，逻辑祖先链仍用于同树判定。
        assert!(nested.is_same_tree_as(&left));
        assert!(left.is_same_tree_as(&nested));
    }

    // P1-4 相关：已 dispose 父上 `child_named` 会立刻 `dispose()`（可能 HoistToParent）。
    // 子自身 DisposeCompletion 应仍可在有限时间内 wait（与 AwaitLocal 父/子竞态分测）。
    #[tokio::test]
    async fn p1_child_named_on_disposed_parent_local_wait_converges() {
        let handle = tokio::runtime::Handle::current();
        let root = EffectScope::root(handle);
        let parent = root.child_named("parent");
        parent.dispose_wait().await.expect("parent dispose_wait");
        assert!(parent.is_disposed());

        let child = parent.child_named("late");
        assert!(child.is_disposed());
        tokio::time::timeout(std::time::Duration::from_secs(2), child.dispose_wait())
            .await
            .expect("late child dispose_wait must not hang")
            .expect("dispose_wait");
    }
}
