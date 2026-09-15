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

use tokio::{runtime::Handle, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use dispose::DisposeCompletion;
use resources::{BoxFuture, ManagedWork, Resources};

static NEXT_EFFECT_ID: AtomicU64 = AtomicU64::new(1);

pub(super) struct EffectScopeInner {
    pub(super) id: u64,
    pub(super) name: String,
    pub(super) handle: Handle,
    pub(super) cancellation: CancellationToken,
    pub(super) disposed: AtomicBool,
    pub(super) parent: Mutex<Option<Weak<EffectScopeInner>>>,
    /// 创建时冻结的祖先 Scope ID。释放会解除 `parent` 弱引用，但生命周期
    /// 回调仍需判断自己是否在被等待的 Effect 树内。
    ancestors: Vec<u64>,
    resources: Mutex<Resources>,
    completion: Mutex<Option<Arc<DisposeCompletion>>>,
}

/// 一个可撤销的资源所有权边界。
#[derive(Clone)]
pub(crate) struct EffectScope {
    pub(super) inner: Arc<EffectScopeInner>,
}

impl EffectScope {
    pub(crate) fn root(handle: Handle) -> Self {
        Self::new("root", handle, CancellationToken::new(), Vec::new())
    }

    fn new(
        name: impl Into<String>,
        handle: Handle,
        cancellation: CancellationToken,
        ancestors: Vec<u64>,
    ) -> Self {
        Self {
            inner: Arc::new(EffectScopeInner {
                id: NEXT_EFFECT_ID.fetch_add(1, Ordering::Relaxed),
                name: name.into(),
                handle,
                cancellation,
                disposed: AtomicBool::new(false),
                parent: Mutex::new(None),
                ancestors,
                resources: Mutex::new(Resources::default()),
                completion: Mutex::new(None),
            }),
        }
    }

    pub(crate) fn child(&self) -> Self {
        self.child_named("effect")
    }

    pub(crate) fn child_named(&self, name: impl Into<String>) -> Self {
        let child = Self::new(
            name,
            self.inner.handle.clone(),
            self.inner.cancellation.child_token(),
            self.inner
                .ancestors
                .iter()
                .copied()
                .chain(std::iter::once(self.inner.id))
                .collect(),
        );
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

    pub(crate) fn runtime_handle(&self) -> Handle {
        self.inner.handle.clone()
    }

    /// 两 Scope 是否属于同一 Effect 树（自身或互为祖先）。
    pub(crate) fn is_same_tree_as(&self, other: &Self) -> bool {
        if Arc::ptr_eq(&self.inner, &other.inner) {
            return true;
        }
        self.inner.ancestors.contains(&other.inner.id)
            || other.inner.ancestors.contains(&self.inner.id)
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
            .map(|resources| resources.children.len())
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

    pub(crate) fn on_dispose(&self, cleanup: impl FnOnce() + Send + 'static) {
        let Ok(mut resources) = self.inner.resources.lock() else {
            cleanup();
            return;
        };
        if self.inner.disposed.load(Ordering::Acquire) {
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
        if self.inner.disposed.load(Ordering::Acquire) {
            return Err(CoreError::ContextDisposed);
        }
        resources
            .async_disposers
            .push(Box::new(move || Box::pin(disposer()) as BoxFuture));
        Ok(())
    }

    pub(crate) fn push_task(&self, task: JoinHandle<()>) {
        let Ok(mut resources) = self.inner.resources.lock() else {
            task.abort();
            return;
        };
        if self.inner.disposed.load(Ordering::Acquire) {
            drop(resources);
            task.abort();
            return;
        }
        resources.work.push(ManagedWork::Task(task));
    }
}

/// 具名 Effect 的公开句柄（诊断与生命周期观察）。
#[derive(Clone)]
pub struct EffectHandle {
    scope: EffectScope,
}

#[allow(dead_code)]
impl EffectHandle {
    #[allow(dead_code)]
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

    #[allow(dead_code)]
    pub(crate) fn scope(&self) -> &EffectScope {
        &self.scope
    }
}
