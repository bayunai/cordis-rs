use crate::CoreError;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use tokio::{runtime::Handle, sync::Notify, task::JoinHandle};
use tokio_util::sync::CancellationToken;

static NEXT_EFFECT_ID: AtomicU64 = AtomicU64::new(1);

type BoxFuture = Pin<Box<dyn Future<Output = Result<(), CoreError>> + Send>>;
type AsyncDisposer = Box<dyn FnOnce() -> BoxFuture + Send>;

enum ManagedWork {
    Task(JoinHandle<()>),
    /// 等待子 Scope `DisposeCompletion`，并把聚合错误带回父协调任务。
    ChildWait(JoinHandle<Result<(), Vec<String>>>),
}

struct DisposeCompletion {
    notify: Notify,
    result: Mutex<Option<Result<(), Vec<String>>>>,
    /// 保持释放协调任务存活，直至本对象被释放。
    retain: Mutex<Option<JoinHandle<()>>>,
}

impl DisposeCompletion {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            notify: Notify::new(),
            result: Mutex::new(None),
            retain: Mutex::new(None),
        })
    }

    fn attach_coordinator(&self, handle: JoinHandle<()>) {
        *self.retain.lock().expect("dispose retain") = Some(handle);
    }

    fn finish(&self, result: Result<(), Vec<String>>) {
        {
            let mut slot = self.result.lock().expect("dispose completion");
            if slot.is_some() {
                return;
            }
            *slot = Some(result);
        }
        self.notify.notify_waiters();
    }

    async fn wait(self: &Arc<Self>) -> Result<(), CoreError> {
        loop {
            {
                let slot = self.result.lock().expect("dispose completion");
                if let Some(result) = slot.as_ref() {
                    return match result {
                        Ok(()) => Ok(()),
                        Err(errors) => Err(CoreError::DisposeFailed {
                            errors: errors.clone(),
                        }),
                    };
                }
            }
            self.notify.notified().await;
        }
    }
}

#[derive(Default)]
struct Resources {
    children: Vec<EffectScope>,
    cleanups: Vec<Box<dyn FnOnce() + Send>>,
    async_disposers: Vec<AsyncDisposer>,
    work: Vec<ManagedWork>,
}

struct EffectScopeInner {
    id: u64,
    name: String,
    handle: Handle,
    cancellation: CancellationToken,
    disposed: AtomicBool,
    parent: Mutex<Option<Weak<EffectScopeInner>>>,
    resources: Mutex<Resources>,
    completion: Mutex<Option<Arc<DisposeCompletion>>>,
}

/// 一个可撤销的资源所有权边界。
#[derive(Clone)]
pub(crate) struct EffectScope {
    inner: Arc<EffectScopeInner>,
}

impl EffectScope {
    pub(crate) fn root(handle: Handle) -> Self {
        Self::new("root", handle, CancellationToken::new())
    }

    fn new(name: impl Into<String>, handle: Handle, cancellation: CancellationToken) -> Self {
        Self {
            inner: Arc::new(EffectScopeInner {
                id: NEXT_EFFECT_ID.fetch_add(1, Ordering::Relaxed),
                name: name.into(),
                handle,
                cancellation,
                disposed: AtomicBool::new(false),
                parent: Mutex::new(None),
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

    pub(crate) fn dispose(&self) {
        let _ = self.begin_dispose(DisposePolicy::HoistToParent);
    }

    /// 非受控关闭：同步 cleanup 与取消，不启动尚未执行的 async disposer。
    pub(crate) fn abandon(&self) {
        if self.inner.disposed.swap(true, Ordering::AcqRel) {
            self.detach_from_parent();
            return;
        }
        let completion = DisposeCompletion::new();
        *self.inner.completion.lock().expect("dispose completion") = Some(completion.clone());
        self.inner.cancellation.cancel();

        let Ok(mut resources) = self.inner.resources.lock() else {
            completion.finish(Ok(()));
            self.detach_from_parent();
            return;
        };
        let children = std::mem::take(&mut resources.children);
        let cleanups = std::mem::take(&mut resources.cleanups);
        let _discarded_async = std::mem::take(&mut resources.async_disposers);
        let work = std::mem::take(&mut resources.work);
        drop(resources);

        for child in children.into_iter().rev() {
            child.abandon();
        }
        for cleanup in cleanups.into_iter().rev() {
            cleanup();
        }
        Self::abort_work(work);
        completion.finish(Ok(()));
        self.detach_from_parent();
    }

    pub(crate) async fn dispose_wait(&self) -> Result<(), CoreError> {
        if let Some(completion) = self.begin_dispose(DisposePolicy::AwaitLocal) {
            return completion.wait().await;
        }
        let completion = self
            .inner
            .completion
            .lock()
            .expect("dispose completion")
            .clone()
            .expect("dispose completion missing after dispose");
        completion.wait().await
    }

    fn begin_dispose(&self, policy: DisposePolicy) -> Option<Arc<DisposeCompletion>> {
        if self.inner.disposed.swap(true, Ordering::AcqRel) {
            self.detach_from_parent();
            return None;
        }

        let completion = DisposeCompletion::new();
        *self.inner.completion.lock().expect("dispose completion") = Some(completion.clone());
        self.inner.cancellation.cancel();

        let Ok(mut resources) = self.inner.resources.lock() else {
            completion.finish(Ok(()));
            self.detach_from_parent();
            return Some(completion);
        };
        let children = std::mem::take(&mut resources.children);
        let cleanups = std::mem::take(&mut resources.cleanups);
        let async_disposers = std::mem::take(&mut resources.async_disposers);
        let existing_work = std::mem::take(&mut resources.work);
        drop(resources);

        for child in children.into_iter().rev() {
            child.dispose();
        }
        for cleanup in cleanups.into_iter().rev() {
            cleanup();
        }

        let mut work = existing_work;
        work.extend(self.take_work_shallow());

        let handle = self.inner.handle.clone();
        let completion_for_task = completion.clone();
        let coordinator = handle.spawn(async move {
            let mut errors = Vec::new();
            for disposer in async_disposers.into_iter().rev() {
                match disposer().await {
                    Ok(()) => {}
                    Err(error) => errors.push(error.to_string()),
                }
            }
            for item in work {
                match item {
                    ManagedWork::Task(join) => {
                        if let Err(error) = join.await {
                            errors.push(format!("managed task join failed: {error}"));
                        }
                    }
                    ManagedWork::ChildWait(join) => match join.await {
                        Ok(Ok(())) => {}
                        Ok(Err(mut nested)) => errors.append(&mut nested),
                        Err(error) => {
                            errors.push(format!("child dispose wait join failed: {error}"));
                        }
                    },
                }
            }
            let result = if errors.is_empty() {
                Ok(())
            } else {
                Err(errors)
            };
            completion_for_task.finish(result);
        });
        completion.attach_coordinator(coordinator);

        match policy {
            DisposePolicy::HoistToParent => {
                let child_completion = completion.clone();
                let waiter = handle.spawn(async move {
                    match child_completion.wait().await {
                        Ok(()) => Ok(()),
                        Err(CoreError::DisposeFailed { errors }) => Err(errors),
                        Err(other) => Err(vec![other.to_string()]),
                    }
                });
                self.hoist_work(ManagedWork::ChildWait(waiter));
                self.detach_from_parent();
            }
            DisposePolicy::AwaitLocal => {
                self.detach_from_parent();
            }
        }

        Some(completion)
    }

    fn hoist_work(&self, work: ManagedWork) {
        if let Some(parent) = self.parent_inner() {
            if let Ok(mut resources) = parent.resources.lock() {
                resources.work.push(work);
                return;
            }
            Self::abort_work(vec![work]);
            return;
        }
        if let Ok(mut resources) = self.inner.resources.lock() {
            resources.work.push(work);
        } else {
            Self::abort_work(vec![work]);
        }
    }

    fn take_work_shallow(&self) -> Vec<ManagedWork> {
        let Ok(mut resources) = self.inner.resources.lock() else {
            return Vec::new();
        };
        std::mem::take(&mut resources.work)
    }

    fn parent_inner(&self) -> Option<Arc<EffectScopeInner>> {
        self.inner
            .parent
            .lock()
            .ok()
            .and_then(|slot| slot.as_ref().and_then(Weak::upgrade))
    }

    fn detach_from_parent(&self) {
        let parent = self
            .inner
            .parent
            .lock()
            .ok()
            .and_then(|mut slot| slot.take())
            .and_then(|weak| weak.upgrade());
        let Some(parent) = parent else {
            return;
        };
        if let Ok(mut resources) = parent.resources.lock() {
            resources
                .children
                .retain(|child| !Arc::ptr_eq(&child.inner, &self.inner));
        }
    }

    fn abort_work(work: Vec<ManagedWork>) {
        for item in work {
            match item {
                ManagedWork::Task(handle) => handle.abort(),
                ManagedWork::ChildWait(handle) => handle.abort(),
            }
        }
    }
}

#[derive(Clone, Copy)]
enum DisposePolicy {
    HoistToParent,
    AwaitLocal,
}

impl Drop for EffectScopeInner {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if !self.disposed.swap(true, Ordering::AcqRel) {
            // 非受控关闭：同步 cleanup；绝不启动尚未执行的 async disposer。
            if let Ok(mut resources) = self.resources.lock() {
                let children = std::mem::take(&mut resources.children);
                let cleanups = std::mem::take(&mut resources.cleanups);
                let _discarded_async = std::mem::take(&mut resources.async_disposers);
                let work = std::mem::take(&mut resources.work);
                drop(resources);
                for child in children.into_iter().rev() {
                    child.abandon();
                }
                for cleanup in cleanups.into_iter().rev() {
                    cleanup();
                }
                let parent = self
                    .parent
                    .lock()
                    .ok()
                    .and_then(|slot| slot.as_ref().and_then(Weak::upgrade));
                if let Some(parent) = parent {
                    if let Ok(mut parent_resources) = parent.resources.lock() {
                        parent_resources.work.extend(work);
                    } else {
                        EffectScope::abort_work(work);
                    }
                } else {
                    EffectScope::abort_work(work);
                }
            }
            if let Ok(mut slot) = self.completion.lock()
                && let Some(completion) = slot.take()
            {
                completion.finish(Ok(()));
            }
        } else if let Ok(mut resources) = self.resources.lock() {
            // 已受控 dispose：ChildWait 代理可能仍在 parent.work；本 Scope 残留 work 上收或丢弃。
            let work = std::mem::take(&mut resources.work);
            drop(resources);
            let parent = self
                .parent
                .lock()
                .ok()
                .and_then(|slot| slot.as_ref().and_then(Weak::upgrade));
            if let Some(parent) = parent {
                if let Ok(mut parent_resources) = parent.resources.lock() {
                    parent_resources.work.extend(work);
                } else {
                    EffectScope::abort_work(work);
                }
            } else {
                // Root 已 dispose 后 Drop：残留 ChildWait 不应 abort（协调由 DisposeCompletion.retain 持有）。
                // 此处 work 若含 root 自挂的 ChildWait，abort 只会取消等待代理，completion 仍由 retain 完成。
                EffectScope::abort_work(work);
            }
        }
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
