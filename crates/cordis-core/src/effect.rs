use crate::CoreError;
use std::{
    future::Future,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

static NEXT_EFFECT_ID: AtomicU64 = AtomicU64::new(1);

enum ManagedWork {
    Task(JoinHandle<()>),
    Disposer(JoinHandle<Result<(), CoreError>>),
}

type AsyncDisposer = Box<dyn FnOnce() -> JoinHandle<Result<(), CoreError>> + Send>;

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
    cancellation: CancellationToken,
    disposed: AtomicBool,
    parent: Mutex<Option<Weak<EffectScopeInner>>>,
    resources: Mutex<Resources>,
}

/// 一个可撤销的资源所有权边界。
#[derive(Clone)]
pub(crate) struct EffectScope {
    inner: Arc<EffectScopeInner>,
}

impl EffectScope {
    pub(crate) fn root() -> Self {
        Self::new("root", CancellationToken::new())
    }

    fn new(name: impl Into<String>, cancellation: CancellationToken) -> Self {
        Self {
            inner: Arc::new(EffectScopeInner {
                id: NEXT_EFFECT_ID.fetch_add(1, Ordering::Relaxed),
                name: name.into(),
                cancellation,
                disposed: AtomicBool::new(false),
                parent: Mutex::new(None),
                resources: Mutex::new(Resources::default()),
            }),
        }
    }

    pub(crate) fn child(&self) -> Self {
        self.child_named("effect")
    }

    pub(crate) fn child_named(&self, name: impl Into<String>) -> Self {
        let child = Self::new(name, self.inner.cancellation.child_token());
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
        resources.async_disposers.push(Box::new(move || {
            tokio::spawn(async move { disposer().await })
        }));
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
        let _ = self.dispose_with(TaskPolicy::HoistToParent);
    }

    pub(crate) async fn dispose_wait(&self) -> Result<(), CoreError> {
        let work = self.dispose_with(TaskPolicy::AwaitLocal);
        Self::await_managed_work(work).await
    }

    fn dispose_with(&self, policy: TaskPolicy) -> Vec<ManagedWork> {
        if self.inner.disposed.swap(true, Ordering::AcqRel) {
            self.detach_from_parent();
            return Vec::new();
        }
        self.inner.cancellation.cancel();
        let Ok(mut resources) = self.inner.resources.lock() else {
            self.detach_from_parent();
            return Vec::new();
        };
        let children = std::mem::take(&mut resources.children);
        let cleanups = std::mem::take(&mut resources.cleanups);
        let async_disposers = std::mem::take(&mut resources.async_disposers);
        drop(resources);

        for child in children.into_iter().rev() {
            child.dispose();
        }
        for cleanup in cleanups.into_iter().rev() {
            cleanup();
        }

        {
            let Ok(mut resources) = self.inner.resources.lock() else {
                self.detach_from_parent();
                return Vec::new();
            };
            for disposer in async_disposers.into_iter().rev() {
                resources.work.push(ManagedWork::Disposer(disposer()));
            }
        }

        let work = self.take_work_shallow();
        match policy {
            TaskPolicy::HoistToParent => {
                if let Some(parent) = self.parent_inner() {
                    if let Ok(mut resources) = parent.resources.lock() {
                        resources.work.extend(work);
                    } else {
                        Self::abort_work(work);
                    }
                } else if let Ok(mut resources) = self.inner.resources.lock() {
                    resources.work.extend(work);
                } else {
                    Self::abort_work(work);
                }
                self.detach_from_parent();
                Vec::new()
            }
            TaskPolicy::AwaitLocal => {
                self.detach_from_parent();
                work
            }
        }
    }

    fn take_work_shallow(&self) -> Vec<ManagedWork> {
        let Ok(mut resources) = self.inner.resources.lock() else {
            return Vec::new();
        };
        std::mem::take(&mut resources.work)
    }

    async fn await_managed_work(work: Vec<ManagedWork>) -> Result<(), CoreError> {
        let mut errors = Vec::new();
        for item in work {
            match item {
                ManagedWork::Task(handle) => {
                    if let Err(error) = handle.await {
                        errors.push(format!("managed task join failed: {error}"));
                    }
                }
                ManagedWork::Disposer(handle) => match handle.await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => errors.push(error.to_string()),
                    Err(error) => errors.push(format!("async disposer join failed: {error}")),
                },
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(CoreError::DisposeFailed { errors })
        }
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
                ManagedWork::Disposer(handle) => handle.abort(),
            }
        }
    }

    fn start_async_disposers(async_disposers: Vec<AsyncDisposer>) -> Vec<ManagedWork> {
        async_disposers
            .into_iter()
            .rev()
            .map(|disposer| ManagedWork::Disposer(disposer()))
            .collect()
    }
}

#[derive(Clone, Copy)]
enum TaskPolicy {
    HoistToParent,
    AwaitLocal,
}

impl Drop for EffectScopeInner {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if !self.disposed.swap(true, Ordering::AcqRel) {
            if let Ok(mut resources) = self.resources.lock() {
                let children = std::mem::take(&mut resources.children);
                let cleanups = std::mem::take(&mut resources.cleanups);
                let async_disposers = std::mem::take(&mut resources.async_disposers);
                let mut work = std::mem::take(&mut resources.work);
                drop(resources);
                for child in children.into_iter().rev() {
                    child.dispose();
                }
                for cleanup in cleanups.into_iter().rev() {
                    cleanup();
                }
                work.extend(EffectScope::start_async_disposers(async_disposers));
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
        } else if let Ok(mut resources) = self.resources.lock() {
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
