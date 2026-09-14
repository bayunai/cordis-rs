use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

static NEXT_EFFECT_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Default)]
struct Resources {
    children: Vec<EffectScope>,
    cleanups: Vec<Box<dyn FnOnce() + Send>>,
    tasks: Vec<JoinHandle<()>>,
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
            .map(|resources| resources.tasks.len())
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
        resources.tasks.push(task);
    }

    pub(crate) fn dispose(&self) {
        let _ = self.dispose_with(TaskPolicy::HoistToParent);
    }

    pub(crate) async fn dispose_wait(&self) {
        let tasks = self.dispose_with(TaskPolicy::AwaitLocal);
        for task in tasks {
            let _ = task.await;
        }
    }

    fn dispose_with(&self, policy: TaskPolicy) -> Vec<JoinHandle<()>> {
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
        drop(resources);

        for child in children.into_iter().rev() {
            child.dispose();
        }
        for cleanup in cleanups.into_iter().rev() {
            cleanup();
        }

        let tasks = self.take_tasks_shallow();
        match policy {
            TaskPolicy::HoistToParent => {
                if let Some(parent) = self.parent_inner() {
                    if let Ok(mut resources) = parent.resources.lock() {
                        resources.tasks.extend(tasks);
                    } else {
                        Self::abort_tasks(tasks);
                    }
                } else if let Ok(mut resources) = self.inner.resources.lock() {
                    resources.tasks.extend(tasks);
                } else {
                    Self::abort_tasks(tasks);
                }
                self.detach_from_parent();
                Vec::new()
            }
            TaskPolicy::AwaitLocal => {
                self.detach_from_parent();
                tasks
            }
        }
    }

    fn take_tasks_shallow(&self) -> Vec<JoinHandle<()>> {
        let Ok(mut resources) = self.inner.resources.lock() else {
            return Vec::new();
        };
        std::mem::take(&mut resources.tasks)
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

    fn abort_tasks(tasks: Vec<JoinHandle<()>>) {
        for task in tasks {
            task.abort();
        }
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
                let tasks = std::mem::take(&mut resources.tasks);
                drop(resources);
                for child in children.into_iter().rev() {
                    child.dispose();
                }
                for cleanup in cleanups.into_iter().rev() {
                    cleanup();
                }
                EffectScope::abort_tasks(tasks);
            }
        } else if let Ok(mut resources) = self.resources.lock() {
            let tasks = std::mem::take(&mut resources.tasks);
            drop(resources);
            let parent = self
                .parent
                .lock()
                .ok()
                .and_then(|slot| slot.as_ref().and_then(Weak::upgrade));
            if let Some(parent) = parent {
                if let Ok(mut parent_resources) = parent.resources.lock() {
                    parent_resources.tasks.extend(tasks);
                } else {
                    EffectScope::abort_tasks(tasks);
                }
            } else {
                EffectScope::abort_tasks(tasks);
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
