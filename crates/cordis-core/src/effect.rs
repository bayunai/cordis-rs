use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicBool, Ordering},
};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct Resources {
    children: Vec<EffectScope>,
    cleanups: Vec<Box<dyn FnOnce() + Send>>,
    tasks: Vec<JoinHandle<()>>,
}

struct EffectScopeInner {
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
        Self::new(CancellationToken::new())
    }

    fn new(cancellation: CancellationToken) -> Self {
        Self {
            inner: Arc::new(EffectScopeInner {
                cancellation,
                disposed: AtomicBool::new(false),
                parent: Mutex::new(None),
                resources: Mutex::new(Resources::default()),
            }),
        }
    }

    pub(crate) fn child(&self) -> Self {
        let child = Self::new(self.inner.cancellation.child_token());
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

    pub(crate) fn is_disposed(&self) -> bool {
        self.inner.disposed.load(Ordering::Acquire)
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
            // 未归属 Scope 的任务必须显式 abort，禁止依赖 JoinHandle Drop 语义。
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

    /// 同步释放：取消令牌、子 Scope 与 cleanup；已归属任务上收到父 Scope（Root 留给 shutdown）。
    pub(crate) fn dispose(&self) {
        let _ = self.dispose_with(TaskPolicy::HoistToParent);
    }

    /// 释放并等待本 Scope 树上的受控任务退出（不上收；无超时）。
    ///
    /// 用于插件热卸载：在挂载替换实例前确保旧任务已结束。
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

        // 子 Scope 同步 dispose，将其任务上收到本 Scope。
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
    /// 同步 dispose / Drop：任务上收到父 Scope。
    HoistToParent,
    /// dispose_wait：任务留在本地供 await。
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
                // 最后兜底：无法上交给父/Root 的任务显式 abort。
                EffectScope::abort_tasks(tasks);
            }
        } else if let Ok(mut resources) = self.resources.lock() {
            let tasks = std::mem::take(&mut resources.tasks);
            drop(resources);
            // 已 dispose：优先上收到仍存活的父 Scope，避免误 abort。
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
