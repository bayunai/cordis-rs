//! Effect 清理与等待协调。
//!
//! 同步 cleanup 立即执行；等待顺序为：子 Scope 完成 → 当前异步 disposer LIFO → 当前受控任务。
//! 资源集合定义见 `resources`，本文件只管处置时序。

use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Weak, atomic::Ordering},
};

use crate::{
    CoreError,
    callback_context::{LifecycleFrame, USER_LIFECYCLE_CALLBACK},
    error::format_panic_message,
};

use super::{
    EffectScope, EffectScopeInner,
    resources::{AsyncDisposer, ManagedWork, abort_work},
};
use futures_util::FutureExt;
use tokio::{sync::Notify, task::JoinHandle};

pub(super) struct DisposeCompletion {
    notify: Notify,
    result: std::sync::Mutex<Option<Result<(), Vec<String>>>>,
    /// 保持释放协调任务存活，直至本对象被释放。
    retain: std::sync::Mutex<Option<JoinHandle<()>>>,
}

impl DisposeCompletion {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            notify: Notify::new(),
            result: std::sync::Mutex::new(None),
            retain: std::sync::Mutex::new(None),
        })
    }

    pub(super) fn attach_coordinator(&self, handle: JoinHandle<()>) {
        *self.retain.lock().expect("dispose retain") = Some(handle);
    }

    pub(super) fn finish(&self, result: Result<(), Vec<String>>) {
        {
            let mut slot = self.result.lock().expect("dispose completion");
            if slot.is_some() {
                return;
            }
            *slot = Some(result);
        }
        self.notify.notify_waiters();
    }

    pub(super) async fn wait(self: &Arc<Self>) -> Result<(), CoreError> {
        loop {
            // 先 pin+enable 登记 waiter，再读结果，避免 finish() 夹在检查与 await 之间丢唤醒。
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
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
            notified.await;
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum DisposePolicy {
    HoistToParent,
    AwaitLocal,
}

fn run_sync_cleanup(cleanup: Box<dyn FnOnce() + Send>) {
    let _ = catch_unwind(AssertUnwindSafe(cleanup));
}

/// 运行同步 cleanup；panic 写入 `errors` 并继续。
fn run_sync_cleanup_collect(cleanup: Box<dyn FnOnce() + Send>, errors: &mut Vec<String>) {
    if let Err(payload) = catch_unwind(AssertUnwindSafe(cleanup)) {
        errors.push(format_panic_message("dispose callback", payload));
    }
}

async fn run_async_disposer(disposer: AsyncDisposer, scope: &EffectScope) -> Result<(), String> {
    USER_LIFECYCLE_CALLBACK
        .scope(
            LifecycleFrame {
                scope: scope.clone(),
            },
            async move {
                let future = catch_unwind(AssertUnwindSafe(disposer))
                    .map_err(|payload| format_panic_message("dispose callback", payload))?;
                match AssertUnwindSafe(future).catch_unwind().await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(error)) => Err(error.to_string()),
                    Err(payload) => Err(format_panic_message("dispose callback", payload)),
                }
            },
        )
        .await
}

impl EffectScope {
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
            run_sync_cleanup(cleanup);
        }
        abort_work(work);
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

    pub(super) fn begin_dispose(&self, policy: DisposePolicy) -> Option<Arc<DisposeCompletion>> {
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
        let mut sync_errors = Vec::new();
        for cleanup in cleanups.into_iter().rev() {
            run_sync_cleanup_collect(cleanup, &mut sync_errors);
        }

        let mut child_waits = Vec::new();
        let mut tasks = Vec::new();
        for item in existing_work.into_iter().chain(self.take_work_shallow()) {
            match item {
                ManagedWork::ChildWait(waiter) => child_waits.push(waiter),
                ManagedWork::Task(task) => tasks.push(task),
            }
        }

        let handle = self.inner.handle.clone();
        let completion_for_task = completion.clone();
        let scope_for_frame = self.clone();
        let coordinator = handle.spawn(async move {
            let mut errors = sync_errors;
            // 子 Scope 必须先完整释放；父 Scope 的 async disposer 才能安全关闭
            // 自己拥有、但可能仍被子资源使用的连接或句柄。
            for join in child_waits {
                match join.await {
                    Ok(Ok(())) => {}
                    Ok(Err(mut nested)) => errors.append(&mut nested),
                    Err(error) => {
                        errors.push(format!("child dispose wait join failed: {error}"));
                    }
                }
            }
            for disposer in async_disposers.into_iter().rev() {
                if let Err(error) = run_async_disposer(disposer, &scope_for_frame).await {
                    errors.push(error);
                }
            }
            for join in tasks {
                if let Err(error) = join.await {
                    errors.push(format!("managed task join failed: {error}"));
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

    pub(super) fn hoist_work(&self, work: ManagedWork) {
        if let Some(parent) = self.parent_inner() {
            if let Ok(mut resources) = parent.resources.lock() {
                resources.work.push(work);
                return;
            }
            abort_work(vec![work]);
            return;
        }
        if let Ok(mut resources) = self.inner.resources.lock() {
            resources.work.push(work);
        } else {
            abort_work(vec![work]);
        }
    }

    pub(super) fn take_work_shallow(&self) -> Vec<ManagedWork> {
        let Ok(mut resources) = self.inner.resources.lock() else {
            return Vec::new();
        };
        std::mem::take(&mut resources.work)
    }

    pub(super) fn parent_inner(&self) -> Option<Arc<EffectScopeInner>> {
        self.inner
            .parent
            .lock()
            .ok()
            .and_then(|slot| slot.as_ref().and_then(Weak::upgrade))
    }

    pub(super) fn detach_from_parent(&self) {
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
                    run_sync_cleanup(cleanup);
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
                        abort_work(work);
                    }
                } else {
                    abort_work(work);
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
                    abort_work(work);
                }
            } else {
                // Root 已 dispose 后 Drop：残留 ChildWait 不应 abort（协调由 DisposeCompletion.retain 持有）。
                // 此处 work 若含 root 自挂的 ChildWait，abort 只会取消等待代理，completion 仍由 retain 完成。
                abort_work(work);
            }
        }
    }
}

#[cfg(test)]
mod completion_wait_tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn dispose_completion_multi_waiter_observes_finish_without_timeout() {
        let completion = DisposeCompletion::new();
        let mut waiters = Vec::new();
        for _ in 0..64 {
            let completion = completion.clone();
            waiters.push(tokio::spawn(async move {
                tokio::time::timeout(Duration::from_secs(2), completion.wait())
                    .await
                    .expect("dispose wait timed out")
            }));
        }
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        completion.finish(Ok(()));
        for waiter in waiters {
            waiter.await.expect("join").expect("dispose ok");
        }
        // 已完成后的后续 waiter 直接读同一结果。
        tokio::time::timeout(Duration::from_secs(1), completion.wait())
            .await
            .expect("late waiter timed out")
            .expect("late waiter ok");
    }

    /// P1-3 表征：协调任务若不调用 `finish`，waiter 会挂起（修复后应改为有限错误返回）。
    #[tokio::test]
    async fn p1_dispose_completion_without_finish_times_out() {
        let completion = DisposeCompletion::new();
        let wait = tokio::time::timeout(Duration::from_millis(200), completion.wait()).await;
        assert!(wait.is_err(), "waiter must hang until finish; got {wait:?}");
    }
}
