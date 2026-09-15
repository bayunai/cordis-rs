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

fn run_sync_cleanup(cleanup: Box<dyn FnOnce() + Send>) {
    let _ = catch_unwind(AssertUnwindSafe(cleanup));
}

/// 运行同步 cleanup；panic 写入 `errors` 并继续。
fn run_sync_cleanup_collect(cleanup: Box<dyn FnOnce() + Send>, errors: &mut Vec<String>) {
    if let Err(payload) = catch_unwind(AssertUnwindSafe(cleanup)) {
        errors.push(format_panic_message("dispose callback", payload));
    }
}

#[derive(Clone, Copy)]
pub(super) enum DisposePolicy {
    HoistToParent,
    AwaitLocal,
}

impl EffectScopeInner {
    fn is_root(&self) -> bool {
        self.parent
            .lock()
            .ok()
            .is_none_or(|slot| slot.as_ref().is_none())
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

struct DisposeFinishGuard {
    completion: Arc<DisposeCompletion>,
    finished: bool,
}

impl Drop for DisposeFinishGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.completion
                .finish(Err(vec!["dispose coordinator aborted".into()]));
        }
    }
}

async fn wait_child_releases(children: Vec<Arc<DisposeCompletion>>, errors: &mut Vec<String>) {
    for child in children {
        match child.wait().await {
            Ok(()) => {}
            Err(CoreError::DisposeFailed { errors: mut nested }) => errors.append(&mut nested),
            Err(other) => errors.push(other.to_string()),
        }
    }
}

impl EffectScope {
    pub(crate) fn dispose(&self) {
        let _ = self.begin_dispose(DisposePolicy::HoistToParent);
    }

    /// 在本 Scope 上启动由当前所有者发起的释放。
    pub(crate) fn dispose_owned(&self) {
        let _ = self.begin_dispose(DisposePolicy::AwaitLocal);
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
        let _child_errors = std::mem::take(&mut resources.child_errors);
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
        self.begin_dispose(DisposePolicy::AwaitLocal).wait().await
    }

    /// 线性化地开始释放，或取得已经开始的同一轮 Completion。
    ///
    /// Completion 在 `disposed` 标志之前发布；父 Scope 可以通过本入口取得任一
    /// 子 Scope 的 Completion，不会漏掉正在进入 `dispose_wait()` 的子节点。
    pub(super) fn begin_dispose(&self, policy: DisposePolicy) -> Arc<DisposeCompletion> {
        let completion = {
            let mut slot = self.inner.completion.lock().expect("dispose completion");
            if let Some(existing) = slot.as_ref() {
                return existing.clone();
            }
            let completion = DisposeCompletion::new();
            *slot = Some(completion.clone());
            completion
        };
        self.inner.disposed.store(true, Ordering::Release);
        self.inner.cancellation.cancel();

        let Ok(mut resources) = self.inner.resources.lock() else {
            completion.finish(Ok(()));
            self.detach_from_parent();
            return completion;
        };
        let children = std::mem::take(&mut resources.children);
        let child_errors = std::mem::take(&mut resources.child_errors);
        let cleanups = std::mem::take(&mut resources.cleanups);
        let async_disposers = std::mem::take(&mut resources.async_disposers);
        let existing_work = std::mem::take(&mut resources.work);
        drop(resources);

        // children 在本 Scope 的释放线性化点被摘出。对子 Scope 调用同一个入口，
        // 无论它是否已自行开始释放，都能取得稳定 Completion。
        let child_releases = children
            .into_iter()
            .rev()
            .map(|child| child.begin_dispose(DisposePolicy::AwaitLocal))
            .collect::<Vec<_>>();
        let mut sync_errors = child_errors;
        for cleanup in cleanups.into_iter().rev() {
            run_sync_cleanup_collect(cleanup, &mut sync_errors);
        }

        let mut tasks = Vec::new();
        for item in existing_work.into_iter().chain(self.take_work_shallow()) {
            match item {
                ManagedWork::Task(task) => tasks.push(task),
            }
        }

        let handle = self.inner.handle.clone();
        let propagate_error_to_parent = match policy {
            DisposePolicy::HoistToParent => true,
            DisposePolicy::AwaitLocal => self
                .parent_inner()
                .as_ref()
                .is_some_and(|parent| !parent.is_root()),
        };
        let completion_for_task = completion.clone();
        let scope_for_frame = self.clone();
        let scope_for_worker = scope_for_frame.clone();
        let supervisor = handle.spawn(async move {
            let mut guard = DisposeFinishGuard {
                completion: completion_for_task.clone(),
                finished: false,
            };
            let worker = tokio::spawn(async move {
                let mut errors = sync_errors;
                wait_child_releases(child_releases, &mut errors).await;
                for disposer in async_disposers.into_iter().rev() {
                    if let Err(error) = run_async_disposer(disposer, &scope_for_worker).await {
                        errors.push(error);
                    }
                }
                for join in tasks {
                    if let Err(error) = join.await {
                        errors.push(format!("managed task join failed: {error}"));
                    }
                }
                if errors.is_empty() {
                    Ok(())
                } else {
                    Err(errors)
                }
            });
            let result = match worker.await {
                Ok(result) => result,
                Err(error) => Err(vec![format!("dispose coordinator join failed: {error}")]),
            };
            // 在 finish 之前于父 resources 锁内原子摘除；仍在 children 中则存款，
            // 已被父 begin_dispose 摘走则禁止再写入，错误只经本 Completion 传递一次。
            let deposit = match (&result, propagate_error_to_parent) {
                (Err(errors), true) => Some(errors.as_slice()),
                _ => None,
            };
            scope_for_frame.handoff_detach_from_parent(deposit);
            completion_for_task.finish(result);
            guard.finished = true;
        });
        completion.attach_coordinator(supervisor);
        completion
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

    /// 从父 children 摘除自身；若仍在列表中且 `deposit_errors` 有值则同锁转入 `child_errors`。
    /// 父已 `take(children)` 时不做存款，由父 wait 本 Completion 聚合错误。
    fn handoff_detach_from_parent(&self, deposit_errors: Option<&[String]>) {
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
            let mut removed = false;
            resources.children.retain(|child| {
                if Arc::ptr_eq(&child.inner, &self.inner) {
                    removed = true;
                    false
                } else {
                    true
                }
            });
            if removed && let Some(errors) = deposit_errors {
                resources.child_errors.extend(errors.iter().cloned());
            }
        }
    }

    pub(super) fn detach_from_parent(&self) {
        self.handoff_detach_from_parent(None);
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
            // 已受控 dispose：协调器持有自身 Completion；残留受控任务上收或丢弃。
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

    #[tokio::test]
    async fn p1_dispose_supervisor_abort_finishes_waiters() {
        let completion = DisposeCompletion::new();
        let mut guard = DisposeFinishGuard {
            completion: completion.clone(),
            finished: false,
        };
        let worker = tokio::spawn(std::future::pending::<()>());
        worker.abort();
        let join = worker.await;
        assert!(join.is_err());
        completion.finish(Err(vec![format!(
            "dispose coordinator join failed: {}",
            join.unwrap_err()
        )]));
        guard.finished = true;
        let result = tokio::time::timeout(Duration::from_secs(2), completion.wait())
            .await
            .expect("waiter must finish");
        match result {
            Err(CoreError::DisposeFailed { errors }) => {
                assert!(errors.iter().any(|item| item.contains("join failed")));
            }
            other => panic!("unexpected: {other:?}"),
        }
        tokio::time::timeout(Duration::from_secs(1), completion.wait())
            .await
            .expect("late waiter")
            .expect_err("same failed result");
    }
}
