//! 后台重算调度。
//!
//! 在 Tokio Runtime 内驱动脏标记后的 inject / Fiber 重算；不拥有业务状态。

use crate::{
    CoreError,
    fiber::{FiberInner, FiberState},
    registry::Registry,
    service::resolver::provider_ids,
};
use std::sync::{Arc, Weak, atomic::Ordering};
use tokio::task::JoinHandle;

impl Registry {
    /// 启动唯一的响应式重算调度器；必须在 Tokio Runtime 内调用一次。
    pub(crate) fn start_scheduler(self: &Arc<Self>) -> Result<(), CoreError> {
        let handle =
            tokio::runtime::Handle::try_current().map_err(|_| CoreError::SchedulerUnavailable)?;
        if self.scheduler_stopped.load(Ordering::Acquire) {
            return Err(CoreError::SchedulerUnavailable);
        }
        if self.scheduler_started.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let registry = Arc::clone(self);
        let cancel = self.scheduler_cancel.clone();
        let task = handle.spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = registry.wake.notified() => {
                        if cancel.is_cancelled() {
                            break;
                        }
                        registry.clone().recompute().await;
                    }
                }
            }
        });
        if let Ok(mut slot) = self.scheduler_task.lock() {
            *slot = Some(task);
        } else {
            task.abort();
            self.scheduler_started.store(false, Ordering::Release);
            return Err(CoreError::SchedulerUnavailable);
        }
        Ok(())
    }

    /// 同步发出停止信号并取出调度器 Handle（幂等）。
    fn request_stop_scheduler(&self) -> Option<JoinHandle<()>> {
        self.scheduler_stopped.store(true, Ordering::Release);
        self.scheduler_cancel.cancel();
        self.wake.notify_one();
        self.scheduler_task
            .lock()
            .ok()
            .and_then(|mut slot| slot.take())
    }

    /// 停止调度器并等待其退出（幂等）。
    pub(crate) async fn stop_scheduler(self: &Arc<Self>) {
        if let Some(task) = self.request_stop_scheduler() {
            let _ = task.await;
        }
    }

    /// 析构路径：发出停止信号并 abort 调度器任务以释放 Registry 强引用。
    pub(crate) fn abort_scheduler(&self) {
        if let Some(task) = self.request_stop_scheduler() {
            task.abort();
        }
    }

    pub(crate) fn scheduler_stopped(&self) -> bool {
        self.scheduler_stopped.load(Ordering::Acquire)
    }

    pub(crate) async fn settle(self: &Arc<Self>) {
        loop {
            // 先登记 waiter，再检查状态，避免丢失 notify_waiters。
            let notified = self.quiescent.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if !self.dirty.load(Ordering::Acquire) && !self.recomputing.load(Ordering::Acquire) {
                let _guard = self.recompute_lock.lock().await;
                if !self.dirty.load(Ordering::Acquire) && !self.recomputing.load(Ordering::Acquire)
                {
                    return;
                }
            }
            if self.scheduler_stopped.load(Ordering::Acquire) {
                let _guard = self.recompute_lock.lock().await;
                if !self.dirty.load(Ordering::Acquire) && !self.recomputing.load(Ordering::Acquire)
                {
                    return;
                }
                drop(_guard);
                self.clone().recompute().await;
                continue;
            }
            self.mark_dirty();
            notified.await;
        }
    }

    pub(crate) fn mark_dirty(self: &Arc<Self>) {
        self.dirty.store(true, Ordering::Release);
        if self.scheduler_stopped.load(Ordering::Acquire) {
            return;
        }
        self.wake.notify_one();
    }

    async fn recompute(self: Arc<Self>) {
        let _guard = self.recompute_lock.lock().await;
        self.recomputing.store(true, Ordering::Release);
        loop {
            if !self.dirty.swap(false, Ordering::AcqRel) {
                break;
            }
            loop {
                let ready = self.take_ready();
                let ready_plugins = self.take_ready_plugin_fibers().await;
                if ready.is_empty() && ready_plugins.is_empty() {
                    break;
                }
                for injection in ready {
                    self.run_injection(injection).await;
                }
                for fiber in ready_plugins {
                    self.run_plugin_fiber(fiber).await;
                }
            }
        }
        self.recomputing.store(false, Ordering::Release);
        self.quiescent.notify_waiters();
        if self.dirty.load(Ordering::Acquire) && !self.scheduler_stopped.load(Ordering::Acquire) {
            self.wake.notify_one();
        }
    }

    async fn take_ready_plugin_fibers(&self) -> Vec<Arc<FiberInner>> {
        let fibers = {
            let Ok(state) = self.state.lock() else {
                return Vec::new();
            };
            state
                .plugin_fibers
                .values()
                .filter_map(Weak::upgrade)
                .filter(|fiber| !fiber.disposed.load(Ordering::Acquire))
                .collect::<Vec<_>>()
        };

        let mut ready = Vec::new();
        for fiber in fibers {
            if fiber.disposed.load(Ordering::Acquire) {
                continue;
            }
            if fiber.busy.lock().map(|guard| *guard).unwrap_or(true) {
                continue;
            }
            let deps = fiber.dependencies.lock().expect("deps").clone();
            let resolved = fiber.resolved_providers.lock().expect("providers").clone();
            let providers = {
                let Ok(state) = self.state.lock() else {
                    continue;
                };
                provider_ids(&state, fiber.node, &deps)
            };
            let state = *fiber.state.lock().expect("state");
            if state == FiberState::Active && resolved != providers {
                let _ = fiber.unload_to_pending_wait().await;
            }
            if fiber.busy.lock().map(|guard| *guard).unwrap_or(true) {
                continue;
            }
            let deps = fiber.dependencies.lock().expect("deps").clone();
            let providers = {
                let Ok(state) = self.state.lock() else {
                    continue;
                };
                provider_ids(&state, fiber.node, &deps)
            };
            let state = *fiber.state.lock().expect("state");
            let deps_ready = providers.len() == deps.len();
            if state == FiberState::Pending && deps_ready {
                ready.push(fiber);
            }
        }
        ready
    }

    async fn run_plugin_fiber(self: &Arc<Self>, fiber: Arc<FiberInner>) {
        // Failed is stored on the fiber; Pending Ok is intentional when deps race.
        let _ = fiber.try_activate().await;
    }
}
