//! 后台重算调度。
//!
//! 在 Tokio Runtime 内驱动脏标记后的 inject / Fiber 重算；不拥有业务状态。

use crate::{
    CoreError,
    callback_context::in_user_lifecycle_callback,
    effect::EffectScope,
    fiber::{FiberInner, FiberState},
    registry::{InjectionId, Registry},
    service::resolver::provider_ids,
};
use std::sync::{Arc, Weak, atomic::Ordering};
use tokio::task::JoinHandle;

tokio::task_local! {
    /// 当前任务正处于锁外执行的 inject / Plugin::apply 用户 Future 中。
    static IN_RECOMPUTE_USER: ();
}

struct RecomputeBatch {
    epoch: u64,
    ready: Vec<InjectionId>,
    stale_scopes: Vec<EffectScope>,
    unload: Vec<Arc<FiberInner>>,
    activate: Vec<Arc<FiberInner>>,
}

impl RecomputeBatch {
    fn is_empty(&self) -> bool {
        self.ready.is_empty()
            && self.stale_scopes.is_empty()
            && self.unload.is_empty()
            && self.activate.is_empty()
    }
}

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
        // 嵌套于本轮 recompute 的用户 Future：已在 flush 中，不可再等待 quiescent。
        if IN_RECOMPUTE_USER.try_with(|_| ()).is_ok() || in_user_lifecycle_callback() {
            return;
        }

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
        self.recompute_epoch.fetch_add(1, Ordering::AcqRel);
        if self.scheduler_stopped.load(Ordering::Acquire) {
            return;
        }
        self.wake.notify_one();
    }

    /// 锁内取批 / 锁外执行用户 Future / 带 epoch 校验收尾。
    async fn recompute(self: Arc<Self>) {
        {
            let _guard = self.recompute_lock.lock().await;
            if self.recomputing.swap(true, Ordering::AcqRel) {
                // 已有会话在飞；本轮脏标记会由在飞会话收尾时看到。
                return;
            }
        }

        loop {
            let batch = {
                let _guard = self.recompute_lock.lock().await;
                let epoch = self.recompute_epoch.load(Ordering::Acquire);
                let _ = self.dirty.swap(false, Ordering::AcqRel);
                let injection = self.take_ready();
                let (unload, activate) = self.classify_plugin_fibers();
                RecomputeBatch {
                    epoch,
                    ready: injection.ready,
                    stale_scopes: injection.stale_scopes,
                    unload,
                    activate,
                }
            };

            if batch.is_empty() {
                let _guard = self.recompute_lock.lock().await;
                let epoch_advanced = self.recompute_epoch.load(Ordering::Acquire) != batch.epoch;
                if self.dirty.load(Ordering::Acquire) || epoch_advanced {
                    continue;
                }
                self.recomputing.store(false, Ordering::Release);
                self.quiescent.notify_waiters();
                if self.dirty.load(Ordering::Acquire)
                    && !self.scheduler_stopped.load(Ordering::Acquire)
                {
                    self.wake.notify_one();
                }
                return;
            }

            // 锁外：用户 sync cleanup / unload wait / inject / Plugin::apply。
            for scope in batch.stale_scopes {
                scope.dispose();
            }
            for fiber in batch.unload {
                let _ = fiber.unload_to_pending_wait().await;
            }
            for injection in batch.ready {
                let registry = self.clone();
                IN_RECOMPUTE_USER
                    .scope((), async move {
                        registry.run_injection(injection).await;
                    })
                    .await;
            }
            for fiber in batch.activate {
                let registry = self.clone();
                IN_RECOMPUTE_USER
                    .scope((), async move {
                        registry.run_plugin_fiber(fiber).await;
                    })
                    .await;
            }
            // unload 后 Pending 可能已就绪且未 mark_dirty；下一轮再取批即可。
        }
    }

    /// 仅识别需 unload / 可 activate 的 Fiber；禁止在此 await。
    fn classify_plugin_fibers(&self) -> (Vec<Arc<FiberInner>>, Vec<Arc<FiberInner>>) {
        let fibers = {
            let Ok(state) = self.state.lock() else {
                return (Vec::new(), Vec::new());
            };
            state
                .plugin_fibers
                .values()
                .filter_map(Weak::upgrade)
                .filter(|fiber| !fiber.disposed.load(Ordering::Acquire))
                .collect::<Vec<_>>()
        };

        let mut unload = Vec::new();
        let mut activate = Vec::new();
        for fiber in fibers {
            if fiber.disposed.load(Ordering::Acquire) {
                continue;
            }
            if fiber.busy.lock().map(|guard| *guard).unwrap_or(true) || fiber.lifecycle_in_flight()
            {
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
                unload.push(fiber);
                continue;
            }
            let deps_ready = providers.len() == deps.len();
            if state == FiberState::Pending && deps_ready {
                activate.push(fiber);
            }
        }
        (unload, activate)
    }

    async fn run_plugin_fiber(self: &Arc<Self>, fiber: Arc<FiberInner>) {
        // Failed is stored on the fiber; Pending Ok is intentional when deps race.
        let _ = fiber.try_activate().await;
    }
}
