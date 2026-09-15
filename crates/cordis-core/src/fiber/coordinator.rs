//! Fiber 生命周期协调器：由 Fiber 持有，调用方取消不中断收敛。

use super::{FiberInner, FiberState};
use crate::{
    CoreError,
    plugin::{Plugin, PluginMetadata},
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use tokio::{sync::Notify, task::JoinHandle};

/// 一轮生命周期操作的共享 completion（同构 DisposeCompletion）。
pub(crate) struct LifecycleCompletion {
    notify: Notify,
    result: Mutex<Option<Result<(), CoreError>>>,
    retain: Mutex<Option<JoinHandle<()>>>,
    /// 首次挂载：调用方已放弃返回的 Handle。
    abandon_handle: AtomicBool,
}

impl LifecycleCompletion {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            notify: Notify::new(),
            result: Mutex::new(None),
            retain: Mutex::new(None),
            abandon_handle: AtomicBool::new(false),
        })
    }

    fn attach_coordinator(&self, handle: JoinHandle<()>) {
        *self.retain.lock().expect("lifecycle retain") = Some(handle);
    }

    fn finish(&self, result: Result<(), CoreError>) {
        {
            let mut slot = self.result.lock().expect("lifecycle completion");
            if slot.is_some() {
                return;
            }
            *slot = Some(result);
        }
        self.notify.notify_waiters();
    }

    pub(crate) async fn wait(self: &Arc<Self>) -> Result<(), CoreError> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let slot = self.result.lock().expect("lifecycle completion");
                if let Some(result) = slot.as_ref() {
                    return result.clone();
                }
            }
            notified.await;
        }
    }

    pub(crate) fn request_abandon_handle(&self) {
        self.abandon_handle.store(true, Ordering::Release);
    }

    pub(crate) fn abandon_handle_requested(&self) -> bool {
        self.abandon_handle.load(Ordering::Acquire)
    }
}

pub(crate) enum LifecycleOp {
    Activate {
        initial_mount: bool,
    },
    Unload,
    Restart,
    Replace {
        plugin: Arc<dyn Plugin>,
        metadata: PluginMetadata,
    },
}

/// 首次挂载 wait 被取消时，向本轮协调器请求放弃 Handle。
pub(crate) struct InitialMountWaitGuard {
    pub(crate) completion: Arc<LifecycleCompletion>,
    pub(crate) completed: bool,
}

impl Drop for InitialMountWaitGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.completion.request_abandon_handle();
        }
    }
}

impl FiberInner {
    pub(crate) fn lifecycle_in_flight(&self) -> bool {
        self.lifecycle
            .lock()
            .map(|slot| slot.is_some())
            .unwrap_or(true)
    }

    /// 启动唯一在途协调器；调用方只 wait，取消 wait 不 abort JoinHandle。
    pub(crate) fn start_lifecycle(
        self: &Arc<Self>,
        op: LifecycleOp,
    ) -> Result<Arc<LifecycleCompletion>, CoreError> {
        if self.disposed.load(Ordering::Acquire) {
            return Err(CoreError::FiberDisposed);
        }
        let completion = {
            let mut busy = self.busy.lock().expect("busy");
            let mut slot = self.lifecycle.lock().expect("lifecycle");
            if *busy || slot.is_some() {
                return Err(CoreError::FiberBusy);
            }
            *busy = true;
            let completion = LifecycleCompletion::new();
            *slot = Some(completion.clone());
            completion
        };

        let fiber = self.clone();
        let completion_for_task = completion.clone();
        let handle = self.parent_scope.runtime_handle().spawn(async move {
            let result = fiber.run_lifecycle_op(op).await;
            fiber.finish_lifecycle(completion_for_task.clone(), result.clone());
        });
        completion.attach_coordinator(handle);
        Ok(completion)
    }

    fn finish_lifecycle(
        self: &Arc<Self>,
        completion: Arc<LifecycleCompletion>,
        result: Result<(), CoreError>,
    ) {
        if let Ok(mut busy) = self.busy.lock() {
            *busy = false;
        }
        if let Ok(mut slot) = self.lifecycle.lock() {
            *slot = None;
        }
        // 若仍停在 Loading/Unloading，强制收敛（panic/早退兜底）。
        let state = *self.state.lock().expect("state");
        if matches!(state, FiberState::Loading | FiberState::Unloading)
            && !self.disposed.load(Ordering::Acquire)
        {
            if let Some(pending) = self.pending_effect.lock().expect("pending_effect").take() {
                pending.dispose();
            }
            let _ = self.transition_if_alive(FiberState::Pending);
        }
        completion.finish(result);
        if let Some(registry) = self.registry.upgrade() {
            registry.mark_dirty_public();
        }
    }

    async fn run_lifecycle_op(self: &Arc<Self>, op: LifecycleOp) -> Result<(), CoreError> {
        match op {
            LifecycleOp::Activate { initial_mount } => {
                self.activate_with_policy(initial_mount).await
            }
            LifecycleOp::Unload => self.unload_effect_to_pending().await,
            LifecycleOp::Restart => {
                self.unload_effect_to_pending().await?;
                self.activate_with_policy(false).await
            }
            LifecycleOp::Replace { plugin, metadata } => {
                self.unload_effect_to_pending().await?;
                *self.plugin.lock().expect("plugin") = plugin;
                *self.dependencies.lock().expect("deps") = metadata.dependencies;
                self.resolved_providers.lock().expect("providers").clear();
                *self.last_error.lock().expect("error") = None;
                if !self.transition_if_alive(FiberState::Pending) {
                    return Err(CoreError::FiberDisposed);
                }
                self.activate_with_policy(false).await
            }
        }
    }

    pub(crate) fn scopes_awaited_by_unmount(&self) -> Vec<crate::effect::EffectScope> {
        let mut scopes = Vec::new();
        if let Ok(guard) = self.effect.lock()
            && let Some(scope) = guard.as_ref()
        {
            scopes.push(scope.clone());
        }
        if let Ok(guard) = self.pending_wait.lock()
            && let Some(scope) = guard.as_ref()
        {
            scopes.push(scope.clone());
        }
        if let Ok(guard) = self.pending_effect.lock()
            && let Some(scope) = guard.as_ref()
        {
            scopes.push(scope.clone());
        }
        scopes
    }
}

#[cfg(test)]
mod lifecycle_completion_tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn lifecycle_completion_multi_waiter_observes_finish_without_timeout() {
        let completion = LifecycleCompletion::new();
        let mut waiters = Vec::new();
        for _ in 0..64 {
            let completion = completion.clone();
            waiters.push(tokio::spawn(async move {
                tokio::time::timeout(Duration::from_secs(2), completion.wait())
                    .await
                    .expect("lifecycle wait timed out")
            }));
        }
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        completion.finish(Ok(()));
        for waiter in waiters {
            waiter.await.expect("join").expect("lifecycle ok");
        }
        tokio::time::timeout(Duration::from_secs(1), completion.wait())
            .await
            .expect("late waiter timed out")
            .expect("late waiter ok");
    }

    /// P1-3 表征：Lifecycle 协调体若不 `finish`，waiter 挂起（修复后应 finally finish）。
    #[tokio::test]
    async fn p1_lifecycle_completion_without_finish_times_out() {
        let completion = LifecycleCompletion::new();
        let wait = tokio::time::timeout(Duration::from_millis(200), completion.wait()).await;
        assert!(wait.is_err(), "waiter must hang until finish; got {wait:?}");
    }
}
