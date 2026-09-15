//! Fiber 生命周期协调器：由 Fiber 持有，调用方取消不中断收敛。

use super::FiberInner;
use crate::{
    CoreError,
    plugin::{Plugin, PluginMetadata},
};
use std::sync::{Arc, Mutex, atomic::Ordering};
use tokio::{sync::Notify, task::JoinHandle};

/// 首次挂载 Handle 交付状态；mutex 保护，禁止 TOCTOU 的普通 bool 检查。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HandleHandoff {
    Preparing,
    ReadyForHandle,
    HandleClaimed,
    Abandoned,
}

/// 一轮生命周期操作的共享 completion（同构 DisposeCompletion）。
pub(crate) struct LifecycleCompletion {
    notify: Notify,
    result: Mutex<Option<Result<(), CoreError>>>,
    retain: Mutex<Option<JoinHandle<()>>>,
}

impl LifecycleCompletion {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            notify: Notify::new(),
            result: Mutex::new(None),
            retain: Mutex::new(None),
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

/// 首次挂载 wait 被取消时，原子 abandon Handle。
pub(crate) struct InitialMountWaitGuard {
    pub(crate) fiber: Arc<FiberInner>,
    pub(crate) completed: bool,
}

impl Drop for InitialMountWaitGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.fiber.abandon_initial_handle();
        }
    }
}

struct LifecycleFinishGuard {
    fiber: Arc<FiberInner>,
    completion: Arc<LifecycleCompletion>,
    finished: bool,
}

impl Drop for LifecycleFinishGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.fiber.finish_lifecycle(
                self.completion.clone(),
                Err(CoreError::CoordinatorAborted {
                    reason: "lifecycle supervisor dropped".into(),
                }),
            );
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

    pub(crate) fn claim_handle(&self) -> bool {
        let mut handoff = self.handoff.lock().expect("handoff");
        match *handoff {
            HandleHandoff::ReadyForHandle => {
                *handoff = HandleHandoff::HandleClaimed;
                true
            }
            HandleHandoff::HandleClaimed => true,
            HandleHandoff::Preparing | HandleHandoff::Abandoned => false,
        }
    }

    pub(crate) fn abandon_initial_handle(self: &Arc<Self>) {
        let previous = {
            let mut handoff = self.handoff.lock().expect("handoff");
            let previous = *handoff;
            if previous != HandleHandoff::HandleClaimed {
                *handoff = HandleHandoff::Abandoned;
            }
            previous
        };
        match previous {
            HandleHandoff::HandleClaimed | HandleHandoff::Abandoned => {}
            HandleHandoff::Preparing => {}
            HandleHandoff::ReadyForHandle => self.dispose_now(),
        }
    }

    pub(crate) fn handoff_abandoned(&self) -> bool {
        matches!(
            *self.handoff.lock().expect("handoff"),
            HandleHandoff::Abandoned
        )
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
        let supervisor = self.parent_scope.runtime_handle().spawn(async move {
            let mut guard = LifecycleFinishGuard {
                fiber: fiber.clone(),
                completion: completion_for_task.clone(),
                finished: false,
            };
            let worker_fiber = fiber.clone();
            let worker = tokio::spawn(async move { worker_fiber.run_lifecycle_op(op).await });
            let result = match worker.await {
                Ok(result) => result,
                Err(error) => {
                    fiber.converge_after_coordinator_abort();
                    Err(CoreError::CoordinatorAborted {
                        reason: format!("lifecycle worker: {error}"),
                    })
                }
            };
            fiber.finish_lifecycle(completion_for_task, result);
            guard.finished = true;
        });
        completion.attach_coordinator(supervisor);
        Ok(completion)
    }

    fn converge_after_coordinator_abort(&self) {
        if self.disposed.load(Ordering::Acquire) {
            return;
        }
        let state = *self.state.lock().expect("state");
        if matches!(
            state,
            crate::fiber::FiberState::Loading | crate::fiber::FiberState::Unloading
        ) {
            let _ = self.transition_if_alive(crate::fiber::FiberState::Failed);
        }
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
        let state = *self.state.lock().expect("state");
        if matches!(
            state,
            crate::fiber::FiberState::Loading | crate::fiber::FiberState::Unloading
        ) && !self.disposed.load(Ordering::Acquire)
        {
            self.discard_activating_to_pending();
            let _ = self.transition_if_alive(crate::fiber::FiberState::Pending);
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
                if !self.transition_if_alive(crate::fiber::FiberState::Pending) {
                    return Err(CoreError::FiberDisposed);
                }
                self.activate_with_policy(false).await
            }
        }
    }

    pub(crate) fn scopes_awaited_by_unmount(&self) -> Vec<crate::effect::EffectScope> {
        self.ownership
            .lock()
            .ok()
            .and_then(|guard| guard.live_scope().cloned())
            .into_iter()
            .collect()
    }
}

#[cfg(test)]
use crate::plugin::PluginKey;

#[cfg(test)]
pub(crate) struct ReadyForHandleGate {
    pub entered: tokio::sync::oneshot::Receiver<()>,
    pub release: tokio::sync::oneshot::Sender<()>,
}

#[cfg(test)]
static READY_GATES: Mutex<Option<(&'static str, ReadyGateSlots)>> = Mutex::new(None);

#[cfg(test)]
struct ReadyGateSlots {
    entered: Option<tokio::sync::oneshot::Sender<()>>,
    release: Option<tokio::sync::oneshot::Receiver<()>>,
}

#[cfg(test)]
impl FiberInner {
    pub(crate) fn arm_ready_for_handle_gate(key: PluginKey) -> ReadyForHandleGate {
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        *READY_GATES.lock().expect("gates") = Some((
            key.as_str(),
            ReadyGateSlots {
                entered: Some(entered_tx),
                release: Some(release_rx),
            },
        ));
        ReadyForHandleGate {
            entered: entered_rx,
            release: release_tx,
        }
    }

    pub(crate) async fn hit_ready_for_handle_gate(&self) {
        let slots = READY_GATES.lock().expect("gates").take();
        let Some((key, mut slots)) = slots else {
            return;
        };
        if key != self.plugin_key.as_str() {
            *READY_GATES.lock().expect("gates") = Some((key, slots));
            return;
        }
        if let Some(tx) = slots.entered.take() {
            let _ = tx.send(());
        }
        if let Some(rx) = slots.release.take() {
            let _ = rx.await;
        }
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

    #[tokio::test]
    async fn p1_lifecycle_supervisor_abort_finishes_waiters() {
        let completion = LifecycleCompletion::new();
        let mut guard = LifecycleFinishGuardForTest {
            completion: completion.clone(),
            finished: false,
        };
        let worker = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        worker.abort();
        let join = worker.await;
        assert!(join.is_err());
        completion.finish(Err(CoreError::CoordinatorAborted {
            reason: format!("lifecycle worker: {}", join.unwrap_err()),
        }));
        guard.finished = true;
        let result = tokio::time::timeout(Duration::from_secs(2), completion.wait())
            .await
            .expect("waiter must finish");
        assert!(matches!(result, Err(CoreError::CoordinatorAborted { .. })));
        tokio::time::timeout(Duration::from_secs(1), completion.wait())
            .await
            .expect("late waiter")
            .expect_err("same aborted result");
    }

    struct LifecycleFinishGuardForTest {
        completion: Arc<LifecycleCompletion>,
        finished: bool,
    }

    impl Drop for LifecycleFinishGuardForTest {
        fn drop(&mut self) {
            if !self.finished {
                self.completion.finish(Err(CoreError::CoordinatorAborted {
                    reason: "lifecycle supervisor dropped".into(),
                }));
            }
        }
    }
}
