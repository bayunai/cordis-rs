//! 依赖就绪后的 apply / reapply。
//!
//! 认领 Pending→Loading，解析依赖并调用 Plugin::apply；生命周期入口在 `lifecycle` / `coordinator`。

use super::{ActivateClaim, EffectOwnership, FiberInner, FiberState, HandleHandoff};
use crate::{
    Context, CoreError, ServiceId,
    callback_context::{LifecycleFrame, USER_LIFECYCLE_CALLBACK},
    effect::EffectScope,
    error::format_panic_message,
};
use futures_util::FutureExt;
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, atomic::Ordering},
};

async fn invoke_plugin_apply(
    plugin: &Arc<dyn crate::plugin::Plugin>,
    context: &Context,
    effect_scope: &EffectScope,
) -> Result<(), String> {
    USER_LIFECYCLE_CALLBACK
        .scope(
            LifecycleFrame {
                scope: effect_scope.clone(),
            },
            async {
                let future = catch_unwind(AssertUnwindSafe(|| plugin.apply(context)))
                    .map_err(|payload| format_panic_message("plugin apply", payload))?;
                match AssertUnwindSafe(future).catch_unwind().await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(error)) => Err(error.to_string()),
                    Err(payload) => Err(format_panic_message("plugin apply", payload)),
                }
            },
        )
        .await
}

impl FiberInner {
    pub(crate) fn claim_activation(&self) -> ActivateClaim {
        if self.disposed.load(Ordering::Acquire) {
            return ActivateClaim::Skipped;
        }
        let changed = {
            let Ok(mut state) = self.state.lock() else {
                return ActivateClaim::Skipped;
            };
            if self.disposed.load(Ordering::Acquire) || *state != FiberState::Pending {
                None
            } else {
                let previous = *state;
                *state = FiberState::Loading;
                Some(previous)
            }
        };
        if let Some(previous) = changed {
            self.publish_transition(previous, FiberState::Loading);
            ActivateClaim::Claimed
        } else {
            ActivateClaim::Skipped
        }
    }

    fn resolve_provider_ids(
        &self,
        registry: &crate::registry::Registry,
        deps: &[ServiceId],
    ) -> Option<Vec<u64>> {
        let mut providers = Vec::with_capacity(deps.len());
        for key in deps {
            let service = registry.resolve_with_id(self.node, *key)?;
            providers.push(service.0);
        }
        Some(providers)
    }

    fn install_activating(&self, scope: EffectScope) -> Result<EffectScope, CoreError> {
        if self.disposed.load(Ordering::Acquire) {
            scope.dispose();
            return Err(CoreError::FiberDisposed);
        }
        let mut ownership = self.ownership.lock().expect("ownership");
        if self.disposed.load(Ordering::Acquire)
            || matches!(*ownership, EffectOwnership::Releasing(_))
        {
            drop(ownership);
            scope.dispose();
            return Err(CoreError::FiberDisposed);
        }
        *ownership = EffectOwnership::Activating(scope.clone());
        Ok(scope)
    }

    fn abandon_loading_to_pending(&self, effect_scope: Option<&EffectScope>, mark_dirty: bool) {
        if let Some(scope) = effect_scope {
            scope.dispose();
        }
        let mut ownership = self.ownership.lock().expect("ownership");
        match std::mem::replace(&mut *ownership, EffectOwnership::Empty) {
            EffectOwnership::Activating(scope) => {
                if effect_scope.is_none_or(|expected| !scope.ptr_eq(expected)) {
                    scope.dispose();
                }
            }
            other => *ownership = other,
        }
        self.resolved_providers.lock().expect("providers").clear();
        *self.last_error.lock().expect("error") = None;
        let _ = self.transition_if_alive(FiberState::Pending);
        if mark_dirty && let Some(registry) = self.registry.upgrade() {
            registry.mark_dirty_public();
        }
    }

    async fn revoke_abandoned_mount(self: &Arc<Self>) -> CoreError {
        self.dispose_wait_inner()
            .await
            .err()
            .filter(|error| !matches!(error, CoreError::DisposeFailed { .. }))
            .unwrap_or(CoreError::FiberDisposed)
    }

    fn mark_ready_for_handle_if_preparing(&self, initial_mount: bool) {
        if !initial_mount {
            return;
        }
        let mut handoff = self.handoff.lock().expect("handoff");
        if *handoff == HandleHandoff::Preparing {
            *handoff = HandleHandoff::ReadyForHandle;
        }
    }

    fn should_revoke_abandoned_mount(&self) -> bool {
        self.handoff_abandoned()
    }

    fn try_commit_active(
        &self,
        scope: EffectScope,
        providers: Vec<u64>,
        initial_mount: bool,
    ) -> Result<(), CoreError> {
        let mut ownership = self.ownership.lock().expect("ownership");
        let mut handoff = self.handoff.lock().expect("handoff");
        if self.disposed.load(Ordering::Acquire)
            || matches!(*ownership, EffectOwnership::Releasing(_))
        {
            return Err(CoreError::FiberDisposed);
        }
        match *handoff {
            HandleHandoff::Abandoned => return Err(CoreError::FiberDisposed),
            _ if initial_mount => match *handoff {
                HandleHandoff::Preparing => *handoff = HandleHandoff::ReadyForHandle,
                HandleHandoff::ReadyForHandle | HandleHandoff::HandleClaimed => {}
                HandleHandoff::Abandoned => unreachable!("handled above"),
            },
            _ => {}
        }
        *ownership = EffectOwnership::Active(scope);
        *self.resolved_providers.lock().expect("providers") = providers;
        *self.last_error.lock().expect("error") = None;
        Ok(())
    }

    /// 协调器内激活主体；`initial_mount` 时尊重调用方取消撤销。
    pub(crate) async fn activate_with_policy(
        self: &Arc<Self>,
        initial_mount: bool,
    ) -> Result<(), CoreError> {
        if self.disposed.load(Ordering::Acquire) {
            return Err(CoreError::FiberDisposed);
        }
        if self.should_revoke_abandoned_mount() {
            return Err(self.revoke_abandoned_mount().await);
        }
        if self.claim_activation() != ActivateClaim::Claimed {
            self.mark_ready_for_handle_if_preparing(initial_mount);
            return Ok(());
        }

        let Some(registry) = self.registry.upgrade() else {
            let _ = self.transition_if_alive(FiberState::Failed);
            return Err(CoreError::ContextDisposed);
        };
        let deps = self.dependencies.lock().expect("deps").clone();
        let Some(providers) = self.resolve_provider_ids(&registry, &deps) else {
            self.abandon_loading_to_pending(None, false);
            if self.should_revoke_abandoned_mount() {
                return Err(self.revoke_abandoned_mount().await);
            }
            self.mark_ready_for_handle_if_preparing(initial_mount);
            return Ok(());
        };

        if self.should_revoke_abandoned_mount() {
            return Err(self.revoke_abandoned_mount().await);
        }

        let plugin = self.plugin.lock().expect("plugin").clone();
        let parent_scope = self.parent_scope.clone();
        if parent_scope.is_disposed() {
            *self.last_error.lock().expect("error") =
                Some("parent scope disposed before apply".into());
            let _ = self.transition_if_alive(FiberState::Failed);
            return Err(CoreError::ContextDisposed);
        }
        let effect_scope = parent_scope.child_named("plugin");
        let effect_scope = match self.install_activating(effect_scope) {
            Ok(scope) => scope,
            Err(error) => return Err(error),
        };
        let mount = match self.mount_ctx.lock().expect("mount").clone() {
            Some(ctx) => ctx,
            None => {
                self.abandon_loading_to_pending(Some(&effect_scope), false);
                *self.last_error.lock().expect("error") = Some("mount context missing".into());
                let _ = self.transition_if_alive(FiberState::Failed);
                return Err(CoreError::ContextDisposed);
            }
        };
        let apply_ctx = Context {
            inner: Arc::new(crate::context::ContextInner {
                id: mount.inner.id,
                registry: mount.inner.registry.clone(),
                scope: effect_scope.clone(),
            }),
        };
        registry.register_effect(
            effect_scope.id(),
            effect_scope.name().to_string(),
            effect_scope.parent_id(),
            Some(mount.inner.id),
            Some(self.id),
            &effect_scope,
        );
        match invoke_plugin_apply(&plugin, &apply_ctx, &effect_scope).await {
            Ok(()) => {
                if self.should_revoke_abandoned_mount() || self.disposed.load(Ordering::Acquire) {
                    return Err(self.revoke_abandoned_mount().await);
                }
                if effect_scope.is_disposed() || parent_scope.is_disposed() {
                    self.abandon_loading_to_pending(Some(&effect_scope), true);
                    *self.last_error.lock().expect("error") =
                        Some("plugin scope was disposed during apply".into());
                    let _ = self.transition_if_alive(FiberState::Failed);
                    return Err(CoreError::PluginApply(
                        "plugin scope was disposed during apply".into(),
                    ));
                }
                let Some(fresh) = self.resolve_provider_ids(&registry, &deps) else {
                    self.abandon_loading_to_pending(Some(&effect_scope), true);
                    self.mark_ready_for_handle_if_preparing(initial_mount);
                    return Ok(());
                };
                if fresh != providers {
                    self.abandon_loading_to_pending(Some(&effect_scope), true);
                    self.mark_ready_for_handle_if_preparing(initial_mount);
                    return Ok(());
                }
                if let Err(error) =
                    self.try_commit_active(effect_scope.clone(), providers, initial_mount)
                {
                    let _ = self.revoke_abandoned_mount().await;
                    return Err(error);
                }
                if !self.transition_if_alive(FiberState::Active) {
                    let _ = self.revoke_abandoned_mount().await;
                    self.resolved_providers.lock().expect("providers").clear();
                    return Err(CoreError::FiberDisposed);
                }
                registry.mark_dirty_public();
                #[cfg(test)]
                if initial_mount {
                    self.hit_ready_for_handle_gate().await;
                    if self.should_revoke_abandoned_mount() || self.disposed.load(Ordering::Acquire)
                    {
                        return Err(self.revoke_abandoned_mount().await);
                    }
                }
                Ok(())
            }
            Err(error) => {
                if self.should_revoke_abandoned_mount() || self.disposed.load(Ordering::Acquire) {
                    return Err(self.revoke_abandoned_mount().await);
                }
                self.abandon_loading_to_pending(Some(&effect_scope), false);
                *self.last_error.lock().expect("error") = Some(error.to_string());
                let _ = self.transition_if_alive(FiberState::Failed);
                self.mark_ready_for_handle_if_preparing(initial_mount);
                Err(CoreError::PluginApply(error.to_string()))
            }
        }
    }

    /// 调度器入口：经协调器激活（非首次挂载）。
    pub(crate) async fn try_activate(self: &Arc<Self>) -> Result<(), CoreError> {
        match self.start_lifecycle(super::coordinator::LifecycleOp::Activate {
            initial_mount: false,
        }) {
            Ok(completion) => completion.wait().await,
            Err(CoreError::FiberBusy) => Ok(()),
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod initial_mount_revoke_tests {
    use crate::{
        Context, CoreError,
        fiber::{EffectOwnership, FiberInner, FiberState, coordinator::HandleHandoff},
        plugin::{Plugin, PluginKey},
    };
    use async_trait::async_trait;
    use std::sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    struct CountingPlugin {
        applies: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Plugin for CountingPlugin {
        fn key(&self) -> PluginKey {
            PluginKey::new("test.unit-abandon-before-apply")
        }
        async fn apply(&self, _ctx: &Context) -> Result<(), CoreError> {
            self.applies.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn fiber_with_abandon(applies: Arc<AtomicUsize>) -> Arc<FiberInner> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let handle = runtime.handle().clone();
        std::mem::forget(runtime);
        Arc::new(FiberInner {
            id: 1,
            plugin_key: PluginKey::new("test.unit-abandon-before-apply"),
            node: 0,
            registry: Weak::new(),
            parent_scope: crate::effect::EffectScope::root(handle),
            plugin: Mutex::new(Arc::new(CountingPlugin { applies })),
            dependencies: Mutex::new(Vec::new()),
            ownership: Mutex::new(EffectOwnership::Empty),
            dispose_result: Mutex::new(None),
            state: Mutex::new(FiberState::Pending),
            last_error: Mutex::new(None),
            resolved_providers: Mutex::new(Vec::new()),
            disposed: AtomicBool::new(false),
            busy: Mutex::new(false),
            lifecycle: Mutex::new(None),
            mount_ctx: Mutex::new(None),
            handoff: Mutex::new(HandleHandoff::Abandoned),
        })
    }

    #[tokio::test]
    async fn abandon_before_apply_revokes_without_invoking_apply() {
        let applies = Arc::new(AtomicUsize::new(0));
        let fiber = fiber_with_abandon(applies.clone());
        let error = fiber
            .activate_with_policy(true)
            .await
            .expect_err("should revoke");
        assert!(matches!(error, CoreError::FiberDisposed));
        assert_eq!(applies.load(Ordering::SeqCst), 0);
        assert!(fiber.disposed.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn abandoned_initial_mount_cannot_be_reactivated_by_scheduler_path() {
        let applies = Arc::new(AtomicUsize::new(0));
        let fiber = fiber_with_abandon(applies.clone());
        let error = fiber
            .activate_with_policy(false)
            .await
            .expect_err("abandoned mount must not reactivate");
        assert!(matches!(error, CoreError::FiberDisposed));
        assert_eq!(applies.load(Ordering::SeqCst), 0);
        assert!(fiber.disposed.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn p1_ready_for_handle_abandon_leaves_no_orphan() {
        use crate::{PluginKey, Runtime, ServiceKey};

        static KEY: PluginKey = PluginKey::new("test.p1.ready-for-handle");
        static VALUE: ServiceKey<u64> = ServiceKey::new("test.p1.ready-for-handle.n@1");

        struct ProvidePlugin;
        #[async_trait]
        impl Plugin for ProvidePlugin {
            fn key(&self) -> PluginKey {
                KEY
            }
            async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
                ctx.provide(VALUE, 7)?;
                Ok(())
            }
        }

        let runtime = Runtime::new().expect("runtime");
        let root = runtime.root();
        let gate = FiberInner::arm_ready_for_handle_gate(KEY);
        let mount = tokio::spawn({
            let root = root.clone();
            async move { root.plugin(Arc::new(ProvidePlugin)).await }
        });
        gate.entered.await.expect("ready for handle");
        mount.abort();
        assert!(matches!(mount.await, Err(error) if error.is_cancelled()));
        let _ = gate.release.send(());
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while std::time::Instant::now() < deadline {
            let snap = runtime.diagnostics();
            let gone = snap
                .plugin_registry
                .iter()
                .all(|group| group.plugin_key != KEY.as_str())
                && snap
                    .plugin_fibers
                    .iter()
                    .all(|fiber| fiber.plugin_key != KEY.as_str());
            if gone && root.get(VALUE).is_err() {
                runtime.shutdown().await.expect("shutdown");
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("orphan Active fiber or service remained after ReadyForHandle abandon");
    }
}
