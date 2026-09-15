//! 依赖就绪后的 apply / reapply。
//!
//! 认领 Pending→Loading，解析依赖并调用 Plugin::apply；生命周期入口在 `lifecycle` / `coordinator`。

use super::{ActivateClaim, FiberInner, FiberState};
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

    fn abandon_loading_to_pending(&self, effect_scope: Option<&EffectScope>, mark_dirty: bool) {
        if let Some(scope) = effect_scope {
            scope.dispose();
        }
        let _ = self.pending_effect.lock().expect("pending_effect").take();
        self.resolved_providers.lock().expect("providers").clear();
        *self.last_error.lock().expect("error") = None;
        let _ = self.transition_if_alive(FiberState::Pending);
        if mark_dirty && let Some(registry) = self.registry.upgrade() {
            registry.mark_dirty_public();
        }
    }

    /// 首次挂载被调用方取消：撤销临时 Scope、注销 Fiber，视为从未成功。
    fn revoke_initial_mount(&self, effect_scope: Option<EffectScope>) {
        if let Some(scope) = effect_scope {
            scope.dispose();
        }
        if let Some(scope) = self.pending_effect.lock().expect("pending_effect").take() {
            scope.dispose();
        }
        if let Some(scope) = self.effect.lock().expect("effect").take() {
            scope.dispose();
        }
        self.resolved_providers.lock().expect("providers").clear();
        *self.last_error.lock().expect("error") = None;
        self.disposed.store(true, Ordering::Release);
        self.transition_disposed();
        if let Some(registry) = self.registry.upgrade() {
            registry.unregister_plugin_fiber(self.id);
        }
    }

    fn should_revoke_initial_mount(&self, initial_mount: bool) -> bool {
        initial_mount && self.caller_cancelled.load(Ordering::Acquire)
    }

    /// 协调器内激活主体；`initial_mount` 时尊重调用方取消撤销。
    pub(crate) async fn activate_with_policy(
        self: &Arc<Self>,
        initial_mount: bool,
    ) -> Result<(), CoreError> {
        if self.disposed.load(Ordering::Acquire) {
            return Err(CoreError::FiberDisposed);
        }
        if self.should_revoke_initial_mount(initial_mount) {
            self.revoke_initial_mount(None);
            return Err(CoreError::FiberDisposed);
        }
        if self.claim_activation() != ActivateClaim::Claimed {
            return Ok(());
        }

        let Some(registry) = self.registry.upgrade() else {
            let _ = self.transition_if_alive(FiberState::Failed);
            return Err(CoreError::ContextDisposed);
        };
        let deps = self.dependencies.lock().expect("deps").clone();
        let Some(providers) = self.resolve_provider_ids(&registry, &deps) else {
            self.abandon_loading_to_pending(None, false);
            return Ok(());
        };

        if self.should_revoke_initial_mount(initial_mount) {
            self.revoke_initial_mount(None);
            return Err(CoreError::FiberDisposed);
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
        *self.pending_effect.lock().expect("pending_effect") = Some(effect_scope.clone());
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
                if self.should_revoke_initial_mount(initial_mount) {
                    self.revoke_initial_mount(Some(effect_scope));
                    return Err(CoreError::FiberDisposed);
                }
                if self.disposed.load(Ordering::Acquire) {
                    let _ = self.pending_effect.lock().expect("pending_effect").take();
                    effect_scope.dispose();
                    self.transition_disposed();
                    return Err(CoreError::FiberDisposed);
                }
                if effect_scope.is_disposed() || parent_scope.is_disposed() {
                    let _ = self.pending_effect.lock().expect("pending_effect").take();
                    effect_scope.dispose();
                    *self.last_error.lock().expect("error") =
                        Some("plugin scope was disposed during apply".into());
                    let _ = self.transition_if_alive(FiberState::Failed);
                    return Err(CoreError::PluginApply(
                        "plugin scope was disposed during apply".into(),
                    ));
                }
                let Some(fresh) = self.resolve_provider_ids(&registry, &deps) else {
                    self.abandon_loading_to_pending(Some(&effect_scope), true);
                    return Ok(());
                };
                if fresh != providers {
                    self.abandon_loading_to_pending(Some(&effect_scope), true);
                    return Ok(());
                }
                let _ = self.pending_effect.lock().expect("pending_effect").take();
                *self.effect.lock().expect("effect") = Some(effect_scope);
                *self.resolved_providers.lock().expect("providers") = providers;
                *self.last_error.lock().expect("error") = None;
                if !self.transition_if_alive(FiberState::Active) {
                    if let Some(scope) = self.effect.lock().expect("effect").take() {
                        scope.dispose();
                    }
                    self.resolved_providers.lock().expect("providers").clear();
                    return Err(CoreError::FiberDisposed);
                }
                registry.mark_dirty_public();
                Ok(())
            }
            Err(error) => {
                let _ = self.pending_effect.lock().expect("pending_effect").take();
                effect_scope.dispose();
                if self.should_revoke_initial_mount(initial_mount) {
                    self.revoke_initial_mount(None);
                    return Err(CoreError::FiberDisposed);
                }
                if self.disposed.load(Ordering::Acquire) {
                    self.transition_disposed();
                    return Err(CoreError::FiberDisposed);
                }
                *self.last_error.lock().expect("error") = Some(error.to_string());
                let _ = self.transition_if_alive(FiberState::Failed);
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
