//! 依赖就绪后的 apply / reapply。
//!
//! 认领 Pending→Loading，解析依赖并调用 Plugin::apply；生命周期入口在 `lifecycle`。

use super::{ActivateClaim, FiberInner, FiberState};
use crate::{Context, CoreError, ServiceId, effect::EffectScope, error::format_panic_message};
use futures_util::FutureExt;
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, atomic::Ordering},
};

async fn invoke_plugin_apply(
    plugin: &Arc<dyn crate::plugin::Plugin>,
    context: &Context,
) -> Result<(), String> {
    let future = catch_unwind(AssertUnwindSafe(|| plugin.apply(context)))
        .map_err(|payload| format_panic_message("plugin apply", payload))?;
    match AssertUnwindSafe(future).catch_unwind().await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(error.to_string()),
        Err(payload) => Err(format_panic_message("plugin apply", payload)),
    }
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
        self.resolved_providers.lock().expect("providers").clear();
        *self.last_error.lock().expect("error") = None;
        let _ = self.transition_if_alive(FiberState::Pending);
        if mark_dirty && let Some(registry) = self.registry.upgrade() {
            registry.mark_dirty_public();
        }
    }

    pub(crate) async fn try_activate(self: &Arc<Self>) -> Result<(), CoreError> {
        if self.disposed.load(Ordering::Acquire) {
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

        let plugin = self.plugin.lock().expect("plugin").clone();
        let parent_scope = self.parent_scope.clone();
        if parent_scope.is_disposed() {
            *self.last_error.lock().expect("error") =
                Some("parent scope disposed before apply".into());
            let _ = self.transition_if_alive(FiberState::Failed);
            return Err(CoreError::ContextDisposed);
        }
        let effect_scope = parent_scope.child_named("plugin");
        let mount = match self.mount_ctx.lock().expect("mount").clone() {
            Some(ctx) => ctx,
            None => {
                effect_scope.dispose();
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
        match invoke_plugin_apply(&plugin, &apply_ctx).await {
            Ok(()) => {
                if self.disposed.load(Ordering::Acquire) {
                    effect_scope.dispose();
                    self.transition_disposed();
                    return Err(CoreError::FiberDisposed);
                }
                if effect_scope.is_disposed() || parent_scope.is_disposed() {
                    effect_scope.dispose();
                    *self.last_error.lock().expect("error") =
                        Some("plugin scope was disposed during apply".into());
                    let _ = self.transition_if_alive(FiberState::Failed);
                    return Err(CoreError::PluginApply(
                        "plugin scope was disposed during apply".into(),
                    ));
                }
                // Loading 期间 Provider 可能被撤销/替换；成功前必须重解析。
                let Some(fresh) = self.resolve_provider_ids(&registry, &deps) else {
                    self.abandon_loading_to_pending(Some(&effect_scope), true);
                    return Ok(());
                };
                if fresh != providers {
                    self.abandon_loading_to_pending(Some(&effect_scope), true);
                    return Ok(());
                }
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
                // Active 提交后的补偿重算覆盖最终校验与提交之间的 Provider 漂移。
                registry.mark_dirty_public();
                Ok(())
            }
            Err(error) => {
                effect_scope.dispose();
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
}
