//! 响应式 inject 收敛。
//!
//! 跟踪依赖、回调与注入相位，并在 provider 变化时重算；不负责插件生命周期。

use crate::{
    CoreError, ServiceId, Services,
    effect::EffectScope,
    registry::{InjectionId, NodeId, Registry},
    service::resolver::{provider_ids, resolve_provider},
};
use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};

type InjectFuture = Pin<Box<dyn Future<Output = Result<(), CoreError>> + Send>>;
pub(crate) type InjectCallback = Arc<dyn Fn(Services, EffectScope) -> InjectFuture + Send + Sync>;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum InjectionPhase {
    Pending,
    Active,
    Failed,
    Disposed,
}

pub(crate) struct InjectionRecord {
    pub(crate) node: NodeId,
    pub(crate) parent_scope: EffectScope,
    pub(crate) dependencies: Vec<ServiceId>,
    pub(crate) callback: InjectCallback,
    pub(crate) child_scope: Option<EffectScope>,
    pub(crate) phase: InjectionPhase,
    pub(crate) resolved_providers: Vec<u64>,
    pub(crate) last_error: Option<String>,
}

impl Registry {
    pub(crate) fn register_injection(
        self: &Arc<Self>,
        node: NodeId,
        parent_scope: EffectScope,
        dependencies: Vec<ServiceId>,
        callback: InjectCallback,
    ) -> Result<InjectionId, CoreError> {
        if dependencies.is_empty() {
            return Err(CoreError::EmptyInjection);
        }
        if parent_scope.is_disposed() {
            return Err(CoreError::ContextDisposed);
        }
        let id = self.allocate_id();
        {
            let mut state = self.state.lock().map_err(|_| CoreError::ContextDisposed)?;
            state.injections.insert(
                id,
                InjectionRecord {
                    node,
                    parent_scope: parent_scope.clone(),
                    dependencies,
                    callback,
                    child_scope: None,
                    phase: InjectionPhase::Pending,
                    resolved_providers: Vec::new(),
                    last_error: None,
                },
            );
        }
        let weak = Arc::downgrade(self);
        parent_scope.on_dispose(move || {
            if let Some(registry) = weak.upgrade() {
                registry.remove_injection(id);
            }
        });
        self.mark_dirty();
        Ok(id)
    }

    fn remove_injection(self: &Arc<Self>, id: InjectionId) {
        let removed = self
            .state
            .lock()
            .ok()
            .and_then(|mut state| state.injections.remove(&id));
        if let Some(mut injection) = removed {
            injection.phase = InjectionPhase::Disposed;
            if let Some(child) = injection.child_scope.take() {
                child.dispose();
            }
            self.mark_dirty();
        }
    }

    pub(crate) fn injection_phase(&self, id: InjectionId) -> Option<InjectionPhase> {
        self.state
            .lock()
            .ok()
            .and_then(|state| state.injections.get(&id).map(|item| item.phase))
    }

    pub(crate) fn take_ready(&self) -> Vec<InjectionId> {
        let mut stale = Vec::new();
        {
            let Ok(mut state) = self.state.lock() else {
                return Vec::new();
            };
            let candidates = state
                .injections
                .iter()
                .map(|(id, injection)| {
                    (
                        *id,
                        injection.parent_scope.is_disposed(),
                        injection.phase,
                        injection.resolved_providers.clone(),
                        provider_ids(&state, injection.node, &injection.dependencies),
                    )
                })
                .collect::<Vec<_>>();
            for (id, parent_disposed, phase, resolved, providers) in candidates {
                if parent_disposed {
                    continue;
                }
                if phase == InjectionPhase::Active && resolved != providers {
                    let Some(injection) = state.injections.get_mut(&id) else {
                        continue;
                    };
                    if let Some(child) = injection.child_scope.take() {
                        stale.push(child);
                    }
                    injection.phase = InjectionPhase::Pending;
                    injection.resolved_providers.clear();
                    injection.last_error = None;
                }
            }
        }
        for scope in stale {
            scope.dispose();
        }

        let Ok(state) = self.state.lock() else {
            return Vec::new();
        };
        state
            .injections
            .iter()
            .filter_map(|(id, injection)| {
                if injection.parent_scope.is_disposed() {
                    return None;
                }
                let providers = provider_ids(&state, injection.node, &injection.dependencies);
                let ready = providers.len() == injection.dependencies.len();
                match injection.phase {
                    InjectionPhase::Pending if ready => Some(*id),
                    InjectionPhase::Failed
                        if ready && injection.resolved_providers != providers =>
                    {
                        Some(*id)
                    }
                    _ => None,
                }
            })
            .collect()
    }

    pub(crate) async fn run_injection(self: &Arc<Self>, id: InjectionId) {
        let Some((callback, services, parent, providers)) = self.snapshot_injection(id) else {
            return;
        };
        let child = parent.child();
        let outcome = callback(services, child.clone()).await;
        let mut failed = None;
        if let Ok(mut state) = self.state.lock() {
            if let Some(injection) = state.injections.get_mut(&id) {
                if injection.parent_scope.is_disposed() {
                    failed = Some(child);
                } else if let Err(error) = outcome {
                    injection.child_scope = None;
                    injection.phase = InjectionPhase::Failed;
                    injection.resolved_providers = providers;
                    injection.last_error = Some(error.to_string());
                    failed = Some(child);
                } else {
                    injection.child_scope = Some(child);
                    injection.phase = InjectionPhase::Active;
                    injection.resolved_providers = providers;
                    injection.last_error = None;
                }
            } else {
                failed = Some(child);
            }
        } else {
            failed = Some(child);
        }
        if let Some(scope) = failed {
            scope.dispose();
        }
    }

    pub(crate) fn snapshot_injection(
        &self,
        id: InjectionId,
    ) -> Option<(InjectCallback, Services, EffectScope, Vec<u64>)> {
        let state = self.state.lock().ok()?;
        let injection = state.injections.get(&id)?;
        let mut values = HashMap::new();
        let mut providers = Vec::with_capacity(injection.dependencies.len());
        for key in &injection.dependencies {
            let provider = resolve_provider(&state, injection.node, *key)?;
            values.insert(*key, provider.value.clone());
            providers.push(provider.id);
        }
        Some((
            injection.callback.clone(),
            Services { values },
            injection.parent_scope.clone(),
            providers,
        ))
    }
}
