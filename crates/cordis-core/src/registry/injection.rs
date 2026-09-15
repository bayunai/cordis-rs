//! 响应式 inject 收敛。
//!
//! 跟踪依赖、回调与注入相位，并在 provider 变化时重算；不负责插件生命周期。

use crate::{
    CoreError, ServiceId, Services,
    callback_context::{LifecycleFrame, USER_LIFECYCLE_CALLBACK},
    effect::EffectScope,
    error::format_panic_message,
    registry::{InjectionId, NodeId, Registry},
    service::resolver::{provider_ids, resolve_provider},
};
use futures_util::FutureExt;
use std::{
    collections::HashMap,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::Arc,
};

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

pub(crate) struct InjectionBatch {
    pub(crate) ready: Vec<InjectionId>,
    pub(crate) stale_scopes: Vec<EffectScope>,
}

pub(crate) struct InjectionSnapshot {
    pub(crate) callback: InjectCallback,
    pub(crate) services: Services,
    pub(crate) parent: EffectScope,
    pub(crate) providers: Vec<u64>,
    pub(crate) node: NodeId,
    pub(crate) deps: Vec<ServiceId>,
}

async fn invoke_callback(
    callback: &InjectCallback,
    services: Services,
    child: EffectScope,
) -> Result<(), String> {
    USER_LIFECYCLE_CALLBACK
        .scope(
            LifecycleFrame {
                scope: child.clone(),
            },
            async {
                let future = catch_unwind(AssertUnwindSafe(|| callback(services, child)))
                    .map_err(|payload| format_panic_message("injection callback", payload))?;
                match AssertUnwindSafe(future).catch_unwind().await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(error)) => Err(error.to_string()),
                    Err(payload) => Err(format_panic_message("injection callback", payload)),
                }
            },
        )
        .await
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

    /// 取待跑注入与需 dispose 的 stale child；调用方须在 `recompute_lock` 外 dispose。
    pub(crate) fn take_ready(&self) -> InjectionBatch {
        let mut stale_scopes = Vec::new();
        {
            let Ok(mut state) = self.state.lock() else {
                return InjectionBatch {
                    ready: Vec::new(),
                    stale_scopes,
                };
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
                        stale_scopes.push(child);
                    }
                    injection.phase = InjectionPhase::Pending;
                    injection.resolved_providers.clear();
                    injection.last_error = None;
                }
            }
        }

        let ready = {
            let Ok(state) = self.state.lock() else {
                return InjectionBatch {
                    ready: Vec::new(),
                    stale_scopes,
                };
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
        };
        InjectionBatch {
            ready,
            stale_scopes,
        }
    }

    pub(crate) async fn run_injection(self: &Arc<Self>, id: InjectionId) {
        let Some(snapshot) = self.snapshot_injection(id) else {
            return;
        };
        let child = snapshot.parent.child();
        let outcome = invoke_callback(&snapshot.callback, snapshot.services, child.clone()).await;
        let fresh = {
            let Ok(state) = self.state.lock() else {
                child.dispose();
                return;
            };
            provider_ids(&state, snapshot.node, &snapshot.deps)
        };
        let mut failed = None;
        if let Ok(mut state) = self.state.lock() {
            if let Some(injection) = state.injections.get_mut(&id) {
                if injection.parent_scope.is_disposed() {
                    failed = Some(child);
                } else if let Err(error) = outcome {
                    injection.child_scope = None;
                    injection.phase = InjectionPhase::Failed;
                    injection.resolved_providers = snapshot.providers;
                    injection.last_error = Some(error.to_string());
                    failed = Some(child);
                } else if fresh.len() != snapshot.deps.len() || fresh != snapshot.providers {
                    // 版本校验：await 期间 provider 漂移则不提交 Active。
                    injection.child_scope = None;
                    injection.phase = InjectionPhase::Pending;
                    injection.resolved_providers.clear();
                    injection.last_error = None;
                    failed = Some(child);
                    drop(state);
                    self.mark_dirty();
                    if let Some(scope) = failed.take() {
                        scope.dispose();
                    }
                    return;
                } else {
                    injection.child_scope = Some(child);
                    injection.phase = InjectionPhase::Active;
                    injection.resolved_providers = snapshot.providers;
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

    pub(crate) fn snapshot_injection(&self, id: InjectionId) -> Option<InjectionSnapshot> {
        let state = self.state.lock().ok()?;
        let injection = state.injections.get(&id)?;
        let mut values = HashMap::new();
        let mut providers = Vec::with_capacity(injection.dependencies.len());
        for key in &injection.dependencies {
            let provider = resolve_provider(&state, injection.node, *key)?;
            values.insert(*key, provider.value.clone());
            providers.push(provider.id);
        }
        Some(InjectionSnapshot {
            callback: injection.callback.clone(),
            services: Services { values },
            parent: injection.parent_scope.clone(),
            providers,
            node: injection.node,
            deps: injection.dependencies.clone(),
        })
    }
}
