//! 响应式 inject 收敛。
//!
//! 跟踪依赖、回调与注入相位，并在 provider 变化时重算；不负责插件生命周期。

use crate::{
    Context, CoreError, ServiceId, Services,
    callback_context::{LifecycleFrame, USER_LIFECYCLE_CALLBACK},
    effect::EffectScope,
    error::format_panic_message,
    registry::provider::ProviderRevision,
    registry::{InjectionId, Registry},
    service::resolver::provider_ids,
};
use futures_util::FutureExt;
use std::{
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
    Loading,
    Active,
    Unloading,
    Failed,
    Disposed,
}

pub(crate) struct InjectionRecord {
    pub(crate) context: Context,
    pub(crate) parent_scope: EffectScope,
    pub(crate) dependencies: Vec<ServiceId>,
    pub(crate) callback: InjectCallback,
    pub(crate) child_scope: Option<EffectScope>,
    pub(crate) phase: InjectionPhase,
    coordinating: bool,
    pub(crate) resolved_providers: Vec<ProviderRevision>,
    pub(crate) last_error: Option<String>,
}

pub(crate) struct InjectionBatch {
    pub(crate) ready: Vec<InjectionId>,
    pub(crate) stale_scopes: Vec<EffectScope>,
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
        context: Context,
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
                    context,
                    parent_scope: parent_scope.clone(),
                    dependencies,
                    callback,
                    child_scope: None,
                    phase: InjectionPhase::Pending,
                    coordinating: false,
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
                        provider_ids(&state, &injection.context, &injection.dependencies),
                    )
                })
                .collect::<Vec<_>>();
            for (id, parent_disposed, phase, resolved, providers) in candidates {
                if parent_disposed {
                    continue;
                }
                let child_disposed = state
                    .injections
                    .get(&id)
                    .and_then(|entry| entry.child_scope.as_ref())
                    .is_some_and(|child| child.is_disposed());
                if phase == InjectionPhase::Active && (resolved != providers || child_disposed) {
                    let Some(injection) = state.injections.get_mut(&id) else {
                        continue;
                    };
                    if let Some(child) = injection.child_scope.clone() {
                        stale_scopes.push(child);
                    }
                    injection.phase = InjectionPhase::Unloading;
                    injection.last_error = None;
                }
            }
        }

        {
            let mut state = self.state.lock().expect("registry");
            let completed = state
                .injections
                .iter()
                .filter_map(|(id, injection)| {
                    if injection.phase != InjectionPhase::Unloading || injection.coordinating {
                        return None;
                    }
                    let result = injection.child_scope.as_ref()?.release_result()?;
                    let baseline =
                        provider_ids(&state, &injection.context, &injection.dependencies);
                    Some((*id, result, baseline))
                })
                .collect::<Vec<_>>();
            for (id, result, baseline) in completed {
                let injection = state.injections.get_mut(&id).expect("completed injection");
                injection.child_scope = None;
                match result {
                    Ok(()) => injection.phase = InjectionPhase::Pending,
                    Err(error) => {
                        injection.phase = InjectionPhase::Failed;
                        injection.last_error = Some(error.to_string());
                        injection.resolved_providers = baseline;
                    }
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
                    let providers =
                        provider_ids(&state, &injection.context, &injection.dependencies);
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
        let (callback, context, parent, deps) = {
            let mut state = self.state.lock().expect("registry");
            let Some(entry) = state.injections.get_mut(&id) else {
                return;
            };
            if !matches!(
                entry.phase,
                InjectionPhase::Pending | InjectionPhase::Failed
            ) || entry.parent_scope.is_disposed()
            {
                return;
            }
            entry.phase = InjectionPhase::Loading;
            entry.coordinating = true;
            (
                entry.callback.clone(),
                entry.context.clone(),
                entry.parent_scope.clone(),
                entry.dependencies.clone(),
            )
        };
        let child = parent.child();
        {
            let mut state = self.state.lock().expect("registry");
            if let Some(entry) = state.injections.get_mut(&id) {
                entry.child_scope = Some(child.clone());
            }
        }
        let execution = child.begin_execution();
        let binding = match &execution {
            Ok(_) => self.bind_dependencies(&context, &deps, &child),
            Err(error) => Err(error.clone()),
        };
        let (providers, outcome) = match binding {
            Ok(Some((providers, services))) => {
                let outcome = invoke_callback(&callback, services, child.clone()).await;
                (providers, outcome)
            }
            Ok(None) => (Vec::new(), Ok(())),
            Err(error) => {
                let state = self.state.lock().expect("registry");
                (
                    provider_ids(&state, &context, &deps),
                    Err(error.to_string()),
                )
            }
        };
        drop(execution);
        let active = {
            let mut state = self.state.lock().expect("registry");
            let fresh = provider_ids(&state, &context, &deps);
            if let Some(entry) = state.injections.get_mut(&id) {
                if outcome.is_ok()
                    && !child.is_disposed()
                    && !parent.is_disposed()
                    && providers.len() == deps.len()
                    && providers == fresh
                {
                    entry.phase = InjectionPhase::Active;
                    entry.coordinating = false;
                    entry.resolved_providers = providers.clone();
                    entry.last_error = None;
                    true
                } else {
                    entry.phase = InjectionPhase::Unloading;
                    false
                }
            } else {
                false
            }
        };
        if !active {
            let cleanup = child.dispose_wait().await;
            let cleanup_failed = cleanup.is_err();
            let error = match (outcome.err(), cleanup.err()) {
                (Some(startup), Some(cleanup)) => Some(format!("{startup}; cleanup: {cleanup}")),
                (Some(startup), None) => Some(startup),
                (None, Some(cleanup)) => Some(cleanup.to_string()),
                (None, None) => None,
            };
            let mut state = self.state.lock().expect("registry");
            let providers = if cleanup_failed {
                provider_ids(&state, &context, &deps)
            } else {
                providers
            };
            if let Some(entry) = state.injections.get_mut(&id)
                && entry
                    .child_scope
                    .as_ref()
                    .is_some_and(|current| current.ptr_eq(&child))
            {
                entry.coordinating = false;
                entry.child_scope = None;
                entry.phase = if error.is_some() {
                    InjectionPhase::Failed
                } else {
                    InjectionPhase::Pending
                };
                entry.last_error = error;
                entry.resolved_providers = providers;
            }
        }
        self.mark_dirty();
    }
}
