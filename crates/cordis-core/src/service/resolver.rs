//! 基于 Context 纯视图链的 Service 解析。

use crate::{
    Context, ProviderAvailability, ServiceId,
    fiber::FiberState,
    registry::{
        RegistryState,
        provider::{ProviderKey, ProviderRecord, ProviderRevision},
    },
};
use std::sync::Arc;

/// 严格解析的三态结果。已注册但未就绪的局部 Provider 会遮蔽父级 Provider。
pub(crate) enum ProviderResolution<'a> {
    Missing,
    Unavailable(Arc<str>),
    Ready(&'a ProviderRecord),
}

/// 不考虑可用性地定位当前视图最接近的 Provider 槽位。
pub(crate) fn resolve_registered_provider<'a>(
    state: &'a RegistryState,
    start: &Context,
    key: ServiceId,
) -> Option<&'a ProviderRecord> {
    if let Some(label) = start.nearest_isolation(key) {
        return state.providers.get(&ProviderKey::Isolated {
            isolation: label.id(),
            service: key,
        });
    }
    let mut current = Some(start.clone());
    while let Some(view) = current {
        if let Some(provider) = state.providers.get(&ProviderKey::Local {
            context: view.identity(),
            service: key,
        }) {
            return Some(provider);
        }
        current = view.parent();
    }
    None
}

fn unavailable_reason(state: &RegistryState, provider: &ProviderRecord) -> Option<Arc<str>> {
    if let ProviderAvailability::Unavailable { reason } = &provider.availability {
        return Some(reason.clone());
    }
    let effect = provider
        .effect_id
        .and_then(|effect_id| state.effects.get(&effect_id))?;
    let fiber_id = effect.fiber_id?;
    let active = state
        .plugin_fibers
        .get(&fiber_id)
        .and_then(std::sync::Weak::upgrade)
        .is_some_and(|fiber| {
            matches!(
                *fiber.state.lock().expect("fiber state"),
                FiberState::Active
            )
        });
    (!active).then(|| Arc::from("provider plugin fiber is not active"))
}

pub(crate) fn effective_availability(
    state: &RegistryState,
    provider: &ProviderRecord,
) -> ProviderAvailability {
    match unavailable_reason(state, provider) {
        Some(reason) => ProviderAvailability::Unavailable { reason },
        None => ProviderAvailability::Ready,
    }
}

pub(crate) fn resolve_provider<'a>(
    state: &'a RegistryState,
    start: &Context,
    key: ServiceId,
) -> Option<&'a ProviderRecord> {
    match resolve_provider_state(state, start, key) {
        ProviderResolution::Ready(provider) => Some(provider),
        ProviderResolution::Missing | ProviderResolution::Unavailable(_) => None,
    }
}

pub(crate) fn resolve_provider_state<'a>(
    state: &'a RegistryState,
    start: &Context,
    key: ServiceId,
) -> ProviderResolution<'a> {
    let Some(provider) = resolve_registered_provider(state, start, key) else {
        return ProviderResolution::Missing;
    };
    match effective_availability(state, provider) {
        ProviderAvailability::Unavailable { reason } => ProviderResolution::Unavailable(reason),
        ProviderAvailability::Ready => ProviderResolution::Ready(provider),
    }
}

pub(crate) fn provider_ids(
    state: &RegistryState,
    context: &Context,
    keys: &[ServiceId],
) -> Vec<ProviderRevision> {
    keys.iter()
        .filter_map(|key| {
            resolve_provider(state, context, *key).map(|provider| ProviderRevision {
                id: provider.id,
                availability_revision: provider.availability_revision,
            })
        })
        .collect()
}
