//! 基于 Context 纯视图链的 Service 解析。

use crate::{
    Context, ServiceId,
    registry::{
        RegistryState,
        provider::{ProviderKey, ProviderRecord},
    },
};

pub(crate) fn resolve_provider<'a>(
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

pub(crate) fn provider_ids(
    state: &RegistryState,
    context: &Context,
    keys: &[ServiceId],
) -> Vec<u64> {
    keys.iter()
        .filter_map(|key| resolve_provider(state, context, *key).map(|provider| provider.id))
        .collect()
}
