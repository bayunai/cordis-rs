//! 层级与隔离解析。
//!
//! 沿父链或隔离标签查找 provider；不含应用/租户/路由等业务作用域。

use crate::{
    ServiceId,
    registry::{
        NodeId, RegistryState,
        provider::{ProviderKey, ProviderRecord},
    },
};

pub(crate) fn lookup_isolation(
    state: &RegistryState,
    start: NodeId,
    key: ServiceId,
) -> Option<u64> {
    let mut node = Some(start);
    while let Some(current) = node {
        if let Some(label) = state
            .nodes
            .get(&current)
            .and_then(|record| record.isolations.get(&key).copied())
        {
            return Some(label);
        }
        node = state.nodes.get(&current).and_then(|item| item.parent);
    }
    None
}

pub(crate) fn resolve_provider(
    state: &RegistryState,
    start: NodeId,
    key: ServiceId,
) -> Option<&ProviderRecord> {
    if let Some(iso) = lookup_isolation(state, start, key) {
        return state.providers.get(&ProviderKey::Isolated {
            isolation: iso,
            service: key,
        });
    }
    let mut node = Some(start);
    while let Some(current) = node {
        if let Some(provider) = state.providers.get(&ProviderKey::Local {
            node: current,
            service: key,
        }) {
            return Some(provider);
        }
        node = state.nodes.get(&current).and_then(|item| item.parent);
    }
    None
}

pub(crate) fn provider_ids(state: &RegistryState, node: NodeId, keys: &[ServiceId]) -> Vec<u64> {
    keys.iter()
        .filter_map(|key| resolve_provider(state, node, *key).map(|provider| provider.id))
        .collect()
}
