//! Provider 记录的注册与撤销。
//!
//! 管理本地与隔离键上的服务实例，以及 Effect 诊断记录；解析路径见 `service/resolver`。

use crate::{
    CoreError, ServiceId,
    effect::EffectScope,
    registry::{NodeId, Registry, RegistryState},
    service::{ErasedService, resolver::lookup_isolation},
};
use std::{any::TypeId, sync::Arc};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) enum ProviderKey {
    Local { node: NodeId, service: ServiceId },
    Isolated { isolation: u64, service: ServiceId },
}

pub(crate) struct ProviderRecord {
    pub(crate) id: u64,
    pub(crate) value: ErasedService,
    pub(crate) effect_id: Option<u64>,
    pub(crate) node: Option<NodeId>,
    pub(crate) isolation: Option<u64>,
}

pub(crate) struct EffectRecord {
    pub(crate) name: String,
    pub(crate) parent: Option<u64>,
    pub(crate) node: Option<NodeId>,
    pub(crate) fiber_id: Option<u64>,
    pub(crate) scope: EffectScope,
}

impl Registry {
    pub(crate) fn resolve_with_id(
        &self,
        node: NodeId,
        key: ServiceId,
    ) -> Option<(u64, ErasedService)> {
        let state = self.state.lock().ok()?;
        crate::service::resolver::resolve_provider(&state, node, key)
            .map(|provider| (provider.id, provider.value.clone()))
    }

    pub(crate) fn register_effect(
        self: &Arc<Self>,
        id: u64,
        name: String,
        parent: Option<u64>,
        node: Option<NodeId>,
        fiber_id: Option<u64>,
        scope: &EffectScope,
    ) {
        if let Ok(mut state) = self.state.lock() {
            state.effects.insert(
                id,
                EffectRecord {
                    name,
                    parent,
                    node,
                    fiber_id,
                    scope: scope.clone(),
                },
            );
        }
        let weak = Arc::downgrade(self);
        scope.on_dispose(move || {
            if let Some(registry) = weak.upgrade()
                && let Ok(mut state) = registry.state.lock()
            {
                state.effects.remove(&id);
            }
        });
    }

    fn lock_service_type(
        state: &mut RegistryState,
        key: ServiceId,
        type_id: TypeId,
    ) -> Result<(), CoreError> {
        match state.service_types.entry(key) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(type_id);
                Ok(())
            }
            std::collections::hash_map::Entry::Occupied(entry) => {
                if *entry.get() == type_id {
                    Ok(())
                } else {
                    Err(CoreError::ServiceKeyTypeConflict { service: key })
                }
            }
        }
    }

    pub(crate) fn provide(
        self: &Arc<Self>,
        node: NodeId,
        key: ServiceId,
        value: ErasedService,
        owner: EffectScope,
    ) -> Result<(), CoreError> {
        if owner.is_disposed() {
            return Err(CoreError::ContextDisposed);
        }
        let provider_id = self.allocate_id();
        {
            let mut state = self.state.lock().map_err(|_| CoreError::ContextDisposed)?;
            Self::lock_service_type(&mut state, key, value.type_id)?;
            let isolation = lookup_isolation(&state, node, key);
            let slot = match isolation {
                Some(iso) => ProviderKey::Isolated {
                    isolation: iso,
                    service: key,
                },
                None => ProviderKey::Local { node, service: key },
            };
            if state.providers.contains_key(&slot) {
                return Err(CoreError::ServiceConflict { service: key });
            }
            state.providers.insert(
                slot,
                ProviderRecord {
                    id: provider_id,
                    value,
                    effect_id: Some(owner.id()),
                    node: Some(node),
                    isolation,
                },
            );
        }
        let weak = Arc::downgrade(self);
        owner.on_dispose(move || {
            if let Some(registry) = weak.upgrade() {
                registry.remove_provider(provider_id);
            }
        });
        self.mark_dirty();
        Ok(())
    }

    fn remove_provider(self: &Arc<Self>, provider_id: u64) {
        let removed = if let Ok(mut state) = self.state.lock() {
            let key = state
                .providers
                .iter()
                .find_map(|(key, value)| (value.id == provider_id).then_some(*key));
            key.is_some_and(|key| state.providers.remove(&key).is_some())
        } else {
            false
        };
        if removed {
            self.mark_dirty();
        }
    }

    pub(crate) fn resolve(&self, node: NodeId, key: ServiceId) -> Option<ErasedService> {
        let state = self.state.lock().ok()?;
        crate::service::resolver::resolve_provider(&state, node, key)
            .map(|provider| provider.value.clone())
    }
}
