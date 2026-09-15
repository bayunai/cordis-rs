//! 上下文节点、父链与配置绑定。
//!
//! 维护节点父子关系、隔离标签与可拦截配置表；provider / inject 逻辑见兄弟模块。

use crate::{
    CoreError, ServiceId,
    config::{ConfigId, ErasedConfig},
    effect::EffectScope,
    isolation::IsolationLabel,
    registry::{NodeId, Registry, provider::ProviderKey},
};
use std::{
    any::{Any, TypeId},
    collections::HashMap,
    sync::Arc,
};

pub(crate) struct NodeRecord {
    pub(crate) parent: Option<NodeId>,
    pub(crate) isolations: HashMap<ServiceId, u64>,
    pub(crate) configs: HashMap<ConfigId, ErasedConfig>,
}

impl Registry {
    pub(crate) fn allocate_isolation_label(&self) -> IsolationLabel {
        let label = self.runtime_token.allocate_label();
        if let Ok(mut state) = self.state.lock() {
            state.isolations_seen.insert(label.id(), ());
        }
        label
    }

    pub(crate) fn add_node(
        self: &Arc<Self>,
        id: NodeId,
        parent: Option<NodeId>,
        isolations: HashMap<ServiceId, u64>,
        configs: HashMap<ConfigId, ErasedConfig>,
    ) -> Result<(), CoreError> {
        let mut state = self.state.lock().map_err(|_| CoreError::ContextDisposed)?;
        for (key, config) in &configs {
            if state
                .config_types
                .get(key)
                .is_some_and(|expected| *expected != config.type_id)
            {
                return Err(CoreError::ConfigKeyTypeConflict { config: *key });
            }
        }
        for (key, config) in &configs {
            state.config_types.entry(*key).or_insert(config.type_id);
        }
        state
            .isolations_seen
            .extend(isolations.values().copied().map(|id| (id, ())));
        state.nodes.insert(
            id,
            NodeRecord {
                parent,
                isolations,
                configs,
            },
        );
        Ok(())
    }

    pub(crate) fn bind_node_lifecycle(self: &Arc<Self>, id: NodeId, scope: &EffectScope) {
        let weak = Arc::downgrade(self);
        scope.on_dispose(move || {
            if let Some(registry) = weak.upgrade() {
                registry.remove_node(id);
            }
        });
    }

    fn remove_node(&self, id: NodeId) {
        if let Ok(mut state) = self.state.lock() {
            state.nodes.remove(&id);
            state.providers.retain(|key, _| match key {
                ProviderKey::Local { node, .. } => *node != id,
                ProviderKey::Isolated { .. } => true,
            });
            for listeners in state.listeners.values_mut() {
                listeners.retain(|listener| listener.node != id);
            }
            state.listeners.retain(|_, listeners| !listeners.is_empty());
            state.effects.retain(|_, effect| effect.node != Some(id));
        }
    }

    pub(crate) fn resolve_config(
        &self,
        node: NodeId,
        key: ConfigId,
        type_id: TypeId,
    ) -> Result<Arc<dyn Any + Send + Sync>, CoreError> {
        let state = self.state.lock().map_err(|_| CoreError::ContextDisposed)?;
        if let Some(expected) = state.config_types.get(&key)
            && *expected != type_id
        {
            return Err(CoreError::ConfigTypeMismatch { config: key });
        }
        let mut current = Some(node);
        while let Some(id) = current {
            let Some(record) = state.nodes.get(&id) else {
                break;
            };
            if let Some(config) = record.configs.get(&key) {
                if config.type_id != type_id {
                    return Err(CoreError::ConfigTypeMismatch { config: key });
                }
                return Ok(config.value.clone());
            }
            current = record.parent;
        }
        Err(CoreError::ConfigUnavailable { config: key })
    }
}
