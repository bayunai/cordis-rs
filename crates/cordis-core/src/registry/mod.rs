//! 内部单一协调枢纽，不是公开 API。
//!
//! 聚合节点、provider、注入与调度；子模块：`node` 上下文节点，`provider` 注册撤销，
//! `injection` 响应式注入收敛，`scheduler` 后台重算调度。

pub(crate) mod injection;
pub(crate) mod node;
pub(crate) mod provider;
pub(crate) mod scheduler;

pub(crate) use injection::InjectionPhase;
pub(crate) use provider::ProviderKey;

pub(crate) type NodeId = u64;
pub(crate) type InjectionId = u64;
pub(crate) type ListenerId = u64;

use crate::{
    ServiceId,
    config::ConfigId,
    diagnostics::{
        ContextIsolationSnapshot, ContextSnapshot, EffectSnapshot, FiberStateSnapshot,
        InjectFiberSnapshot, IsolationSnapshot, PluginRegistryFiberSnapshot,
        PluginRegistrySnapshot, ProviderSnapshot, RuntimeSnapshot,
    },
    event::listener::{EventContract, EventListener},
    fiber::{FiberInner, FiberState, FiberStateChange},
    isolation::RuntimeToken,
    plugin::{PluginKey, group::PluginGroup},
    registry::{
        injection::InjectionRecord,
        node::NodeRecord,
        provider::{EffectRecord, ProviderRecord},
    },
    service::resolver::resolve_provider,
};
use std::{
    any::TypeId,
    collections::HashMap,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tokio::sync::{Notify, broadcast};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub(crate) struct RegistryState {
    pub(crate) nodes: HashMap<NodeId, NodeRecord>,
    pub(crate) providers: HashMap<ProviderKey, ProviderRecord>,
    pub(crate) injections: HashMap<InjectionId, InjectionRecord>,
    pub(crate) listeners: HashMap<&'static str, Vec<EventListener>>,
    pub(crate) effects: HashMap<u64, EffectRecord>,
    pub(crate) plugin_fibers: HashMap<u64, Weak<FiberInner>>,
    pub(crate) plugin_index: HashMap<PluginKey, PluginGroup>,
    pub(crate) isolations_seen: HashMap<u64, ()>,
    pub(crate) service_types: HashMap<ServiceId, TypeId>,
    pub(crate) config_types: HashMap<ConfigId, TypeId>,
    pub(crate) event_contracts: HashMap<&'static str, EventContract>,
}

pub(crate) struct Registry {
    pub(crate) state: Mutex<RegistryState>,
    pub(crate) runtime_token: RuntimeToken,
    pub(crate) next_id: AtomicU64,
    pub(crate) dirty: AtomicBool,
    pub(crate) recomputing: AtomicBool,
    pub(crate) wake: Notify,
    pub(crate) quiescent: Notify,
    pub(crate) recompute_lock: tokio::sync::Mutex<()>,
    pub(crate) scheduler_started: AtomicBool,
    pub(crate) scheduler_stopped: AtomicBool,
    pub(crate) scheduler_cancel: CancellationToken,
    pub(crate) scheduler_task: Mutex<Option<JoinHandle<()>>>,
    pub(crate) fiber_state_events: broadcast::Sender<FiberStateChange>,
}

impl Registry {
    pub(crate) fn new() -> Arc<Self> {
        let (fiber_state_events, _) = broadcast::channel(1024);
        Arc::new(Self {
            state: Mutex::new(RegistryState {
                nodes: HashMap::new(),
                providers: HashMap::new(),
                injections: HashMap::new(),
                listeners: HashMap::new(),
                effects: HashMap::new(),
                plugin_fibers: HashMap::new(),
                plugin_index: HashMap::new(),
                isolations_seen: HashMap::new(),
                service_types: HashMap::new(),
                config_types: HashMap::new(),
                event_contracts: HashMap::new(),
            }),
            runtime_token: RuntimeToken::new(),
            next_id: AtomicU64::new(1),
            dirty: AtomicBool::new(false),
            recomputing: AtomicBool::new(false),
            wake: Notify::new(),
            quiescent: Notify::new(),
            recompute_lock: tokio::sync::Mutex::new(()),
            scheduler_started: AtomicBool::new(false),
            scheduler_stopped: AtomicBool::new(false),
            scheduler_cancel: CancellationToken::new(),
            scheduler_task: Mutex::new(None),
            fiber_state_events,
        })
    }

    pub(crate) fn runtime_token(&self) -> &RuntimeToken {
        &self.runtime_token
    }

    pub(crate) fn mark_dirty_public(self: &Arc<Self>) {
        self.mark_dirty();
    }

    pub(crate) fn allocate_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    pub(crate) fn diagnostics(&self) -> RuntimeSnapshot {
        let Ok(state) = self.state.lock() else {
            return RuntimeSnapshot::default();
        };
        let contexts = state
            .nodes
            .iter()
            .map(|(id, node)| ContextSnapshot {
                id: *id,
                parent: node.parent,
                isolations: node
                    .isolations
                    .iter()
                    .map(|(service, label_id)| ContextIsolationSnapshot {
                        service: service.as_str(),
                        label_id: *label_id,
                    })
                    .collect(),
                config_keys: node.configs.keys().map(|key| key.as_str()).collect(),
            })
            .collect();
        let mut isolations: Vec<IsolationSnapshot> = state
            .isolations_seen
            .keys()
            .copied()
            .map(|id| IsolationSnapshot { id })
            .collect();
        isolations.sort_by_key(|item| item.id);
        let providers = state
            .providers
            .iter()
            .map(|(_key, provider)| ProviderSnapshot {
                node: provider.node,
                isolation: provider.isolation,
                service: match _key {
                    ProviderKey::Local { service, .. } | ProviderKey::Isolated { service, .. } => {
                        service.as_str()
                    }
                },
                provider_id: provider.id,
                effect_id: provider.effect_id,
            })
            .collect();
        let inject_fibers = state
            .injections
            .iter()
            .map(|(id, injection)| {
                let missing = injection
                    .dependencies
                    .iter()
                    .filter(|key| resolve_provider(&state, injection.node, **key).is_none())
                    .map(|key| key.as_str())
                    .collect::<Vec<_>>();
                InjectFiberSnapshot {
                    id: *id,
                    node: injection.node,
                    phase: FiberStateSnapshot::from(injection.phase),
                    dependencies: injection
                        .dependencies
                        .iter()
                        .map(|key| key.as_str())
                        .collect(),
                    missing_dependencies: missing,
                    last_error: injection.last_error.clone(),
                }
            })
            .collect();
        let effects = state
            .effects
            .iter()
            .map(|(id, effect)| EffectSnapshot {
                id: *id,
                name: effect.name.clone(),
                parent: effect.parent,
                node: effect.node,
                fiber_id: effect.fiber_id,
                cancelled: effect.scope.is_cancelled(),
                disposed: effect.scope.is_disposed(),
                child_count: effect.scope.child_scope_count(),
                task_count: effect.scope.task_count(),
                cleanup_count: effect.scope.cleanup_count(),
            })
            .collect();
        let plugin_fiber_arcs = state
            .plugin_fibers
            .values()
            .filter_map(Weak::upgrade)
            .collect::<Vec<_>>();
        let mut plugin_registry: Vec<PluginRegistrySnapshot> = state
            .plugin_index
            .iter()
            .map(|(key, group)| PluginRegistrySnapshot {
                plugin_key: key.as_str(),
                unmounting: group.unmounting,
                fibers: group
                    .fibers
                    .iter()
                    .filter_map(Weak::upgrade)
                    .map(|fiber| {
                        let state = match *fiber.state.lock().expect("state") {
                            FiberState::Pending => FiberStateSnapshot::Pending,
                            FiberState::Loading => FiberStateSnapshot::Loading,
                            FiberState::Active => FiberStateSnapshot::Active,
                            FiberState::Failed => FiberStateSnapshot::Failed,
                            FiberState::Unloading => FiberStateSnapshot::Unloading,
                            FiberState::Disposed => FiberStateSnapshot::Disposed,
                        };
                        PluginRegistryFiberSnapshot {
                            id: fiber.id,
                            state,
                        }
                    })
                    .collect(),
            })
            .collect();
        plugin_registry.sort_by_key(|item| item.plugin_key);
        drop(state);
        let plugin_fibers = plugin_fiber_arcs
            .iter()
            .map(|fiber| fiber.snapshot())
            .collect();
        RuntimeSnapshot {
            contexts,
            isolations,
            providers,
            plugin_fibers,
            plugin_registry,
            inject_fibers,
            effects,
        }
    }
}
