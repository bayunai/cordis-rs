use crate::{
    CoreError, ServiceId, Services,
    config::{ConfigId, ErasedConfig},
    diagnostics::{
        ContextIsolationSnapshot, ContextSnapshot, EffectSnapshot, FiberStateSnapshot,
        InjectFiberSnapshot, IsolationSnapshot, PluginRegistryFiberSnapshot,
        PluginRegistrySnapshot, ProviderSnapshot, RuntimeSnapshot,
    },
    effect::EffectScope,
    fiber::{FiberInner, FiberState, FiberStateChange},
    isolation::{IsolationLabel, RuntimeToken},
    plugin::PluginKey,
    service::ErasedService,
};
use async_trait::async_trait;
use futures_util::future::join_all;
use std::{
    any::{Any, TypeId},
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tokio::sync::{Notify, broadcast};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub(crate) type NodeId = u64;
pub(crate) type InjectionId = u64;
pub(crate) type ListenerId = u64;

type InjectFuture = Pin<Box<dyn Future<Output = Result<(), CoreError>> + Send>>;
pub(crate) type InjectCallback = Arc<dyn Fn(Services, EffectScope) -> InjectFuture + Send + Sync>;

type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send>>;
type ObserveHandler = Arc<dyn Fn(&(dyn Any + Send + Sync)) -> Result<(), CoreError> + Send + Sync>;
pub(crate) type EventFilter =
    Arc<dyn Fn(&(dyn Any + Send + Sync)) -> Result<bool, CoreError> + Send + Sync>;
type ErasedNext = Box<
    dyn FnOnce(Box<dyn Any + Send + Sync>) -> BoxFut<Result<Box<dyn Any + Send + Sync>, CoreError>>
        + Send,
>;
pub(crate) type WaterfallHandler = Arc<
    dyn Fn(
            Box<dyn Any + Send + Sync>,
            ErasedNext,
        ) -> BoxFut<Result<Box<dyn Any + Send + Sync>, CoreError>>
        + Send
        + Sync,
>;

#[derive(Clone)]
pub(crate) struct ListenMeta {
    pub once: bool,
    pub prepend: bool,
    pub global: bool,
    pub once_gate: Arc<AtomicBool>,
    pub filter: Option<EventFilter>,
}

impl ListenMeta {
    pub(crate) fn new(
        once: bool,
        prepend: bool,
        global: bool,
        filter: Option<EventFilter>,
    ) -> Self {
        Self {
            once,
            prepend,
            global,
            once_gate: Arc::new(AtomicBool::new(false)),
            filter,
        }
    }
}

impl Default for ListenMeta {
    fn default() -> Self {
        Self::new(false, false, false, None)
    }
}

#[async_trait]
pub(crate) trait SerialHandlerErased: Send + Sync {
    async fn invoke(
        &self,
        payload: &(dyn Any + Send + Sync),
    ) -> Result<Option<Box<dyn Any + Send + Sync>>, CoreError>;
}

#[async_trait]
pub(crate) trait ParallelHandlerErased: Send + Sync {
    async fn invoke(&self, payload: &(dyn Any + Send + Sync)) -> Result<(), CoreError>;
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum EventMode {
    Observe,
    Waterfall,
    Serial,
    Parallel,
}

#[derive(Clone, Copy, Debug)]
struct EventContract {
    mode: EventMode,
    payload: TypeId,
    answer: Option<TypeId>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum InjectionPhase {
    Pending,
    Active,
    Failed,
    Disposed,
}

struct NodeRecord {
    parent: Option<NodeId>,
    isolations: HashMap<ServiceId, u64>,
    configs: HashMap<ConfigId, ErasedConfig>,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) enum ProviderKey {
    Local { node: NodeId, service: ServiceId },
    Isolated { isolation: u64, service: ServiceId },
}

struct ProviderRecord {
    id: u64,
    value: ErasedService,
    effect_id: Option<u64>,
    node: Option<NodeId>,
    isolation: Option<u64>,
}

struct EffectRecord {
    name: String,
    parent: Option<u64>,
    node: Option<NodeId>,
    fiber_id: Option<u64>,
    scope: EffectScope,
}

struct InjectionRecord {
    node: NodeId,
    parent_scope: EffectScope,
    dependencies: Vec<ServiceId>,
    callback: InjectCallback,
    child_scope: Option<EffectScope>,
    phase: InjectionPhase,
    resolved_providers: Vec<u64>,
    last_error: Option<String>,
}

enum EventHandlerKind {
    Observe(ObserveHandler),
    Waterfall(WaterfallHandler),
    Serial(Arc<dyn SerialHandlerErased>),
    Parallel(Arc<dyn ParallelHandlerErased>),
}

struct EventListener {
    id: ListenerId,
    node: NodeId,
    kind: EventHandlerKind,
    meta: ListenMeta,
}

struct PluginGroup {
    fibers: Vec<Weak<FiberInner>>,
    unmounting: bool,
}

struct RegistryState {
    nodes: HashMap<NodeId, NodeRecord>,
    providers: HashMap<ProviderKey, ProviderRecord>,
    injections: HashMap<InjectionId, InjectionRecord>,
    listeners: HashMap<&'static str, Vec<EventListener>>,
    effects: HashMap<u64, EffectRecord>,
    plugin_fibers: HashMap<u64, Weak<FiberInner>>,
    plugin_index: HashMap<PluginKey, PluginGroup>,
    isolations_seen: HashMap<u64, ()>,
    service_types: HashMap<ServiceId, TypeId>,
    config_types: HashMap<ConfigId, TypeId>,
    event_contracts: HashMap<&'static str, EventContract>,
}

pub(crate) struct Registry {
    state: Mutex<RegistryState>,
    runtime_token: RuntimeToken,
    next_id: AtomicU64,
    dirty: AtomicBool,
    recomputing: AtomicBool,
    wake: Notify,
    quiescent: Notify,
    recompute_lock: tokio::sync::Mutex<()>,
    scheduler_started: AtomicBool,
    scheduler_stopped: AtomicBool,
    scheduler_cancel: CancellationToken,
    scheduler_task: Mutex<Option<JoinHandle<()>>>,
    fiber_state_events: broadcast::Sender<FiberStateChange>,
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

    pub(crate) fn subscribe_fiber_states(&self) -> broadcast::Receiver<FiberStateChange> {
        self.fiber_state_events.subscribe()
    }

    pub(crate) fn publish_fiber_state(&self, change: FiberStateChange) {
        let _ = self.fiber_state_events.send(change);
    }

    pub(crate) fn runtime_token(&self) -> &RuntimeToken {
        &self.runtime_token
    }

    pub(crate) fn register_plugin_fiber(
        self: &Arc<Self>,
        fiber: Arc<FiberInner>,
    ) -> Result<(), CoreError> {
        let key = fiber.plugin_key;
        {
            let mut state = self.state.lock().map_err(|_| CoreError::ContextDisposed)?;
            if state
                .plugin_index
                .get(&key)
                .is_some_and(|group| group.unmounting)
            {
                return Err(CoreError::PluginUnmounting { plugin: key });
            }
            state.plugin_fibers.insert(fiber.id, Arc::downgrade(&fiber));
            let group = state
                .plugin_index
                .entry(key)
                .or_insert_with(|| PluginGroup {
                    fibers: Vec::new(),
                    unmounting: false,
                });
            group.fibers.push(Arc::downgrade(&fiber));
        }
        self.mark_dirty();
        Ok(())
    }

    pub(crate) fn unregister_plugin_fiber(&self, id: u64) {
        if let Ok(mut state) = self.state.lock() {
            let key = state
                .plugin_fibers
                .get(&id)
                .and_then(Weak::upgrade)
                .map(|fiber| fiber.plugin_key);
            state.plugin_fibers.remove(&id);
            if let Some(key) = key
                && let Some(group) = state.plugin_index.get_mut(&key)
            {
                group
                    .fibers
                    .retain(|weak| weak.upgrade().is_some_and(|fiber| fiber.id != id));
                if group.fibers.is_empty() && !group.unmounting {
                    state.plugin_index.remove(&key);
                }
            }
        }
    }

    pub(crate) fn begin_plugin_unmount(
        &self,
        key: PluginKey,
    ) -> Result<Vec<Arc<FiberInner>>, CoreError> {
        let mut state = self.state.lock().map_err(|_| CoreError::ContextDisposed)?;
        let group = state
            .plugin_index
            .entry(key)
            .or_insert_with(|| PluginGroup {
                fibers: Vec::new(),
                unmounting: false,
            });
        if group.unmounting {
            return Err(CoreError::PluginUnmounting { plugin: key });
        }
        group.unmounting = true;
        group.fibers.retain(|weak| weak.strong_count() > 0);
        Ok(group
            .fibers
            .iter()
            .filter_map(Weak::upgrade)
            .filter(|fiber| !fiber.disposed.load(Ordering::Acquire))
            .collect())
    }

    pub(crate) fn finish_plugin_unmount(&self, key: PluginKey) {
        if let Ok(mut state) = self.state.lock() {
            state.plugin_index.remove(&key);
        }
    }

    pub(crate) fn allocate_isolation_label(&self) -> IsolationLabel {
        let label = self.runtime_token.allocate_label();
        if let Ok(mut state) = self.state.lock() {
            state.isolations_seen.insert(label.id(), ());
        }
        label
    }

    pub(crate) fn mark_dirty_public(self: &Arc<Self>) {
        self.mark_dirty();
    }

    pub(crate) fn resolve_with_id(
        &self,
        node: NodeId,
        key: ServiceId,
    ) -> Option<(u64, ErasedService)> {
        let state = self.state.lock().ok()?;
        resolve_provider(&state, node, key).map(|provider| (provider.id, provider.value.clone()))
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

    /// 启动唯一的响应式重算调度器；必须在 Tokio Runtime 内调用一次。
    pub(crate) fn start_scheduler(self: &Arc<Self>) -> Result<(), CoreError> {
        let handle =
            tokio::runtime::Handle::try_current().map_err(|_| CoreError::SchedulerUnavailable)?;
        if self.scheduler_stopped.load(Ordering::Acquire) {
            return Err(CoreError::SchedulerUnavailable);
        }
        if self.scheduler_started.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let registry = Arc::clone(self);
        let cancel = self.scheduler_cancel.clone();
        let task = handle.spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = registry.wake.notified() => {
                        if cancel.is_cancelled() {
                            break;
                        }
                        registry.clone().recompute().await;
                    }
                }
            }
        });
        if let Ok(mut slot) = self.scheduler_task.lock() {
            *slot = Some(task);
        } else {
            task.abort();
            self.scheduler_started.store(false, Ordering::Release);
            return Err(CoreError::SchedulerUnavailable);
        }
        Ok(())
    }

    /// 同步发出停止信号并取出调度器 Handle（幂等）。
    pub(crate) fn request_stop_scheduler(&self) -> Option<JoinHandle<()>> {
        self.scheduler_stopped.store(true, Ordering::Release);
        self.scheduler_cancel.cancel();
        self.wake.notify_one();
        self.scheduler_task
            .lock()
            .ok()
            .and_then(|mut slot| slot.take())
    }

    /// 停止调度器并等待其退出（幂等）。
    pub(crate) async fn stop_scheduler(self: &Arc<Self>) {
        if let Some(task) = self.request_stop_scheduler() {
            let _ = task.await;
        }
    }

    /// 析构路径：发出停止信号并 abort 调度器任务以释放 Registry 强引用。
    pub(crate) fn abort_scheduler(&self) {
        if let Some(task) = self.request_stop_scheduler() {
            task.abort();
        }
    }

    pub(crate) fn scheduler_stopped(&self) -> bool {
        self.scheduler_stopped.load(Ordering::Acquire)
    }

    pub(crate) fn allocate_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
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

    fn lock_event_contract(
        state: &mut RegistryState,
        event_id: &'static str,
        mode: EventMode,
        payload: TypeId,
        answer: Option<TypeId>,
    ) -> Result<(), CoreError> {
        match state.event_contracts.entry(event_id) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(EventContract {
                    mode,
                    payload,
                    answer,
                });
                Ok(())
            }
            std::collections::hash_map::Entry::Occupied(entry) => {
                let existing = entry.get();
                if existing.mode != mode {
                    return Err(CoreError::EventModeMismatch { event: event_id });
                }
                if existing.payload != payload {
                    return Err(CoreError::EventKeyTypeConflict { event: event_id });
                }
                if existing.answer != answer {
                    return Err(CoreError::EventAnswerTypeConflict { event: event_id });
                }
                Ok(())
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
        resolve_provider(&state, node, key).map(|provider| provider.value.clone())
    }

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

    pub(crate) fn subscribe_observe(
        self: &Arc<Self>,
        node: NodeId,
        event_id: &'static str,
        type_id: TypeId,
        handler: ObserveHandler,
        owner: &EffectScope,
        meta: ListenMeta,
    ) -> Result<ListenerId, CoreError> {
        self.subscribe_kind(
            node,
            event_id,
            EventMode::Observe,
            type_id,
            None,
            EventHandlerKind::Observe(handler),
            owner,
            meta,
        )
    }

    pub(crate) fn subscribe_waterfall(
        self: &Arc<Self>,
        node: NodeId,
        event_id: &'static str,
        type_id: TypeId,
        handler: WaterfallHandler,
        owner: &EffectScope,
        meta: ListenMeta,
    ) -> Result<ListenerId, CoreError> {
        self.subscribe_kind(
            node,
            event_id,
            EventMode::Waterfall,
            type_id,
            None,
            EventHandlerKind::Waterfall(handler),
            owner,
            meta,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn subscribe_serial(
        self: &Arc<Self>,
        node: NodeId,
        event_id: &'static str,
        payload: TypeId,
        answer: TypeId,
        handler: Arc<dyn SerialHandlerErased>,
        owner: &EffectScope,
        meta: ListenMeta,
    ) -> Result<ListenerId, CoreError> {
        self.subscribe_kind(
            node,
            event_id,
            EventMode::Serial,
            payload,
            Some(answer),
            EventHandlerKind::Serial(handler),
            owner,
            meta,
        )
    }

    pub(crate) fn subscribe_parallel(
        self: &Arc<Self>,
        node: NodeId,
        event_id: &'static str,
        type_id: TypeId,
        handler: Arc<dyn ParallelHandlerErased>,
        owner: &EffectScope,
        meta: ListenMeta,
    ) -> Result<ListenerId, CoreError> {
        self.subscribe_kind(
            node,
            event_id,
            EventMode::Parallel,
            type_id,
            None,
            EventHandlerKind::Parallel(handler),
            owner,
            meta,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn subscribe_kind(
        self: &Arc<Self>,
        node: NodeId,
        event_id: &'static str,
        mode: EventMode,
        payload: TypeId,
        answer: Option<TypeId>,
        kind: EventHandlerKind,
        owner: &EffectScope,
        meta: ListenMeta,
    ) -> Result<ListenerId, CoreError> {
        if owner.is_disposed() {
            return Err(CoreError::ContextDisposed);
        }
        let id = self.allocate_id();
        {
            let mut state = self.state.lock().map_err(|_| CoreError::ContextDisposed)?;
            Self::lock_event_contract(&mut state, event_id, mode, payload, answer)?;
            let list = state.listeners.entry(event_id).or_default();
            let listener = EventListener {
                id,
                node,
                kind,
                meta,
            };
            if listener.meta.prepend {
                list.insert(0, listener);
            } else {
                list.push(listener);
            }
        }
        let weak = Arc::downgrade(self);
        owner.on_dispose(move || {
            if let Some(registry) = weak.upgrade() {
                registry.unsubscribe_event(event_id, id);
            }
        });
        Ok(id)
    }

    pub(crate) fn unsubscribe_event(&self, event_id: &'static str, listener_id: ListenerId) {
        if let Ok(mut state) = self.state.lock()
            && let Some(listeners) = state.listeners.get_mut(event_id)
        {
            listeners.retain(|item| item.id != listener_id);
            if listeners.is_empty() {
                state.listeners.remove(event_id);
            }
        }
    }

    fn select_matching_listeners<'a>(
        state: &'a RegistryState,
        event_id: &'static str,
        emitter: NodeId,
        mode: EventMode,
    ) -> Result<Vec<&'a EventListener>, CoreError> {
        let Some(listeners) = state.listeners.get(event_id) else {
            return Ok(Vec::new());
        };
        let mut globals = Vec::new();
        let mut locals = Vec::new();
        for listener in listeners {
            let matches_mode = matches!(
                (&listener.kind, mode),
                (EventHandlerKind::Observe(_), EventMode::Observe)
                    | (EventHandlerKind::Waterfall(_), EventMode::Waterfall)
                    | (EventHandlerKind::Serial(_), EventMode::Serial)
                    | (EventHandlerKind::Parallel(_), EventMode::Parallel)
            );
            if !matches_mode {
                return Err(CoreError::EventModeMismatch { event: event_id });
            }
            if listener.meta.global {
                globals.push(listener);
            } else if is_ancestor_or_self(state, listener.node, emitter) {
                locals.push(listener);
            }
        }
        globals.extend(locals);
        Ok(globals)
    }

    fn apply_filter(
        filter: &Option<EventFilter>,
        payload: &(dyn Any + Send + Sync),
    ) -> Result<bool, CoreError> {
        match filter {
            None => Ok(true),
            Some(filter) => filter(payload).map_err(|error| match error {
                CoreError::EventListener(_) => error,
                other => CoreError::EventListener(other.to_string()),
            }),
        }
    }

    pub(crate) fn emit_event(
        &self,
        node: NodeId,
        event_id: &'static str,
        type_id: TypeId,
        payload: &(dyn Any + Send + Sync),
    ) -> Result<(), CoreError> {
        let selected = {
            let mut state = self.state.lock().map_err(|_| CoreError::ContextDisposed)?;
            Self::lock_event_contract(&mut state, event_id, EventMode::Observe, type_id, None)?;
            let matched =
                Self::select_matching_listeners(&state, event_id, node, EventMode::Observe)?;
            matched
                .into_iter()
                .map(|listener| {
                    let EventHandlerKind::Observe(handler) = &listener.kind else {
                        unreachable!();
                    };
                    (listener.id, handler.clone(), listener.meta.clone())
                })
                .collect::<Vec<_>>()
        };
        for (id, handler, meta) in selected {
            if !Self::apply_filter(&meta.filter, payload)? {
                continue;
            }
            if meta.once {
                if meta
                    .once_gate
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    continue;
                }
                self.unsubscribe_event(event_id, id);
            }
            handler(payload).map_err(|error| match error {
                CoreError::EventListener(_) => error,
                other => CoreError::EventListener(other.to_string()),
            })?;
        }
        Ok(())
    }

    pub(crate) async fn waterfall_event<T: Send + Sync + 'static>(
        self: &Arc<Self>,
        node: NodeId,
        event_id: &'static str,
        value: T,
    ) -> Result<T, CoreError> {
        let type_id = TypeId::of::<T>();
        let payload_box: Box<dyn Any + Send + Sync> = Box::new(value);
        let selected = {
            let mut state = self.state.lock().map_err(|_| CoreError::ContextDisposed)?;
            Self::lock_event_contract(&mut state, event_id, EventMode::Waterfall, type_id, None)?;
            let matched =
                Self::select_matching_listeners(&state, event_id, node, EventMode::Waterfall)?;
            matched
                .into_iter()
                .map(|listener| {
                    let EventHandlerKind::Waterfall(handler) = &listener.kind else {
                        unreachable!();
                    };
                    (listener.id, handler.clone(), listener.meta.clone())
                })
                .collect::<Vec<_>>()
        };

        let mut next: ErasedNext = Box::new(|boxed| Box::pin(async move { Ok(boxed) }));
        for (id, handler, meta) in selected.into_iter().rev() {
            let prev = next;
            let registry = self.clone();
            next = Box::new(move |boxed| {
                Box::pin(async move {
                    if !Registry::apply_filter(&meta.filter, boxed.as_ref())? {
                        return prev(boxed).await;
                    }
                    if meta.once
                        && meta
                            .once_gate
                            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                            .is_err()
                    {
                        return prev(boxed).await;
                    }
                    if meta.once {
                        registry.unsubscribe_event(event_id, id);
                    }
                    handler(boxed, prev).await
                })
            });
        }
        let boxed = next(payload_box).await?;
        boxed
            .downcast::<T>()
            .map(|value| *value)
            .map_err(|_| CoreError::EventTypeMismatch { event: event_id })
    }

    pub(crate) async fn serial_event<T: Send + Sync + 'static, R: Send + Sync + 'static>(
        &self,
        node: NodeId,
        event_id: &'static str,
        payload: &T,
    ) -> Result<Option<R>, CoreError> {
        let selected = {
            let mut state = self.state.lock().map_err(|_| CoreError::ContextDisposed)?;
            Self::lock_event_contract(
                &mut state,
                event_id,
                EventMode::Serial,
                TypeId::of::<T>(),
                Some(TypeId::of::<R>()),
            )?;
            let matched =
                Self::select_matching_listeners(&state, event_id, node, EventMode::Serial)?;
            matched
                .into_iter()
                .map(|listener| {
                    let EventHandlerKind::Serial(handler) = &listener.kind else {
                        unreachable!();
                    };
                    (listener.id, handler.clone(), listener.meta.clone())
                })
                .collect::<Vec<_>>()
        };
        for (id, handler, meta) in selected {
            if !Self::apply_filter(&meta.filter, payload)? {
                continue;
            }
            if meta.once {
                if meta
                    .once_gate
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    continue;
                }
                self.unsubscribe_event(event_id, id);
            }
            match handler.invoke(payload).await.map_err(|error| match error {
                CoreError::EventListener(_) => error,
                other => CoreError::EventListener(other.to_string()),
            })? {
                Some(boxed) => {
                    let answer = boxed
                        .downcast::<R>()
                        .map_err(|_| CoreError::EventTypeMismatch { event: event_id })?;
                    return Ok(Some(*answer));
                }
                None => continue,
            }
        }
        Ok(None)
    }

    pub(crate) async fn parallel_event(
        &self,
        node: NodeId,
        event_id: &'static str,
        type_id: TypeId,
        payload: &(dyn Any + Send + Sync),
    ) -> Result<(), CoreError> {
        let selected = {
            let mut state = self.state.lock().map_err(|_| CoreError::ContextDisposed)?;
            Self::lock_event_contract(&mut state, event_id, EventMode::Parallel, type_id, None)?;
            let matched =
                Self::select_matching_listeners(&state, event_id, node, EventMode::Parallel)?;
            matched
                .into_iter()
                .map(|listener| {
                    let EventHandlerKind::Parallel(handler) = &listener.kind else {
                        unreachable!();
                    };
                    (listener.id, handler.clone(), listener.meta.clone())
                })
                .collect::<Vec<_>>()
        };
        let mut eligible = Vec::new();
        for (id, handler, meta) in selected {
            if Self::apply_filter(&meta.filter, payload)? {
                eligible.push((id, handler, meta));
            }
        }

        let mut handlers = Vec::new();
        for (id, handler, meta) in eligible {
            if meta.once {
                if meta
                    .once_gate
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    continue;
                }
                self.unsubscribe_event(event_id, id);
            }
            handlers.push(handler);
        }
        let results = join_all(handlers.iter().map(|handler| handler.invoke(payload))).await;
        let errors = results
            .into_iter()
            .filter_map(|result| match result {
                Ok(()) => None,
                Err(CoreError::EventListener(message)) => Some(message),
                Err(other) => Some(other.to_string()),
            })
            .collect::<Vec<_>>();
        if errors.is_empty() {
            Ok(())
        } else {
            Err(CoreError::ParallelDispatchFailed {
                event: event_id,
                errors,
            })
        }
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

    pub(crate) async fn settle(self: &Arc<Self>) {
        loop {
            // 先登记 waiter，再检查状态，避免丢失 notify_waiters。
            let notified = self.quiescent.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if !self.dirty.load(Ordering::Acquire) && !self.recomputing.load(Ordering::Acquire) {
                let _guard = self.recompute_lock.lock().await;
                if !self.dirty.load(Ordering::Acquire) && !self.recomputing.load(Ordering::Acquire)
                {
                    return;
                }
            }
            if self.scheduler_stopped.load(Ordering::Acquire) {
                let _guard = self.recompute_lock.lock().await;
                if !self.dirty.load(Ordering::Acquire) && !self.recomputing.load(Ordering::Acquire)
                {
                    return;
                }
                drop(_guard);
                self.clone().recompute().await;
                continue;
            }
            self.mark_dirty();
            notified.await;
        }
    }

    fn mark_dirty(self: &Arc<Self>) {
        self.dirty.store(true, Ordering::Release);
        if self.scheduler_stopped.load(Ordering::Acquire) {
            return;
        }
        self.wake.notify_one();
    }

    async fn recompute(self: Arc<Self>) {
        let _guard = self.recompute_lock.lock().await;
        self.recomputing.store(true, Ordering::Release);
        loop {
            if !self.dirty.swap(false, Ordering::AcqRel) {
                break;
            }
            loop {
                let ready = self.take_ready();
                let ready_plugins = self.take_ready_plugin_fibers().await;
                if ready.is_empty() && ready_plugins.is_empty() {
                    break;
                }
                for injection in ready {
                    self.run_injection(injection).await;
                }
                for fiber in ready_plugins {
                    self.run_plugin_fiber(fiber).await;
                }
            }
        }
        self.recomputing.store(false, Ordering::Release);
        self.quiescent.notify_waiters();
        if self.dirty.load(Ordering::Acquire) && !self.scheduler_stopped.load(Ordering::Acquire) {
            self.wake.notify_one();
        }
    }

    fn take_ready(&self) -> Vec<InjectionId> {
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

    async fn run_injection(self: &Arc<Self>, id: InjectionId) {
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

    fn snapshot_injection(
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

    async fn take_ready_plugin_fibers(&self) -> Vec<Arc<FiberInner>> {
        let fibers = {
            let Ok(state) = self.state.lock() else {
                return Vec::new();
            };
            state
                .plugin_fibers
                .values()
                .filter_map(Weak::upgrade)
                .filter(|fiber| !fiber.disposed.load(Ordering::Acquire))
                .collect::<Vec<_>>()
        };

        let mut ready = Vec::new();
        for fiber in fibers {
            if fiber.disposed.load(Ordering::Acquire) {
                continue;
            }
            if fiber.busy.lock().map(|guard| *guard).unwrap_or(true) {
                continue;
            }
            let deps = fiber.dependencies.lock().expect("deps").clone();
            let resolved = fiber.resolved_providers.lock().expect("providers").clone();
            let providers = {
                let Ok(state) = self.state.lock() else {
                    continue;
                };
                provider_ids(&state, fiber.node, &deps)
            };
            let state = *fiber.state.lock().expect("state");
            if state == FiberState::Active && resolved != providers {
                let _ = fiber.unload_to_pending_wait().await;
            }
            if fiber.busy.lock().map(|guard| *guard).unwrap_or(true) {
                continue;
            }
            let deps = fiber.dependencies.lock().expect("deps").clone();
            let providers = {
                let Ok(state) = self.state.lock() else {
                    continue;
                };
                provider_ids(&state, fiber.node, &deps)
            };
            let state = *fiber.state.lock().expect("state");
            let deps_ready = providers.len() == deps.len();
            if state == FiberState::Pending && deps_ready {
                ready.push(fiber);
            }
        }
        ready
    }

    async fn run_plugin_fiber(self: &Arc<Self>, fiber: Arc<FiberInner>) {
        // Failed is stored on the fiber; Pending Ok is intentional when deps race.
        let _ = fiber.try_activate().await;
    }
}

fn lookup_isolation(state: &RegistryState, start: NodeId, key: ServiceId) -> Option<u64> {
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

fn resolve_provider(
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

fn provider_ids(state: &RegistryState, node: NodeId, keys: &[ServiceId]) -> Vec<u64> {
    keys.iter()
        .filter_map(|key| resolve_provider(state, node, *key).map(|provider| provider.id))
        .collect()
}

fn is_ancestor_or_self(state: &RegistryState, maybe_ancestor: NodeId, node: NodeId) -> bool {
    let mut current = Some(node);
    while let Some(id) = current {
        if id == maybe_ancestor {
            return true;
        }
        current = state.nodes.get(&id).and_then(|item| item.parent);
    }
    false
}
