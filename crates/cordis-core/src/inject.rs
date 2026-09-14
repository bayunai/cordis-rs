use crate::{
    CoreError, ServiceId, Services,
    diagnostics::{
        ContextSnapshot, FiberSnapshot, InjectionPhaseSnapshot, ProviderSnapshot, RuntimeSnapshot,
    },
    effect::EffectScope,
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
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub(crate) type NodeId = u64;
pub(crate) type InjectionId = u64;
pub(crate) type PluginId = u64;
pub(crate) type ListenerId = u64;

type InjectFuture = Pin<Box<dyn Future<Output = Result<(), CoreError>> + Send>>;
pub(crate) type InjectCallback = Arc<dyn Fn(Services, EffectScope) -> InjectFuture + Send + Sync>;

type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send>>;
type ObserveHandler = Arc<dyn Fn(&(dyn Any + Send + Sync)) -> Result<(), CoreError> + Send + Sync>;
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
}

struct ProviderRecord {
    id: u64,
    value: ErasedService,
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
}

struct PluginRecord {
    id: PluginId,
    node: NodeId,
}

struct RegistryState {
    nodes: HashMap<NodeId, NodeRecord>,
    providers: HashMap<(NodeId, ServiceId), ProviderRecord>,
    injections: HashMap<InjectionId, InjectionRecord>,
    listeners: HashMap<&'static str, Vec<EventListener>>,
    plugins: HashMap<PluginId, PluginRecord>,
    service_types: HashMap<ServiceId, TypeId>,
    event_contracts: HashMap<&'static str, EventContract>,
}

pub(crate) struct Registry {
    state: Mutex<RegistryState>,
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
}

impl Registry {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(RegistryState {
                nodes: HashMap::new(),
                providers: HashMap::new(),
                injections: HashMap::new(),
                listeners: HashMap::new(),
                plugins: HashMap::new(),
                service_types: HashMap::new(),
                event_contracts: HashMap::new(),
            }),
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
        })
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

    pub(crate) fn add_node(self: &Arc<Self>, id: NodeId, parent: Option<NodeId>) {
        if let Ok(mut state) = self.state.lock() {
            state.nodes.insert(id, NodeRecord { parent });
        }
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
            state.providers.retain(|(node, _), _| *node != id);
            state.plugins.retain(|_, plugin| plugin.node != id);
            for listeners in state.listeners.values_mut() {
                listeners.retain(|listener| listener.node != id);
            }
            state.listeners.retain(|_, listeners| !listeners.is_empty());
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
            let slot = (node, key);
            if state.providers.contains_key(&slot) {
                return Err(CoreError::ServiceConflict { service: key });
            }
            state.providers.insert(
                slot,
                ProviderRecord {
                    id: provider_id,
                    value,
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

    pub(crate) fn register_plugin(
        self: &Arc<Self>,
        node: NodeId,
        owner: &EffectScope,
    ) -> Result<PluginId, CoreError> {
        if owner.is_disposed() {
            return Err(CoreError::ContextDisposed);
        }
        let id = self.allocate_id();
        {
            let mut state = self.state.lock().map_err(|_| CoreError::ContextDisposed)?;
            state.plugins.insert(id, PluginRecord { id, node });
        }
        let weak = Arc::downgrade(self);
        owner.on_dispose(move || {
            if let Some(registry) = weak.upgrade()
                && let Ok(mut state) = registry.state.lock()
            {
                state.plugins.remove(&id);
            }
        });
        Ok(id)
    }

    pub(crate) fn subscribe_observe(
        self: &Arc<Self>,
        node: NodeId,
        event_id: &'static str,
        type_id: TypeId,
        handler: ObserveHandler,
        owner: &EffectScope,
    ) -> Result<ListenerId, CoreError> {
        self.subscribe_kind(
            node,
            event_id,
            EventMode::Observe,
            type_id,
            None,
            EventHandlerKind::Observe(handler),
            owner,
        )
    }

    pub(crate) fn subscribe_waterfall(
        self: &Arc<Self>,
        node: NodeId,
        event_id: &'static str,
        type_id: TypeId,
        handler: WaterfallHandler,
        owner: &EffectScope,
    ) -> Result<ListenerId, CoreError> {
        self.subscribe_kind(
            node,
            event_id,
            EventMode::Waterfall,
            type_id,
            None,
            EventHandlerKind::Waterfall(handler),
            owner,
        )
    }

    pub(crate) fn subscribe_serial(
        self: &Arc<Self>,
        node: NodeId,
        event_id: &'static str,
        payload: TypeId,
        answer: TypeId,
        handler: Arc<dyn SerialHandlerErased>,
        owner: &EffectScope,
    ) -> Result<ListenerId, CoreError> {
        self.subscribe_kind(
            node,
            event_id,
            EventMode::Serial,
            payload,
            Some(answer),
            EventHandlerKind::Serial(handler),
            owner,
        )
    }

    pub(crate) fn subscribe_parallel(
        self: &Arc<Self>,
        node: NodeId,
        event_id: &'static str,
        type_id: TypeId,
        handler: Arc<dyn ParallelHandlerErased>,
        owner: &EffectScope,
    ) -> Result<ListenerId, CoreError> {
        self.subscribe_kind(
            node,
            event_id,
            EventMode::Parallel,
            type_id,
            None,
            EventHandlerKind::Parallel(handler),
            owner,
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
    ) -> Result<ListenerId, CoreError> {
        if owner.is_disposed() {
            return Err(CoreError::ContextDisposed);
        }
        let id = self.allocate_id();
        {
            let mut state = self.state.lock().map_err(|_| CoreError::ContextDisposed)?;
            Self::lock_event_contract(&mut state, event_id, mode, payload, answer)?;
            state
                .listeners
                .entry(event_id)
                .or_default()
                .push(EventListener { id, node, kind });
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

    pub(crate) fn emit_event(
        &self,
        node: NodeId,
        event_id: &'static str,
        type_id: TypeId,
        payload: &(dyn Any + Send + Sync),
    ) -> Result<(), CoreError> {
        let handlers = {
            let mut state = self.state.lock().map_err(|_| CoreError::ContextDisposed)?;
            Self::lock_event_contract(&mut state, event_id, EventMode::Observe, type_id, None)?;
            let Some(listeners) = state.listeners.get(event_id) else {
                return Ok(());
            };
            let mut selected = Vec::new();
            for listener in listeners {
                if !is_ancestor_or_self(&state, listener.node, node) {
                    continue;
                }
                match &listener.kind {
                    EventHandlerKind::Observe(handler) => selected.push(handler.clone()),
                    _ => return Err(CoreError::EventModeMismatch { event: event_id }),
                }
            }
            selected
        };
        for handler in handlers {
            handler(payload).map_err(|error| match error {
                CoreError::EventListener(_) => error,
                other => CoreError::EventListener(other.to_string()),
            })?;
        }
        Ok(())
    }

    pub(crate) async fn waterfall_event<T: Send + Sync + 'static>(
        &self,
        node: NodeId,
        event_id: &'static str,
        value: T,
    ) -> Result<T, CoreError> {
        let type_id = TypeId::of::<T>();
        let handlers = {
            let mut state = self.state.lock().map_err(|_| CoreError::ContextDisposed)?;
            Self::lock_event_contract(&mut state, event_id, EventMode::Waterfall, type_id, None)?;
            let Some(listeners) = state.listeners.get(event_id) else {
                return Ok(value);
            };
            let mut selected = Vec::new();
            for listener in listeners {
                if !is_ancestor_or_self(&state, listener.node, node) {
                    continue;
                }
                match &listener.kind {
                    EventHandlerKind::Waterfall(handler) => selected.push(handler.clone()),
                    _ => return Err(CoreError::EventModeMismatch { event: event_id }),
                }
            }
            selected
        };

        let mut next: ErasedNext = Box::new(|boxed| Box::pin(async move { Ok(boxed) }));
        for handler in handlers.into_iter().rev() {
            let prev = next;
            next = Box::new(move |boxed| Box::pin(async move { handler(boxed, prev).await }));
        }
        let boxed = next(Box::new(value)).await?;
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
        let handlers = {
            let mut state = self.state.lock().map_err(|_| CoreError::ContextDisposed)?;
            Self::lock_event_contract(
                &mut state,
                event_id,
                EventMode::Serial,
                TypeId::of::<T>(),
                Some(TypeId::of::<R>()),
            )?;
            let Some(listeners) = state.listeners.get(event_id) else {
                return Ok(None);
            };
            let mut selected = Vec::new();
            for listener in listeners {
                if !is_ancestor_or_self(&state, listener.node, node) {
                    continue;
                }
                match &listener.kind {
                    EventHandlerKind::Serial(handler) => selected.push(handler.clone()),
                    _ => return Err(CoreError::EventModeMismatch { event: event_id }),
                }
            }
            selected
        };
        for handler in handlers {
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
        let handlers = {
            let mut state = self.state.lock().map_err(|_| CoreError::ContextDisposed)?;
            Self::lock_event_contract(&mut state, event_id, EventMode::Parallel, type_id, None)?;
            let Some(listeners) = state.listeners.get(event_id) else {
                return Ok(());
            };
            let mut selected = Vec::new();
            for listener in listeners {
                if !is_ancestor_or_self(&state, listener.node, node) {
                    continue;
                }
                match &listener.kind {
                    EventHandlerKind::Parallel(handler) => selected.push(handler.clone()),
                    _ => return Err(CoreError::EventModeMismatch { event: event_id }),
                }
            }
            selected
        };
        let results = join_all(handlers.iter().map(|handler| handler.invoke(payload))).await;
        let errors: Vec<String> = results
            .into_iter()
            .filter_map(|result| match result {
                Ok(()) => None,
                Err(CoreError::EventListener(message)) => Some(message),
                Err(other) => Some(other.to_string()),
            })
            .collect();
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
            })
            .collect();
        let providers = state
            .providers
            .iter()
            .map(|((node, service), provider)| ProviderSnapshot {
                node: *node,
                service: service.as_str(),
                provider_id: provider.id,
            })
            .collect();
        let fibers = state
            .injections
            .iter()
            .map(|(id, injection)| {
                let missing = injection
                    .dependencies
                    .iter()
                    .filter(|key| resolve_provider(&state, injection.node, **key).is_none())
                    .map(|key| key.as_str())
                    .collect::<Vec<_>>();
                FiberSnapshot {
                    id: *id,
                    node: injection.node,
                    phase: InjectionPhaseSnapshot::from(injection.phase),
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
        let plugins = state
            .plugins
            .values()
            .map(|plugin| crate::diagnostics::PluginSnapshot {
                id: plugin.id,
                node: plugin.node,
            })
            .collect();
        RuntimeSnapshot {
            contexts,
            providers,
            fibers,
            plugins,
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
                if ready.is_empty() {
                    break;
                }
                for injection in ready {
                    self.run_injection(injection).await;
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
}

fn resolve_provider(
    state: &RegistryState,
    start: NodeId,
    key: ServiceId,
) -> Option<&ProviderRecord> {
    let mut node = Some(start);
    while let Some(current) = node {
        if let Some(provider) = state.providers.get(&(current, key)) {
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
