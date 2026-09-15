//! 监听器元数据与 once / prepend / filter。
//!
//! 负责注册、合约锁定与匹配选择；实际并行/串行/瀑布派发见 `dispatch`。

use crate::{
    Context, CoreError,
    effect::EffectScope,
    registry::{ListenerId, Registry, RegistryState},
};
use async_trait::async_trait;
use std::{
    any::{Any, TypeId},
    future::Future,
    pin::Pin,
    sync::{Arc, atomic::AtomicBool},
};

type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send>>;
type ObserveHandler = Arc<dyn Fn(&(dyn Any + Send + Sync)) -> Result<(), CoreError> + Send + Sync>;
pub(crate) type EventFilter =
    Arc<dyn Fn(&(dyn Any + Send + Sync)) -> Result<bool, CoreError> + Send + Sync>;
pub(crate) type ErasedNext = Box<
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
pub(crate) struct EventContract {
    pub(crate) mode: EventMode,
    pub(crate) payload: TypeId,
    pub(crate) answer: Option<TypeId>,
}

pub(crate) enum EventHandlerKind {
    Observe(ObserveHandler),
    Waterfall(WaterfallHandler),
    Serial(Arc<dyn SerialHandlerErased>),
    Parallel(Arc<dyn ParallelHandlerErased>),
}

pub(crate) struct EventListener {
    pub(crate) id: ListenerId,
    pub(crate) context: Context,
    pub(crate) kind: EventHandlerKind,
    pub(crate) meta: ListenMeta,
}

impl Registry {
    pub(crate) fn subscribe_observe(
        self: &Arc<Self>,
        context: Context,
        event_id: &'static str,
        type_id: TypeId,
        handler: ObserveHandler,
        owner: &EffectScope,
        meta: ListenMeta,
    ) -> Result<ListenerId, CoreError> {
        self.subscribe_kind(
            context,
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
        context: Context,
        event_id: &'static str,
        type_id: TypeId,
        handler: WaterfallHandler,
        owner: &EffectScope,
        meta: ListenMeta,
    ) -> Result<ListenerId, CoreError> {
        self.subscribe_kind(
            context,
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
        context: Context,
        event_id: &'static str,
        payload: TypeId,
        answer: TypeId,
        handler: Arc<dyn SerialHandlerErased>,
        owner: &EffectScope,
        meta: ListenMeta,
    ) -> Result<ListenerId, CoreError> {
        self.subscribe_kind(
            context,
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
        context: Context,
        event_id: &'static str,
        type_id: TypeId,
        handler: Arc<dyn ParallelHandlerErased>,
        owner: &EffectScope,
        meta: ListenMeta,
    ) -> Result<ListenerId, CoreError> {
        self.subscribe_kind(
            context,
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
        context: Context,
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
                context,
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

    pub(crate) fn select_matching_listeners<'a>(
        state: &'a RegistryState,
        event_id: &'static str,
        emitter: &Context,
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
            } else if emitter.is_descendant_of(&listener.context) {
                locals.push(listener);
            }
        }
        globals.extend(locals);
        Ok(globals)
    }

    pub(crate) fn apply_filter(
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

    pub(crate) fn lock_event_contract(
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
}
