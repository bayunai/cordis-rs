//! 并行 / 串行 / 瀑布分发。
//!
//! 执行已选中的监听器；事件不控制、不回滚 Core 生命周期。

use crate::{
    CoreError,
    registry::{NodeId, Registry},
};
use futures_util::future::join_all;
use std::{
    any::{Any, TypeId},
    sync::{Arc, atomic::Ordering},
};

use super::listener::{ErasedNext, EventHandlerKind, EventMode};

impl Registry {
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
}
