//! 按 PluginDefinitionId 归组、挂载索引与统一卸载。
//!
//! 维护同 Key 的 Fiber 索引与 unmount 流程；Plugin 契约定义在 `plugin/mod`。

use crate::{
    CoreError,
    fiber::{FiberInner, FiberStateChange},
    plugin::{PluginDefinition, PluginDefinitionId},
    registry::Registry,
};
use std::sync::{Arc, Mutex};
use tokio::{
    sync::{Notify, broadcast},
    task::JoinHandle,
};

/// 单次按 Key 卸载的共享完成结果。
///
/// 协调器独立于首个 `Runtime::unmount()` 调用者；等待者取消不影响实际释放。
pub(crate) struct UnmountCompletion {
    notify: Notify,
    result: Mutex<Option<Result<usize, CoreError>>>,
    retain: Mutex<Option<JoinHandle<()>>>,
}

impl UnmountCompletion {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            notify: Notify::new(),
            result: Mutex::new(None),
            retain: Mutex::new(None),
        })
    }

    pub(crate) fn attach_coordinator(&self, handle: JoinHandle<()>) {
        *self.retain.lock().expect("unmount retain") = Some(handle);
    }

    pub(crate) fn finish(&self, result: Result<usize, CoreError>) {
        {
            let mut slot = self.result.lock().expect("unmount completion");
            if slot.is_some() {
                return;
            }
            *slot = Some(result);
        }
        self.notify.notify_waiters();
    }

    pub(crate) async fn wait(self: &Arc<Self>) -> Result<usize, CoreError> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(result) = self.result.lock().expect("unmount completion").as_ref() {
                return result.clone();
            }
            notified.await;
        }
    }
}

pub(crate) struct DefinitionGroup {
    pub(crate) definition: PluginDefinition,
    pub(crate) fibers: Vec<u64>,
    pub(crate) unmounting: bool,
    pub(crate) unmount_completion: Option<Arc<UnmountCompletion>>,
}

impl Registry {
    pub(crate) fn subscribe_fiber_states(&self) -> broadcast::Receiver<FiberStateChange> {
        self.fiber_state_events.subscribe()
    }

    pub(crate) fn publish_fiber_state(&self, change: FiberStateChange) {
        let _ = self.fiber_state_events.send(change);
    }

    pub(crate) fn register_fiber(
        self: &Arc<Self>,
        fiber: Arc<FiberInner>,
    ) -> Result<(), CoreError> {
        let key = fiber.definition.id();
        let mut state = self.state.lock().map_err(|_| CoreError::ContextDisposed)?;
        let weak = Arc::downgrade(&fiber);
        // Lock order is Registry then parent resources, matching diagnostics.
        fiber.parent_scope.register_owned(
            || {
                if state
                    .plugin_index
                    .get(&key)
                    .is_some_and(|group| group.unmounting)
                {
                    return Err(CoreError::PluginUnmounting { plugin: key });
                }
                state.fibers.insert(fiber.id, fiber.clone());
                let group = state
                    .plugin_index
                    .entry(key)
                    .or_insert_with(|| DefinitionGroup {
                        definition: fiber.definition.clone(),
                        fibers: Vec::new(),
                        unmounting: false,
                        unmount_completion: None,
                    });
                group.fibers.push(fiber.id);
                let dependencies = fiber.dependencies.lock().expect("dependencies");
                let unavailable_dependencies =
                    crate::registry::dependency_diagnostics(&state, &fiber.context, &dependencies)
                        .1;
                self.publish_fiber_state(FiberStateChange {
                    fiber_id: fiber.id,
                    definition_id: key,
                    parent_fiber_id: fiber.parent_fiber_id,
                    previous: None,
                    current: crate::FiberState::Pending,
                    unavailable_dependencies,
                });
                Ok(())
            },
            move || {
                if let Some(fiber) = weak.upgrade() {
                    fiber.dispose_now();
                }
            },
        )
    }

    pub(crate) fn unregister_fiber(&self, id: u64) {
        if let Ok(mut state) = self.state.lock() {
            let key = state.fibers.get(&id).map(|fiber| fiber.definition.id());
            state.fibers.remove(&id);
            if let Some(key) = key
                && let Some(group) = state.plugin_index.get_mut(&key)
            {
                group.fibers.retain(|fiber_id| *fiber_id != id);
                if group.fibers.is_empty() && !group.unmounting {
                    state.plugin_index.remove(&key);
                }
            }
        }
    }

    pub(crate) fn fibers_for_definition(
        &self,
        key: PluginDefinitionId,
    ) -> Result<Vec<Arc<FiberInner>>, CoreError> {
        let state = self.state.lock().map_err(|_| CoreError::ContextDisposed)?;
        let Some(group) = state.plugin_index.get(&key) else {
            return Ok(Vec::new());
        };
        Ok(group
            .fibers
            .iter()
            .filter_map(|id| state.fibers.get(id).cloned())
            .collect())
    }

    pub(crate) fn begin_plugin_unmount(
        &self,
        key: PluginDefinitionId,
    ) -> Result<(Vec<Arc<FiberInner>>, Arc<UnmountCompletion>), CoreError> {
        let mut state = self.state.lock().map_err(|_| CoreError::ContextDisposed)?;
        let Some(group) = state.plugin_index.get_mut(&key) else {
            let completion = UnmountCompletion::new();
            completion.finish(Ok(0));
            return Ok((Vec::new(), completion));
        };
        if group.unmounting {
            return Err(CoreError::PluginUnmounting { plugin: key });
        }
        group.unmounting = true;
        let completion = UnmountCompletion::new();
        group.unmount_completion = Some(completion.clone());
        let ids = group.fibers.clone();
        let fibers = ids
            .iter()
            .filter_map(|id| state.fibers.get(id).cloned())
            .filter(|fiber| fiber.needs_unmount_wait())
            .collect();
        Ok((fibers, completion))
    }

    pub(crate) fn finish_plugin_unmount(
        &self,
        key: PluginDefinitionId,
        completion: &Arc<UnmountCompletion>,
    ) {
        if let Ok(mut state) = self.state.lock()
            && state.plugin_index.get(&key).is_some_and(|group| {
                group
                    .unmount_completion
                    .as_ref()
                    .is_some_and(|current| Arc::ptr_eq(current, completion))
            })
        {
            state.plugin_index.remove(&key);
        }
    }
}
