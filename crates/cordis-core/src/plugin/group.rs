//! 按 PluginKey 归组、挂载索引与统一卸载。
//!
//! 维护同 Key 的 Fiber 索引与 unmount 流程；Plugin 契约定义在 `plugin/mod`。

use crate::{
    CoreError,
    fiber::{FiberInner, FiberStateChange},
    plugin::PluginKey,
    registry::Registry,
};
use std::sync::{Arc, Weak, atomic::Ordering};
use tokio::sync::broadcast;

pub(crate) struct PluginGroup {
    pub(crate) fibers: Vec<Weak<FiberInner>>,
    pub(crate) unmounting: bool,
}

impl Registry {
    pub(crate) fn subscribe_fiber_states(&self) -> broadcast::Receiver<FiberStateChange> {
        self.fiber_state_events.subscribe()
    }

    pub(crate) fn publish_fiber_state(&self, change: FiberStateChange) {
        let _ = self.fiber_state_events.send(change);
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
}
