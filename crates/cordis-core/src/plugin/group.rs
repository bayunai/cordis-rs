//! 按 PluginKey 归组、挂载索引与统一卸载。
//!
//! 维护同 Key 的 Fiber 索引与 unmount 流程；Plugin 契约定义在 `plugin/mod`。

use crate::{
    CoreError,
    fiber::{FiberInner, FiberStateChange},
    plugin::PluginKey,
    registry::Registry,
};
use std::sync::{Arc, Mutex, Weak};
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

pub(crate) struct PluginGroup {
    pub(crate) fibers: Vec<Weak<FiberInner>>,
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
                    unmount_completion: None,
                });
            group.fibers.push(Arc::downgrade(&fiber));
        }
        // 此处仅完成分组占位与卸载冲突检查。Fiber 保持 Preparing 时 scheduler
        // 不会调度它；首次生命周期完成后由 `finish_lifecycle()` 统一标脏。
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

    pub(crate) fn plugin_fibers_for_key(
        &self,
        key: PluginKey,
    ) -> Result<Vec<Arc<FiberInner>>, CoreError> {
        let state = self.state.lock().map_err(|_| CoreError::ContextDisposed)?;
        let Some(group) = state.plugin_index.get(&key) else {
            return Ok(Vec::new());
        };
        Ok(group.fibers.iter().filter_map(Weak::upgrade).collect())
    }

    pub(crate) fn begin_plugin_unmount(
        &self,
        key: PluginKey,
    ) -> Result<(Vec<Arc<FiberInner>>, Arc<UnmountCompletion>), CoreError> {
        let mut state = self.state.lock().map_err(|_| CoreError::ContextDisposed)?;
        let group = state
            .plugin_index
            .entry(key)
            .or_insert_with(|| PluginGroup {
                fibers: Vec::new(),
                unmounting: false,
                unmount_completion: None,
            });
        if group.unmounting {
            return Err(CoreError::PluginUnmounting { plugin: key });
        }
        group.unmounting = true;
        let completion = UnmountCompletion::new();
        group.unmount_completion = Some(completion.clone());
        group.fibers.retain(|weak| weak.strong_count() > 0);
        let fibers = group
            .fibers
            .iter()
            .filter_map(Weak::upgrade)
            .filter(|fiber| fiber.needs_unmount_wait())
            .collect();
        Ok((fibers, completion))
    }

    pub(crate) fn finish_plugin_unmount(
        &self,
        key: PluginKey,
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
