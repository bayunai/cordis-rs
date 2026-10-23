//! PluginManager：Entry 库存与启停；委托 ConfigEditor user 层。

use async_trait::async_trait;
use cordis_core::{Context, CoreError, FiberState, Plugin, PluginKey, ServiceId, ServiceKey};
use cordis_loader::{LOADER, Loader, LoaderControlError, LoaderEvent};
use cordis_plugin_config_editor::{
    CONFIG_EDITOR, ConfigEditability, ConfigEditorService, ConfigReadOnlyReason,
};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

/// PluginManager 服务 Key。
pub static PLUGIN_MANAGER: ServiceKey<PluginManagerService> =
    ServiceKey::new("cordis.plugin-manager");

/// 与 ConfigEditor 共用的只读原因（对齐 DSH `readOnlyReason`）。
pub type ReadOnlyReason = ConfigReadOnlyReason;

/// Host 注入的管理器配置。
#[derive(Clone, Debug)]
pub struct PluginManagerConfig {
    /// 根 Include 条目完整路径，默认 `include`。
    pub include_root: String,
}

impl Default for PluginManagerConfig {
    fn default() -> Self {
        Self {
            include_root: "include".into(),
        }
    }
}

impl PluginManagerConfig {
    pub fn new(include_root: impl Into<String>) -> Self {
        Self {
            include_root: include_root.into(),
        }
    }
}

/// 根 Include 下一条可管理（或只读列出）的 Entry。
#[derive(Clone, Debug)]
pub struct PluginInfo {
    pub path: String,
    pub factory: String,
    pub group: bool,
    pub enabled: bool,
    pub state: Option<FiberState>,
    pub last_error: Option<String>,
    pub read_only_reason: Option<ReadOnlyReason>,
}

/// 库存已失效；订阅者须重新 `list_plugins`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginManagerEvent {
    /// Loader 快照已变；`revision` 来自 `LoaderEvent::SnapshotChanged`。
    Changed { revision: u64 },
    /// 订阅落后，中间事件已丢；revision 未知，须完整刷新（勿信任此前版本）。
    Resync,
}

/// 将 Loader 订阅结果映射为 PluginManager 事件；`Closed` 返回 `None`（结束转发）。
pub(crate) fn event_from_loader_recv(
    result: Result<LoaderEvent, broadcast::error::RecvError>,
) -> Option<PluginManagerEvent> {
    match result {
        Ok(LoaderEvent::SnapshotChanged { revision }) => {
            Some(PluginManagerEvent::Changed { revision })
        }
        Err(broadcast::error::RecvError::Lagged(_)) => Some(PluginManagerEvent::Resync),
        Err(broadcast::error::RecvError::Closed) => None,
    }
}

/// 插件库存与启停门面。
#[derive(Clone)]
pub struct PluginManagerService {
    loader: Loader,
    editor: ConfigEditorService,
    include_root: String,
    events: Arc<Mutex<Option<broadcast::Sender<PluginManagerEvent>>>>,
}

impl PluginManagerService {
    /// 根 Include 下业务 Entry 库存（含 Group；Group 带 `read_only_reason`）。
    pub async fn list_plugins(&self) -> Result<Vec<PluginInfo>, LoaderControlError> {
        self.ensure_alive()?;
        let snapshot = self.loader.await_idle().await?;
        let prefix = format!("{}:", self.include_root);
        let mut out = Vec::new();
        for entry in &snapshot.entries {
            let path = entry.path.as_str();
            if path == self.include_root || !path.starts_with(&prefix) {
                continue;
            }
            // 树快照的 group 标志优先；其余与 ConfigEditor.editability 同源。
            let read_only_reason = if entry.group {
                Some(ConfigReadOnlyReason::Group)
            } else {
                match self.editor.editability(path).await? {
                    ConfigEditability::Editable => None,
                    ConfigEditability::ReadOnly(reason) => Some(reason),
                }
            };
            out.push(PluginInfo {
                path: path.to_owned(),
                factory: entry.name.clone(),
                group: entry.group,
                enabled: entry.enabled,
                state: entry.state,
                last_error: entry.last_error.clone(),
                read_only_reason,
            });
        }
        Ok(out)
    }

    /// 持久化启停并 reconcile（写 user patch）。
    pub async fn set_plugin_enabled(
        &self,
        path: impl AsRef<str>,
        enabled: bool,
    ) -> Result<(), LoaderControlError> {
        self.ensure_alive()?;
        let path = path.as_ref();
        let prefix = format!("{}:", self.include_root);
        if path == self.include_root || !path.starts_with(&prefix) {
            return Err(LoaderControlError::EntryNotFound {
                path: path.to_owned(),
            });
        }
        if !self.editor.can_edit(path).await? {
            return Err(LoaderControlError::ConfigNotEditable);
        }
        self.editor.set_enabled(path, enabled).await
    }

    /// 订阅插件实例共享的失效事件；不创建任何订阅专属任务。
    pub fn subscribe(&self) -> Result<broadcast::Receiver<PluginManagerEvent>, LoaderControlError> {
        self.events
            .lock()
            .expect("manager events")
            .as_ref()
            .map(broadcast::Sender::subscribe)
            .ok_or(LoaderControlError::LoaderUnavailable)
    }

    fn ensure_alive(&self) -> Result<(), LoaderControlError> {
        if self.events.lock().expect("manager events").is_some() {
            Ok(())
        } else {
            Err(LoaderControlError::LoaderUnavailable)
        }
    }

    /// 根 Include 路径。
    pub fn include_root(&self) -> &str {
        &self.include_root
    }
}

/// Host 显式挂载的插件管理器；须在 ConfigEditor 之后挂载。
pub struct PluginManagerPlugin {
    config: PluginManagerConfig,
}

impl PluginManagerPlugin {
    pub fn new(config: PluginManagerConfig) -> Self {
        Self { config }
    }
}

impl Default for PluginManagerPlugin {
    fn default() -> Self {
        Self::new(PluginManagerConfig::default())
    }
}

#[async_trait]
impl Plugin for PluginManagerPlugin {
    fn key(&self) -> PluginKey {
        PluginKey::new("cordis.plugin-manager")
    }

    fn inject(&self) -> Vec<ServiceId> {
        vec![LOADER.id(), CONFIG_EDITOR.id()]
    }

    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        let loader = (*ctx.get(LOADER)?).clone();
        let editor = (*ctx.get(CONFIG_EDITOR)?).clone();
        let mut loader_rx = loader
            .subscribe()
            .map_err(|error| CoreError::PluginApply(error.to_string()))?;
        let (tx, _) = broadcast::channel(64);
        let events = Arc::new(Mutex::new(Some(tx.clone())));
        let effect = ctx.effect()?;
        let released_events = events.clone();
        effect.on_dispose(move || {
            released_events.lock().expect("manager events").take();
        });
        // 每个插件实例仅有一个受管转发任务。无订阅者是正常状态，不应结束该实例。
        effect.spawn(move |cancel| async move {
            loop {
                let event = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => break,
                    result = loader_rx.recv() => match event_from_loader_recv(result) {
                        Some(event) => event,
                        None => break,
                    },
                };
                // send 仅在当前没有订阅者时失败；事件本身是有损失效通知。
                if tx.send(event).is_err() {
                    continue;
                }
            }
        })?;
        ctx.provide(
            PLUGIN_MANAGER,
            PluginManagerService {
                loader,
                editor,
                include_root: self.config.include_root.clone(),
                events,
            },
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loader_recv_maps_changed_resync_and_closed() {
        assert_eq!(
            event_from_loader_recv(Ok(LoaderEvent::SnapshotChanged { revision: 7 })),
            Some(PluginManagerEvent::Changed { revision: 7 })
        );
        assert_eq!(
            event_from_loader_recv(Err(broadcast::error::RecvError::Lagged(3))),
            Some(PluginManagerEvent::Resync)
        );
        assert_eq!(
            event_from_loader_recv(Err(broadcast::error::RecvError::Closed)),
            None
        );
    }
}
