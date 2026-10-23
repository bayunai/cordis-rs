//! Settings 能力：分层表单投影；写回委托 ConfigEditor 的 user 层。

use crate::form::{self, ConfigOperation, EntryConfigView};
use async_trait::async_trait;
use cordis_core::{Context, CoreError, Plugin, PluginKey, ServiceId, ServiceKey};
use cordis_loader::{
    ConfigRevision, LOADER, Loader, LoaderControlError, LoaderEvent,
};
use cordis_plugin_config_editor::{
    CONFIG_EDITOR, ConfigEditability, ConfigEditorService, ConfigReadOnlyReason,
};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

/// Settings 服务 Key。
pub static SETTINGS: ServiceKey<SettingsService> = ServiceKey::new("cordis.settings");

/// Host 注入的设置服务配置。
#[derive(Clone, Debug)]
pub struct SettingsConfig {
    /// 根 Include 条目完整路径，默认 `include`。
    pub include_root: String,
}

impl Default for SettingsConfig {
    fn default() -> Self {
        Self {
            include_root: "include".into(),
        }
    }
}

impl SettingsConfig {
    pub fn new(include_root: impl Into<String>) -> Self {
        Self {
            include_root: include_root.into(),
        }
    }
}

/// 可表单配置的条目目录项（轻量；完整投影用 `describe`）。
#[derive(Clone, Debug)]
pub struct SettingsCatalogEntry {
    pub path: String,
    pub factory: String,
    pub editable: bool,
    pub read_only_reason: Option<ConfigReadOnlyReason>,
}

/// 表单目录或配置投影已失效；订阅者须重新 `list` / `describe`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettingsEvent {
    /// Loader 快照已变；`revision` 来自 `LoaderEvent::SnapshotChanged`。
    Changed { revision: u64 },
    /// 订阅落后，中间事件已丢；revision 未知，须完整刷新（勿信任此前版本）。
    Resync,
}

/// 将 Loader 订阅结果映射为 Settings 事件；`Closed` 返回 `None`（结束转发）。
pub(crate) fn event_from_loader_recv(
    result: Result<LoaderEvent, broadcast::error::RecvError>,
) -> Option<SettingsEvent> {
    match result {
        Ok(LoaderEvent::SnapshotChanged { revision }) => Some(SettingsEvent::Changed { revision }),
        Err(broadcast::error::RecvError::Lagged(_)) => Some(SettingsEvent::Resync),
        Err(broadcast::error::RecvError::Closed) => None,
    }
}

/// 管理界面表单投影；提交经 ConfigEditor user 层，不 bake Include path 源文件。
#[derive(Clone)]
pub struct SettingsService {
    loader: Loader,
    editor: ConfigEditorService,
    include_root: String,
    events: Arc<Mutex<Option<broadcast::Sender<SettingsEvent>>>>,
}

impl SettingsService {
    /// 根 Include 下可表单配置的条目目录（不含 Group）。
    pub async fn list(&self) -> Result<Vec<SettingsCatalogEntry>, LoaderControlError> {
        self.ensure_alive()?;
        let snapshot = self.loader.await_idle().await?;
        let prefix = format!("{}:", self.include_root);
        let mut out = Vec::new();
        for entry in &snapshot.entries {
            let path = entry.path.as_str();
            if path == self.include_root || !path.starts_with(&prefix) || entry.group {
                continue;
            }
            let (editable, read_only_reason) = match self.editor.editability(path).await? {
                ConfigEditability::Editable => (true, None),
                ConfigEditability::ReadOnly(reason) => (false, Some(reason)),
            };
            out.push(SettingsCatalogEntry {
                path: path.to_owned(),
                factory: entry.name.clone(),
                editable,
                read_only_reason,
            });
        }
        Ok(out)
    }

    /// 安全分层配置描述；不要求 Fiber Active。能描述不代表允许 `mutate`。
    pub async fn describe(
        &self,
        path: impl AsRef<str>,
    ) -> Result<EntryConfigView, LoaderControlError> {
        self.ensure_alive()?;
        let path = path.as_ref();
        let layers = self.editor.layers(path).await?;
        let editability = self.editor.editability(path).await?;
        form::view_from_layers(&layers, editability)
    }

    /// 有序字段编辑；写在 user 覆盖上，`expected` 必须来自最近一次 `describe`。
    pub async fn mutate(
        &self,
        path: impl AsRef<str>,
        expected: ConfigRevision,
        operations: Vec<ConfigOperation>,
    ) -> Result<EntryConfigView, LoaderControlError> {
        self.ensure_alive()?;
        let path = path.as_ref();
        if !matches!(
            self.editor.editability(path).await?,
            ConfigEditability::Editable
        ) {
            return Err(LoaderControlError::ConfigNotEditable);
        }
        let layers = self.editor.layers(path).await?;
        if expected != layers.revision {
            return Err(LoaderControlError::ConfigRevisionConflict);
        }
        let mut config = layers.effective;
        for operation in operations {
            form::apply_operation(&mut config, &layers.base, operation)?;
        }
        form::view_from_layers(
            &cordis_plugin_config_editor::LayeredEntryConfig {
                path: layers.path.clone(),
                factory: layers.factory.clone(),
                schema: layers.schema.clone(),
                resolved_schema: layers.resolved_schema.clone(),
                base: layers.base.clone(),
                user: Some(config.clone()),
                effective: config.clone(),
                resolved: None,
                revision: layers.revision,
            },
            ConfigEditability::Editable,
        )?;
        let committed = self.editor.edit(path, expected, config).await?;
        form::view_from_layers(&committed, ConfigEditability::Editable)
    }

    /// 订阅本插件实例共享的失效事件；不创建任何订阅专属任务。
    pub fn subscribe(&self) -> Result<broadcast::Receiver<SettingsEvent>, LoaderControlError> {
        self.events
            .lock()
            .expect("settings events")
            .as_ref()
            .map(broadcast::Sender::subscribe)
            .ok_or(LoaderControlError::LoaderUnavailable)
    }

    /// 根 Include 路径。
    pub fn include_root(&self) -> &str {
        &self.include_root
    }

    fn ensure_alive(&self) -> Result<(), LoaderControlError> {
        if self.events.lock().expect("settings events").is_some() {
            Ok(())
        } else {
            Err(LoaderControlError::LoaderUnavailable)
        }
    }
}

/// Host 显式挂载的设置插件；须在 ConfigEditor 之后挂载。
pub struct SettingsPlugin {
    config: SettingsConfig,
}

impl SettingsPlugin {
    pub fn new(config: SettingsConfig) -> Self {
        Self { config }
    }
}

impl Default for SettingsPlugin {
    fn default() -> Self {
        Self::new(SettingsConfig::default())
    }
}

#[async_trait]
impl Plugin for SettingsPlugin {
    fn key(&self) -> PluginKey {
        PluginKey::new("cordis.settings")
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
            released_events.lock().expect("settings events").take();
        });
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
                if tx.send(event).is_err() {
                    continue;
                }
            }
        })?;
        ctx.provide(
            SETTINGS,
            SettingsService {
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
            Some(SettingsEvent::Changed { revision: 7 })
        );
        assert_eq!(
            event_from_loader_recv(Err(broadcast::error::RecvError::Lagged(3))),
            Some(SettingsEvent::Resync)
        );
        assert_eq!(
            event_from_loader_recv(Err(broadcast::error::RecvError::Closed)),
            None
        );
    }
}
