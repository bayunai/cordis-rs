//! PluginManager：Entry 库存与启停；委托 ConfigEditor user 层。

use async_trait::async_trait;
use cordis_core::{Context, CoreError, FiberState, Plugin, PluginKey, ServiceId, ServiceKey};
use cordis_loader::{LOADER, Loader, LoaderControlError, LoaderEvent};
use cordis_plugin_config_editor::{CONFIG_EDITOR, ConfigEditorService};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

/// PluginManager 服务 Key。
pub static PLUGIN_MANAGER: ServiceKey<PluginManagerService> =
    ServiceKey::new("cordis.plugin-manager");

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

/// 条目不可经 PluginManager 启停的原因（对齐 DSH `readOnlyReason`）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadOnlyReason {
    /// Group 仅组织树，不接受启停。
    Group,
    /// 嵌套 Include 载体；启停由树 CRUD / 重载路径处理。
    NestedInclude,
    /// 不属于 ConfigEditor 可编辑的 deploy 条目（含嵌套来源内的业务条目）。
    UnmanagedEntry,
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
    Changed { revision: u64 },
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
            let read_only_reason = if entry.group {
                Some(ReadOnlyReason::Group)
            } else if entry.name == "cordis:include" {
                Some(ReadOnlyReason::NestedInclude)
            } else if !self.editor.can_edit(path).await? {
                Some(ReadOnlyReason::UnmanagedEntry)
            } else {
                None
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
        let snapshot = self.loader.await_idle().await?;
        let entry = snapshot
            .entry(path)
            .ok_or_else(|| LoaderControlError::EntryNotFound {
                path: path.to_owned(),
            })?;
        if entry.group {
            return Err(LoaderControlError::ConfigNotEditable);
        }
        if entry.name == "cordis:include" {
            return Err(LoaderControlError::ConfigNotEditable);
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
                    result = loader_rx.recv() => match result {
                        Ok(LoaderEvent::SnapshotChanged { revision }) => {
                            PluginManagerEvent::Changed { revision }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            PluginManagerEvent::Changed { revision: 0 }
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
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
