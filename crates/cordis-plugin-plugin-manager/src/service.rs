//! PluginManager：Entry 库存与启停；委托 ConfigEditor user 层。

use async_trait::async_trait;
use cordis_core::{Context, CoreError, FiberState, Plugin, PluginKey, ServiceId, ServiceKey};
use cordis_loader::{LOADER, LoaderControlError, LoaderEvent};
use cordis_plugin_config_editor::{
    CONFIG_EDITOR, ConfigEditability, ConfigEditorService, ConfigReadOnlyReason,
};
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

/// PluginManager 服务 Key。
pub static PLUGIN_MANAGER: ServiceKey<PluginManagerService> =
    ServiceKey::new("cordis.plugin-manager");

/// 与 ConfigEditor 共用的只读原因（对齐 DSH `readOnlyReason`）。
pub type ReadOnlyReason = ConfigReadOnlyReason;

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

/// 插件库存与启停门面。
#[derive(Clone)]
pub struct PluginManagerService {
    editor: ConfigEditorService,
    revision: Arc<Mutex<Option<watch::Sender<u64>>>>,
}

impl PluginManagerService {
    /// 根 Include 下业务 Entry 库存（含 Group）；能力与状态来自 ConfigEditor 同事务目录。
    pub async fn list_plugins(&self) -> Result<Vec<PluginInfo>, LoaderControlError> {
        self.ensure_alive()?;
        Ok(self
            .editor
            .managed_entries()
            .await?
            .into_iter()
            .map(|entry| {
                let read_only_reason = match entry.editability {
                    ConfigEditability::Editable => None,
                    ConfigEditability::ReadOnly(reason) => Some(reason),
                };
                PluginInfo {
                    path: entry.path,
                    factory: entry.factory,
                    group: entry.group,
                    enabled: entry.enabled,
                    state: entry.state,
                    last_error: entry.last_error,
                    read_only_reason,
                }
            })
            .collect())
    }

    /// 持久化启停并 reconcile（写 user patch）。
    pub async fn set_plugin_enabled(
        &self,
        path: impl AsRef<str>,
        enabled: bool,
    ) -> Result<(), LoaderControlError> {
        self.ensure_alive()?;
        let path = path.as_ref();
        let root = self.editor.include_root();
        let prefix = format!("{root}:");
        if path == root || !path.starts_with(&prefix) {
            return Err(LoaderControlError::EntryNotFound {
                path: path.to_owned(),
            });
        }
        if !self.editor.can_edit(path).await? {
            return Err(LoaderControlError::ConfigNotEditable);
        }
        self.editor.set_enabled(path, enabled).await
    }

    /// 订阅本服务实例的失效代次；不是 Loader revision，也不能用于配置写入。
    /// 初值为零，每次收到变更或检测到上游事件缺失时推进；无订阅者也保留最新值。
    /// 消费者只需比较是否变化并完整刷新，不依赖事件数量；重挂实例重新计数。
    pub fn subscribe(&self) -> Result<watch::Receiver<u64>, LoaderControlError> {
        self.revision
            .lock()
            .expect("manager revision")
            .as_ref()
            .map(|tx| tx.subscribe())
            .ok_or(LoaderControlError::LoaderUnavailable)
    }

    fn ensure_alive(&self) -> Result<(), LoaderControlError> {
        if self.revision.lock().expect("manager revision").is_some() {
            Ok(())
        } else {
            Err(LoaderControlError::LoaderUnavailable)
        }
    }

    /// 编辑范围根路径（来自 ConfigEditor）。
    pub fn include_root(&self) -> &str {
        self.editor.include_root()
    }
}

/// Host 显式挂载的插件管理器；须在 ConfigEditor 之后挂载。
#[derive(Default)]
pub struct PluginManagerPlugin;

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
        let (tx, _) = watch::channel(0u64);
        let revision = Arc::new(Mutex::new(Some(tx.clone())));
        let effect = ctx.effect()?;
        let released = revision.clone();
        effect.on_dispose(move || {
            released.lock().expect("manager revision").take();
        });
        effect.spawn(move |cancel| async move {
            loop {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => break,
                    result = loader_rx.recv() => match result {
                        Ok(LoaderEvent::SnapshotChanged { .. })
                        | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            tx.send_modify(|value| *value = value.wrapping_add(1));
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    },
                };
            }
        })?;
        ctx.provide(PLUGIN_MANAGER, PluginManagerService { editor, revision })?;
        Ok(())
    }
}
