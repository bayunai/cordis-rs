//! Settings 能力：分层表单投影；写回委托 ConfigEditor 的 user 层。

use crate::form::{self, ConfigOperation, EntryConfigView};
use async_trait::async_trait;
use cordis_core::{Context, CoreError, Plugin, PluginKey, ServiceId, ServiceKey};
use cordis_loader::{ConfigRevision, LOADER, LoaderControlError, LoaderEvent};
use cordis_plugin_config_editor::{
    CONFIG_EDITOR, ConfigEditability, ConfigEditorService, ConfigReadOnlyReason,
};
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

/// Settings 服务 Key。
pub static SETTINGS: ServiceKey<SettingsService> = ServiceKey::new("cordis.settings");

/// 可表单配置的条目目录项（轻量；完整投影用 `describe`）。
#[derive(Clone, Debug)]
pub struct SettingsCatalogEntry {
    pub path: String,
    pub factory: String,
    pub editable: bool,
    pub read_only_reason: Option<ConfigReadOnlyReason>,
}

/// 管理界面表单投影；提交经 ConfigEditor user 层，不 bake Include path 源文件。
#[derive(Clone)]
pub struct SettingsService {
    editor: ConfigEditorService,
    /// `None` 表示插件已卸载。
    revision: Arc<Mutex<Option<watch::Sender<u64>>>>,
}

impl SettingsService {
    /// 根 Include 下可表单配置的条目目录（不含 Group）；来自 ConfigEditor 同事务库存。
    pub async fn list(&self) -> Result<Vec<SettingsCatalogEntry>, LoaderControlError> {
        self.ensure_alive()?;
        let mut out = Vec::new();
        for entry in self.editor.managed_entries().await? {
            if entry.group {
                continue;
            }
            let (editable, read_only_reason) = match entry.editability {
                ConfigEditability::Editable => (true, None),
                ConfigEditability::ReadOnly(reason) => (false, Some(reason)),
            };
            out.push(SettingsCatalogEntry {
                path: entry.path,
                factory: entry.factory,
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
        form::view_from_layers(&self.editor.layers(path).await?)
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
        let layers = self.editor.layers(path).await?;
        if !matches!(layers.editability, ConfigEditability::Editable) {
            return Err(LoaderControlError::ConfigNotEditable);
        }
        if expected != layers.revision {
            return Err(LoaderControlError::ConfigRevisionConflict);
        }
        let mut config = layers.effective;
        for operation in operations {
            // Unset 只使用 raw `base`，不写入 Serde 默认。
            form::apply_operation(&mut config, &layers.base, operation)?;
        }
        form::view_from_layers(&cordis_plugin_config_editor::LayeredEntryConfig {
            path: layers.path.clone(),
            factory: layers.factory.clone(),
            schema: layers.schema.clone(),
            resolved_schema: layers.resolved_schema.clone(),
            base: layers.base.clone(),
            inherited: layers.inherited.clone(),
            user: Some(config.clone()),
            effective: config.clone(),
            resolved: None,
            editability: ConfigEditability::Editable,
            revision: layers.revision,
        })?;
        let committed = self.editor.edit(path, expected, config).await?;
        form::view_from_layers(&committed)
    }

    /// 订阅本服务实例的失效代次；不是 Loader revision，也不能用于配置写入。
    /// 初值为零，每次收到变更或检测到上游事件缺失时推进；无订阅者也保留最新值。
    /// 消费者只需比较是否变化并完整刷新，不依赖事件数量；重挂实例重新计数。
    pub fn subscribe(&self) -> Result<watch::Receiver<u64>, LoaderControlError> {
        self.revision
            .lock()
            .expect("settings revision")
            .as_ref()
            .map(|tx| tx.subscribe())
            .ok_or(LoaderControlError::LoaderUnavailable)
    }

    /// 编辑范围根路径（来自 ConfigEditor）。
    pub fn include_root(&self) -> &str {
        self.editor.include_root()
    }

    fn ensure_alive(&self) -> Result<(), LoaderControlError> {
        if self.revision.lock().expect("settings revision").is_some() {
            Ok(())
        } else {
            Err(LoaderControlError::LoaderUnavailable)
        }
    }
}

/// Host 显式挂载的设置插件；须在 ConfigEditor 之后挂载。
#[derive(Default)]
pub struct SettingsPlugin;

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
        let (tx, _) = watch::channel(0u64);
        let revision = Arc::new(Mutex::new(Some(tx.clone())));
        let effect = ctx.effect()?;
        let released = revision.clone();
        effect.on_dispose(move || {
            released.lock().expect("settings revision").take();
        });
        effect.spawn(move |cancel| async move {
            loop {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => break,
                    result = loader_rx.recv() => match result {
                        Ok(LoaderEvent::SnapshotChanged { .. })
                        | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            // 独立失效代次，不混用上游 revision；即使暂无页面也保存变更。
                            tx.send_modify(|value| *value = value.wrapping_add(1));
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    },
                };
            }
        })?;
        ctx.provide(SETTINGS, SettingsService { editor, revision })?;
        Ok(())
    }
}
