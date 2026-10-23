//! Settings 能力：分层表单投影；写回委托 ConfigEditor 的 user 层。

use crate::form::{self, ConfigOperation, EntryConfigView};
use async_trait::async_trait;
use cordis_core::{Context, CoreError, Plugin, PluginKey, ServiceId, ServiceKey};
use cordis_loader::{ConfigRevision, LoaderControlError};
use cordis_plugin_config_editor::{CONFIG_EDITOR, ConfigEditorService};

/// Settings 服务 Key。
pub static SETTINGS: ServiceKey<SettingsService> = ServiceKey::new("cordis.settings");

/// 管理界面表单投影；提交经 ConfigEditor user 层，不 bake Include path 源文件。
#[derive(Clone)]
pub struct SettingsService {
    editor: ConfigEditorService,
}

impl SettingsService {
    /// 安全分层配置描述；不要求 Fiber Active。
    pub async fn describe(
        &self,
        path: impl AsRef<str>,
    ) -> Result<EntryConfigView, LoaderControlError> {
        let layers = self.editor.layers(path).await?;
        form::view_from_layers(&layers)
    }

    /// 有序字段编辑；写在 user 覆盖上，`expected` 必须来自最近一次 `describe`。
    pub async fn mutate(
        &self,
        path: impl AsRef<str>,
        expected: ConfigRevision,
        operations: Vec<ConfigOperation>,
    ) -> Result<EntryConfigView, LoaderControlError> {
        let path = path.as_ref();
        let layers = self.editor.layers(path).await?;
        if expected != layers.revision {
            return Err(LoaderControlError::ConfigRevisionConflict);
        }
        let mut config = layers.effective;
        for operation in operations {
            form::apply_operation(&mut config, operation)?;
        }
        form::view_from_layers(&cordis_plugin_config_editor::LayeredEntryConfig {
            path: layers.path.clone(),
            factory: layers.factory.clone(),
            schema: layers.schema.clone(),
            base: layers.base.clone(),
            user: Some(config.clone()),
            effective: config.clone(),
            revision: layers.revision,
        })?;
        let committed = self.editor.edit(path, expected, config).await?;
        form::view_from_layers(&committed)
    }
}

/// Host 显式挂载的设置插件；须在 ConfigEditor 之后挂载。
pub struct SettingsPlugin;

#[async_trait]
impl Plugin for SettingsPlugin {
    fn key(&self) -> PluginKey {
        PluginKey::new("cordis.settings")
    }

    fn inject(&self) -> Vec<ServiceId> {
        vec![CONFIG_EDITOR.id()]
    }

    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        let editor = (*ctx.get(CONFIG_EDITOR)?).clone();
        ctx.provide(SETTINGS, SettingsService { editor })?;
        Ok(())
    }
}
