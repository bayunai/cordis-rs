//! ConfigEditor：user 层持久化与受信任整表读写。

use crate::{
    layers::{ConfigEditorConfig, LayerEngine, LayeredEntryConfig},
    user_patch::UserPatchStore,
};
use async_trait::async_trait;
use cordis_core::{Context, CoreError, Plugin, PluginKey, ServiceId, ServiceKey};
use cordis_loader::{ConfigRevision, EntryConfigSnapshot, LOADER, Loader, LoaderControlError};
use std::sync::Arc;

/// ConfigEditor 服务 Key。
pub static CONFIG_EDITOR: ServiceKey<ConfigEditorService> = ServiceKey::new("cordis.config-editor");

/// 受信任配置编辑：user patch 为表单默认写路径；整表 commit 仅供运维。
#[derive(Clone)]
pub struct ConfigEditorService {
    loader: Loader,
    engine: Arc<LayerEngine>,
}

impl ConfigEditorService {
    /// 是否具有本编辑器拥有的 deploy 声明；嵌套来源和未登记条目不冒充可编辑。
    pub async fn can_edit(&self, path: impl AsRef<str>) -> Result<bool, LoaderControlError> {
        self.engine.can_edit(path.as_ref()).await
    }
    /// 分层读取（base=deploy / user / effective=deploy∘user∘home）；可含明文秘密。
    pub async fn layers(
        &self,
        path: impl AsRef<str>,
    ) -> Result<LayeredEntryConfig, LoaderControlError> {
        self.engine.layers(path.as_ref()).await
    }

    /// 受信任读取当前有效配置（含 user/home）；可含明文秘密。
    pub async fn read(
        &self,
        path: impl AsRef<str>,
    ) -> Result<EntryConfigSnapshot, LoaderControlError> {
        self.loader.snapshot_entry_config(path).await
    }

    /// 更新 user 覆盖并 reconcile；不写空根或 deploy 文件。
    ///
    /// `config` 为下一份 **effective** 整表；若与 deploy base 相等则清除该条目的 user 覆盖。
    /// 若 home 盖住导致生效值不等于 `config`，拒绝写入。
    pub async fn edit(
        &self,
        path: impl AsRef<str>,
        expected: ConfigRevision,
        config: toml::Value,
    ) -> Result<LayeredEntryConfig, LoaderControlError> {
        self.engine.edit_user(path.as_ref(), expected, config).await
    }

    /// 启停写入 user patch 的 `disabled`；与 deploy base 一致时清除该覆盖。
    pub async fn set_enabled(
        &self,
        path: impl AsRef<str>,
        enabled: bool,
    ) -> Result<(), LoaderControlError> {
        self.engine.set_enabled(path.as_ref(), enabled).await
    }

    /// 受信任整表写回来源树（可能 bake 进 Include path）；表单路径不得使用。
    pub async fn commit_source(
        &self,
        path: impl AsRef<str>,
        expected: ConfigRevision,
        config: toml::Value,
    ) -> Result<EntryConfigSnapshot, LoaderControlError> {
        self.loader
            .commit_entry_config(path, expected, config)
            .await
    }

    /// user patch 文档路径。
    pub fn user_patch_path(&self) -> &std::path::Path {
        self.engine.store.path()
    }
}

/// Host 显式挂载的配置编辑器；须在 Loader 与根 Include 之后挂载。
pub struct ConfigEditorPlugin {
    config: ConfigEditorConfig,
}

impl ConfigEditorPlugin {
    pub fn new(config: ConfigEditorConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl Plugin for ConfigEditorPlugin {
    fn key(&self) -> PluginKey {
        PluginKey::new("cordis.config-editor")
    }

    fn inject(&self) -> Vec<ServiceId> {
        vec![LOADER.id()]
    }

    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        let loader = (*ctx.get(LOADER)?).clone();
        let store = Arc::new(UserPatchStore::new(self.config.user_patch_path.clone()));
        let engine = Arc::new(LayerEngine {
            loader: loader.clone(),
            store,
            include_root: self.config.include_root.clone(),
            deploy_patch_paths: self.config.deploy_patch_paths.clone(),
            home_patch_path: self.config.home_patch_path.clone(),
        });
        // 先提供服务，再 sync：deploy insert 可能立刻挂载依赖 CONFIG_EDITOR 的插件。
        ctx.provide(
            CONFIG_EDITOR,
            ConfigEditorService {
                loader,
                engine: engine.clone(),
            },
        )?;
        engine
            .sync_patches()
            .await
            .map_err(|error| CoreError::PluginApply(error.to_string()))?;
        Ok(())
    }
}
