//! 静态 Include：以独立 TOML 文件驱动的嵌套 EntryTree。
//!
//! Host 须显式 `catalog.register(IncludeFactory)`；首版仅 TOML + 显式
//! [`LoaderSubtree::reload`](cordis_loader::LoaderSubtree::reload)，无文件监听。
//!
//! 管理子树请用 [`Loader::subtree`](cordis_loader::Loader::subtree)（路径为 Include 条目完整路径）。

use async_trait::async_trait;
use cordis_core::{Context, CoreError, Plugin, PluginKey};
use cordis_loader::{ENTRY_LOCATION, ExtensionFactory, LoaderError};
use schemars::JsonSchema;
use serde::Deserialize;
use std::{path::PathBuf, sync::Arc};

/// Include 工厂配置：指向完整 v3 `ExtensionsConfig` TOML。
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct IncludeConfig {
    /// 相对路径相对于承载 Include Entry 的配置文件目录；绝对路径直接使用。
    pub path: PathBuf,
}

/// Catalog Factory ID：`cordis:include`。
pub struct IncludeFactory;

impl ExtensionFactory for IncludeFactory {
    type Config = IncludeConfig;

    fn id(&self) -> &'static str {
        "cordis:include"
    }

    fn build(&self, config: IncludeConfig) -> Result<Arc<dyn Plugin>, LoaderError> {
        Ok(Arc::new(IncludePlugin { path: config.path }))
    }
}

struct IncludePlugin {
    path: PathBuf,
}

#[async_trait]
impl Plugin for IncludePlugin {
    fn key(&self) -> PluginKey {
        PluginKey::new("cordis.include")
    }

    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        let location = ctx
            .config(ENTRY_LOCATION)
            .map_err(|error| CoreError::PluginApply(error.to_string()))?;
        let subtree = location
            .loader
            .attach_file_subtree(ctx, &self.path)
            .await
            .map_err(|error| CoreError::PluginApply(error.to_string()))?;
        ctx.effect_named("include-subtree")?
            .on_dispose_async(move || {
                let subtree = subtree.clone();
                async move {
                    subtree
                        .detach()
                        .await
                        .map_err(|error| CoreError::PluginApply(error.to_string()))
                }
            })?;
        Ok(())
    }
}
