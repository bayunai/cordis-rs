//! 独立文件配置树：读取、覆盖、来源目录、版本检查与原子写回由 Include 拥有。
//! Loader 只认识 [`cordis_loader::EntryTreeSource`]，不解析格式或读取文件。
//! 显式 reload 由子树控制面调用；本插件不安装文件监听器。

mod codec;
mod config;
mod patches;
mod root;
mod source;

pub use codec::read_extensions_file;
pub use config::{EntryPatch, IncludeConfig};
pub use patches::apply_patches;
pub use root::{include_root_entry, load_extensions};
pub use source::{FileSource, INCLUDE_LOCATION, IncludeLocation};

use cordis_core::PluginKey;
use cordis_loader::{ExtensionFactory, LoaderError, PreparedExtension};

/// 文件来源树的显式 Catalog Factory；不隐式安装到任意 Host。
pub struct IncludeFactory;

impl ExtensionFactory for IncludeFactory {
    type Config = IncludeConfig;
    fn id(&self) -> &'static str {
        "cordis:include"
    }
    fn prepare(&self, config: IncludeConfig) -> Result<PreparedExtension, LoaderError> {
        config
            .validate()
            .map_err(|error| LoaderError::invalid_config(error.to_string()))?;
        Ok(PreparedExtension::tree(
            PluginKey::new("cordis.include"),
            config.path.clone(),
            config,
            |_, config, ctx| async move { FileSource::from_config(config, &ctx).await },
        ))
    }
}
