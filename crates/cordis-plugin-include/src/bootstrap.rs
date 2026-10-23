use crate::{IncludeConfig, codec::Format};
use cordis_loader::{CONFIG_VERSION, EntryOptions, ExtensionsConfig, SourceError, SourceErrorKind};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::{Path, PathBuf},
};

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BootstrapConfig {
    pub version: u32,
    pub config: BootstrapConfigSource,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BootstrapConfigSource {
    pub driver: ConfigDriver,
    pub path: PathBuf,
}
#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ConfigDriver {
    File,
}

/// 启动锚点只由文件插件解释；根 Loader 不再读取 bootstrap 或文件来源。
pub fn load_bootstrap(path: impl AsRef<Path>) -> Result<(BootstrapConfig, PathBuf), SourceError> {
    let path = path.as_ref();
    let absolute = path
        .canonicalize()
        .map_err(|_| SourceError::new(SourceErrorKind::Io, "无法读取 Include 启动锚点"))?;
    let text = fs::read_to_string(&absolute)
        .map_err(|_| SourceError::new(SourceErrorKind::Io, "无法读取 Include 启动锚点"))?;
    let config: BootstrapConfig = toml::from_str(&text)
        .map_err(|_| SourceError::new(SourceErrorKind::InvalidData, "Include 启动锚点结构无效"))?;
    if config.version != CONFIG_VERSION {
        return Err(SourceError::new(
            SourceErrorKind::InvalidData,
            "Include 启动锚点要求 version = 3",
        ));
    }
    let resolved = if config.config.path.is_absolute() {
        config.config.path.clone()
    } else {
        absolute
            .parent()
            .ok_or_else(|| {
                SourceError::new(SourceErrorKind::InvalidData, "Include 启动锚点没有父目录")
            })?
            .join(&config.config.path)
    };
    Format::for_path(&resolved)?;
    Ok((config, resolved))
}
/// 由应用显式指定根 Include ID；例如 `include`，框架不保留特定名称。
/// 文件内条目的全局路径以该 ID 为前缀，不提供无前缀别名。
pub fn bootstrap_entry(
    id: impl Into<String>,
    bootstrap_path: impl AsRef<Path>,
) -> Result<EntryOptions, SourceError> {
    let (_, path) = load_bootstrap(bootstrap_path)?;
    let config = toml::Value::try_from(IncludeConfig::new(path))
        .map_err(|_| SourceError::new(SourceErrorKind::InvalidData, "无法表示 Include 启动配置"))?;
    Ok(EntryOptions::new(id, "cordis:include").with_config(config))
}
/// 只读工具入口，按扩展名读取完整 v3 文档，不应用另外的运行时 patches。
pub fn load_extensions(path: impl AsRef<Path>) -> Result<ExtensionsConfig, SourceError> {
    let path = path.as_ref();
    let bytes = fs::read(path)
        .map_err(|_| SourceError::new(SourceErrorKind::Io, "无法读取 Include 配置文档"))?;
    Format::for_path(path)?.parse(&bytes)
}
