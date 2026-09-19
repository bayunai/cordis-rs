//! 严格 TOML 配置合同：`bootstrap.toml` 与 `extensions.toml`。

use crate::{catalog::ExtensionCatalog, error::LoaderError};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, path::Path};

/// 当前唯一支持的配置版本。
pub const CONFIG_VERSION: u32 = 1;

/// 启动锚点。不得承载插件业务配置。
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BootstrapConfig {
    pub version: u32,
    pub config: BootstrapConfigSource,
}

/// 运行期主配置来源。首版仅实现 `file`。
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BootstrapConfigSource {
    pub driver: ConfigDriver,
    pub path: std::path::PathBuf,
}

/// 配置存储驱动。未知取值由 serde 直接失败。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ConfigDriver {
    File,
}

/// 运行期扩展清单。
///
/// `extensions` 必填：缺少该字段直接失败；有意清空须显式写 `extensions = []`。
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExtensionsConfig {
    pub version: u32,
    pub extensions: Vec<ExtensionEntry>,
}

/// 单个扩展实例。
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExtensionEntry {
    pub instance: String,
    pub factory: String,
    pub enabled: bool,
    #[serde(default = "empty_table")]
    pub config: toml::Value,
}

fn empty_table() -> toml::Value {
    toml::Value::Table(toml::Table::new())
}

pub(crate) fn parse_toml<T>(text: &str, path: &Path) -> Result<T, LoaderError>
where
    T: serde::de::DeserializeOwned,
{
    toml::from_str(text).map_err(|source| LoaderError::toml(path.to_path_buf(), source))
}

impl BootstrapConfig {
    /// 从 TOML 文本解析；未知字段与缺字段直接失败。
    pub fn from_toml_str(text: &str) -> Result<Self, LoaderError> {
        let config: Self = parse_toml(text, Path::new("<bootstrap>"))?;
        ensure_version(config.version)?;
        Ok(config)
    }
}

impl ExtensionsConfig {
    /// 从 TOML 文本解析；未知字段与缺字段直接失败。
    pub fn from_toml_str(text: &str) -> Result<Self, LoaderError> {
        parse_toml(text, Path::new("<extensions>"))
    }

    /// 校验版本、实例唯一性，以及工厂是否已注册。
    pub fn validate(&self, catalog: &ExtensionCatalog) -> Result<(), LoaderError> {
        ensure_version(self.version)?;
        let mut seen = HashSet::new();
        for entry in &self.extensions {
            if !seen.insert(entry.instance.as_str()) {
                return Err(LoaderError::DuplicateInstance {
                    instance: entry.instance.clone(),
                });
            }
            if catalog.get(&entry.factory).is_none() {
                return Err(LoaderError::UnknownFactory {
                    instance: entry.instance.clone(),
                    factory: entry.factory.clone(),
                });
            }
        }
        Ok(())
    }
}

impl ExtensionEntry {
    pub fn new(instance: impl Into<String>, factory: impl Into<String>, enabled: bool) -> Self {
        Self {
            instance: instance.into(),
            factory: factory.into(),
            enabled,
            config: empty_table(),
        }
    }

    pub fn with_config(mut self, config: toml::Value) -> Self {
        self.config = config;
        self
    }
}

pub(crate) fn ensure_version(version: u32) -> Result<(), LoaderError> {
    if version == CONFIG_VERSION {
        Ok(())
    } else {
        Err(LoaderError::UnsupportedVersion { found: version })
    }
}
