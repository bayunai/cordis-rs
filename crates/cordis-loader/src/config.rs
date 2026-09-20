//! `extensions.toml` 的 v2 EntryTree 配置合同。

use crate::{catalog::ExtensionCatalog, error::LoaderError};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashSet},
    path::Path,
};

pub const CONFIG_VERSION: u32 = 2;
pub const GROUP_NAME: &str = "cordis:group";

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BootstrapConfig {
    pub version: u32,
    pub config: BootstrapConfigSource,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BootstrapConfigSource {
    pub driver: ConfigDriver,
    pub path: std::path::PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ConfigDriver {
    File,
}

/// Loader 的完整持久化树。`extensions = []` 是唯一的空树写法。
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExtensionsConfig {
    pub version: u32,
    pub extensions: Vec<EntryOptions>,
}

/// 条目配置中的依赖声明，外形与 DeepSeek `Inject` 对齐。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum InjectConfig {
    List(Vec<String>),
    Map(BTreeMap<String, toml::Value>),
}

/// 一个普通插件或内建 Group 节点。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EntryOptions {
    pub id: String,
    pub name: String,
    #[serde(default = "empty_table")]
    pub config: toml::Value,
    #[serde(default)]
    pub group: bool,
    #[serde(default)]
    pub disabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inject: Option<InjectConfig>,
}

fn empty_table() -> toml::Value {
    toml::Value::Table(toml::Table::new())
}

impl EntryOptions {
    pub fn new(id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            config: empty_table(),
            group: false,
            disabled: false,
            inject: None,
        }
    }
    pub fn group(id: impl Into<String>, children: Vec<EntryOptions>) -> Result<Self, LoaderError> {
        Ok(Self {
            id: id.into(),
            name: GROUP_NAME.into(),
            config: toml::Value::try_from(children).map_err(|error| LoaderError::InvalidEntry {
                path: "<group>".into(),
                message: error.to_string(),
            })?,
            group: true,
            disabled: false,
            inject: None,
        })
    }
    pub fn with_config(mut self, config: toml::Value) -> Self {
        self.config = config;
        self
    }
    pub fn with_inject(mut self, inject: InjectConfig) -> Self {
        self.inject = Some(inject);
        self
    }
    pub fn children(&self) -> Result<Vec<EntryOptions>, LoaderError> {
        self.config
            .clone()
            .try_into()
            .map_err(|error| LoaderError::InvalidEntry {
                path: self.id.clone(),
                message: format!("Group config 必须是条目数组: {error}"),
            })
    }
    pub(crate) fn set_children(&mut self, children: Vec<EntryOptions>) -> Result<(), LoaderError> {
        self.config =
            toml::Value::try_from(children).map_err(|error| LoaderError::InvalidEntry {
                path: self.id.clone(),
                message: error.to_string(),
            })?;
        Ok(())
    }
}

impl ExtensionsConfig {
    pub fn from_toml_str(text: &str) -> Result<Self, LoaderError> {
        parse_toml(text, Path::new("<extensions>"))
    }
    pub fn validate(&self, catalog: &ExtensionCatalog) -> Result<(), LoaderError> {
        ensure_version(self.version)?;
        validate_entries(&self.extensions, catalog, None)
    }
}

fn validate_entries(
    entries: &[EntryOptions],
    catalog: &ExtensionCatalog,
    parent: Option<&str>,
) -> Result<(), LoaderError> {
    let mut seen = HashSet::new();
    for entry in entries {
        let path = parent.map_or_else(
            || entry.id.clone(),
            |parent| format!("{parent}:{id}", id = entry.id),
        );
        if entry.id.is_empty() || entry.id.contains(':') {
            return Err(LoaderError::InvalidEntry {
                path,
                message: "id 不得为空或包含 ':'".into(),
            });
        }
        if !seen.insert(entry.id.as_str()) {
            return Err(LoaderError::DuplicateEntry { path });
        }
        validate_inject(entry, catalog, &path)?;
        if entry.group {
            if entry.name != GROUP_NAME {
                return Err(LoaderError::InvalidEntry {
                    path,
                    message: format!("Group 的 name 必须为 `{GROUP_NAME}`"),
                });
            }
            validate_entries(&entry.children()?, catalog, Some(&path))?;
        } else if entry.name == GROUP_NAME {
            return Err(LoaderError::InvalidEntry {
                path,
                message: format!("`{GROUP_NAME}` 必须设置 group = true"),
            });
        } else if !catalog.contains(&entry.name) {
            return Err(LoaderError::UnknownFactory {
                instance: path,
                factory: entry.name.clone(),
            });
        }
    }
    Ok(())
}

fn validate_inject(
    entry: &EntryOptions,
    catalog: &ExtensionCatalog,
    path: &str,
) -> Result<(), LoaderError> {
    let Some(inject) = &entry.inject else {
        return Ok(());
    };
    match inject {
        InjectConfig::List(ids) => {
            for id in ids {
                catalog.validate_injection(id, None, path)?;
            }
        }
        InjectConfig::Map(values) => {
            for (id, value) in values {
                catalog.validate_injection(id, Some(value), path)?;
            }
        }
    }
    Ok(())
}

pub(crate) fn parse_toml<T>(text: &str, path: &Path) -> Result<T, LoaderError>
where
    T: serde::de::DeserializeOwned,
{
    toml::from_str(text).map_err(|source| LoaderError::toml(path.to_path_buf(), source))
}
impl BootstrapConfig {
    pub fn from_toml_str(text: &str) -> Result<Self, LoaderError> {
        let config: Self = parse_toml(text, Path::new("<bootstrap>"))?;
        ensure_version(config.version)?;
        Ok(config)
    }
}
pub(crate) fn ensure_version(version: u32) -> Result<(), LoaderError> {
    if version == CONFIG_VERSION {
        Ok(())
    } else {
        Err(LoaderError::UnsupportedVersion { found: version })
    }
}
