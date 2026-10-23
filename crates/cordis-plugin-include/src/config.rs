use cordis_loader::{EntryOptions, InjectConfig, IsolateConfig, SourceError, SourceErrorKind};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// 显式文件来源。initial 仅在首次读取确认为文件不存在时使用，不修复非法已有文件。
#[derive(Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct IncludeConfig {
    pub path: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Vec<serde_json::Value>>")]
    #[schemars(extend("writeOnly" = true))]
    pub initial: Option<Vec<EntryOptions>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(extend("writeOnly" = true))]
    pub patches: Vec<EntryPatch>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enable_logs: Option<bool>,
}
impl IncludeConfig {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            initial: None,
            patches: Vec::new(),
            enable_logs: None,
        }
    }
    pub(crate) fn validate(&self) -> Result<(), SourceError> {
        if self.path.as_os_str().is_empty() {
            return Err(SourceError::new(
                SourceErrorKind::InvalidData,
                "Include 路径不能为空",
            ));
        }
        Ok(())
    }
}

/// id 是文件内完整局部路径；insert 无 id 表示向文件根插入，有 id 则必须指向 Group。
/// 非插入 patch 只能覆盖已有条目字段，name 仅作 Factory 身份校验。
#[derive(Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EntryPatch {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<Vec<serde_json::Value>>")]
    pub insert: Option<Vec<EntryOptions>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<serde_json::Value>")]
    pub config: Option<toml::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<serde_json::Value>")]
    pub inject: Option<InjectConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Option<serde_json::Value>")]
    pub isolate: Option<IsolateConfig>,
}
