//! 表单 DTO、字段操作与安全描述组装。

use crate::secrets;
use cordis_loader::{ConfigRevision, EntryId, LoaderControlError};
use cordis_plugin_config_editor::LayeredEntryConfig;

/// 表字段操作。空路径不合法；数组只能整体替换。刻意不实现 Debug，避免泄露输入。
#[derive(Clone)]
pub enum ConfigOperation {
    Set {
        path: Vec<String>,
        value: toml::Value,
    },
    Unset {
        path: Vec<String>,
    },
}

/// 脱敏字段的存在状态；不包含值、摘要或可恢复的凭据。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecretFieldState {
    /// 配置表内的键路径；数组内含秘密时记录整个数组的路径。
    pub path: Vec<String>,
    /// 原始声明是否包含该字段，不代表解析后的默认值是否存在。
    pub is_set: bool,
}

/// 可交给应用传输层的分层描述。
#[derive(Clone, Debug)]
pub struct EntryConfigView {
    /// 包含 Group / Include 前缀的完整 Entry 路径。
    pub path: EntryId,
    pub factory: String,
    /// 用于表单展示的安全投影；最终校验仍由 Rust 类型和 prepare 完成。
    pub schema: serde_json::Value,
    /// Include path 源文件中的声明（已脱敏）。
    pub base: toml::Value,
    /// 本机 user 整表覆盖（已脱敏）；无覆盖时为 `None`。
    pub user: Option<toml::Value>,
    /// deploy ∘ user ∘ home 的原始合成配置，字段编辑以此为基础，不能把默认值写回用户层。
    pub config: toml::Value,
    /// Active 实例的已解析配置快照（含 Serde 默认值，已脱敏）；其他状态明确为 None。
    pub value: Option<serde_json::Value>,
    pub secrets: Vec<SecretFieldState>,
    pub revision: ConfigRevision,
}

pub(crate) fn view_from_layers(
    layers: &LayeredEntryConfig,
) -> Result<EntryConfigView, LoaderControlError> {
    let (schema, config, secrets) = secrets::redact(&layers.schema, &layers.effective)
        .map_err(|message| LoaderControlError::SecretSchema { message })?;
    let (_, base, _) = secrets::redact(&layers.schema, &layers.base)
        .map_err(|message| LoaderControlError::SecretSchema { message })?;
    let user = match &layers.user {
        Some(raw) => {
            let (_, user, _) = secrets::redact(&layers.schema, raw)
                .map_err(|message| LoaderControlError::SecretSchema { message })?;
            Some(user)
        }
        None => None,
    };
    let value = layers
        .resolved
        .as_ref()
        .map(|resolved| secrets::redact_json(&layers.resolved_schema, resolved))
        .transpose()
        .map_err(|message| LoaderControlError::SecretSchema { message })?;
    Ok(EntryConfigView {
        path: layers.path.clone(),
        factory: layers.factory.clone(),
        schema,
        base,
        user,
        config,
        value,
        secrets: secrets
            .into_iter()
            .map(|(path, is_set)| SecretFieldState { path, is_set })
            .collect(),
        revision: layers.revision,
    })
}

pub(crate) fn apply_operation(
    config: &mut toml::Value,
    operation: ConfigOperation,
) -> Result<(), LoaderControlError> {
    let (path, value) = match operation {
        ConfigOperation::Set { path, value } => (path, Some(value)),
        ConfigOperation::Unset { path } => (path, None),
    };
    let (last, parents) = path
        .split_last()
        .ok_or(LoaderControlError::InvalidConfigPath)?;
    let mut table = config
        .as_table_mut()
        .ok_or(LoaderControlError::InvalidConfigPath)?;
    for key in parents {
        if !table.contains_key(key) && value.is_some() {
            table.insert(key.clone(), toml::Value::Table(toml::Table::new()));
        }
        table = table
            .get_mut(key)
            .and_then(toml::Value::as_table_mut)
            .ok_or(LoaderControlError::InvalidConfigPath)?;
    }
    if let Some(value) = value {
        table.insert(last.clone(), value);
    } else if table.remove(last).is_none() {
        return Err(LoaderControlError::InvalidConfigPath);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_are_table_keys_and_arrays_are_atomic() {
        let mut config: toml::Value =
            toml::from_str("rows=[1,2]\n[auth]\ntoken='secret'\n").unwrap();
        assert!(matches!(
            apply_operation(
                &mut config,
                ConfigOperation::Set {
                    path: vec!["rows".into(), "0".into()],
                    value: 3.into()
                }
            ),
            Err(LoaderControlError::InvalidConfigPath)
        ));
        apply_operation(
            &mut config,
            ConfigOperation::Set {
                path: vec!["rows".into()],
                value: toml::Value::Array(vec![3.into()]),
            },
        )
        .unwrap();
        assert_eq!(config["rows"].as_array().unwrap().len(), 1);
    }
}
