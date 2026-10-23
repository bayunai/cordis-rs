//! 表单 DTO、字段操作与安全描述组装。

use crate::secrets;
use cordis_loader::{ConfigRevision, EntryId, LoaderControlError};
use cordis_plugin_config_editor::{ConfigEditability, ConfigReadOnlyReason, LayeredEntryConfig};

/// 表字段操作。空路径不合法。刻意不实现 Debug，避免泄露输入。
///
/// `Unset` 对齐 DSH：对象字段恢复 `base` 继承（有则写入该值，无则删键）；
/// 父节点为数组时删除该下标元素。不能硬删下层已提供的继承值。
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
    /// 去除 user 后的继承 **raw**（输入 Schema 脱敏）；Unset 恢复源。
    pub base: toml::Value,
    /// 对 raw 继承的类型投影（含 Serde 默认，序列化 Schema 脱敏）；解析失败为 `None`。
    pub inherited: Option<serde_json::Value>,
    /// 本机 user 整表覆盖（已脱敏）；无覆盖时为 `None`。
    pub user: Option<toml::Value>,
    /// deploy ∘ user ∘ home 的原始合成配置，字段编辑以此为基础。
    pub config: toml::Value,
    /// Active 实例的已解析配置快照（含 Serde 默认值，已脱敏）；其他状态明确为 None。
    pub value: Option<serde_json::Value>,
    pub secrets: Vec<SecretFieldState>,
    /// 是否允许经 `mutate` 写入；与 `read_only_reason` 同源。
    pub editable: bool,
    /// 不可编辑时的原因；能 describe 不代表允许 mutate。
    pub read_only_reason: Option<ConfigReadOnlyReason>,
    pub revision: ConfigRevision,
}

pub(crate) fn view_from_layers(
    layers: &LayeredEntryConfig,
) -> Result<EntryConfigView, LoaderControlError> {
    let (schema, config, secrets) = secrets::redact(&layers.schema, &layers.effective)
        .map_err(|message| LoaderControlError::SecretSchema { message })?;
    // base 始终是输入空间 raw，按 input schema 脱敏（与 mutate 路径键一致）。
    let (_, base, _) = secrets::redact(&layers.schema, &layers.base)
        .map_err(|message| LoaderControlError::SecretSchema { message })?;
    let inherited = layers
        .inherited
        .as_ref()
        .map(|value| secrets::redact_json(&layers.resolved_schema, value))
        .transpose()
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
    let (editable, read_only_reason) = match layers.editability {
        ConfigEditability::Editable => (true, None),
        ConfigEditability::ReadOnly(reason) => (false, Some(reason)),
    };
    Ok(EntryConfigView {
        path: layers.path.clone(),
        factory: layers.factory.clone(),
        schema,
        base,
        inherited,
        user,
        config,
        value,
        secrets: secrets
            .into_iter()
            .map(|(path, is_set)| SecretFieldState { path, is_set })
            .collect(),
        editable,
        read_only_reason,
        revision: layers.revision,
    })
}

/// 应用一次字段操作。`base` 仅 `Unset` 恢复继承时使用。
pub(crate) fn apply_operation(
    config: &mut toml::Value,
    base: &toml::Value,
    operation: ConfigOperation,
) -> Result<(), LoaderControlError> {
    match operation {
        ConfigOperation::Set { path, value } => apply_set(config, &path, value),
        ConfigOperation::Unset { path } => apply_unset(config, base, &path),
    }
}

fn apply_set(
    config: &mut toml::Value,
    path: &[String],
    value: toml::Value,
) -> Result<(), LoaderControlError> {
    let (last, parents) = path
        .split_last()
        .ok_or(LoaderControlError::InvalidConfigPath)?;
    let mut cursor = config
        .as_table_mut()
        .ok_or(LoaderControlError::InvalidConfigPath)?;
    for key in parents {
        if !cursor.contains_key(key) {
            cursor.insert(key.clone(), toml::Value::Table(toml::Table::new()));
        }
        cursor = cursor
            .get_mut(key)
            .and_then(toml::Value::as_table_mut)
            .ok_or(LoaderControlError::InvalidConfigPath)?;
    }
    cursor.insert(last.clone(), value);
    Ok(())
}

fn apply_unset(
    config: &mut toml::Value,
    base: &toml::Value,
    path: &[String],
) -> Result<(), LoaderControlError> {
    let (_last, parents) = path
        .split_last()
        .ok_or(LoaderControlError::InvalidConfigPath)?;
    if parent_is_array(config, parents)? {
        return remove_path(config, path);
    }
    match get_path(base, path) {
        Some(inherited) => apply_set(config, path, inherited.clone()),
        None => remove_path(config, path),
    }
}

fn parent_is_array(root: &toml::Value, parents: &[String]) -> Result<bool, LoaderControlError> {
    if parents.is_empty() {
        return Ok(false);
    }
    Ok(get_path(root, parents).is_some_and(toml::Value::is_array))
}

fn get_path<'a>(root: &'a toml::Value, path: &[String]) -> Option<&'a toml::Value> {
    let mut cursor = root;
    for key in path {
        cursor = match cursor {
            toml::Value::Table(table) => table.get(key)?,
            toml::Value::Array(items) => {
                let index: usize = key.parse().ok()?;
                items.get(index)?
            }
            _ => return None,
        };
    }
    Some(cursor)
}

fn remove_path(root: &mut toml::Value, path: &[String]) -> Result<(), LoaderControlError> {
    let (last, parents) = path
        .split_last()
        .ok_or(LoaderControlError::InvalidConfigPath)?;
    let mut cursor = root;
    for key in parents {
        cursor = match cursor {
            toml::Value::Table(table) => table
                .get_mut(key)
                .ok_or(LoaderControlError::InvalidConfigPath)?,
            toml::Value::Array(items) => {
                let index: usize = key
                    .parse()
                    .map_err(|_| LoaderControlError::InvalidConfigPath)?;
                items
                    .get_mut(index)
                    .ok_or(LoaderControlError::InvalidConfigPath)?
            }
            _ => return Err(LoaderControlError::InvalidConfigPath),
        };
    }
    match cursor {
        toml::Value::Table(table) => {
            if table.remove(last).is_none() {
                return Err(LoaderControlError::InvalidConfigPath);
            }
        }
        toml::Value::Array(items) => {
            let index: usize = last
                .parse()
                .map_err(|_| LoaderControlError::InvalidConfigPath)?;
            if index >= items.len() {
                return Err(LoaderControlError::InvalidConfigPath);
            }
            items.remove(index);
        }
        _ => return Err(LoaderControlError::InvalidConfigPath),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_are_table_keys_and_arrays_are_atomic_for_set() {
        let mut config: toml::Value =
            toml::from_str("rows=[1,2]\n[auth]\ntoken='secret'\n").unwrap();
        let base = config.clone();
        assert!(matches!(
            apply_operation(
                &mut config,
                &base,
                ConfigOperation::Set {
                    path: vec!["rows".into(), "0".into()],
                    value: 3.into()
                }
            ),
            Err(LoaderControlError::InvalidConfigPath)
        ));
        apply_operation(
            &mut config,
            &base,
            ConfigOperation::Set {
                path: vec!["rows".into()],
                value: toml::Value::Array(vec![3.into()]),
            },
        )
        .unwrap();
        assert_eq!(config["rows"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn unset_object_restores_inherited_base_value() {
        let mut config: toml::Value = toml::from_str("limit = 9\nname = 'user'\n").unwrap();
        let base: toml::Value = toml::from_str("limit = 1\n").unwrap();
        apply_operation(
            &mut config,
            &base,
            ConfigOperation::Unset {
                path: vec!["limit".into()],
            },
        )
        .unwrap();
        assert_eq!(config["limit"].as_integer(), Some(1));
        assert_eq!(config["name"].as_str(), Some("user"));
    }

    #[test]
    fn unset_object_without_base_removes_key() {
        let mut config: toml::Value = toml::from_str("extra = true\n").unwrap();
        let base: toml::Value = toml::from_str("").unwrap();
        apply_operation(
            &mut config,
            &base,
            ConfigOperation::Unset {
                path: vec!["extra".into()],
            },
        )
        .unwrap();
        assert!(config.get("extra").is_none());
    }

    #[test]
    fn unset_array_index_removes_element() {
        let mut config: toml::Value = toml::from_str("rows=[1,2,3]\n").unwrap();
        let base = config.clone();
        apply_operation(
            &mut config,
            &base,
            ConfigOperation::Unset {
                path: vec!["rows".into(), "1".into()],
            },
        )
        .unwrap();
        assert_eq!(
            config["rows"].as_array().unwrap(),
            &vec![1.into(), 3.into()]
        );
    }
}
