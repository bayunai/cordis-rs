//! 面向管理客户端的配置描述、乐观并发与字段操作。原始配置仍由 Loader 持有。
use crate::{
    EntryId, EntryOptions, EntryUpdate, LoaderControlError, PreparedExtension,
    bootstrap::write_extensions,
    loader::{patch_entry, source_path_tree},
    tree::{RuntimeTree, ensure_tree_revision_control},
};
use std::{
    fmt,
    panic::{AssertUnwindSafe, catch_unwind},
    str::FromStr,
    sync::{Arc, atomic::Ordering},
};

/// 只用于等值比较；绑定一棵树的实例身份与协调代次，不是文件或热快照版本。
///
/// 当前按整棵配置树检查并发：同树其他 Entry 的协调也会使令牌失效。
/// Display / FromStr 仅供应用传输层往返，令牌不提供身份认证或访问授权。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConfigRevision {
    tree: u128,
    generation: u64,
}
impl ConfigRevision {
    pub(crate) fn new(tree: u128, generation: u64) -> Self {
        Self { tree, generation }
    }
}

impl fmt::Display for ConfigRevision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:032x}:{:016x}", self.tree, self.generation)
    }
}

impl FromStr for ConfigRevision {
    type Err = LoaderControlError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let invalid = || LoaderControlError::InvalidConfigRevision;
        let (tree, generation) = value.split_once(':').ok_or_else(invalid)?;
        if tree.len() != 32
            || generation.len() != 16
            || !tree
                .bytes()
                .chain(generation.bytes())
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(invalid());
        }
        let tree = u128::from_str_radix(tree, 16).map_err(|_| invalid())?;
        Ok(Self::new(
            tree,
            u64::from_str_radix(generation, 16).map_err(|_| invalid())?,
        ))
    }
}

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

/// 可交给应用传输层的描述。value 不包含秘密，schema 也移除了秘密值载荷。
#[derive(Clone, Debug)]
pub struct EntryConfigView {
    /// 包含 Group / Include 前缀的完整 Entry 路径。
    pub path: EntryId,
    pub factory: String,
    /// 用于表单展示的安全投影；最终校验仍由 Rust 类型和 prepare 完成。
    pub schema: serde_json::Value,
    /// 已加载的原始声明，已删除秘密字段；不展开 Serde 默认值或继承层。
    pub value: toml::Value,
    pub secrets: Vec<SecretFieldState>,
    pub revision: ConfigRevision,
}

pub(crate) async fn describe(
    tree: &Arc<RuntimeTree>,
    local: &str,
) -> Result<EntryConfigView, LoaderControlError> {
    let _operation = tree.operations.lock().await;
    ensure_available(tree, local)?;
    tree.await_idle().await?;
    ensure_available(tree, local)?;
    describe_locked(tree, local)
}

pub(crate) async fn mutate(
    tree: &Arc<RuntimeTree>,
    local: &str,
    expected: ConfigRevision,
    operations: Vec<ConfigOperation>,
) -> Result<EntryConfigView, LoaderControlError> {
    let tree = tree.clone();
    let local = local.to_owned();
    // 调用方取消等待不能把“协调已经提交、文件尚未写回”留成永久半提交。
    // 正在执行的任务持有 operations 锁，树释放仍会等待它结算。
    tokio::spawn(async move { mutate_owned(&tree, &local, expected, operations).await })
        .await
        .map_err(|_| LoaderControlError::ConfigOperationAborted)?
}

async fn mutate_owned(
    tree: &Arc<RuntimeTree>,
    local: &str,
    expected: ConfigRevision,
    operations: Vec<ConfigOperation>,
) -> Result<EntryConfigView, LoaderControlError> {
    let _operation = tree.operations.lock().await;
    ensure_available(tree, local)?;
    tree.await_idle().await?;
    ensure_available(tree, local)?;
    if expected != tree.config_revision() {
        return Err(LoaderControlError::ConfigRevisionConflict);
    }
    let path = source_path_tree(tree)?;
    ensure_tree_revision_control(tree, &path)?;
    let mut target = tree.desired.lock().expect("desired").clone();
    let mut entry = find(&target.extensions, local)?;
    if entry.group {
        return Err(LoaderControlError::ConfigNotEditable);
    }
    for operation in operations {
        apply_operation(&mut entry.config, operation)?;
    }
    // 先检查管理描述可安全生成，避免写入后才发现 Schema 无法脱敏。
    view(tree, local, &entry)?;
    let factory = tree.catalog.get(&entry.name).expect("registered factory");
    // 不转发可能含输入值的反序列化错误或 Factory panic 消息。
    let prepared: PreparedExtension =
        catch_unwind(AssertUnwindSafe(|| factory.prepare(&entry.config)))
            .map_err(|_| LoaderControlError::ConfigValidation)?
            .map_err(|_| LoaderControlError::ConfigValidation)?;
    patch_entry(
        &mut target.extensions,
        local,
        EntryUpdate {
            config: Some(entry.config),
            ..Default::default()
        },
    )?;
    let full = full_path(tree, local);
    let snapshot = tree
        .apply_prepared(target.clone(), None, Some((full, prepared)))
        .await
        .map_err(|error| match error {
            // Factory 构造、比较及 PluginKey 预检也不能通过错误回显配置。
            crate::LoaderError::FactoryPanic { .. }
            | crate::LoaderError::PluginBuild { .. }
            | crate::LoaderError::PluginPanic { .. } => LoaderControlError::ConfigValidation,
            other => LoaderControlError::Loader(other),
        })?;
    // 沿用 Loader 的运行时优先语义：写回失败不回滚已生效的 Fiber 或热快照。
    // 调用方必须按 Persist 处理并重新读取，不可将错误视为“从未执行”。
    match write_extensions(&path, &target) {
        Ok(revision) => *tree.revision.lock().expect("revision") = Some(revision),
        Err(error) => {
            return Err(LoaderControlError::Persist {
                snapshot: Box::new(snapshot),
                path,
                message: error.to_string(),
            });
        }
    }
    describe_locked(tree, local)
}

fn describe_locked(tree: &RuntimeTree, local: &str) -> Result<EntryConfigView, LoaderControlError> {
    let entry = find(&tree.desired.lock().expect("desired").extensions, local)?;
    view(tree, local, &entry)
}

fn view(
    tree: &RuntimeTree,
    local: &str,
    entry: &EntryOptions,
) -> Result<EntryConfigView, LoaderControlError> {
    if entry.group {
        return Err(LoaderControlError::ConfigNotEditable);
    }
    let descriptor = tree
        .catalog
        .factories()?
        .into_iter()
        .find(|f| f.id == entry.name)
        .ok_or(LoaderControlError::ConfigNotEditable)?;
    let (schema, value, secrets) = crate::config_secrets::redact(&descriptor.schema, &entry.config)
        .map_err(|message| LoaderControlError::SecretSchema { message })?;
    Ok(EntryConfigView {
        path: full_path(tree, local),
        factory: entry.name.clone(),
        schema,
        value,
        secrets: secrets
            .into_iter()
            .map(|(path, is_set)| SecretFieldState { path, is_set })
            .collect(),
        revision: tree.config_revision(),
    })
}

fn full_path(tree: &RuntimeTree, local: &str) -> EntryId {
    if tree.prefix.as_str().is_empty() {
        local.into()
    } else {
        format!("{}:{local}", tree.prefix).into()
    }
}

fn ensure_available(tree: &Arc<RuntimeTree>, local: &str) -> Result<(), LoaderControlError> {
    let loader = tree
        .loader
        .lock()
        .expect("loader weak")
        .upgrade()
        .ok_or(LoaderControlError::LoaderUnavailable)?;
    if !loader.alive.load(Ordering::Acquire) {
        return Err(LoaderControlError::LoaderUnavailable);
    }
    if !tree.prefix.as_str().is_empty()
        && !loader
            .subtree_owning_path(full_path(tree, local).as_str())
            .is_some_and(|active| Arc::ptr_eq(&active, tree))
    {
        return Err(LoaderControlError::LoaderUnavailable);
    }
    Ok(())
}

fn find(entries: &[EntryOptions], path: &str) -> Result<EntryOptions, LoaderControlError> {
    let (head, tail) = path
        .split_once(':')
        .map_or((path, None), |(a, b)| (a, Some(b)));
    if head.is_empty() {
        return Err(LoaderControlError::InvalidConfigPath);
    }
    let entry = entries
        .iter()
        .find(|e| e.id == head)
        .ok_or_else(|| LoaderControlError::EntryNotFound { path: path.into() })?;
    if let Some(tail) = tail {
        if !entry.group {
            return Err(LoaderControlError::EntryNotFound { path: path.into() });
        }
        find(&entry.children()?, tail)
    } else {
        Ok(entry.clone())
    }
}

fn apply_operation(
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
        apply_operation(
            &mut config,
            ConfigOperation::Set {
                path: vec!["auth".into()],
                value: toml::Value::Table(toml::Table::new()),
            },
        )
        .unwrap();
        assert!(config["auth"].get("token").is_none());
        apply_operation(
            &mut config,
            ConfigOperation::Set {
                path: vec!["nested".into(), "0".into()],
                value: true.into(),
            },
        )
        .unwrap();
        assert_eq!(config["nested"]["0"].as_bool(), Some(true));
        assert!(
            apply_operation(
                &mut config,
                ConfigOperation::Unset {
                    path: vec!["missing".into()]
                }
            )
            .is_err()
        );
        assert!(apply_operation(&mut config, ConfigOperation::Unset { path: vec![] }).is_err());
    }
}
