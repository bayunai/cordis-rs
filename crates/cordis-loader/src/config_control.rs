//! 受信任的 Entry 配置快照与整表提交；表单投影与字段操作属于 settings 插件。
use crate::{
    EntryId, EntryOptions, EntryUpdate, LoaderControlError, PreparedExtension,
    loader::{commit_tree_locked, patch_entry},
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

/// 受信任快照：Catalog Schema 原样，配置可含明文秘密。不得原样发给浏览器。
#[derive(Clone, Debug)]
pub struct EntryConfigSnapshot {
    /// 包含 Group / Include 前缀的完整 Entry 路径。
    pub path: EntryId,
    pub factory: String,
    /// Catalog 登记的 Schema，未经秘密投影。
    pub schema: serde_json::Value,
    /// 已加载的原始声明，可含秘密。
    pub config: toml::Value,
    pub revision: ConfigRevision,
}

pub(crate) async fn snapshot(
    tree: &Arc<RuntimeTree>,
    local: &str,
) -> Result<EntryConfigSnapshot, LoaderControlError> {
    let _operation = tree.operations.lock().await;
    ensure_available(tree, local)?;
    tree.await_idle().await?;
    ensure_available(tree, local)?;
    snapshot_locked(tree, local)
}

pub(crate) async fn commit(
    tree: &Arc<RuntimeTree>,
    local: &str,
    expected: ConfigRevision,
    config: toml::Value,
) -> Result<EntryConfigSnapshot, LoaderControlError> {
    let tree = tree.clone();
    let local = local.to_owned();
    // 调用方取消等待不能把“协调已经提交、文件尚未写回”留成永久半提交。
    // 正在执行的任务持有 operations 锁，树释放仍会等待它结算。
    tokio::spawn(async move { commit_owned(&tree, &local, expected, config).await })
        .await
        .map_err(|_| LoaderControlError::ConfigOperationAborted)?
}

async fn commit_owned(
    tree: &Arc<RuntimeTree>,
    local: &str,
    expected: ConfigRevision,
    config: toml::Value,
) -> Result<EntryConfigSnapshot, LoaderControlError> {
    let _operation = tree.operations.lock().await;
    ensure_available(tree, local)?;
    tree.await_idle().await?;
    ensure_available(tree, local)?;
    if expected != tree.config_revision() {
        return Err(LoaderControlError::ConfigRevisionConflict);
    }
    ensure_tree_revision_control(tree).await?;
    let mut target = tree.desired.lock().expect("desired").clone();
    let mut entry = find(&target.extensions, local)?;
    if entry.group {
        return Err(LoaderControlError::ConfigNotEditable);
    }
    entry.config = config;
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
    commit_tree_locked(tree, target, Some((full, prepared)))
        .await
        .map_err(|error| match error {
            // Factory 构造、比较及 PluginKey 预检也不能通过错误回显配置。
            LoaderControlError::Loader(
                crate::LoaderError::FactoryPanic { .. }
                | crate::LoaderError::PluginBuild { .. }
                | crate::LoaderError::PluginPanic { .. },
            ) => LoaderControlError::ConfigValidation,
            other => other,
        })?;
    snapshot_locked(tree, local)
}

fn snapshot_locked(
    tree: &RuntimeTree,
    local: &str,
) -> Result<EntryConfigSnapshot, LoaderControlError> {
    let entry = find(&tree.desired.lock().expect("desired").extensions, local)?;
    view(tree, local, &entry)
}

fn view(
    tree: &RuntimeTree,
    local: &str,
    entry: &EntryOptions,
) -> Result<EntryConfigSnapshot, LoaderControlError> {
    if entry.group {
        return Err(LoaderControlError::ConfigNotEditable);
    }
    let descriptor = tree
        .catalog
        .factories()?
        .into_iter()
        .find(|f| f.id == entry.name)
        .ok_or(LoaderControlError::ConfigNotEditable)?;
    Ok(EntryConfigSnapshot {
        path: full_path(tree, local),
        factory: entry.name.clone(),
        schema: descriptor.schema,
        config: entry.config.clone(),
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
    if !loader.alive.load(Ordering::Acquire) || tree.closing.load(Ordering::Acquire) {
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
