//! EntryTree 管理控制面（根树与可附着子树）。

use crate::{
    EntryTreeSource,
    catalog::{FactoryDescriptor, InjectionDescriptorInfo, IsolationDescriptorInfo},
    config::{EntryOptions, ExtensionsConfig, InjectConfig, IsolateConfig},
    error::LoaderError,
    plugin::LoaderInner,
    snapshot::{EntryId, LoaderSnapshot},
    tree::{RuntimeTree, ensure_tree_revision_control},
};
use cordis_core::{Context, ServiceKey};
use std::sync::Arc;
use tokio::sync::broadcast;

/// Loader 的运行时快照已失效。
///
/// 订阅者必须重新读取快照，不能靠事件载荷重建树。一次 reconcile 即使返回错误，
/// 也可能留下 `Failed` 节点或释放旧节点；只要已执行 reconcile，便会发送此事件。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoaderEvent {
    SnapshotChanged { revision: u64 },
}

pub static LOADER: ServiceKey<Loader> = ServiceKey::new("cordis.loader@1");

#[derive(Debug, Clone, Default)]
pub struct EntryUpdate {
    pub config: Option<toml::Value>,
    pub disabled: Option<bool>,
    pub inject: Option<Option<InjectConfig>>,
    pub isolate: Option<Option<IsolateConfig>>,
}

#[derive(Debug, thiserror::Error)]
pub enum LoaderControlError {
    #[error("配置提交任务异常终止，请重新读取配置与运行状态")]
    ConfigOperationAborted,
    #[error("配置 revision 格式无效")]
    InvalidConfigRevision,
    #[error("配置已发生变化，请重新读取后提交")]
    ConfigRevisionConflict,
    #[error("配置路径无效")]
    InvalidConfigPath,
    #[error("结构条目不支持配置表单编辑")]
    ConfigNotEditable,
    #[error("配置 Schema 无法安全描述: {message}")]
    SecretSchema { message: String },
    #[error("配置校验失败；请检查字段类型与插件约束")]
    ConfigValidation,
    #[error(transparent)]
    Loader(#[from] LoaderError),
    #[error("Loader 已关闭，Loader 不再可用")]
    LoaderUnavailable,
    #[error("条目 {path} 已存在")]
    EntryExists { path: String },
    #[error("条目 {path} 不存在")]
    EntryNotFound { path: String },
    #[error("目标父 Group {path} 不存在或不是 Group")]
    ParentNotGroup { path: String },
    #[error("不能将条目移动到自己或自己的子树: {path}")]
    InvalidMove { path: String },
    #[error("不支持跨树移动条目: {path}")]
    CrossTreeMove { path: String },
    #[error("配置来源已在 Loader 外修改；请先 reload: {source_id}")]
    ConfigConflict { source_id: String },
    #[error("配置来源失败: {source}")]
    Source { source: crate::SourceError },
    #[error("运行时已生效，但未能写回配置来源 {source_id}: {message}")]
    Persist {
        snapshot: Box<LoaderSnapshot>,
        source_id: String,
        message: String,
    },
}

impl LoaderControlError {
    pub(crate) fn from_source(source_id: String, source: crate::SourceError) -> Self {
        if source.kind == crate::SourceErrorKind::Conflict {
            Self::ConfigConflict { source_id }
        } else {
            Self::Source { source }
        }
    }
}

#[derive(Clone)]
pub struct Loader {
    inner: Arc<LoaderInner>,
}

impl Loader {
    /// 描述完整路径指定的配置，包括 Include 内部条目；不要求 Fiber Active。
    pub async fn describe_config(
        &self,
        path: impl AsRef<str>,
    ) -> Result<crate::EntryConfigView, LoaderControlError> {
        let inner = self.inner()?;
        let path = path.as_ref();
        let tree = inner
            .subtree_owning_path(path)
            .unwrap_or_else(|| inner.root.clone());
        let local = config_local_path(&tree, path);
        crate::config_control::describe(&tree, local).await
    }

    /// 安全字段编辑；expected_revision 必须来自最近一次配置描述。
    ///
    /// 已接受的提交不会因调用方取消等待而中断。普通/热更新沿用 Factory 策略；
    /// Persist 表示运行时已经生效但文件写回失败，不提供副作用回滚。
    pub async fn mutate_config(
        &self,
        path: impl AsRef<str>,
        expected_revision: crate::ConfigRevision,
        operations: Vec<crate::ConfigOperation>,
    ) -> Result<crate::EntryConfigView, LoaderControlError> {
        let inner = self.inner()?;
        let path = path.as_ref();
        let tree = inner
            .subtree_owning_path(path)
            .unwrap_or_else(|| inner.root.clone());
        crate::config_control::mutate(
            &tree,
            config_local_path(&tree, path),
            expected_revision,
            operations,
        )
        .await
    }
    pub(crate) fn new(inner: Arc<LoaderInner>) -> Self {
        Self { inner }
    }

    /// 已加载的初始声明或后续成功提交的声明；不代表各条目均为 Active。
    pub fn entries(&self) -> Result<ExtensionsConfig, LoaderControlError> {
        Ok(self.inner()?.root.desired.lock().expect("desired").clone())
    }

    pub fn entry_context(&self, path: impl AsRef<str>) -> Result<Context, LoaderControlError> {
        Ok(self.inner()?.entry_context(path.as_ref())?)
    }

    pub fn factories(&self) -> Result<Vec<FactoryDescriptor>, LoaderControlError> {
        Ok(self.inner()?.catalog.factories()?)
    }

    pub fn injections(&self) -> Result<Vec<InjectionDescriptorInfo>, LoaderControlError> {
        Ok(self.inner()?.catalog.injections())
    }

    pub fn isolations(&self) -> Result<Vec<IsolationDescriptorInfo>, LoaderControlError> {
        Ok(self.inner()?.catalog.isolations())
    }

    /// 等待受管运行树稳定并返回实际状态，包括 Failed/Pending 条目。
    /// 业务生命周期错误由变更调用、条目诊断和日志报告，不使整棵树不可读取。
    /// Loader 关闭及内部协调错误仍返回错误。
    pub async fn await_idle(&self) -> Result<LoaderSnapshot, LoaderControlError> {
        let snapshot = self.inner()?.await_idle_all().await;
        self.inner()?;
        Ok(snapshot?)
    }

    /// 订阅后续 Loader 事件。已发生的 revision 不会重放。
    pub fn subscribe(&self) -> Result<broadcast::Receiver<LoaderEvent>, LoaderControlError> {
        Ok(self.inner()?.events.subscribe())
    }

    /// 在条目 Context 上附着来源树。物理位置、编解码与写回均由来源实现。
    pub async fn attach_subtree(
        &self,
        owner: &Context,
        source: Arc<dyn EntryTreeSource>,
    ) -> Result<LoaderSubtree, LoaderControlError> {
        let inner = self.inner()?;
        let tree = inner.attach_subtree(owner, source).await?;
        Ok(LoaderSubtree {
            loader: inner,
            tree,
        })
    }

    /// 按 Include 条目完整路径查找已附着子树控制面。
    pub fn subtree(&self, prefix: impl AsRef<str>) -> Result<LoaderSubtree, LoaderControlError> {
        let inner = self.inner()?;
        let prefix = EntryId::from(prefix.as_ref());
        let tree = inner
            .subtrees
            .lock()
            .expect("subtree registry")
            .by_prefix
            .get(&prefix)
            .cloned()
            .ok_or_else(|| LoaderError::UnknownInstance {
                instance: prefix.to_string(),
            })?;
        Ok(LoaderSubtree {
            loader: inner,
            tree,
        })
    }

    pub async fn create(
        &self,
        entry: EntryOptions,
        parent: Option<&str>,
        position: Option<usize>,
    ) -> Result<LoaderSnapshot, LoaderControlError> {
        let inner = self.inner()?;
        reject_cross_tree(&inner, parent)?;
        let parent = parent.map(str::to_owned);
        mutate_tree(&inner.root, move |config| {
            insert_entry(&mut config.extensions, parent.as_deref(), position, entry)
        })
        .await
        .map(|_| inner.aggregate_snapshot())
    }

    pub async fn update(
        &self,
        path: impl AsRef<str>,
        patch: EntryUpdate,
        parent: Option<Option<&str>>,
        position: Option<usize>,
    ) -> Result<LoaderSnapshot, LoaderControlError> {
        let path = path.as_ref().to_owned();
        let inner = self.inner()?;
        if inner.subtree_owning_path(&path).is_some() {
            return Err(LoaderControlError::CrossTreeMove { path });
        }
        if let Some(Some(parent)) = parent {
            reject_cross_tree(&inner, Some(parent))?;
        }
        let parent = parent.map(|parent| parent.map(str::to_owned));
        mutate_tree(&inner.root, move |config| {
            let destination = if let Some(parent) = parent.as_ref() {
                let parent = parent.as_deref();
                if parent.is_some_and(|parent| {
                    parent == path || parent.starts_with(&(path.clone() + ":"))
                }) {
                    return Err(LoaderControlError::InvalidMove { path: path.clone() });
                }
                let entry = remove_entry(&mut config.extensions, &path)?;
                let next_path = join_path(parent, &entry.id);
                insert_entry(&mut config.extensions, parent, position, entry)?;
                next_path
            } else {
                path.clone()
            };
            patch_entry(&mut config.extensions, &destination, patch)
        })
        .await
        .map(|_| inner.aggregate_snapshot())
    }

    pub async fn remove(
        &self,
        path: impl AsRef<str>,
    ) -> Result<LoaderSnapshot, LoaderControlError> {
        let path = path.as_ref().to_owned();
        let inner = self.inner()?;
        if inner.subtree_owning_path(&path).is_some() {
            return Err(LoaderControlError::CrossTreeMove { path });
        }
        mutate_tree(&inner.root, move |config| {
            remove_entry(&mut config.extensions, &path).map(|_| ())
        })
        .await
        .map(|_| inner.aggregate_snapshot())
    }

    pub async fn reload(&self) -> Result<LoaderSnapshot, LoaderControlError> {
        let inner = self.inner()?;
        inner.root.reload().await?;
        Ok(inner.aggregate_snapshot())
    }

    fn inner(&self) -> Result<Arc<LoaderInner>, LoaderControlError> {
        if self.inner.alive.load(std::sync::atomic::Ordering::Acquire) {
            Ok(self.inner.clone())
        } else {
            Err(LoaderControlError::LoaderUnavailable)
        }
    }
}

/// Include 等载体持有的子树控制面；写操作只触及该子树 TOML。
#[derive(Clone)]
pub struct LoaderSubtree {
    loader: Arc<LoaderInner>,
    pub(crate) tree: Arc<RuntimeTree>,
}

impl LoaderSubtree {
    fn reject_nested_path(&self, local: &str) -> Result<(), LoaderControlError> {
        let full = format!("{}:{local}", self.tree.prefix);
        if self
            .loader
            .subtree_owning_path(&full)
            .is_some_and(|tree| !Arc::ptr_eq(&tree, &self.tree))
        {
            return Err(LoaderControlError::CrossTreeMove { path: full });
        }
        Ok(())
    }
    /// 已加载的该来源树声明，不代表每个节点均已 Active。
    pub fn entries(&self) -> Result<ExtensionsConfig, LoaderControlError> {
        self.ensure_alive()?;
        Ok(self.tree.desired.lock().expect("desired").clone())
    }
    /// 子树局部路径对应的配置描述；返回值中的路径为全路径。
    pub async fn describe_config(
        &self,
        path: impl AsRef<str>,
    ) -> Result<crate::EntryConfigView, LoaderControlError> {
        self.ensure_alive()?;
        crate::config_control::describe(&self.tree, path.as_ref()).await
    }

    /// 按子树局部路径提交配置操作，只写所属配置文件；返回描述使用完整路径。
    /// 并发令牌、取消等待和 Persist 语义与 Loader::mutate_config 相同。
    pub async fn mutate_config(
        &self,
        path: impl AsRef<str>,
        expected_revision: crate::ConfigRevision,
        operations: Vec<crate::ConfigOperation>,
    ) -> Result<crate::EntryConfigView, LoaderControlError> {
        self.ensure_alive()?;
        crate::config_control::mutate(&self.tree, path.as_ref(), expected_revision, operations)
            .await
    }
    pub fn snapshot(&self) -> Result<LoaderSnapshot, LoaderControlError> {
        self.ensure_alive()?;
        Ok(self.tree.local_snapshot())
    }

    pub async fn await_idle(&self) -> Result<LoaderSnapshot, LoaderControlError> {
        self.ensure_alive()?;
        let snapshot = self.tree.await_operations_idle().await;
        self.ensure_alive()?;
        Ok(snapshot?)
    }

    pub async fn reload(&self) -> Result<LoaderSnapshot, LoaderControlError> {
        self.ensure_alive()?;
        Ok(self.tree.reload().await?)
    }

    pub async fn create(
        &self,
        entry: EntryOptions,
        parent: Option<&str>,
        position: Option<usize>,
    ) -> Result<LoaderSnapshot, LoaderControlError> {
        self.ensure_alive()?;
        if let Some(parent) = parent {
            self.reject_nested_path(parent)?;
        }
        let parent = parent.map(str::to_owned);
        mutate_tree(&self.tree, move |config| {
            insert_entry(&mut config.extensions, parent.as_deref(), position, entry)
        })
        .await
    }

    pub async fn update(
        &self,
        path: impl AsRef<str>,
        patch: EntryUpdate,
        parent: Option<Option<&str>>,
        position: Option<usize>,
    ) -> Result<LoaderSnapshot, LoaderControlError> {
        self.ensure_alive()?;
        let path = path.as_ref().to_owned();
        self.reject_nested_path(&path)?;
        if let Some(Some(parent)) = parent {
            self.reject_nested_path(parent)?;
        }
        let parent = parent.map(|parent| parent.map(str::to_owned));
        mutate_tree(&self.tree, move |config| {
            let destination = if let Some(parent) = parent.as_ref() {
                let parent = parent.as_deref();
                if parent.is_some_and(|parent| {
                    parent == path || parent.starts_with(&(path.clone() + ":"))
                }) {
                    return Err(LoaderControlError::InvalidMove { path: path.clone() });
                }
                let entry = remove_entry(&mut config.extensions, &path)?;
                let next_path = join_path(parent, &entry.id);
                insert_entry(&mut config.extensions, parent, position, entry)?;
                next_path
            } else {
                path.clone()
            };
            patch_entry(&mut config.extensions, &destination, patch)
        })
        .await
    }

    pub async fn remove(
        &self,
        path: impl AsRef<str>,
    ) -> Result<LoaderSnapshot, LoaderControlError> {
        self.ensure_alive()?;
        let path = path.as_ref().to_owned();
        self.reject_nested_path(&path)?;
        mutate_tree(&self.tree, move |config| {
            remove_entry(&mut config.extensions, &path).map(|_| ())
        })
        .await
    }

    /// 由 Include dispose 调用：后序释放并注销。
    pub async fn detach(self) -> Result<(), LoaderControlError> {
        // 控制面已关闭不代表提交已经结算。停机时也必须等待来源树操作并注销资源。
        self.loader.detach_subtree(&self.tree).await?;
        Ok(())
    }

    fn ensure_alive(&self) -> Result<(), LoaderControlError> {
        if self.loader.alive.load(std::sync::atomic::Ordering::Acquire)
            && !self.tree.closing.load(std::sync::atomic::Ordering::Acquire)
            && !self.tree.owner.is_disposed()
        {
            Ok(())
        } else {
            Err(LoaderControlError::LoaderUnavailable)
        }
    }
}

fn reject_cross_tree(inner: &LoaderInner, parent: Option<&str>) -> Result<(), LoaderControlError> {
    if let Some(parent) = parent
        && inner.subtree_owning_path(parent).is_some()
    {
        return Err(LoaderControlError::CrossTreeMove {
            path: parent.into(),
        });
    }
    Ok(())
}

fn config_local_path<'a>(tree: &RuntimeTree, full: &'a str) -> &'a str {
    if tree.prefix.as_str().is_empty() {
        full
    } else {
        &full[tree.prefix.as_str().len() + 1..]
    }
}

async fn mutate_tree<F>(
    tree: &Arc<RuntimeTree>,
    mutate: F,
) -> Result<LoaderSnapshot, LoaderControlError>
where
    F: FnOnce(&mut ExtensionsConfig) -> Result<(), LoaderControlError> + Send + 'static,
{
    let tree = tree.clone();
    tokio::spawn(async move {
        let _operation = tree.operations.lock().await;
        tree.ensure_open()?;
        tree.await_idle().await?;
        ensure_tree_revision_control(&tree).await?;
        let mut target = tree.desired.lock().expect("desired").clone();
        mutate(&mut target)?;
        commit_tree_locked(&tree, target, None).await
    })
    .await
    .map_err(|_| LoaderControlError::ConfigOperationAborted)?
}

/// 唯一提交路径：来源检查与预检在生命周期前，来源写回在协调成功后。
pub(crate) async fn commit_tree_locked(
    tree: &Arc<RuntimeTree>,
    target: ExtensionsConfig,
    prepared: Option<(EntryId, crate::PreparedExtension)>,
) -> Result<LoaderSnapshot, LoaderControlError> {
    tree.ensure_open()?;
    let source = tree.source.lock().expect("source").clone();
    if let Some(source) = &source {
        source
            .validate_write(&target)
            .await
            .map_err(|error| LoaderControlError::from_source(source.identity(), error))?;
    }
    let snapshot = tree.apply_prepared(target.clone(), None, prepared).await?;
    let Some(source) = source else {
        return Ok(snapshot);
    };
    let expected = tree
        .revision
        .lock()
        .expect("revision")
        .clone()
        .ok_or(LoaderError::NoConfigSource)?;
    match source.write(&target, &expected).await {
        Ok(revision) => {
            *tree.revision.lock().expect("revision") = Some(revision);
            Ok(snapshot)
        }
        Err(error) => Err(LoaderControlError::Persist {
            snapshot: Box::new(snapshot),
            source_id: source.identity(),
            message: error.to_string(),
        }),
    }
}

fn path_parts(path: &str) -> Result<Vec<&str>, LoaderControlError> {
    let parts = path
        .split(':')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if parts.is_empty() || parts.join(":") != path {
        Err(LoaderControlError::EntryNotFound { path: path.into() })
    } else {
        Ok(parts)
    }
}

fn join_path(parent: Option<&str>, id: &str) -> String {
    parent.map_or_else(|| id.into(), |parent| format!("{parent}:{id}"))
}

fn insert_entry(
    entries: &mut Vec<EntryOptions>,
    parent: Option<&str>,
    position: Option<usize>,
    entry: EntryOptions,
) -> Result<(), LoaderControlError> {
    match parent {
        None => insert_local(entries, position, entry, None),
        Some(parent) => insert_nested(entries, &path_parts(parent)?, parent, position, entry),
    }
}

fn insert_local(
    entries: &mut Vec<EntryOptions>,
    position: Option<usize>,
    entry: EntryOptions,
    parent: Option<&str>,
) -> Result<(), LoaderControlError> {
    if entries.iter().any(|item| item.id == entry.id) {
        return Err(LoaderControlError::EntryExists {
            path: join_path(parent, &entry.id),
        });
    }
    let index = position.unwrap_or(entries.len()).min(entries.len());
    entries.insert(index, entry);
    Ok(())
}

fn insert_nested(
    entries: &mut [EntryOptions],
    parts: &[&str],
    full_parent: &str,
    position: Option<usize>,
    entry: EntryOptions,
) -> Result<(), LoaderControlError> {
    let group = entries
        .iter_mut()
        .find(|item| item.id == parts[0])
        .ok_or_else(|| LoaderControlError::ParentNotGroup {
            path: full_parent.into(),
        })?;
    if !group.group {
        return Err(LoaderControlError::ParentNotGroup {
            path: full_parent.into(),
        });
    }
    let mut children = group.children()?;
    if parts.len() == 1 {
        insert_local(&mut children, position, entry, Some(full_parent))?;
    } else {
        insert_nested(&mut children, &parts[1..], full_parent, position, entry)?;
    }
    group.set_children(children)?;
    Ok(())
}

fn remove_entry(
    entries: &mut Vec<EntryOptions>,
    path: &str,
) -> Result<EntryOptions, LoaderControlError> {
    remove_nested(entries, &path_parts(path)?, path)
}

#[allow(clippy::ptr_arg)]
fn remove_nested(
    entries: &mut Vec<EntryOptions>,
    parts: &[&str],
    full_path: &str,
) -> Result<EntryOptions, LoaderControlError> {
    let index = entries
        .iter()
        .position(|item| item.id == parts[0])
        .ok_or_else(|| LoaderControlError::EntryNotFound {
            path: full_path.into(),
        })?;
    if parts.len() == 1 {
        return Ok(entries.remove(index));
    }
    let group = &mut entries[index];
    if !group.group {
        return Err(LoaderControlError::EntryNotFound {
            path: full_path.into(),
        });
    }
    let mut children = group.children()?;
    let result = remove_nested(&mut children, &parts[1..], full_path)?;
    group.set_children(children)?;
    Ok(result)
}

pub(crate) fn patch_entry(
    entries: &mut [EntryOptions],
    path: &str,
    patch: EntryUpdate,
) -> Result<(), LoaderControlError> {
    patch_nested(entries, &path_parts(path)?, path, patch)
}

fn patch_nested(
    entries: &mut [EntryOptions],
    parts: &[&str],
    full_path: &str,
    patch: EntryUpdate,
) -> Result<(), LoaderControlError> {
    let entry = entries
        .iter_mut()
        .find(|item| item.id == parts[0])
        .ok_or_else(|| LoaderControlError::EntryNotFound {
            path: full_path.into(),
        })?;
    if parts.len() == 1 {
        if let Some(config) = patch.config {
            entry.config = config;
        }
        if let Some(disabled) = patch.disabled {
            entry.disabled = disabled;
        }
        if let Some(inject) = patch.inject {
            entry.inject = inject;
        }
        if let Some(isolate) = patch.isolate {
            entry.isolate = isolate;
        }
        return Ok(());
    }
    if !entry.group {
        return Err(LoaderControlError::EntryNotFound {
            path: full_path.into(),
        });
    }
    let mut children = entry.children()?;
    patch_nested(&mut children, &parts[1..], full_path, patch)?;
    entry.set_children(children)?;
    Ok(())
}
