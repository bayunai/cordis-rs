//! EntryTree 管理控制面（根树与可附着子树）。

use crate::{
    EntryTreeSource,
    catalog::{FactoryDescriptor, InjectionDescriptorInfo, IsolationDescriptorInfo},
    config::{EntryOptions, ExtensionsConfig, InjectConfig, IsolateConfig},
    error::{ConfigProjectionError, LoaderError},
    plugin::LoaderInner,
    snapshot::{EntryId, LoaderSnapshot},
    tree::{RuntimeTree, ensure_tree_revision_control},
};
use cordis_core::{Context, ServiceKey};
use std::sync::Arc;
use tokio::sync::broadcast;
#[path = "entry_move.rs"]
mod entry_move;

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

/// 跨树移动一侧的持久化结果，不包含配置正文。
#[derive(Debug, Clone)]
pub enum MovePersistence {
    NotRequired,
    Written {
        source_id: String,
    },
    Failed {
        source_id: String,
        error: Box<crate::SourceError>,
    },
    NotAttempted {
        source_id: String,
    },
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
    #[error("条目移动已提交但未完全结算；源：{origin:?}；目标：{destination:?}；生命周期错误数：{count}", count = .lifecycle_errors.len())]
    Move {
        snapshot: Box<LoaderSnapshot>,
        origin: MovePersistence,
        destination: MovePersistence,
        lifecycle_errors: Vec<LoaderError>,
    },
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

    /// 个别 Entry 挂载/清理失败；组成变更通常已生效，调用方可继续。
    pub fn is_entry_lifecycle_failure(&self) -> bool {
        match self {
            Self::Loader(error) => error.is_entry_lifecycle_failure(),
            Self::Move {
                lifecycle_errors,
                origin,
                destination,
                ..
            } => {
                !lifecycle_errors.is_empty()
                    && lifecycle_errors
                        .iter()
                        .all(LoaderError::is_entry_lifecycle_failure)
                    && !matches!(
                        origin,
                        MovePersistence::Failed { .. } | MovePersistence::NotAttempted { .. }
                    )
                    && !matches!(
                        destination,
                        MovePersistence::Failed { .. } | MovePersistence::NotAttempted { .. }
                    )
            }
            _ => false,
        }
    }
}

#[derive(Clone)]
pub struct Loader {
    pub(crate) inner: Arc<LoaderInner>,
}

impl Loader {
    /// 串行执行跨树配置事务。回调内只使用传入的事务控制面，不能递归调用
    /// Loader/LoaderSubtree 变更方法；所有直接 CRUD、配置提交及 reload 均等待本边界。
    /// 已接受的事务不因调用方取消等待而中断。事务不提供业务副作用回滚。
    pub async fn config_transaction<F, T>(&self, operation: F) -> Result<T, LoaderControlError>
    where
        T: Send + 'static,
        F: for<'a> FnOnce(
                &'a ConfigTransaction,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<T, LoaderControlError>> + Send + 'a>,
            > + Send
            + 'static,
    {
        let loader = self.clone();
        tokio::spawn(async move {
            let inner = loader.inner()?;
            let _control = inner.config_operations.lock().await;
            loader.inner()?;
            operation(&ConfigTransaction { loader }).await
        })
        .await
        .map_err(|_| LoaderControlError::ConfigOperationAborted)?
    }

    /// 受信任读取完整路径上的原始配置与 Catalog Schema；可含明文秘密。
    ///
    /// 不要求 Fiber Active。不得把返回值原样发给浏览器；表单投影由 settings 插件完成。
    pub async fn snapshot_entry_config(
        &self,
        path: impl AsRef<str>,
    ) -> Result<crate::EntryConfigSnapshot, LoaderControlError> {
        let inner = self.inner()?;
        let _control = inner.config_operations.lock().await;
        let path = path.as_ref();
        let (tree, local) = resolve_entry_path(&inner, path);
        crate::config_control::snapshot(&tree, &local).await
    }

    /// 整表替换 Entry 配置；expected 必须来自最近一次快照或表单描述。
    ///
    /// 已接受的提交不会因调用方取消等待而中断。普通/热更新沿用 Factory 策略；
    /// Persist 表示运行时已经生效但文件写回失败，不提供副作用回滚。
    pub async fn commit_entry_config(
        &self,
        path: impl AsRef<str>,
        expected_revision: crate::ConfigRevision,
        config: toml::Value,
    ) -> Result<crate::EntryConfigSnapshot, LoaderControlError> {
        let inner = self.inner()?;
        let path = path.as_ref();
        let (tree, local) = resolve_entry_path(&inner, path);
        crate::config_control::commit(&tree, &local, expected_revision, config).await
    }

    pub(crate) fn new(inner: Arc<LoaderInner>) -> Self {
        Self { inner }
    }

    /// 内存根中已通过预检的声明；业务挂载失败仍保留以供修复，不代表均为 Active。
    pub fn entries(&self) -> Result<ExtensionsConfig, LoaderControlError> {
        Ok(self.inner()?.root.desired.lock().expect("desired").clone())
    }

    pub fn entry_context(&self, path: impl AsRef<str>) -> Result<Context, LoaderControlError> {
        Ok(self.inner()?.entry_context(path.as_ref())?)
    }

    pub fn factories(&self) -> Result<Vec<FactoryDescriptor>, LoaderControlError> {
        Ok(self.inner()?.catalog.factories()?)
    }

    /// 受信任：对 Factory 执行 `prepare` 校验，不修改条目树。
    /// 表单路径在写入 user / reconcile 前调用，使禁用条目也会校验（执行业务 `prepare`）。
    pub fn prepare_entry_config(
        &self,
        factory: &str,
        config: &toml::Value,
    ) -> Result<(), LoaderControlError> {
        self.inner()?
            .catalog
            .prepare(factory, config)
            .map(|_| ())
            .map_err(map_config_type_error)
    }

    /// 类型投影（含 Serde 默认）；不调用业务 `prepare`，不修改条目树。
    pub fn project_entry_config(
        &self,
        factory: &str,
        config: &toml::Value,
    ) -> Result<serde_json::Value, ConfigProjectionError> {
        self.inner()
            .map_err(|_| ConfigProjectionError::LoaderUnavailable)?
            .catalog
            .project(factory, config)
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
        let inner = self.inner()?;
        let _control = inner.config_operations.lock().await;
        let snapshot = inner.await_idle_all().await;
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

    /// 在完整路径所指树上创建条目。`parent` 为 Include 载体路径时插入该来源树顶层。
    pub async fn create(
        &self,
        entry: EntryOptions,
        parent: Option<&str>,
        position: Option<usize>,
    ) -> Result<LoaderSnapshot, LoaderControlError> {
        let inner = self.inner()?;
        let (tree, parent_local) = match parent {
            None => (inner.root.clone(), None),
            Some(parent) => resolve_parent_path(&inner, parent),
        };
        let retain = Arc::ptr_eq(&tree, &inner.root);
        mutate_tree(&tree, retain, move |config| {
            insert_entry(
                &mut config.extensions,
                parent_local.as_deref(),
                position,
                entry,
            )
        })
        .await
        .map(|_| inner.aggregate_snapshot())
    }

    /// 按 Include 地址更新或移动。`Some(None)` 移到 Loader 根树，
    /// `Some(Some(parent))` 指定目标 Group 或 Include 树根；`None` 保持父级。
    /// 更换父级会完整释放旧轮再挂载；调用方取消等待不会中断已接受的操作。
    pub async fn update(
        &self,
        path: impl AsRef<str>,
        patch: EntryUpdate,
        parent: Option<Option<&str>>,
        position: Option<usize>,
    ) -> Result<LoaderSnapshot, LoaderControlError> {
        entry_move::update(
            self.inner()?,
            path.as_ref().to_owned(),
            patch,
            parent.map(|parent| parent.map(str::to_owned)),
            position,
            None,
        )
        .await
    }

    /// 按完整路径删除条目。
    pub async fn remove(
        &self,
        path: impl AsRef<str>,
    ) -> Result<LoaderSnapshot, LoaderControlError> {
        let path = path.as_ref().to_owned();
        let inner = self.inner()?;
        let (tree, local_path) = resolve_entry_path(&inner, &path);
        mutate_tree(&tree, false, move |config| {
            remove_entry(&mut config.extensions, &local_path).map(|_| ())
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

/// 配置事务的受限控制面；不能克隆、不能逃逸回调，也不暴露互斥锁。
pub struct ConfigTransaction {
    loader: Loader,
}

impl ConfigTransaction {
    pub async fn snapshot_entry_config(
        &self,
        path: impl AsRef<str>,
    ) -> Result<crate::EntryConfigSnapshot, LoaderControlError> {
        let inner = self.loader.inner()?;
        let path = path.as_ref();
        let (tree, local) = resolve_entry_path(&inner, path);
        crate::config_control::snapshot(&tree, &local).await
    }

    pub async fn await_idle(&self) -> Result<LoaderSnapshot, LoaderControlError> {
        Ok(self.loader.inner()?.await_idle_all().await?)
    }

    pub fn prepare_entry_config(
        &self,
        factory: &str,
        config: &toml::Value,
    ) -> Result<(), LoaderControlError> {
        self.loader.prepare_entry_config(factory, config)
    }

    /// 提交载体配置时指定本次必须激活的子条目。
    pub async fn commit_entry_config_requiring(
        &self,
        path: impl AsRef<str>,
        expected: crate::ConfigRevision,
        config: toml::Value,
        required: impl AsRef<str>,
    ) -> Result<crate::EntryConfigSnapshot, LoaderControlError> {
        let inner = self.loader.inner()?;
        let (tree, local) = resolve_entry_path(&inner, path.as_ref());
        crate::config_control::commit_owned(
            &tree,
            &local,
            expected,
            config,
            Some(EntryId::from(required.as_ref())),
        )
        .await
    }

    /// 复用既有整表提交链路，不再次获取事务准入锁。
    pub async fn commit_entry_config(
        &self,
        path: impl AsRef<str>,
        expected: crate::ConfigRevision,
        config: toml::Value,
    ) -> Result<crate::EntryConfigSnapshot, LoaderControlError> {
        let inner = self.loader.inner()?;
        let path = path.as_ref();
        let (tree, local) = resolve_entry_path(&inner, path);
        crate::config_control::commit_owned(&tree, &local, expected, config, None).await
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
            return Err(LoaderControlError::EntryNotFound { path: full });
        }
        Ok(())
    }
    /// 已加载的该来源树声明，不代表每个节点均已 Active。
    pub fn entries(&self) -> Result<ExtensionsConfig, LoaderControlError> {
        self.ensure_alive()?;
        Ok(self.tree.desired.lock().expect("desired").clone())
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
        mutate_tree(&self.tree, false, move |config| {
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
        let prefix = self.tree.prefix.to_string();
        let full = format!("{prefix}:{path}");
        let parent = parent.map(|parent| {
            Some(match parent {
                Some(parent) => format!("{prefix}:{parent}"),
                None => prefix.clone(),
            })
        });
        entry_move::update(
            self.loader.clone(),
            full,
            patch,
            parent,
            position,
            Some(self.tree.clone()),
        )
        .await
        .map(|_| self.tree.local_snapshot())
    }

    pub async fn remove(
        &self,
        path: impl AsRef<str>,
    ) -> Result<LoaderSnapshot, LoaderControlError> {
        self.ensure_alive()?;
        let path = path.as_ref().to_owned();
        self.reject_nested_path(&path)?;
        mutate_tree(&self.tree, false, move |config| {
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

/// 条目完整路径 → 所属树与树内局部路径。Include 载体本身落在父树（通常为内存根）。
fn resolve_entry_path(inner: &LoaderInner, full: &str) -> (Arc<RuntimeTree>, String) {
    if let Some(tree) = inner.subtree_owning_path(full) {
        let local = config_local_path(&tree, full).to_owned();
        (tree, local)
    } else {
        (inner.root.clone(), full.to_owned())
    }
}

/// 父 Group / 来源树根的完整路径 → 所属树与局部 parent（`None` = 该树顶层）。
fn resolve_parent_path(inner: &LoaderInner, full: &str) -> (Arc<RuntimeTree>, Option<String>) {
    if let Some(tree) = inner.subtree_at_prefix(full) {
        return (tree, None);
    }
    let (tree, local) = resolve_entry_path(inner, full);
    if local.is_empty() {
        (tree, None)
    } else {
        (tree, Some(local))
    }
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
    retain_created_declaration: bool,
    mutate: F,
) -> Result<LoaderSnapshot, LoaderControlError>
where
    F: FnOnce(&mut ExtensionsConfig) -> Result<(), LoaderControlError> + Send + 'static,
{
    let tree = tree.clone();
    tokio::spawn(async move {
        let loader = tree
            .loader
            .lock()
            .expect("loader weak")
            .upgrade()
            .ok_or(LoaderControlError::LoaderUnavailable)?;
        let _control = loader.config_operations.lock().await;
        let _operation = tree.operations.lock().await;
        tree.ensure_open()?;
        tree.await_idle().await?;
        ensure_tree_revision_control(&tree).await?;
        let mut target = tree.desired.lock().expect("desired").clone();
        mutate(&mut target)?;
        let retained = retain_created_declaration.then(|| target.clone());
        let result = commit_tree_locked(&tree, target, None, None).await;
        if let (Some(declaration), Err(LoaderControlError::Loader(error))) = (retained, &result)
            && error.is_entry_lifecycle_failure()
        {
            // create 的业务失败仍保留可修复声明；更新失败和来源树提交规则不变。
            *tree.desired.lock().expect("desired") = declaration;
        }
        result
    })
    .await
    .map_err(|_| LoaderControlError::ConfigOperationAborted)?
}

/// 唯一提交路径：来源检查与预检在生命周期前，来源写回在协调成功后。
pub(crate) async fn commit_tree_locked(
    tree: &Arc<RuntimeTree>,
    target: ExtensionsConfig,
    prepared: Option<(EntryId, crate::PreparedExtension)>,
    required: Option<EntryId>,
) -> Result<LoaderSnapshot, LoaderControlError> {
    tree.ensure_open()?;
    let source = tree.source.lock().expect("source").clone();
    if let Some(source) = &source {
        source
            .validate_write(&target)
            .await
            .map_err(|error| LoaderControlError::from_source(source.identity(), error))?;
    }
    let snapshot = tree
        .apply_prepared(target.clone(), None, prepared, required)
        .await?;
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
        Err(error) => {
            tree.mark_unsettled();
            Err(LoaderControlError::Persist {
                snapshot: Box::new(snapshot),
                source_id: source.identity(),
                message: error.to_string(),
            })
        }
    }
}

/// 在同一 EntryTree 的嵌套声明中按本地 ID 查找；Group 不是地址命名空间。
pub(crate) fn find_entry(
    entries: &[EntryOptions],
    id: &str,
) -> Result<EntryOptions, LoaderControlError> {
    if id.is_empty() || id.contains(':') {
        return Err(LoaderControlError::EntryNotFound { path: id.into() });
    }
    fn find(
        entries: &[EntryOptions],
        id: &str,
    ) -> Result<Option<EntryOptions>, LoaderControlError> {
        for entry in entries {
            if entry.id == id {
                return Ok(Some(entry.clone()));
            }
            if entry.group
                && let Some(found) = find(&entry.children()?, id)?
            {
                return Ok(Some(found));
            }
        }
        Ok(None)
    }
    find(entries, id)?.ok_or_else(|| LoaderControlError::EntryNotFound { path: id.into() })
}

fn edit_entry(
    entries: &mut [EntryOptions],
    id: &str,
    edit: &mut impl FnMut(&mut EntryOptions) -> Result<(), LoaderControlError>,
) -> Result<bool, LoaderControlError> {
    for entry in entries {
        if entry.id == id {
            edit(entry)?;
            return Ok(true);
        }
        if entry.group {
            let mut children = entry.children()?;
            if edit_entry(&mut children, id, edit)? {
                entry.set_children(children)?;
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn insert_entry(
    entries: &mut Vec<EntryOptions>,
    parent: Option<&str>,
    position: Option<usize>,
    entry: EntryOptions,
) -> Result<(), LoaderControlError> {
    match find_entry(entries, &entry.id) {
        Ok(_) => return Err(LoaderControlError::EntryExists { path: entry.id }),
        Err(LoaderControlError::EntryNotFound { .. }) => {}
        Err(error) => return Err(error),
    }
    let Some(parent) = parent else {
        let index = position.unwrap_or(entries.len()).min(entries.len());
        entries.insert(index, entry);
        return Ok(());
    };
    if parent.is_empty() || parent.contains(':') {
        return Err(LoaderControlError::ParentNotGroup {
            path: parent.into(),
        });
    }
    let mut candidate = Some(entry);
    if !edit_entry(entries, parent, &mut |group| {
        if !group.group {
            return Err(LoaderControlError::ParentNotGroup {
                path: parent.into(),
            });
        }
        let mut children = group.children()?;
        let index = position.unwrap_or(children.len()).min(children.len());
        children.insert(index, candidate.take().expect("one matching group"));
        group.set_children(children)?;
        Ok(())
    })? {
        return Err(LoaderControlError::ParentNotGroup {
            path: parent.into(),
        });
    }
    Ok(())
}

fn remove_entry(
    entries: &mut Vec<EntryOptions>,
    id: &str,
) -> Result<EntryOptions, LoaderControlError> {
    find_entry(entries, id)?;
    fn remove(
        entries: &mut Vec<EntryOptions>,
        id: &str,
    ) -> Result<Option<EntryOptions>, LoaderControlError> {
        if let Some(index) = entries.iter().position(|entry| entry.id == id) {
            return Ok(Some(entries.remove(index)));
        }
        for entry in entries {
            if entry.group {
                let mut children = entry.children()?;
                if let Some(found) = remove(&mut children, id)? {
                    entry.set_children(children)?;
                    return Ok(Some(found));
                }
            }
        }
        Ok(None)
    }
    remove(entries, id)?.ok_or_else(|| LoaderControlError::EntryNotFound { path: id.into() })
}

fn move_local(
    config: &mut ExtensionsConfig,
    path: &str,
    parent: Option<&str>,
    position: Option<usize>,
) -> Result<(), LoaderControlError> {
    let entry = find_entry(&config.extensions, path)?;
    if let Some(parent) = parent {
        let descendant = if entry.group {
            match find_entry(&entry.children()?, parent) {
                Ok(_) => true,
                Err(LoaderControlError::EntryNotFound { .. }) => false,
                Err(error) => return Err(error),
            }
        } else {
            false
        };
        if parent == path || descendant {
            return Err(LoaderControlError::InvalidMove { path: path.into() });
        }
    }
    let entry = remove_entry(&mut config.extensions, path)?;
    insert_entry(&mut config.extensions, parent, position, entry)
}

fn map_config_type_error(error: LoaderError) -> LoaderControlError {
    match error {
        LoaderError::InvalidConfig { .. }
        | LoaderError::FactoryPanic { .. }
        | LoaderError::PluginBuild { .. }
        | LoaderError::UnknownFactory { .. } => LoaderControlError::ConfigValidation,
        other => LoaderControlError::Loader(other),
    }
}

pub(crate) fn patch_entry(
    entries: &mut [EntryOptions],
    path: &str,
    patch: EntryUpdate,
) -> Result<(), LoaderControlError> {
    find_entry(entries, path)?;
    edit_entry(entries, path, &mut |entry| {
        if let Some(config) = &patch.config {
            entry.config = config.clone();
        }
        if let Some(disabled) = patch.disabled {
            entry.disabled = disabled;
        }
        if let Some(inject) = &patch.inject {
            entry.inject = inject.clone();
        }
        if let Some(isolate) = &patch.isolate {
            entry.isolate = isolate.clone();
        }
        Ok(())
    })?;
    Ok(())
}
