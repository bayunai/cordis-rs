//! 单棵可附着的运行时 EntryTree（根树或 Include 子树）。

use crate::{
    EntryTreeSource, PreparedExtension, SourceRevision,
    catalog::{ExtensionCatalog, NamedIsolationLabels},
    config::{EntryOptions, ExtensionsConfig},
    config_control::ConfigRevision,
    error::LoaderError,
    meta::{ENTRY_LOCATION, EntryLocation},
    plugin::RuntimeEntry,
    reconcile::{
        DesiredNode, ReconcilePlan, build_plan_plugins, index_desired_tree, plan_reconcile,
    },
    snapshot::{EntryId, EntrySnapshot, LoaderSnapshot},
};
use cordis_core::{Context, Fiber};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

pub(crate) struct ReconcileCompletion {
    notify: Notify,
    result: Mutex<Option<Result<LoaderSnapshot, LoaderError>>>,
    retain: Mutex<Option<JoinHandle<()>>>,
}

impl ReconcileCompletion {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            notify: Notify::new(),
            result: Mutex::new(None),
            retain: Mutex::new(None),
        })
    }

    fn attach(&self, task: JoinHandle<()>) {
        *self.retain.lock().expect("reconcile retain") = Some(task);
    }

    fn finish(&self, result: Result<LoaderSnapshot, LoaderError>) {
        let mut result_slot = self.result.lock().expect("reconcile completion");
        if result_slot.is_none() {
            *result_slot = Some(result);
            self.notify.notify_waiters();
        }
    }

    pub(crate) async fn wait(self: &Arc<Self>) -> Result<LoaderSnapshot, LoaderError> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(result) = self.result.lock().expect("reconcile completion").as_ref() {
                return result.clone();
            }
            notified.await;
        }
    }
}

/// 一棵内存条目树，可选来源对象提供读取、版本检查及持久化。
pub(crate) struct RuntimeTree {
    pub(crate) lifecycle_logs: crate::lifecycle_logs::LifecycleLogs,
    pub(crate) config_identity: u128,
    pub(crate) config_generation: std::sync::atomic::AtomicU64,
    pub(crate) catalog: ExtensionCatalog,
    /// 弱引用宿主，用于附着/注销子树；根树在构造后由 LoaderInner 回填。
    pub(crate) loader: Mutex<Weak<super::plugin::LoaderInner>>,
    pub(crate) owner: Context,
    pub(crate) prefix: EntryId,
    pub(crate) source: Mutex<Option<Arc<dyn EntryTreeSource>>>,
    pub(crate) source_id: Option<String>,
    pub(crate) parent_source: Option<String>,
    pub(crate) parent_tree: Weak<RuntimeTree>,
    pub(crate) release: Mutex<Option<Arc<crate::release::ReleaseCompletion>>>,
    pub(crate) closing: std::sync::atomic::AtomicBool,
    unsettled: std::sync::atomic::AtomicBool,
    pub(crate) desired: Mutex<ExtensionsConfig>,
    pub(crate) revision: Mutex<Option<SourceRevision>>,
    pub(crate) entries: Mutex<BTreeMap<EntryId, RuntimeEntry>>,
    pub(crate) order: Mutex<Vec<EntryId>>,
    pub(crate) named_labels: Mutex<NamedIsolationLabels>,
    reconcile: Mutex<Option<Arc<ReconcileCompletion>>>,
    pub(crate) operations: tokio::sync::Mutex<()>,
}

/// 完整预检结果可跨提交边界保留，避免树载体更新在其他生命周期开始后才构造子插件。
pub(crate) struct PreparedTreePlan {
    desired: BTreeMap<EntryId, DesiredNode>,
    plan: ReconcilePlan,
    config: ExtensionsConfig,
    pub(crate) revision: Option<SourceRevision>,
}

impl RuntimeTree {
    pub(crate) fn new_root(
        catalog: ExtensionCatalog,
        owner: Context,
    ) -> Result<Arc<Self>, LoaderError> {
        Ok(Arc::new(Self {
            lifecycle_logs: crate::lifecycle_logs::LifecycleLogs::new(
                owner
                    .logger_named("loader")
                    .map_err(|source| LoaderError::Runtime { source })?,
            ),
            config_identity: next_config_identity()?,
            config_generation: std::sync::atomic::AtomicU64::new(0),
            catalog,
            loader: Mutex::new(Weak::new()),
            owner,
            prefix: EntryId::from(""),
            source: Mutex::new(None),
            source_id: None,
            parent_source: None,
            parent_tree: Weak::new(),
            release: Mutex::new(None),
            closing: std::sync::atomic::AtomicBool::new(false),
            unsettled: std::sync::atomic::AtomicBool::new(false),
            desired: Mutex::new(ExtensionsConfig {
                version: crate::config::CONFIG_VERSION,
                extensions: Vec::new(),
            }),
            revision: Mutex::new(None),
            entries: Mutex::new(BTreeMap::new()),
            order: Mutex::new(Vec::new()),
            named_labels: Mutex::new(NamedIsolationLabels::new()),
            reconcile: Mutex::new(None),
            operations: tokio::sync::Mutex::new(()),
        }))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_subtree(
        catalog: ExtensionCatalog,
        loader: Weak<super::plugin::LoaderInner>,
        owner: Context,
        prefix: EntryId,
        source: Arc<dyn EntryTreeSource>,
        parent_source: Option<String>,
        parent_tree: Weak<RuntimeTree>,
        revision: SourceRevision,
    ) -> Result<Arc<Self>, LoaderError> {
        let source_id = source.identity();
        let owner = source.context(&owner)?;
        Ok(Arc::new(Self {
            lifecycle_logs: crate::lifecycle_logs::LifecycleLogs::new(
                owner
                    .logger_named("loader")
                    .map_err(|source| LoaderError::Runtime { source })?,
            ),
            config_identity: next_config_identity()?,
            config_generation: std::sync::atomic::AtomicU64::new(0),
            catalog,
            loader: Mutex::new(loader),
            owner,
            prefix,
            source: Mutex::new(Some(source)),
            source_id: Some(source_id),
            parent_source,
            parent_tree,
            release: Mutex::new(None),
            closing: std::sync::atomic::AtomicBool::new(false),
            unsettled: std::sync::atomic::AtomicBool::new(true),
            desired: Mutex::new(ExtensionsConfig {
                version: crate::config::CONFIG_VERSION,
                extensions: Vec::new(),
            }),
            revision: Mutex::new(Some(revision)),
            entries: Mutex::new(BTreeMap::new()),
            order: Mutex::new(Vec::new()),
            named_labels: Mutex::new(NamedIsolationLabels::new()),
            reconcile: Mutex::new(None),
            operations: tokio::sync::Mutex::new(()),
        }))
    }

    /// 来源树初始声明通过预检后，业务生命周期失败不撤销来源控制面。
    pub(crate) async fn initialize(
        self: &Arc<Self>,
        config: ExtensionsConfig,
    ) -> Result<LoaderSnapshot, LoaderError> {
        match self.apply(config.clone(), None).await {
            Ok(snapshot) => Ok(snapshot),
            Err(error) if error.is_entry_lifecycle_failure() => {
                *self.desired.lock().expect("desired") = config;
                Ok(self.local_snapshot())
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn apply(
        self: &Arc<Self>,
        config: ExtensionsConfig,
        revision: Option<SourceRevision>,
    ) -> Result<LoaderSnapshot, LoaderError> {
        self.apply_prepared(config, revision, None, None).await
    }

    pub(crate) fn config_revision(&self) -> ConfigRevision {
        ConfigRevision::new(
            self.config_identity,
            self.config_generation
                .load(std::sync::atomic::Ordering::Acquire),
        )
    }

    pub(crate) async fn apply_prepared(
        self: &Arc<Self>,
        config: ExtensionsConfig,
        revision: Option<SourceRevision>,
        prepared: Option<(EntryId, PreparedExtension)>,
        required: Option<EntryId>,
    ) -> Result<LoaderSnapshot, LoaderError> {
        let retry_failed = revision.is_some() && prepared.is_none();
        let plan = self
            .prepare_plan(config, revision, prepared, required, retry_failed)
            .await?;
        self.apply_plan(plan).await
    }

    pub(crate) fn prepare_plan(
        &self,
        config: ExtensionsConfig,
        revision: Option<SourceRevision>,
        prepared: Option<(EntryId, PreparedExtension)>,
        required: Option<EntryId>,
        retry_failed: bool,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<PreparedTreePlan, LoaderError>> + Send + '_>,
    > {
        // 来源定义可递归预检嵌套树，装箱打断 Future 类型递归。
        Box::pin(async move {
            let prefix = if self.prefix.as_str().is_empty() {
                None
            } else {
                Some(self.prefix.as_str())
            };
            let (mut desired, roots) = index_desired_tree(&self.catalog, &config, prefix)?;
            let required = required.or_else(|| prepared.as_ref().map(|(path, _)| path.clone()));
            let mut plan = {
                let entries = self.entries.lock().expect("entries");
                plan_reconcile(&entries, &desired, &roots, retry_failed, required.as_ref())?
            };
            let committed = self.desired.lock().expect("desired").clone();
            let (committed, _) = index_desired_tree(&self.catalog, &committed, prefix)?;
            plan.committed = committed
                .into_iter()
                .map(|(path, node)| (path, node.options))
                .collect();
            plan.required = required;
            build_plan_plugins(
                &self.catalog,
                &mut desired,
                &mut plan,
                &self.entries,
                prepared,
            )
            .await?;
            Ok(PreparedTreePlan {
                desired,
                plan,
                config,
                revision,
            })
        })
    }

    // 显式装箱打断“树载体协调嵌套树”的异步类型递归；不另起不受等待的业务任务。
    pub(crate) fn apply_plan(
        self: &Arc<Self>,
        prepared: PreparedTreePlan,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<LoaderSnapshot, LoaderError>> + Send + 'static>,
    > {
        let tree = self.clone();
        Box::pin(async move {
            tree.ensure_open()?;
            let PreparedTreePlan {
                desired,
                plan,
                config,
                revision,
            } = prepared;
            let completion = {
                let mut slot = tree.reconcile.lock().expect("reconcile slot");
                if slot.is_some() {
                    return Err(LoaderError::ReconcileBusy);
                }
                let completion = ReconcileCompletion::new();
                // 生命周期失败可能已经替换或释放部分实例，因此在协调开始时失效令牌，
                // 而不是仅在完整 desired 或配置文件成功提交后递增。
                tree.config_generation
                    .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                tree.unsettled
                    .store(true, std::sync::atomic::Ordering::Release);
                *slot = Some(completion.clone());
                tree.spawn_reconcile(completion.clone(), desired, plan, config, revision);
                completion
            };
            let result = completion.wait().await;
            if let Some(loader) = tree.loader.lock().expect("loader weak").upgrade() {
                loader.publish_snapshot_changed();
            }
            result
        })
    }

    /// 对外稳定边界包含异步来源预检与写回；内部已持锁的提交使用 await_idle。
    pub(crate) async fn await_operations_idle(
        self: &Arc<Self>,
    ) -> Result<LoaderSnapshot, LoaderError> {
        let _operation = self.operations.lock().await;
        self.await_idle().await
    }

    pub(crate) async fn await_idle(self: &Arc<Self>) -> Result<LoaderSnapshot, LoaderError> {
        let completion = { self.reconcile.lock().expect("reconcile slot").clone() };
        if let Some(completion) = completion {
            match completion.wait().await {
                Ok(_) => {}
                Err(error) if error.is_entry_lifecycle_failure() => {}
                Err(error) => return Err(error),
            }
        }
        Ok(self.local_snapshot())
    }

    fn spawn_reconcile(
        self: &Arc<Self>,
        completion: Arc<ReconcileCompletion>,
        desired: BTreeMap<EntryId, DesiredNode>,
        plan: ReconcilePlan,
        config: ExtensionsConfig,
        revision: Option<SourceRevision>,
    ) {
        let tree = self.clone();
        let task_completion = completion.clone();
        let task = tokio::runtime::Handle::current().spawn(async move {
            let worker = tree.clone();
            let result =
                match tokio::spawn(async move { worker.execute_reconcile(desired, plan).await })
                    .await
                {
                    Ok(result) => result,
                    Err(error) => Err(LoaderError::ReconcileAborted {
                        reason: format!("reconcile worker: {error}"),
                    }),
                };
            // 运行期失败可留下局部生命周期结果，但不能发布尚未提交的热值、
            // 来源版本或完整目标。初次声明保留由 initialize/create 单独处理。
            if result.is_ok() {
                *tree.desired.lock().expect("desired") = config;
                if let Some(revision) = revision {
                    *tree.revision.lock().expect("revision") = Some(revision);
                }
                tree.prune_named_isolation_labels();
                tree.unsettled
                    .store(false, std::sync::atomic::Ordering::Release);
            }
            let mut slot = tree.reconcile.lock().expect("reconcile slot");
            if slot
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &task_completion))
            {
                *slot = None;
            }
            drop(slot);
            task_completion.finish(result);
        });
        completion.attach(task);
    }

    pub(crate) fn local_snapshot(&self) -> LoaderSnapshot {
        let entries = self.entries.lock().expect("entries");
        let order = self.order.lock().expect("entry order");
        let mut paths = order.clone();
        for path in entries.keys() {
            if !paths.contains(path) {
                paths.push(path.clone());
            }
        }
        LoaderSnapshot {
            entries: paths
                .iter()
                .filter_map(|path| {
                    entries.get(path).map(|entry| EntrySnapshot {
                        path: path.clone(),
                        // 子树顶层项运行时挂载到 `owner`，其本地 parent 保持 None；
                        // 对外快照仍须表达它由 Include 条目承载。
                        parent: entry.parent.clone().or_else(|| {
                            (!self.prefix.as_str().is_empty()).then(|| self.prefix.clone())
                        }),
                        name: entry.name.clone(),
                        group: entry.group,
                        enabled: entry.enabled,
                        plugin_key: entry.plugin_key,
                        fiber_id: entry.fiber.as_ref().map(Fiber::id),
                        state: entry.fiber.as_ref().map(Fiber::state),
                        last_error: entry.fiber.as_ref().and_then(Fiber::last_error),
                    })
                })
                .collect(),
        }
    }

    pub(crate) fn entry_context(&self, path: &str) -> Option<Context> {
        self.entries
            .lock()
            .expect("entries")
            .get(&EntryId::from(path))
            .map(|entry| entry.context.clone())
    }

    pub(crate) fn prune_named_isolation_labels(&self) {
        let desired = self.desired.lock().expect("desired");
        let mut live = std::collections::HashSet::new();
        collect_named_isolation_refs(&self.catalog, &desired.extensions, &mut live);
        let mut labels = self.named_labels.lock().expect("named labels");
        labels.retain(|key, _| live.contains(key));
    }

    pub(crate) fn inject_entry_location(&self, path: &EntryId, context: Context) -> Context {
        let loader = self
            .loader
            .lock()
            .expect("loader weak")
            .upgrade()
            .map(crate::loader::Loader::new)
            .expect("loader alive during mount");
        context
            .intercept(
                ENTRY_LOCATION,
                EntryLocation {
                    path: path.to_string(),
                    tree_prefix: self.prefix.to_string(),
                    loader,
                },
            )
            .expect("entry location intercept")
    }

    /// 后序释放全部条目并清空运行时槽位（用于 detach）。
    pub(crate) async fn dispose_entries(self: &Arc<Self>) -> Result<(), LoaderError> {
        self.closing
            .store(true, std::sync::atomic::Ordering::Release);
        let _operation = self.operations.lock().await;
        // 等待进行中的 reconcile
        let mut first_error = None;
        if let Err(error) = self.await_idle().await {
            self.lifecycle_logs.error(&error);
            first_error = Some(error);
        }
        let paths: Vec<_> = {
            let entries = self.entries.lock().expect("entries");
            let mut paths: Vec<_> = entries.keys().cloned().collect();
            paths.sort_by(|left, right| {
                right
                    .as_str()
                    .matches(':')
                    .count()
                    .cmp(&left.as_str().matches(':').count())
                    .then_with(|| left.cmp(right))
            });
            paths
        };
        for path in paths {
            if let Err(error) = self.dispose_and_remove(&path).await {
                self.lifecycle_logs.error(&error);
                first_error.get_or_insert(error);
            }
        }
        *self.order.lock().expect("entry order") = Vec::new();
        *self.desired.lock().expect("desired") = ExtensionsConfig {
            version: crate::config::CONFIG_VERSION,
            extensions: Vec::new(),
        };
        self.named_labels.lock().expect("named labels").clear();
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub(crate) async fn reload(self: &Arc<Self>) -> Result<LoaderSnapshot, LoaderError> {
        let tree = self.clone();
        // read 可能初始化缺失来源。接受后保留整次 reload，取消等待不遗留文件工作。
        tokio::spawn(async move {
            let loader = tree.loader.lock().expect("loader weak").upgrade().ok_or(
                LoaderError::ReconcileAborted {
                    reason: "Loader 已释放".into(),
                },
            )?;
            let _control = loader.config_operations.lock().await;
            let _operation = tree.operations.lock().await;
            tree.ensure_open()?;
            let source = tree
                .source
                .lock()
                .expect("source")
                .clone()
                .ok_or(LoaderError::NoConfigSource)?;
            let document = source.read().await?;
            if tree.can_skip_reload(&document)? {
                return Ok(tree.local_snapshot());
            }
            tree.apply(document.config, Some(document.revision)).await
        })
        .await
        .map_err(|_| LoaderError::ReconcileAborted {
            reason: "来源重载任务异常结束".into(),
        })?
    }

    fn can_skip_reload(&self, document: &crate::SourceDocument) -> Result<bool, LoaderError> {
        if self.needs_reconcile()
            || self.revision.lock().expect("revision").as_ref() != Some(&document.revision)
            || *self.desired.lock().expect("desired") != document.config
        {
            return Ok(false);
        }
        let prefix = (!self.prefix.as_str().is_empty()).then_some(self.prefix.as_str());
        // 只建立结构差分，不调用 Factory。Pending 本身不导致重新构造。
        let (desired, roots) = index_desired_tree(&self.catalog, &document.config, prefix)?;
        let entries = self.entries.lock().expect("entries");
        Ok(plan_reconcile(&entries, &desired, &roots, true, None)?.is_empty())
    }

    pub(crate) fn needs_reconcile(&self) -> bool {
        self.unsettled.load(std::sync::atomic::Ordering::Acquire)
            || self.entries.lock().expect("entries").values().any(|entry| {
                entry.context.is_disposed()
                    || (entry.enabled && entry.fiber.is_none())
                    || entry
                        .binding
                        .as_ref()
                        .is_some_and(|binding| binding.needs_reconcile())
                    || entry.fiber.as_ref().is_some_and(|fiber| {
                        matches!(
                            fiber.state(),
                            cordis_core::FiberState::Loading
                                | cordis_core::FiberState::Unloading
                                | cordis_core::FiberState::Failed
                                | cordis_core::FiberState::Disposed
                        )
                    })
            })
    }

    pub(crate) fn mark_unsettled(&self) {
        self.unsettled
            .store(true, std::sync::atomic::Ordering::Release);
    }

    pub(crate) fn ensure_open(&self) -> Result<(), LoaderError> {
        if self.closing.load(std::sync::atomic::Ordering::Acquire) || self.owner.is_disposed() {
            Err(LoaderError::Source {
                source: crate::SourceError::new(
                    crate::SourceErrorKind::Unavailable,
                    "条目树已关闭",
                ),
            })
        } else {
            Ok(())
        }
    }
}

fn next_config_identity() -> Result<u128, LoaderError> {
    let mut bytes = [0; 16];
    getrandom::fill(&mut bytes).map_err(|_| LoaderError::ConfigIdentityUnavailable)?;
    Ok(u128::from_ne_bytes(bytes))
}

/// 供控制面把 revision 冲突转成 `ConfigConflict`。
pub(crate) async fn ensure_tree_revision_control(
    tree: &RuntimeTree,
) -> Result<(), crate::loader::LoaderControlError> {
    let source = tree.source.lock().expect("source").clone();
    if let Some(source) = source {
        let revision = tree
            .revision
            .lock()
            .expect("revision")
            .clone()
            .ok_or(LoaderError::NoConfigSource)?;
        source.check_revision(&revision).await.map_err(|error| {
            crate::loader::LoaderControlError::from_source(source.identity(), error)
        })?;
    }
    Ok(())
}

fn collect_named_isolation_refs(
    catalog: &ExtensionCatalog,
    entries: &[EntryOptions],
    out: &mut std::collections::HashSet<(cordis_core::ServiceId, String)>,
) {
    for entry in entries {
        catalog.collect_named_isolation_refs(entry.isolate.as_ref(), out);
        if entry.group
            && let Ok(children) = entry.children()
        {
            collect_named_isolation_refs(catalog, &children, out);
        }
    }
}

/// 按根树前序聚合：遇到附着子树的 Include 路径时插入其子树 order。
pub(crate) fn aggregate_snapshot(
    root: &RuntimeTree,
    subtrees_by_prefix: &HashMap<EntryId, Arc<RuntimeTree>>,
) -> LoaderSnapshot {
    let mut seen = std::collections::HashSet::new();
    let mut entries = Vec::new();

    fn append_tree(
        tree: &RuntimeTree,
        subtrees_by_prefix: &HashMap<EntryId, Arc<RuntimeTree>>,
        seen: &mut std::collections::HashSet<EntryId>,
        out: &mut Vec<EntrySnapshot>,
    ) {
        let local = tree.local_snapshot();
        for entry in local.entries {
            if !seen.insert(entry.path.clone()) {
                continue;
            }
            let path = entry.path.clone();
            out.push(entry);
            if let Some(child) = subtrees_by_prefix.get(&path) {
                append_tree(child, subtrees_by_prefix, seen, out);
            }
        }
    }

    append_tree(root, subtrees_by_prefix, &mut seen, &mut entries);
    LoaderSnapshot { entries }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cordis_core::{CoreError, Runtime};

    #[tokio::test]
    async fn idle_observer_waits_for_failed_reconcile_but_returns_actual_snapshot() {
        let runtime = Runtime::new().unwrap();
        let tree = RuntimeTree::new_root(ExtensionCatalog::new(), runtime.root()).unwrap();
        let completion = ReconcileCompletion::new();
        *tree.reconcile.lock().unwrap() = Some(completion.clone());
        let mut observer = Box::pin(tree.await_idle());
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut observer)
                .await
                .is_err()
        );
        completion.finish(Err(LoaderError::Lifecycle {
            instance: "broken".into(),
            source: CoreError::PluginApply("expected failure".into()),
        }));
        assert!(observer.await.unwrap().entries.is_empty());
        assert!(matches!(
            completion.wait().await,
            Err(LoaderError::Lifecycle { .. })
        ));
        runtime.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn idle_observer_does_not_hide_lifecycle_coordinator_failure() {
        let runtime = Runtime::new().unwrap();
        let tree = RuntimeTree::new_root(ExtensionCatalog::new(), runtime.root()).unwrap();
        let completion = ReconcileCompletion::new();
        *tree.reconcile.lock().unwrap() = Some(completion.clone());
        completion.finish(Err(LoaderError::Lifecycle {
            instance: "broken".into(),
            source: CoreError::CoordinatorAborted {
                reason: "worker aborted".into(),
            },
        }));
        assert!(matches!(
            tree.await_idle().await,
            Err(LoaderError::Lifecycle {
                source: CoreError::CoordinatorAborted { .. },
                ..
            })
        ));
        runtime.shutdown().await.unwrap();
    }
}
