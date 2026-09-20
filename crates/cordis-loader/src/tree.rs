//! 单棵可附着的运行时 EntryTree（根树或 Include 子树）。

use crate::{
    bootstrap::load_extensions_source,
    catalog::{ExtensionCatalog, NamedIsolationLabels},
    config::{EntryOptions, ExtensionsConfig},
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
    path::{Path, PathBuf},
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

/// 一棵文件（或内存）驱动的 EntryTree 运行时状态。
pub(crate) struct RuntimeTree {
    pub(crate) catalog: ExtensionCatalog,
    /// 弱引用宿主，用于附着/注销子树；根树在构造后由 LoaderInner 回填。
    pub(crate) loader: Mutex<Weak<super::plugin::LoaderInner>>,
    pub(crate) owner: Context,
    pub(crate) prefix: EntryId,
    /// 规范化绝对路径；内存源为 `None`。
    pub(crate) file_key: Option<PathBuf>,
    /// 父树配置文件路径（用于循环检测）；根为 `None`。
    pub(crate) parent_file: Option<PathBuf>,
    pub(crate) source_dir: Option<PathBuf>,
    /// 配置源路径在树创建时确定，后续不会变更。
    pub(crate) extensions_path: Option<PathBuf>,
    pub(crate) desired: Mutex<ExtensionsConfig>,
    pub(crate) revision: Mutex<Option<String>>,
    pub(crate) entries: Mutex<BTreeMap<EntryId, RuntimeEntry>>,
    pub(crate) order: Mutex<Vec<EntryId>>,
    pub(crate) named_labels: Mutex<NamedIsolationLabels>,
    reconcile: Mutex<Option<Arc<ReconcileCompletion>>>,
    pub(crate) operations: tokio::sync::Mutex<()>,
}

impl RuntimeTree {
    pub(crate) fn new_root(
        catalog: ExtensionCatalog,
        owner: Context,
        extensions_path: Option<PathBuf>,
        revision: Option<String>,
    ) -> Arc<Self> {
        let (file_key, source_dir) = match &extensions_path {
            Some(path) => {
                let key = canonicalize_existing(path).ok();
                let dir = path.parent().map(Path::to_path_buf);
                (key.or_else(|| Some(path.clone())), dir)
            }
            None => (None, None),
        };
        Arc::new(Self {
            catalog,
            loader: Mutex::new(Weak::new()),
            owner,
            prefix: EntryId::from(""),
            file_key,
            parent_file: None,
            source_dir,
            extensions_path,
            desired: Mutex::new(ExtensionsConfig {
                version: crate::config::CONFIG_VERSION,
                extensions: Vec::new(),
            }),
            revision: Mutex::new(revision),
            entries: Mutex::new(BTreeMap::new()),
            order: Mutex::new(Vec::new()),
            named_labels: Mutex::new(NamedIsolationLabels::new()),
            reconcile: Mutex::new(None),
            operations: tokio::sync::Mutex::new(()),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_subtree(
        catalog: ExtensionCatalog,
        loader: Weak<super::plugin::LoaderInner>,
        owner: Context,
        prefix: EntryId,
        file_path: PathBuf,
        file_key: PathBuf,
        parent_file: Option<PathBuf>,
        revision: String,
    ) -> Arc<Self> {
        let source_dir = file_path.parent().map(Path::to_path_buf);
        Arc::new(Self {
            catalog,
            loader: Mutex::new(loader),
            owner,
            prefix,
            file_key: Some(file_key),
            parent_file,
            source_dir,
            extensions_path: Some(file_path),
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
        })
    }

    pub(crate) async fn apply(
        self: &Arc<Self>,
        config: ExtensionsConfig,
        revision: Option<String>,
    ) -> Result<LoaderSnapshot, LoaderError> {
        let prefix = if self.prefix.as_str().is_empty() {
            None
        } else {
            Some(self.prefix.as_str())
        };
        let (mut desired, roots) = index_desired_tree(&self.catalog, &config, prefix)?;
        let plan = {
            let entries = self.entries.lock().expect("entries");
            plan_reconcile(&entries, &desired, &roots)?
        };
        build_plan_plugins(&self.catalog, &mut desired, &plan, &self.entries)?;
        let completion = {
            let mut slot = self.reconcile.lock().expect("reconcile slot");
            if slot.is_some() {
                return Err(LoaderError::ReconcileBusy);
            }
            let completion = ReconcileCompletion::new();
            *slot = Some(completion.clone());
            self.spawn_reconcile(completion.clone(), desired, plan, config, revision);
            completion
        };
        completion.wait().await
    }

    pub(crate) async fn await_idle(self: &Arc<Self>) -> Result<LoaderSnapshot, LoaderError> {
        let completion = { self.reconcile.lock().expect("reconcile slot").clone() };
        if let Some(completion) = completion {
            completion.wait().await
        } else {
            Ok(self.local_snapshot())
        }
    }

    fn spawn_reconcile(
        self: &Arc<Self>,
        completion: Arc<ReconcileCompletion>,
        desired: BTreeMap<EntryId, DesiredNode>,
        plan: ReconcilePlan,
        config: ExtensionsConfig,
        revision: Option<String>,
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
            if result.is_ok() {
                *tree.desired.lock().expect("desired") = config;
                if let Some(revision) = revision {
                    *tree.revision.lock().expect("revision") = Some(revision);
                }
                tree.prune_named_isolation_labels();
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
                    source_dir: self.source_dir.clone(),
                    loader,
                },
            )
            .expect("entry location intercept")
    }

    /// 后序释放全部条目并清空运行时槽位（用于 detach）。
    pub(crate) async fn dispose_all(self: &Arc<Self>) -> Result<(), LoaderError> {
        let _operation = self.operations.lock().await;
        // 等待进行中的 reconcile
        let _ = self.await_idle().await;
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
            self.dispose_and_remove(&path).await?;
        }
        *self.order.lock().expect("entry order") = Vec::new();
        *self.desired.lock().expect("desired") = ExtensionsConfig {
            version: crate::config::CONFIG_VERSION,
            extensions: Vec::new(),
        };
        self.named_labels.lock().expect("named labels").clear();
        Ok(())
    }

    pub(crate) async fn reload(self: &Arc<Self>) -> Result<LoaderSnapshot, LoaderError> {
        let _operation = self.operations.lock().await;
        let path = self
            .extensions_path
            .clone()
            .ok_or(LoaderError::NoConfigSource)?;
        let (config, revision) = load_extensions_source(&path)?;
        self.apply(config, Some(revision)).await
    }
}

/// 供控制面把 revision 冲突转成 `ConfigConflict`。
pub(crate) fn ensure_tree_revision_control(
    tree: &RuntimeTree,
    path: &PathBuf,
) -> Result<(), crate::loader::LoaderControlError> {
    let (_, current) = load_extensions_source(path)?;
    if tree.revision.lock().expect("revision").as_deref() == Some(current.as_str()) {
        Ok(())
    } else {
        Err(crate::loader::LoaderControlError::ConfigConflict { path: path.clone() })
    }
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

pub(crate) fn canonicalize_existing(path: &Path) -> Result<PathBuf, LoaderError> {
    path.canonicalize()
        .map_err(|source| LoaderError::io(path.to_path_buf(), source))
}

pub(crate) fn resolve_include_path(
    source_dir: Option<&Path>,
    configured: &Path,
) -> Result<PathBuf, LoaderError> {
    if configured.is_absolute() {
        Ok(configured.to_path_buf())
    } else {
        let base = source_dir.ok_or(LoaderError::RelativePathWithoutSource)?;
        Ok(base.join(configured))
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
