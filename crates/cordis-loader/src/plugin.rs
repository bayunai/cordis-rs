//! 可挂载的静态 EntryTree Loader 插件（按 Entry/子树差分 reconcile）。

use crate::{
    bootstrap::{load_bootstrap, load_extensions_source},
    catalog::ExtensionCatalog,
    config::{EntryOptions, ExtensionsConfig},
    error::{LoaderError, format_panic_message},
    loader::{LOADER, Loader},
    snapshot::{EntryId, EntrySnapshot, LoaderSnapshot},
};
use async_trait::async_trait;
use cordis_core::{Context, CoreError, Fiber, FiberState, Plugin, PluginKey, ServiceId};
use std::{
    collections::{BTreeMap, BTreeSet},
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{sync::Notify, task::JoinHandle};

struct MountedEntry {
    parent: Option<EntryId>,
    name: String,
    group: bool,
    enabled: bool,
    fiber: Option<Fiber>,
    plugin_key: Option<PluginKey>,
    context: Context,
    /// 普通 Entry 的工厂 id；Group 为 `None`。
    factory: Option<String>,
    /// 挂载时的自身配置（Group 的 children 嵌在 `config` 中，比较时单独处理）。
    options: EntryOptions,
}

enum ConfigSource {
    Memory {
        config: ExtensionsConfig,
    },
    File {
        path: PathBuf,
        config: ExtensionsConfig,
        revision: String,
    },
}

pub struct LoaderPlugin {
    catalog: ExtensionCatalog,
    source: ConfigSource,
    state: Mutex<Option<Arc<LoaderInner>>>,
}

impl LoaderPlugin {
    pub fn new(catalog: ExtensionCatalog, config: ExtensionsConfig) -> Result<Self, LoaderError> {
        preflight(&catalog, &config)?;
        Ok(Self {
            catalog,
            source: ConfigSource::Memory { config },
            state: Mutex::new(None),
        })
    }

    pub fn bootstrap(
        catalog: ExtensionCatalog,
        bootstrap_path: impl AsRef<Path>,
    ) -> Result<Self, LoaderError> {
        let (_, path) = load_bootstrap(bootstrap_path)?;
        let (config, revision) = load_extensions_source(&path)?;
        preflight(&catalog, &config)?;
        Ok(Self {
            catalog,
            source: ConfigSource::File {
                path,
                config,
                revision,
            },
            state: Mutex::new(None),
        })
    }

    fn initial_state(&self, context: Context) -> Arc<LoaderInner> {
        let (config, extensions_path, revision) = match &self.source {
            ConfigSource::Memory { config } => (config.clone(), None, None),
            ConfigSource::File {
                path,
                config,
                revision,
            } => (config.clone(), Some(path.clone()), Some(revision.clone())),
        };
        Arc::new(LoaderInner {
            catalog: self.catalog.clone(),
            context,
            extensions_path: Mutex::new(extensions_path),
            desired: Mutex::new(ExtensionsConfig {
                version: crate::config::CONFIG_VERSION,
                extensions: Vec::new(),
            }),
            revision: Mutex::new(revision),
            entries: Mutex::new(BTreeMap::new()),
            order: Mutex::new(Vec::new()),
            reconcile: Mutex::new(None),
            loader_operations: tokio::sync::Mutex::new(()),
            alive: AtomicBool::new(true),
            initial: Mutex::new(Some(config)),
        })
    }
}

#[async_trait]
impl Plugin for LoaderPlugin {
    fn key(&self) -> PluginKey {
        PluginKey::new("cordis.loader")
    }

    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        self.state.lock().expect("loader plugin state").take();
        let inner = self.initial_state(ctx.clone());
        ctx.provide(LOADER, Loader::new(inner.clone()))?;
        ctx.effect_named("loader-state")?.on_dispose({
            let inner = inner.clone();
            move || inner.alive.store(false, Ordering::Release)
        });
        *self.state.lock().expect("loader plugin state") = Some(inner.clone());
        let config = inner
            .initial
            .lock()
            .expect("initial config")
            .take()
            .expect("LoaderPlugin applies once per state");
        inner
            .apply(config, None)
            .await
            .map(|_| ())
            .map_err(|error| CoreError::PluginApply(error.to_string()))
    }
}

struct ReconcileCompletion {
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

    async fn wait(self: &Arc<Self>) -> Result<LoaderSnapshot, LoaderError> {
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

pub(crate) struct LoaderInner {
    pub(crate) catalog: ExtensionCatalog,
    pub(crate) context: Context,
    pub(crate) extensions_path: Mutex<Option<PathBuf>>,
    pub(crate) desired: Mutex<ExtensionsConfig>,
    pub(crate) revision: Mutex<Option<String>>,
    entries: Mutex<BTreeMap<EntryId, MountedEntry>>,
    order: Mutex<Vec<EntryId>>,
    reconcile: Mutex<Option<Arc<ReconcileCompletion>>>,
    pub(crate) loader_operations: tokio::sync::Mutex<()>,
    pub(crate) alive: AtomicBool,
    initial: Mutex<Option<ExtensionsConfig>>,
}

/// 目标树节点（预检索引；Plugin 按需填充）。
struct DesiredNode {
    parent: Option<EntryId>,
    options: EntryOptions,
    children: Vec<EntryId>,
    plugin: Option<Arc<dyn Plugin>>,
    plugin_key: Option<PluginKey>,
}

struct ReconcilePlan {
    /// 整节点删除（含子树根）；执行时后序释放并移出 map。
    remove: BTreeSet<EntryId>,
    /// 仅释放 Fiber，保留 map 槽位（禁用）。
    dispose_fiber: BTreeSet<EntryId>,
    /// `Fiber::replace`。
    replace: BTreeSet<EntryId>,
    /// 新建或重新挂载 Fiber（含 Group 重建）。
    mount: BTreeSet<EntryId>,
    /// 需 `factory.build` 的普通启用 Entry。
    build: BTreeSet<EntryId>,
    /// 目标前序。
    desired_order: Vec<EntryId>,
}

impl LoaderInner {
    pub(crate) async fn apply(
        self: &Arc<Self>,
        config: ExtensionsConfig,
        revision: Option<String>,
    ) -> Result<LoaderSnapshot, LoaderError> {
        let (mut desired, roots) = index_desired_tree(&self.catalog, &config)?;
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
            Ok(self.snapshot())
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
        let inner = self.clone();
        let task_completion = completion.clone();
        let task = tokio::runtime::Handle::current().spawn(async move {
            let worker_inner = inner.clone();
            let result =
                match tokio::spawn(
                    async move { worker_inner.execute_reconcile(desired, plan).await },
                )
                .await
                {
                    Ok(result) => result,
                    Err(error) => Err(LoaderError::ReconcileAborted {
                        reason: format!("reconcile worker: {error}"),
                    }),
                };
            if result.is_ok() {
                *inner.desired.lock().expect("desired") = config;
                if let Some(revision) = revision {
                    *inner.revision.lock().expect("revision") = Some(revision);
                }
            }
            let mut slot = inner.reconcile.lock().expect("reconcile slot");
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

    async fn execute_reconcile(
        &self,
        desired: BTreeMap<EntryId, DesiredNode>,
        plan: ReconcilePlan,
    ) -> Result<LoaderSnapshot, LoaderError> {
        // 1) 后序删除整节点
        let remove_order = postorder_among(&plan.remove, &self.order.lock().expect("entry order"));
        for path in remove_order {
            self.dispose_and_remove(&path).await?;
        }

        // 2) 后序仅释放 Fiber（禁用）
        let dispose_order = postorder_among(
            &plan.dispose_fiber,
            &self.order.lock().expect("entry order"),
        );
        for path in dispose_order {
            self.dispose_fiber_only(&path).await?;
        }

        // 3) 同步保留节点上的 options / enabled（含 Group disabled 开关）
        {
            let mut entries = self.entries.lock().expect("entries");
            for (path, node) in &desired {
                if let Some(entry) = entries.get_mut(path) {
                    let parent_disabled = ancestor_disabled(path, &desired);
                    entry.options = node.options.clone();
                    entry.name = node.options.name.clone();
                    entry.enabled = !parent_disabled && !node.options.disabled;
                    if entry.group {
                        entry.factory = None;
                    } else {
                        entry.factory = Some(node.options.name.clone());
                    }
                }
            }
        }

        // 4) replace
        for path in &plan.replace {
            self.replace_entry(path, &desired).await?;
        }

        // 5) 前序挂载
        for path in &plan.desired_order {
            if plan.mount.contains(path) {
                self.mount_one(path, &desired).await?;
            }
        }

        // 6) 纯排序 / 最终前序
        *self.order.lock().expect("entry order") = plan.desired_order;
        Ok(self.snapshot())
    }

    async fn dispose_and_remove(&self, path: &EntryId) -> Result<(), LoaderError> {
        let mut fiber = {
            let mut entries = self.entries.lock().expect("entries");
            let Some(mut entry) = entries.remove(path) else {
                return Ok(());
            };
            entry.fiber.take()
        };
        if let Some(mut fiber) = fiber.take() {
            fiber
                .dispose_wait()
                .await
                .map_err(|source| LoaderError::Lifecycle {
                    instance: path.to_string(),
                    source,
                })?;
        }
        self.order
            .lock()
            .expect("entry order")
            .retain(|item| item != path);
        Ok(())
    }

    async fn dispose_fiber_only(&self, path: &EntryId) -> Result<(), LoaderError> {
        let mut fiber = {
            let mut entries = self.entries.lock().expect("entries");
            let Some(entry) = entries.get_mut(path) else {
                return Ok(());
            };
            entry.enabled = false;
            entry.plugin_key = None;
            entry.fiber.take()
        };
        if let Some(mut fiber) = fiber.take() {
            fiber
                .dispose_wait()
                .await
                .map_err(|source| LoaderError::Lifecycle {
                    instance: path.to_string(),
                    source,
                })?;
        }
        Ok(())
    }

    async fn replace_entry(
        &self,
        path: &EntryId,
        desired: &BTreeMap<EntryId, DesiredNode>,
    ) -> Result<(), LoaderError> {
        let node = desired.get(path).expect("replace target");
        let plugin = node.plugin.clone().expect("replace plugin");
        let dependencies = {
            let entries = self.entries.lock().expect("entries");
            let entry = entries.get(path).expect("mounted replace target");
            let (_, dependencies) = self.catalog.resolve_injections(
                node.options.inject.as_ref(),
                path.as_str(),
                entry.context.clone(),
            )?;
            dependencies
        };
        let mut fiber = {
            let mut entries = self.entries.lock().expect("entries");
            let entry = entries.get_mut(path).expect("mounted replace target");
            entry.options = node.options.clone();
            entry.fiber.take().expect("replace requires existing fiber")
        };
        let replace_result = fiber
            .replace(Arc::new(ConfiguredPlugin::new(plugin, dependencies)))
            .await;
        let failed = fiber.state() == FiberState::Failed;
        let last_error = fiber.last_error();
        {
            let mut entries = self.entries.lock().expect("entries");
            let entry = entries.get_mut(path).expect("mounted replace target");
            if replace_result.is_ok() && !failed {
                entry.plugin_key = node.plugin_key;
            }
            entry.fiber = Some(fiber);
        }
        replace_result.map_err(|source| LoaderError::Lifecycle {
            instance: path.to_string(),
            source,
        })?;
        if failed {
            return Err(LoaderError::Lifecycle {
                instance: path.to_string(),
                source: CoreError::PluginApply(
                    last_error.unwrap_or_else(|| "plugin replace failed".into()),
                ),
            });
        }
        Ok(())
    }

    async fn mount_one(
        &self,
        path: &EntryId,
        desired: &BTreeMap<EntryId, DesiredNode>,
    ) -> Result<(), LoaderError> {
        let node = desired.get(path).expect("mount target");
        let parent_disabled = ancestor_disabled(path, desired);
        let enabled = !parent_disabled && !node.options.disabled;

        let parent_context = {
            let entries = self.entries.lock().expect("entries");
            match &node.parent {
                Some(parent) => entries.get(parent).map(|entry| entry.context.clone()),
                None => Some(self.context.clone()),
            }
        };
        let parent_context = parent_context.ok_or_else(|| LoaderError::UnknownInstance {
            instance: node
                .parent
                .as_ref()
                .map(|parent| parent.to_string())
                .unwrap_or_default(),
        })?;

        // 已存在槽位（例如禁用后重新启用）：先丢掉旧 fiber（应已空）。
        let stale = {
            let mut entries = self.entries.lock().expect("entries");
            entries.get_mut(path).and_then(|entry| entry.fiber.take())
        };
        if let Some(mut fiber) = stale {
            fiber
                .dispose_wait()
                .await
                .map_err(|source| LoaderError::Lifecycle {
                    instance: path.to_string(),
                    source,
                })?;
        }

        let domain = if node.options.group {
            parent_context
                .extend()
                .map_err(|source| LoaderError::Runtime { source })?
        } else {
            parent_context
        };
        let (context, dependencies) =
            self.catalog
                .resolve_injections(node.options.inject.as_ref(), path.as_str(), domain)?;

        let (fiber, plugin_key, mount_failed, mount_error) = if node.options.group {
            let fiber = context
                .plugin(Arc::new(ConfiguredPlugin::new(
                    Arc::new(GroupPlugin),
                    dependencies,
                )))
                .await
                .map_err(|source| LoaderError::Lifecycle {
                    instance: path.to_string(),
                    source,
                })?;
            let failed = fiber.state() == FiberState::Failed;
            let last_error = fiber.last_error();
            (
                Some(fiber),
                Some(PluginKey::new("cordis.loader.group")),
                failed,
                last_error,
            )
        } else if enabled {
            let plugin = node
                .plugin
                .clone()
                .expect("enabled ordinary entry must be built");
            let fiber = context
                .plugin(Arc::new(ConfiguredPlugin::new(plugin, dependencies)))
                .await
                .map_err(|source| LoaderError::Lifecycle {
                    instance: path.to_string(),
                    source,
                })?;
            let failed = fiber.state() == FiberState::Failed;
            let last_error = fiber.last_error();
            (Some(fiber), node.plugin_key, failed, last_error)
        } else {
            (None, None, false, None)
        };

        let mut entries = self.entries.lock().expect("entries");
        entries.insert(
            path.clone(),
            MountedEntry {
                parent: node.parent.clone(),
                name: node.options.name.clone(),
                group: node.options.group,
                enabled,
                fiber,
                plugin_key,
                context,
                factory: (!node.options.group).then(|| node.options.name.clone()),
                options: node.options.clone(),
            },
        );
        drop(entries);
        if mount_failed {
            return Err(LoaderError::Lifecycle {
                instance: path.to_string(),
                source: CoreError::PluginApply(
                    mount_error.unwrap_or_else(|| "plugin apply failed".into()),
                ),
            });
        }
        Ok(())
    }

    fn snapshot(&self) -> LoaderSnapshot {
        let entries = self.entries.lock().expect("entries");
        let order = self.order.lock().expect("entry order");
        // 生命周期失败可发生在最终目标顺序提交之前。先保留最近一次成功的顺序，
        // 再补入实际已挂载、但尚未进入 order 的节点，确保 Failed/Pending 诊断可见。
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
                        parent: entry.parent.clone(),
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

    pub(crate) fn entry_context(&self, path: &str) -> Result<Context, LoaderError> {
        self.entries
            .lock()
            .expect("entries")
            .get(&EntryId::from(path))
            .map(|entry| entry.context.clone())
            .ok_or_else(|| LoaderError::UnknownInstance {
                instance: path.into(),
            })
    }
}

fn ancestor_disabled(path: &EntryId, desired: &BTreeMap<EntryId, DesiredNode>) -> bool {
    let mut current = desired.get(path).and_then(|node| node.parent.clone());
    while let Some(parent) = current {
        let Some(node) = desired.get(&parent) else {
            break;
        };
        if node.options.disabled {
            return true;
        }
        current = node.parent.clone();
    }
    false
}

fn postorder_among(paths: &BTreeSet<EntryId>, current_order: &[EntryId]) -> Vec<EntryId> {
    let mut result = current_order
        .iter()
        .rev()
        .filter(|path| paths.contains(path))
        .cloned()
        .collect::<Vec<_>>();
    // 失败 reconcile 可能已插入节点、但尚未来得及提交 order。它们也必须在
    // 下一次收敛中被释放；按路径深度倒序保证父节点晚于后代处理。
    let mut unlisted = paths
        .iter()
        .filter(|path| !result.contains(path))
        .cloned()
        .collect::<Vec<_>>();
    unlisted.sort_by(|left, right| {
        right
            .as_str()
            .matches(':')
            .count()
            .cmp(&left.as_str().matches(':').count())
            .then_with(|| left.cmp(right))
    });
    result.extend(unlisted);
    result
}

fn index_desired_tree(
    catalog: &ExtensionCatalog,
    config: &ExtensionsConfig,
) -> Result<(BTreeMap<EntryId, DesiredNode>, Vec<EntryId>), LoaderError> {
    config.validate(catalog)?;
    let mut out = BTreeMap::new();
    let roots = index_entries(&config.extensions, None, &mut out)?;
    Ok((out, roots))
}

fn index_entries(
    entries: &[EntryOptions],
    parent: Option<&str>,
    out: &mut BTreeMap<EntryId, DesiredNode>,
) -> Result<Vec<EntryId>, LoaderError> {
    let mut child_ids = Vec::new();
    for options in entries {
        let path = EntryId::from(parent.map_or_else(
            || options.id.clone(),
            |parent| format!("{parent}:{id}", id = options.id),
        ));
        let children = if options.group {
            index_entries(&options.children()?, Some(path.as_str()), out)?
        } else {
            Vec::new()
        };
        child_ids.push(path.clone());
        out.insert(
            path.clone(),
            DesiredNode {
                parent: parent.map(EntryId::from),
                options: options.clone(),
                children,
                plugin: None,
                plugin_key: None,
            },
        );
    }
    Ok(child_ids)
}

fn plan_reconcile(
    mounted: &BTreeMap<EntryId, MountedEntry>,
    desired: &BTreeMap<EntryId, DesiredNode>,
    roots: &[EntryId],
) -> Result<ReconcilePlan, LoaderError> {
    let mut remove = BTreeSet::new();
    let mut dispose_fiber = BTreeSet::new();
    let mut replace = BTreeSet::new();
    let mut mount = BTreeSet::new();
    let mut build = BTreeSet::new();
    let mut rebuild_subtrees = BTreeSet::new();

    let desired_order = preorder_from_desired(desired, roots);

    for path in mounted.keys() {
        if !desired.contains_key(path) {
            remove.insert(path.clone());
        }
    }

    for (path, node) in desired {
        let Some(current) = mounted.get(path) else {
            mount.insert(path.clone());
            if !node.options.group {
                let parent_disabled = ancestor_disabled(path, desired);
                if !parent_disabled && !node.options.disabled {
                    build.insert(path.clone());
                }
            } else {
                // Group 新建时整棵子树由 mount 前序创建；子节点也会在本循环中各自加入 mount。
            }
            continue;
        };

        if current.group != node.options.group {
            return Err(LoaderError::InvalidEntry {
                path: path.to_string(),
                message: "不允许在同一路径上将普通 Entry 与 Group 互换".into(),
            });
        }

        if !node.options.group {
            let factory = current.factory.as_deref().unwrap_or("");
            if factory != node.options.name {
                return Err(LoaderError::FactoryChanged {
                    instance: path.to_string(),
                    from: factory.into(),
                    to: node.options.name.clone(),
                });
            }
        }

        if node.options.group {
            if current.options.inject != node.options.inject {
                rebuild_subtrees.insert(path.clone());
            } else {
                let parent_disabled = ancestor_disabled(path, desired);
                let want_enabled = !parent_disabled && !node.options.disabled;
                if current.options.disabled != node.options.disabled
                    || current.enabled != want_enabled
                {
                    plan_group_disable_toggle(
                        path,
                        desired,
                        mounted,
                        &mut dispose_fiber,
                        &mut mount,
                        &mut build,
                    );
                }
            }
        } else {
            let parent_disabled = ancestor_disabled(path, desired);
            let want_enabled = !parent_disabled && !node.options.disabled;
            let inject_changed = current.options.inject != node.options.inject;
            let config_changed = current.options.config != node.options.config;
            let failed = current
                .fiber
                .as_ref()
                .is_some_and(|fiber| fiber.state() == FiberState::Failed);
            let disposed = current
                .fiber
                .as_ref()
                .is_some_and(|fiber| fiber.is_disposed());

            if inject_changed
                || current.enabled != want_enabled
                || (want_enabled && (current.fiber.is_none() || disposed))
            {
                if current.fiber.is_some() {
                    dispose_fiber.insert(path.clone());
                }
                if want_enabled {
                    mount.insert(path.clone());
                    build.insert(path.clone());
                }
            } else if want_enabled && (config_changed || failed) {
                replace.insert(path.clone());
                build.insert(path.clone());
            }
        }
    }

    let rebuild_paths: Vec<_> = rebuild_subtrees.iter().cloned().collect();
    for root in rebuild_paths {
        for path in subtree_paths_mounted(&root, mounted) {
            remove.insert(path);
        }
        for path in subtree_paths_desired(&root, desired) {
            mount.insert(path.clone());
            dispose_fiber.remove(&path);
            replace.remove(&path);
            if let Some(node) = desired.get(&path)
                && !node.options.group
            {
                let parent_disabled = ancestor_disabled(&path, desired);
                if !parent_disabled && !node.options.disabled {
                    build.insert(path);
                } else {
                    build.remove(&path);
                }
            }
        }
    }

    for path in remove.iter().cloned().collect::<Vec<_>>() {
        if !mount.contains(&path) {
            replace.remove(&path);
            dispose_fiber.remove(&path);
            build.remove(&path);
        }
    }

    // dispose_fiber 与 mount 同 path：先释放再挂载（启停/inject 变更）。
    // remove 与 mount 同 path：子树重建，先删后挂。

    Ok(ReconcilePlan {
        remove,
        dispose_fiber,
        replace,
        mount,
        build,
        desired_order,
    })
}

fn plan_group_disable_toggle(
    group: &EntryId,
    desired: &BTreeMap<EntryId, DesiredNode>,
    mounted: &BTreeMap<EntryId, MountedEntry>,
    dispose_fiber: &mut BTreeSet<EntryId>,
    mount: &mut BTreeSet<EntryId>,
    build: &mut BTreeSet<EntryId>,
) {
    for path in subtree_paths_desired(group, desired) {
        if &path == group {
            continue;
        }
        let Some(node) = desired.get(&path) else {
            continue;
        };
        let parent_disabled = ancestor_disabled(&path, desired);
        let want_enabled = !parent_disabled && !node.options.disabled;
        let currently_enabled = mounted.get(&path).is_some_and(|entry| entry.enabled);
        if currently_enabled && !want_enabled {
            dispose_fiber.insert(path);
        } else if !currently_enabled && want_enabled {
            mount.insert(path.clone());
            if !node.options.group {
                build.insert(path);
            }
        }
    }
}

fn subtree_paths_mounted(
    root: &EntryId,
    mounted: &BTreeMap<EntryId, MountedEntry>,
) -> Vec<EntryId> {
    let mut out = Vec::new();
    fn walk(path: &EntryId, mounted: &BTreeMap<EntryId, MountedEntry>, out: &mut Vec<EntryId>) {
        let children: Vec<_> = mounted
            .iter()
            .filter(|(_, entry)| entry.parent.as_ref() == Some(path))
            .map(|(child, _)| child.clone())
            .collect();
        for child in children {
            walk(&child, mounted, out);
        }
        out.push(path.clone());
    }
    walk(root, mounted, &mut out);
    out
}

fn subtree_paths_desired(root: &EntryId, desired: &BTreeMap<EntryId, DesiredNode>) -> Vec<EntryId> {
    let mut out = Vec::new();
    fn walk(path: &EntryId, desired: &BTreeMap<EntryId, DesiredNode>, out: &mut Vec<EntryId>) {
        if let Some(node) = desired.get(path) {
            for child in &node.children {
                walk(child, desired, out);
            }
        }
        out.push(path.clone());
    }
    walk(root, desired, &mut out);
    out
}

fn preorder_from_desired(
    desired: &BTreeMap<EntryId, DesiredNode>,
    roots: &[EntryId],
) -> Vec<EntryId> {
    let mut out = Vec::new();
    fn walk(path: &EntryId, desired: &BTreeMap<EntryId, DesiredNode>, out: &mut Vec<EntryId>) {
        out.push(path.clone());
        if let Some(node) = desired.get(path) {
            for child in &node.children {
                walk(child, desired, out);
            }
        }
    }
    for root in roots {
        walk(root, desired, &mut out);
    }
    out
}

fn build_plan_plugins(
    catalog: &ExtensionCatalog,
    desired: &mut BTreeMap<EntryId, DesiredNode>,
    plan: &ReconcilePlan,
    mounted: &Mutex<BTreeMap<EntryId, MountedEntry>>,
) -> Result<(), LoaderError> {
    let mounted = mounted.lock().expect("entries");
    for path in &plan.build {
        let node = desired.get_mut(path).expect("build target");
        let factory = catalog.get(&node.options.name).expect("validated factory");
        let plugin = catch_unwind(AssertUnwindSafe(|| factory.build(&node.options.config)))
            .map_err(|payload| LoaderError::FactoryPanic {
                message: format_panic_message("extension factory build", payload),
            })?
            .map_err(|error| LoaderError::PluginBuild {
                instance: path.to_string(),
                factory: node.options.name.clone(),
                message: error.to_string(),
            })?;
        let plugin_key = catch_unwind(AssertUnwindSafe(|| plugin.key())).map_err(|payload| {
            LoaderError::PluginPanic {
                message: format_panic_message("plugin key", payload),
            }
        })?;
        if let Some(current) = mounted.get(path)
            && !current.group
            && current.factory.as_deref() == Some(node.options.name.as_str())
            && let Some(expected) = current.plugin_key
            && expected != plugin_key
        {
            return Err(LoaderError::PluginKeyChanged {
                instance: path.to_string(),
                expected,
                actual: plugin_key,
            });
        }
        node.plugin = Some(plugin);
        node.plugin_key = Some(plugin_key);
    }
    Ok(())
}

fn preflight(catalog: &ExtensionCatalog, config: &ExtensionsConfig) -> Result<(), LoaderError> {
    config.validate(catalog)?;
    Ok(())
}

struct GroupPlugin;

#[async_trait]
impl Plugin for GroupPlugin {
    fn key(&self) -> PluginKey {
        PluginKey::new("cordis.loader.group")
    }

    async fn apply(&self, _: &Context) -> Result<(), CoreError> {
        Ok(())
    }
}

struct ConfiguredPlugin {
    inner: Arc<dyn Plugin>,
    dependencies: Vec<ServiceId>,
}

impl ConfiguredPlugin {
    fn new(inner: Arc<dyn Plugin>, dependencies: Vec<ServiceId>) -> Self {
        Self {
            inner,
            dependencies,
        }
    }
}

#[async_trait]
impl Plugin for ConfiguredPlugin {
    fn key(&self) -> PluginKey {
        self.inner.key()
    }

    fn inject(&self) -> Vec<ServiceId> {
        let mut dependencies = self.dependencies.clone();
        for dependency in self.inner.inject() {
            if !dependencies.contains(&dependency) {
                dependencies.push(dependency);
            }
        }
        dependencies
    }

    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        self.inner.apply(ctx).await
    }
}
