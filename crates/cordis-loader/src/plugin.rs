//! 可挂载的静态 EntryTree Loader 插件（按 Entry/子树差分 reconcile）。

use crate::{
    EntryTreeSource,
    catalog::ExtensionCatalog,
    config::ExtensionsConfig,
    error::LoaderError,
    loader::{LOADER, Loader, LoaderEvent},
    snapshot::{EntryId, LoaderSnapshot},
    tree::{RuntimeTree, aggregate_snapshot},
};
use async_trait::async_trait;
use cordis_core::{Context, CoreError, Fiber, FiberState, Plugin, PluginKey};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tokio::sync::{Notify, broadcast};

/// Loader 维护的运行时条目槽位；禁用或等待重挂载时可暂时没有 Fiber。
pub(crate) struct RuntimeEntry {
    pub(crate) parent: Option<EntryId>,
    pub(crate) name: String,
    pub(crate) group: bool,
    pub(crate) enabled: bool,
    pub(crate) fiber: Option<Fiber>,
    pub(crate) plugin_key: Option<PluginKey>,
    pub(crate) context: Context,
    /// 普通 Entry 的工厂 id；Group 为 `None`。
    pub(crate) factory: Option<String>,
    /// 挂载时的自身配置（Group 的 children 嵌在 `config` 中，比较时单独处理）。
    pub(crate) options: crate::config::EntryOptions,
    pub(crate) binding: Option<Arc<dyn crate::prepared::ConfigBinding>>,
    /// 当前已安装实例实际解析的完整配置；只在 Active 时向控制面投影。
    pub(crate) resolved: Option<serde_json::Value>,
}

pub struct LoaderPlugin {
    catalog: ExtensionCatalog,
}

impl LoaderPlugin {
    /// 仅创建空内存 Loader；所有条目（包括启动 Include）均通过 Loader::create 登记。
    pub fn new(catalog: ExtensionCatalog) -> Self {
        Self { catalog }
    }

    fn initial_state(&self, context: Context) -> Result<Arc<LoaderInner>, LoaderError> {
        let root = RuntimeTree::new_root(self.catalog.clone(), context)?;
        let (events, _) = broadcast::channel(64);
        let inner = Arc::new(LoaderInner {
            catalog: self.catalog.clone(),
            root: root.clone(),
            subtrees: Mutex::new(SubtreeRegistry::default()),
            alive: AtomicBool::new(true),
            events,
            change_revision: AtomicU64::new(0),
            state_progress: Notify::new(),
            config_operations: tokio::sync::Mutex::new(()),
        });
        *root.loader.lock().expect("loader weak") = Arc::downgrade(&inner);
        Ok(inner)
    }
}

#[async_trait]
impl Plugin for LoaderPlugin {
    fn key(&self) -> PluginKey {
        PluginKey::new("cordis.loader")
    }

    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        let inner = self
            .initial_state(ctx.clone())
            .map_err(|error| CoreError::PluginApply(error.to_string()))?;
        ctx.provide(LOADER, Loader::new(inner.clone()))?;
        let fiber_states = ctx.subscribe_fiber_states()?;
        let state_task = tokio::spawn(crate::fiber_events::observe_fiber_events(
            inner.clone(),
            fiber_states,
        ));
        ctx.effect_named("loader-fiber-events")?
            .on_dispose(move || state_task.abort());
        ctx.effect_named("loader-state")?.on_dispose({
            let inner = inner.clone();
            move || {
                inner.alive.store(false, Ordering::Release);
                inner.state_progress.notify_waiters();
                for tree in std::iter::once(inner.root.clone()).chain(inner.runtime_trees()) {
                    for entry in tree.entries.lock().expect("entries").values_mut() {
                        entry.binding = None;
                    }
                }
            }
        });
        Ok(())
    }
}

pub(crate) struct LoaderInner {
    /// 跨来源配置事务与直接控制操作共用的准入锁；内部递归协调不重复获取。
    pub(crate) config_operations: tokio::sync::Mutex<()>,
    pub(crate) catalog: ExtensionCatalog,
    pub(crate) root: Arc<RuntimeTree>,
    pub(crate) subtrees: Mutex<SubtreeRegistry>,
    pub(crate) alive: AtomicBool,
    pub(crate) events: broadcast::Sender<LoaderEvent>,
    change_revision: AtomicU64,
    state_progress: Notify,
}

/// 所有子树索引必须作为一个原子注册表读写。
///
/// `by_source` 保证来源身份只附着一次；`by_prefix` 提供路径定位与快照聚合。
#[derive(Default)]
pub(crate) struct SubtreeRegistry {
    pub(crate) by_source: HashMap<String, Arc<RuntimeTree>>,
    pub(crate) by_prefix: HashMap<EntryId, Arc<RuntimeTree>>,
}

impl LoaderInner {
    pub(crate) async fn await_idle_all(self: &Arc<Self>) -> Result<LoaderSnapshot, LoaderError> {
        loop {
            let state_progress = self.state_progress.notified();
            tokio::pin!(state_progress);
            state_progress.as_mut().enable();

            if !self.alive.load(Ordering::Acquire) {
                return Err(LoaderError::Runtime {
                    source: CoreError::ContextDisposed,
                });
            }

            let _ = self.root.await_operations_idle().await?;
            for tree in self.runtime_trees() {
                let _ = tree.await_operations_idle().await?;
            }
            if !self.alive.load(Ordering::Acquire) {
                return Err(LoaderError::Runtime {
                    source: CoreError::ContextDisposed,
                });
            }
            if !self.has_transient_fibers() {
                return Ok(self.aggregate_snapshot());
            }
            state_progress.await;
        }
    }

    /// 发布运行树快照失效通知；消费者必须重新读取完整快照。
    pub(crate) fn publish_snapshot_changed(&self) {
        let revision = self.change_revision.fetch_add(1, Ordering::AcqRel) + 1;
        let _ = self.events.send(LoaderEvent::SnapshotChanged { revision });
    }

    /// 只观察本 Loader 条目树中的 Fiber；无关 Runtime 工作不会影响 Loader 稳定边界。
    pub(crate) fn observe_fiber_state(&self, change: cordis_core::FiberStateChange) {
        let mut tracked = false;
        for tree in std::iter::once(self.root.clone()).chain(self.runtime_trees()) {
            tracked |= tree.observe_lifecycle(&change);
        }
        if !tracked && !self.manages_fiber(change.fiber_id) {
            return;
        }
        self.prune_disposed_bindings();
        self.publish_snapshot_changed();
        self.state_progress.notify_waiters();
    }

    pub(crate) fn notify_state_progress(&self) {
        self.state_progress.notify_waiters();
    }

    pub(crate) fn prune_disposed_bindings(&self) {
        for tree in std::iter::once(self.root.clone()).chain(self.runtime_trees()) {
            for entry in tree.entries.lock().expect("entries").values_mut() {
                if entry.fiber.as_ref().is_some_and(Fiber::is_disposed) {
                    entry.binding = None;
                }
            }
        }
    }

    pub(crate) fn runtime_trees(&self) -> Vec<Arc<RuntimeTree>> {
        self.subtrees
            .lock()
            .expect("subtree registry")
            .by_prefix
            .values()
            .cloned()
            .collect()
    }

    fn manages_fiber(&self, fiber_id: u64) -> bool {
        self.tree_has_fiber(&self.root, fiber_id)
            || self
                .runtime_trees()
                .iter()
                .any(|tree| self.tree_has_fiber(tree, fiber_id))
    }

    fn has_transient_fibers(&self) -> bool {
        self.tree_has_transient_fibers(&self.root)
            || self
                .runtime_trees()
                .iter()
                .any(|tree| self.tree_has_transient_fibers(tree))
    }

    fn tree_has_fiber(&self, tree: &RuntimeTree, fiber_id: u64) -> bool {
        tree.entries.lock().expect("entries").values().any(|entry| {
            entry
                .fiber
                .as_ref()
                .is_some_and(|fiber| fiber.id() == fiber_id)
        })
    }

    fn tree_has_transient_fibers(&self, tree: &RuntimeTree) -> bool {
        tree.entries.lock().expect("entries").values().any(|entry| {
            entry.fiber.as_ref().is_some_and(|fiber| {
                matches!(fiber.state(), FiberState::Loading | FiberState::Unloading)
            })
        })
    }

    pub(crate) fn aggregate_snapshot(&self) -> LoaderSnapshot {
        let registry = self.subtrees.lock().expect("subtree registry");
        aggregate_snapshot(&self.root, &registry.by_prefix)
    }

    pub(crate) fn entry_context(&self, path: &str) -> Result<Context, LoaderError> {
        if let Some(ctx) = self.root.entry_context(path) {
            return Ok(ctx);
        }
        let subtrees: Vec<_> = self
            .subtrees
            .lock()
            .expect("subtree registry")
            .by_prefix
            .values()
            .cloned()
            .collect();
        for tree in subtrees {
            if let Some(ctx) = tree.entry_context(path) {
                return Ok(ctx);
            }
        }
        Err(LoaderError::UnknownInstance {
            instance: path.into(),
        })
    }

    /// 路径是否属于某棵已附着子树的**内部**条目（不含 Include 载体自身路径）。
    pub(crate) fn subtree_owning_path(&self, path: &str) -> Option<Arc<RuntimeTree>> {
        let registry = self.subtrees.lock().expect("subtree registry");
        let mut best: Option<Arc<RuntimeTree>> = None;
        for (prefix, tree) in &registry.by_prefix {
            let p = prefix.as_str();
            if p.is_empty() {
                continue;
            }
            if path.starts_with(&(p.to_string() + ":")) {
                let deeper = best
                    .as_ref()
                    .map(|current| current.prefix.as_str().len() < p.len())
                    .unwrap_or(true);
                if deeper {
                    best = Some(tree.clone());
                }
            }
        }
        best
    }

    pub(crate) async fn attach_subtree(
        self: &Arc<Self>,
        owner: &Context,
        source: Arc<dyn EntryTreeSource>,
    ) -> Result<Arc<RuntimeTree>, LoaderError> {
        if owner.is_disposed() || !self.alive.load(Ordering::Acquire) {
            return Err(crate::SourceError::new(
                crate::SourceErrorKind::Unavailable,
                "来源树所属 Context 已关闭",
            )
            .into());
        }
        let location = owner
            .config(crate::meta::ENTRY_LOCATION)
            .map_err(|_| LoaderError::EntryLocationMissing)?;
        let source_id = source.identity();
        let owner_tree = {
            let registry = self.subtrees.lock().expect("subtree registry");
            let tree = if location.tree_prefix.is_empty() {
                self.root.clone()
            } else {
                registry
                    .by_prefix
                    .get(&EntryId::from(location.tree_prefix.as_str()))
                    .cloned()
                    .ok_or_else(|| LoaderError::UnknownInstance {
                        instance: location.tree_prefix.clone(),
                    })?
            };
            tree.ensure_open()?;
            tree
        };
        let document = source.read().await?;
        preflight(&self.catalog, &document.config)?;

        let prefix = EntryId::from(location.path.as_str());
        // 读取/预检在锁外；检查与保留在同一注册表临界区，不能被并发 attach 穿插。
        let tree = {
            let mut registry = self.subtrees.lock().expect("subtree registry");
            if owner.is_disposed()
                || !self.alive.load(Ordering::Acquire)
                || (!location.tree_prefix.is_empty()
                    && !registry
                        .by_prefix
                        .get(&owner_tree.prefix)
                        .is_some_and(|current| Arc::ptr_eq(current, &owner_tree)))
            {
                return Err(crate::SourceError::new(
                    crate::SourceErrorKind::Unavailable,
                    "来源树所属实例已关闭或被替换",
                )
                .into());
            }
            owner_tree.ensure_open()?;

            // 先检查祖先链，确保循环比一般重复来源得到更准确的诊断。
            let parent_source = owner_tree.source_id.clone();
            let mut cursor = parent_source.clone();
            while let Some(current) = cursor {
                if current == source_id {
                    return Err(LoaderError::SourceCycle { source_id });
                }
                cursor = registry
                    .by_source
                    .get(&current)
                    .and_then(|tree| tree.parent_source.clone());
            }

            if registry.by_source.contains_key(&source_id)
                || registry.by_prefix.contains_key(&prefix)
            {
                return Err(LoaderError::DuplicateSubtreeSource { source_id });
            }

            let tree = RuntimeTree::new_subtree(
                self.catalog.clone(),
                Arc::downgrade(self),
                owner.clone(),
                prefix.clone(),
                source,
                parent_source,
                Arc::downgrade(&owner_tree),
                document.revision,
            )?;
            registry.by_source.insert(source_id, tree.clone());
            registry.by_prefix.insert(prefix, tree.clone());
            tree
        };

        match tree.initialize(document.config).await {
            Ok(_) => Ok(tree),
            Err(error) => {
                if let Err(cleanup) = self.detach_subtree(&tree).await {
                    tree.lifecycle_logs.error(&cleanup);
                }
                Err(error)
            }
        }
    }

    pub(crate) fn unregister_subtree(&self, tree: &Arc<RuntimeTree>) {
        let mut registry = self.subtrees.lock().expect("subtree registry");
        if let Some(key) = &tree.source_id
            && registry
                .by_source
                .get(key)
                .is_some_and(|current| Arc::ptr_eq(current, tree))
        {
            registry.by_source.remove(key);
        }
        if registry
            .by_prefix
            .get(&tree.prefix)
            .is_some_and(|current| Arc::ptr_eq(current, tree))
        {
            registry.by_prefix.remove(&tree.prefix);
        }
    }

    pub(crate) fn detach_subtree(
        self: &Arc<Self>,
        tree: &Arc<RuntimeTree>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), LoaderError>> + Send + 'static>,
    > {
        crate::release::detach(self.clone(), tree.clone())
    }
}

fn preflight(catalog: &ExtensionCatalog, config: &ExtensionsConfig) -> Result<(), LoaderError> {
    config.validate(catalog)?;
    Ok(())
}
