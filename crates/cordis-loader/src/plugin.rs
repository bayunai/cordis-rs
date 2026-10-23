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
}

pub struct LoaderPlugin {
    catalog: ExtensionCatalog,
    config: ExtensionsConfig,
    state: Mutex<Option<Arc<LoaderInner>>>,
}

impl LoaderPlugin {
    pub fn new(catalog: ExtensionCatalog, config: ExtensionsConfig) -> Result<Self, LoaderError> {
        preflight(&catalog, &config)?;
        Ok(Self {
            catalog,
            config,
            state: Mutex::new(None),
        })
    }

    fn initial_state(&self, context: Context) -> Result<Arc<LoaderInner>, LoaderError> {
        let config = self.config.clone();
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
            initial: Mutex::new(Some(config)),
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
        self.state.lock().expect("loader plugin state").take();
        let inner = self
            .initial_state(ctx.clone())
            .map_err(|error| CoreError::PluginApply(error.to_string()))?;
        ctx.provide(LOADER, Loader::new(inner.clone()))?;
        let mut fiber_states = ctx.subscribe_fiber_states()?;
        let state_inner = inner.clone();
        let state_task = tokio::spawn(async move {
            loop {
                match fiber_states.recv().await {
                    Ok(change) => state_inner.observe_fiber_state(change.fiber_id),
                    // 事件流只用于失效通知；滞后时重新投影完整快照即可。
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        state_inner.prune_disposed_bindings();
                        state_inner.publish_snapshot_changed();
                        state_inner.state_progress.notify_waiters();
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
                if !state_inner.alive.load(Ordering::Acquire) {
                    break;
                }
            }
        });
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
        *self.state.lock().expect("loader plugin state") = Some(inner.clone());
        let config = inner
            .initial
            .lock()
            .expect("initial config")
            .take()
            .expect("LoaderPlugin applies once per state");
        inner
            .root
            .initialize(config)
            .await
            .map(|_| ())
            .map_err(|error| CoreError::PluginApply(error.to_string()))
    }
}

pub(crate) struct LoaderInner {
    pub(crate) catalog: ExtensionCatalog,
    pub(crate) root: Arc<RuntimeTree>,
    pub(crate) subtrees: Mutex<SubtreeRegistry>,
    pub(crate) alive: AtomicBool,
    pub(crate) events: broadcast::Sender<LoaderEvent>,
    change_revision: AtomicU64,
    state_progress: Notify,
    initial: Mutex<Option<ExtensionsConfig>>,
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

            let _ = self.root.await_idle().await?;
            for tree in self.runtime_trees() {
                let _ = tree.await_idle().await?;
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
    fn observe_fiber_state(&self, fiber_id: u64) {
        if !self.manages_fiber(fiber_id) {
            return;
        }
        self.prune_disposed_bindings();
        self.publish_snapshot_changed();
        self.state_progress.notify_waiters();
    }

    fn prune_disposed_bindings(&self) {
        for tree in std::iter::once(self.root.clone()).chain(self.runtime_trees()) {
            for entry in tree.entries.lock().expect("entries").values_mut() {
                if entry.fiber.as_ref().is_some_and(Fiber::is_disposed) {
                    entry.binding = None;
                }
            }
        }
    }

    fn runtime_trees(&self) -> Vec<Arc<RuntimeTree>> {
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
        let document = source.read()?;
        preflight(&self.catalog, &document.config)?;

        let prefix = EntryId::from(location.path.as_str());
        // 读取/预检在锁外；检查与保留在同一注册表临界区，不能被并发 attach 穿插。
        let tree = {
            let mut registry = self.subtrees.lock().expect("subtree registry");
            let owner_tree = if location.tree_prefix.is_empty() {
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
                document.revision,
            )?;
            registry.by_source.insert(source_id, tree.clone());
            registry.by_prefix.insert(prefix, tree.clone());
            tree
        };

        match tree.initialize(document.config).await {
            Ok(_) => Ok(tree),
            Err(error) => {
                let _ = tree.dispose_all().await;
                self.unregister_subtree(&tree);
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

    pub(crate) async fn detach_subtree(&self, tree: &Arc<RuntimeTree>) -> Result<(), LoaderError> {
        // 先卸嵌套子树（前缀以本树 prefix 开头的更长前缀）
        let nested: Vec<_> = {
            let registry = self.subtrees.lock().expect("subtree registry");
            let base = tree.prefix.as_str();
            registry
                .by_prefix
                .iter()
                .filter(|(prefix, _)| {
                    let p = prefix.as_str();
                    !p.is_empty()
                        && p != base
                        && (base.is_empty() || p.starts_with(&(base.to_string() + ":")))
                })
                .map(|(_, t)| t.clone())
                .collect()
        };
        // 深者优先
        let mut nested = nested;
        nested.sort_by(|a, b| {
            b.prefix
                .as_str()
                .matches(':')
                .count()
                .cmp(&a.prefix.as_str().matches(':').count())
        });
        for child in nested {
            child.dispose_all().await?;
            self.unregister_subtree(&child);
        }
        tree.dispose_all().await?;
        self.unregister_subtree(tree);
        self.publish_snapshot_changed();
        Ok(())
    }
}

fn preflight(catalog: &ExtensionCatalog, config: &ExtensionsConfig) -> Result<(), LoaderError> {
    config.validate(catalog)?;
    Ok(())
}
