//! 可挂载的静态 EntryTree Loader 插件（按 Entry/子树差分 reconcile）。

use crate::{
    bootstrap::{load_bootstrap, load_extensions_source},
    catalog::ExtensionCatalog,
    config::ExtensionsConfig,
    error::LoaderError,
    loader::{LOADER, Loader},
    snapshot::{EntryId, LoaderSnapshot},
    tree::{RuntimeTree, aggregate_snapshot, canonicalize_existing, resolve_include_path},
};
use async_trait::async_trait;
use cordis_core::{Context, CoreError, Fiber, Plugin, PluginKey};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

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
        let root = RuntimeTree::new_root(self.catalog.clone(), context, extensions_path, revision);
        let inner = Arc::new(LoaderInner {
            catalog: self.catalog.clone(),
            root: root.clone(),
            subtrees: Mutex::new(SubtreeRegistry::default()),
            alive: AtomicBool::new(true),
            initial: Mutex::new(Some(config)),
        });
        *root.loader.lock().expect("loader weak") = Arc::downgrade(&inner);
        inner
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
            .root
            .apply(config, None)
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
    initial: Mutex<Option<ExtensionsConfig>>,
}

/// 所有子树索引必须作为一个原子注册表读写。
///
/// `by_file` 保证一个配置文件只附着一次；`by_prefix` 提供路径定位与快照聚合。
#[derive(Default)]
pub(crate) struct SubtreeRegistry {
    pub(crate) by_file: HashMap<PathBuf, Arc<RuntimeTree>>,
    pub(crate) by_prefix: HashMap<EntryId, Arc<RuntimeTree>>,
}

impl LoaderInner {
    pub(crate) async fn await_idle_all(self: &Arc<Self>) -> Result<LoaderSnapshot, LoaderError> {
        let _ = self.root.await_idle().await?;
        let subtrees: Vec<_> = self
            .subtrees
            .lock()
            .expect("subtree registry")
            .by_prefix
            .values()
            .cloned()
            .collect();
        for tree in subtrees {
            let _ = tree.await_idle().await?;
        }
        Ok(self.aggregate_snapshot())
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

    pub(crate) async fn attach_file_subtree(
        self: &Arc<Self>,
        owner: &Context,
        configured: &Path,
    ) -> Result<Arc<RuntimeTree>, LoaderError> {
        let location = owner
            .config(crate::meta::ENTRY_LOCATION)
            .map_err(|_| LoaderError::EntryLocationMissing)?;
        let resolved = resolve_include_path(location.source_dir.as_deref(), configured)?;
        if !resolved.is_file() {
            return Err(LoaderError::Io {
                path: resolved.clone(),
                message: "include file does not exist".into(),
            });
        }
        let file_key = canonicalize_existing(&resolved)?;

        let (config, revision) = load_extensions_source(&resolved)?;
        preflight(&self.catalog, &config)?;

        let prefix = EntryId::from(location.path.as_str());
        // 读取/预检在锁外；检查与保留在同一注册表临界区，不能被并发 attach 穿插。
        let tree = {
            let mut registry = self.subtrees.lock().expect("subtree registry");
            let owner_tree = registry
                .by_prefix
                .get(&EntryId::from(location.tree_prefix.as_str()))
                .cloned()
                .unwrap_or_else(|| self.root.clone());

            // 先检查祖先链，确保循环比一般重复来源得到更准确的诊断。
            let parent_file = owner_tree.file_key.clone();
            let mut cursor = parent_file.clone();
            while let Some(current) = cursor {
                if current == file_key {
                    return Err(LoaderError::IncludeCycle { path: file_key });
                }
                cursor = registry
                    .by_file
                    .get(&current)
                    .and_then(|tree| tree.parent_file.clone())
                    .or_else(|| {
                        if self.root.file_key.as_ref() == Some(&current) {
                            self.root.parent_file.clone()
                        } else {
                            None
                        }
                    });
            }

            if registry.by_file.contains_key(&file_key)
                || self
                    .root
                    .file_key
                    .as_ref()
                    .is_some_and(|root| root == &file_key)
                || registry.by_prefix.contains_key(&prefix)
            {
                return Err(LoaderError::DuplicateSubtreeSource { path: file_key });
            }

            let tree = RuntimeTree::new_subtree(
                self.catalog.clone(),
                Arc::downgrade(self),
                owner.clone(),
                prefix.clone(),
                resolved,
                file_key.clone(),
                parent_file,
                revision.clone(),
            );
            registry.by_file.insert(file_key.clone(), tree.clone());
            registry.by_prefix.insert(prefix, tree.clone());
            tree
        };

        match tree.apply(config, Some(revision)).await {
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
        if let Some(key) = &tree.file_key
            && registry
                .by_file
                .get(key)
                .is_some_and(|current| Arc::ptr_eq(current, tree))
        {
            registry.by_file.remove(key);
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
        Ok(())
    }
}

fn preflight(catalog: &ExtensionCatalog, config: &ExtensionsConfig) -> Result<(), LoaderError> {
    config.validate(catalog)?;
    Ok(())
}
