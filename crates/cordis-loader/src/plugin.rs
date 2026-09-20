//! 可挂载的静态 EntryTree Loader 插件（按 Entry/子树差分 reconcile）。

use crate::{
    bootstrap::{load_bootstrap, load_extensions_source},
    catalog::ExtensionCatalog,
    config::{EntryOptions, ExtensionsConfig},
    error::LoaderError,
    loader::{LOADER, Loader},
    reconcile::{
        DesiredNode, ReconcilePlan, build_plan_plugins, index_desired_tree, plan_reconcile,
    },
    snapshot::{EntryId, EntrySnapshot, LoaderSnapshot},
};
use async_trait::async_trait;
use cordis_core::{Context, CoreError, Fiber, Plugin, PluginKey};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{sync::Notify, task::JoinHandle};

pub(crate) struct MountedEntry {
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
    pub(crate) options: EntryOptions,
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
    pub(crate) entries: Mutex<BTreeMap<EntryId, MountedEntry>>,
    pub(crate) order: Mutex<Vec<EntryId>>,
    reconcile: Mutex<Option<Arc<ReconcileCompletion>>>,
    pub(crate) loader_operations: tokio::sync::Mutex<()>,
    pub(crate) alive: AtomicBool,
    initial: Mutex<Option<ExtensionsConfig>>,
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

    pub(crate) fn snapshot(&self) -> LoaderSnapshot {
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

fn preflight(catalog: &ExtensionCatalog, config: &ExtensionsConfig) -> Result<(), LoaderError> {
    config.validate(catalog)?;
    Ok(())
}
