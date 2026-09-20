//! 可挂载的静态 EntryTree Loader 插件。

use crate::{
    bootstrap::{load_bootstrap, load_extensions_source},
    catalog::ExtensionCatalog,
    config::{EntryOptions, ExtensionsConfig},
    error::{LoaderError, format_panic_message},
    loader::{LOADER, Loader},
    snapshot::{EntryId, EntrySnapshot, LoaderSnapshot},
};
use async_trait::async_trait;
use cordis_core::{Context, CoreError, Fiber, Plugin, PluginKey, ServiceId};
use std::{
    collections::BTreeMap,
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

impl LoaderInner {
    pub(crate) async fn apply(
        self: &Arc<Self>,
        config: ExtensionsConfig,
        revision: Option<String>,
    ) -> Result<LoaderSnapshot, LoaderError> {
        let prepared = prepare_tree(&self.catalog, &config)?;
        let completion = {
            let mut slot = self.reconcile.lock().expect("reconcile slot");
            if slot.is_some() {
                return Err(LoaderError::ReconcileBusy);
            }
            let completion = ReconcileCompletion::new();
            *slot = Some(completion.clone());
            self.spawn_reconcile(completion.clone(), prepared, config, revision);
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
        prepared: Vec<PreparedEntry>,
        config: ExtensionsConfig,
        revision: Option<String>,
    ) {
        let inner = self.clone();
        let task_completion = completion.clone();
        let task = tokio::runtime::Handle::current().spawn(async move {
            let worker_inner = inner.clone();
            let result =
                match tokio::spawn(async move { worker_inner.rebuild(prepared).await }).await {
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
    async fn rebuild(&self, prepared: Vec<PreparedEntry>) -> Result<LoaderSnapshot, LoaderError> {
        self.dispose_all().await?;
        self.mount_entries(&prepared, None, self.context.clone(), false)
            .await?;
        Ok(self.snapshot())
    }
    async fn dispose_all(&self) -> Result<(), LoaderError> {
        let order = std::mem::take(&mut *self.order.lock().expect("entry order"));
        let mut entries = std::mem::take(&mut *self.entries.lock().expect("entries"));
        for path in order.into_iter().rev() {
            if let Some(mut entry) = entries.remove(&path)
                && let Some(mut fiber) = entry.fiber.take()
            {
                fiber
                    .dispose_wait()
                    .await
                    .map_err(|source| LoaderError::Lifecycle {
                        instance: path.to_string(),
                        source,
                    })?;
            }
        }
        Ok(())
    }
    async fn mount_entries(
        &self,
        prepared: &[PreparedEntry],
        parent: Option<EntryId>,
        parent_context: Context,
        parent_disabled: bool,
    ) -> Result<(), LoaderError> {
        for entry in prepared {
            let context = parent_context
                .extend()
                .map_err(|source| LoaderError::Runtime { source })?;
            let (context, dependencies) = self.catalog.resolve_injections(
                entry.options.inject.as_ref(),
                entry.path.as_str(),
                context,
            )?;
            let enabled = !parent_disabled && !entry.options.disabled;
            let (fiber, plugin_key) = if entry.options.group {
                (
                    Some(
                        context
                            .plugin(Arc::new(ConfiguredPlugin::new(
                                Arc::new(GroupPlugin),
                                dependencies,
                            )))
                            .await
                            .map_err(|source| LoaderError::Lifecycle {
                                instance: entry.path.to_string(),
                                source,
                            })?,
                    ),
                    Some(PluginKey::new("cordis.loader.group")),
                )
            } else if enabled {
                let plugin = entry
                    .plugin
                    .clone()
                    .expect("prepared enabled ordinary entry");
                (
                    Some(
                        context
                            .plugin(Arc::new(ConfiguredPlugin::new(plugin, dependencies)))
                            .await
                            .map_err(|source| LoaderError::Lifecycle {
                                instance: entry.path.to_string(),
                                source,
                            })?,
                    ),
                    entry.plugin_key,
                )
            } else {
                (None, None)
            };
            self.order
                .lock()
                .expect("entry order")
                .push(entry.path.clone());
            self.entries.lock().expect("entries").insert(
                entry.path.clone(),
                MountedEntry {
                    parent: parent.clone(),
                    name: entry.options.name.clone(),
                    group: entry.options.group,
                    enabled,
                    fiber,
                    plugin_key,
                    context: context.clone(),
                },
            );
            if entry.options.group {
                Box::pin(self.mount_entries(
                    &entry.children,
                    Some(entry.path.clone()),
                    context,
                    parent_disabled || entry.options.disabled,
                ))
                .await?;
            }
        }
        Ok(())
    }
    fn snapshot(&self) -> LoaderSnapshot {
        let entries = self.entries.lock().expect("entries");
        let order = self.order.lock().expect("entry order");
        LoaderSnapshot {
            entries: order
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

struct PreparedEntry {
    path: EntryId,
    options: EntryOptions,
    plugin: Option<Arc<dyn Plugin>>,
    plugin_key: Option<PluginKey>,
    children: Vec<PreparedEntry>,
}
fn preflight(catalog: &ExtensionCatalog, config: &ExtensionsConfig) -> Result<(), LoaderError> {
    prepare_tree(catalog, config).map(|_| ())
}
fn prepare_tree(
    catalog: &ExtensionCatalog,
    config: &ExtensionsConfig,
) -> Result<Vec<PreparedEntry>, LoaderError> {
    config.validate(catalog)?;
    prepare_entries(catalog, &config.extensions, None, false)
}
fn prepare_entries(
    catalog: &ExtensionCatalog,
    entries: &[EntryOptions],
    parent: Option<&str>,
    parent_disabled: bool,
) -> Result<Vec<PreparedEntry>, LoaderError> {
    entries
        .iter()
        .map(|options| {
            let path = EntryId::from(parent.map_or_else(
                || options.id.clone(),
                |parent| format!("{parent}:{id}", id = options.id),
            ));
            let effective_disabled = parent_disabled || options.disabled;
            let children = if options.group {
                prepare_entries(
                    catalog,
                    &options.children()?,
                    Some(path.as_str()),
                    effective_disabled,
                )?
            } else {
                Vec::new()
            };
            let (plugin, plugin_key) = if !options.group && !effective_disabled {
                let factory = catalog.get(&options.name).expect("validated factory");
                let plugin = catch_unwind(AssertUnwindSafe(|| factory.build(&options.config)))
                    .map_err(|payload| LoaderError::FactoryPanic {
                        message: format_panic_message("extension factory build", payload),
                    })?
                    .map_err(|error| LoaderError::PluginBuild {
                        instance: path.to_string(),
                        factory: options.name.clone(),
                        message: error.to_string(),
                    })?;
                let plugin_key =
                    catch_unwind(AssertUnwindSafe(|| plugin.key())).map_err(|payload| {
                        LoaderError::PluginPanic {
                            message: format_panic_message("plugin key", payload),
                        }
                    })?;
                (Some(plugin), Some(plugin_key))
            } else {
                (None, None)
            };
            Ok(PreparedEntry {
                path,
                options: options.clone(),
                plugin,
                plugin_key,
                children,
            })
        })
        .collect()
}
