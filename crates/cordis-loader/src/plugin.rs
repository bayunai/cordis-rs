//! 可挂载的静态 Catalog Loader 插件及其 reconcile 状态。

use crate::{
    bootstrap::{load_bootstrap, load_extensions_source},
    catalog::ExtensionCatalog,
    config::ExtensionsConfig,
    error::LoaderError,
    loader::{LOADER, Loader},
    reconcile::{self, PreparedInstance, ReconcilePlan},
    snapshot::{InstanceId, InstanceSnapshot, LoaderSnapshot},
};
use async_trait::async_trait;
use cordis_core::{Context, CoreError, Fiber, FiberState, Plugin, PluginKey};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{sync::Notify, task::JoinHandle};

pub(crate) struct MountedInstance {
    pub(crate) factory: String,
    pub(crate) plugin_key: PluginKey,
    pub(crate) config: toml::Value,
    pub(crate) fiber: Fiber,
    pub(crate) context: Context,
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

/// 由 LoaderPlugin 作为普通 Cordis 插件挂载的静态 Catalog Loader。
pub struct LoaderPlugin {
    catalog: ExtensionCatalog,
    source: ConfigSource,
    state: Mutex<Option<Arc<LoaderInner>>>,
}

impl LoaderPlugin {
    /// 创建不带文件持久化来源的内存 LoaderPlugin。
    pub fn new(catalog: ExtensionCatalog, config: ExtensionsConfig) -> Result<Self, LoaderError> {
        preflight(&catalog, &config)?;
        Ok(Self {
            catalog,
            source: ConfigSource::Memory { config },
            state: Mutex::new(None),
        })
    }

    /// 读取 Bootstrap 与扩展清单并预检，返回可由 `Context::plugin` 挂载的插件。
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
        Arc::new_cyclic(|weak| LoaderInner {
            catalog: self.catalog.clone(),
            context,
            extensions_path: Mutex::new(extensions_path),
            desired: Mutex::new(ExtensionsConfig {
                version: crate::config::CONFIG_VERSION,
                extensions: Vec::new(),
            }),
            revision: Mutex::new(revision),
            instances: Mutex::new(HashMap::new()),
            order: Mutex::new(Vec::new()),
            reconcile: Mutex::new(None),
            loader_operations: tokio::sync::Mutex::new(()),
            alive: AtomicBool::new(true),
            loader: Loader::new(weak.clone()),
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
        if let Some(previous) = self.state.lock().expect("loader plugin state").take() {
            previous.alive.store(false, Ordering::Release);
        }
        let inner = self.initial_state(ctx.extend()?);
        ctx.provide(LOADER, inner.loader.clone())?;
        ctx.effect_named("loader")?.on_dispose({
            let inner = inner.clone();
            move || inner.alive.store(false, Ordering::Release)
        });
        *self.state.lock().expect("loader plugin state") = Some(inner.clone());
        let config = inner
            .initial
            .lock()
            .expect("initial config")
            .take()
            .expect("LoaderPlugin apply only initializes once per state");
        inner
            .apply(config, None)
            .await
            .map(|_| ())
            .map_err(|error| CoreError::PluginApply(error.to_string()))
    }
}

/// 一轮 reconcile 的共享 completion；取消等待者不会中止编排。
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
        let mut slot = self.result.lock().expect("reconcile completion");
        if slot.is_none() {
            *slot = Some(result);
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

struct ReconcileFinishGuard {
    inner: Arc<LoaderInner>,
    completion: Arc<ReconcileCompletion>,
    finished: bool,
}

impl Drop for ReconcileFinishGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.inner.finish_reconcile(
                self.completion.clone(),
                Err(LoaderError::ReconcileAborted {
                    reason: "reconcile supervisor dropped".into(),
                }),
            );
        }
    }
}

pub(crate) struct LoaderInner {
    pub(crate) catalog: ExtensionCatalog,
    pub(crate) context: Context,
    pub(crate) extensions_path: Mutex<Option<PathBuf>>,
    pub(crate) desired: Mutex<ExtensionsConfig>,
    pub(crate) revision: Mutex<Option<String>>,
    instances: Mutex<HashMap<InstanceId, MountedInstance>>,
    order: Mutex<Vec<InstanceId>>,
    reconcile: Mutex<Option<Arc<ReconcileCompletion>>>,
    pub(crate) loader_operations: tokio::sync::Mutex<()>,
    pub(crate) alive: AtomicBool,
    pub(crate) loader: Loader,
    initial: Mutex<Option<ExtensionsConfig>>,
}

impl LoaderInner {
    pub(crate) async fn apply(
        self: &Arc<Self>,
        config: ExtensionsConfig,
        revision: Option<String>,
    ) -> Result<LoaderSnapshot, LoaderError> {
        if !self.alive.load(Ordering::Acquire) {
            return Err(LoaderError::Runtime {
                source: CoreError::ContextDisposed,
            });
        }
        let completion = {
            let mut slot = self.reconcile.lock().expect("reconcile slot");
            if slot.is_some() {
                return Err(LoaderError::ReconcileBusy);
            }
            let plan = {
                let instances = self.instances.lock().expect("instances");
                let order = self.order.lock().expect("order");
                reconcile::prepare(&self.catalog, &instances, &order, &config)?
            };
            let completion = ReconcileCompletion::new();
            *slot = Some(completion.clone());
            self.spawn_reconcile(completion.clone(), plan, config, revision);
            completion
        };
        completion.wait().await
    }

    pub(crate) async fn await_idle(self: &Arc<Self>) -> Result<LoaderSnapshot, LoaderError> {
        let pending = self.reconcile.lock().expect("reconcile slot").clone();
        if let Some(completion) = pending {
            completion.wait().await
        } else if self.alive.load(Ordering::Acquire) {
            Ok(self.snapshot())
        } else {
            Err(LoaderError::Runtime {
                source: CoreError::ContextDisposed,
            })
        }
    }

    fn spawn_reconcile(
        self: &Arc<Self>,
        completion: Arc<ReconcileCompletion>,
        plan: ReconcilePlan,
        config: ExtensionsConfig,
        revision: Option<String>,
    ) {
        let inner = self.clone();
        let task_completion = completion.clone();
        let task = tokio::runtime::Handle::current().spawn(async move {
            let mut guard = ReconcileFinishGuard {
                inner: inner.clone(),
                completion: task_completion.clone(),
                finished: false,
            };
            let worker_inner = inner.clone();
            let worker = tokio::spawn(async move { worker_inner.run_reconcile(plan).await });
            let result = match worker.await {
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
            inner.finish_reconcile(task_completion, result);
            guard.finished = true;
        });
        completion.attach(task);
    }

    fn finish_reconcile(
        &self,
        completion: Arc<ReconcileCompletion>,
        result: Result<LoaderSnapshot, LoaderError>,
    ) {
        let mut slot = self.reconcile.lock().expect("reconcile slot");
        if slot
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, &completion))
        {
            *slot = None;
        }
        drop(slot);
        completion.finish(result);
    }

    fn snapshot(&self) -> LoaderSnapshot {
        let instances = self.instances.lock().expect("instances");
        let order = self.order.lock().expect("order");
        LoaderSnapshot {
            instances: order
                .iter()
                .filter_map(|id| {
                    let mounted = instances.get(id)?;
                    Some(InstanceSnapshot {
                        instance: id.clone(),
                        factory: mounted.factory.clone(),
                        plugin_key: mounted.plugin_key,
                        fiber_id: mounted.fiber.id(),
                        state: mounted.fiber.state(),
                        last_error: mounted.fiber.last_error(),
                    })
                })
                .collect(),
        }
    }

    pub(crate) fn entry_context(&self, instance: &str) -> Result<Context, LoaderError> {
        self.instances
            .lock()
            .expect("instances")
            .get(&InstanceId::from(instance))
            .map(|mounted| mounted.context.clone())
            .ok_or_else(|| LoaderError::UnknownInstance {
                instance: instance.to_string(),
            })
    }

    async fn run_reconcile(&self, plan: ReconcilePlan) -> Result<LoaderSnapshot, LoaderError> {
        for id in plan.removes {
            self.remove_instance(id).await?;
        }
        for item in plan.replaces {
            self.replace_instance(item).await?;
        }
        for item in plan.adds {
            self.add_instance(item).await?;
        }
        Ok(self.snapshot())
    }

    async fn remove_instance(&self, id: InstanceId) -> Result<(), LoaderError> {
        let Some(mut mounted) = self.instances.lock().expect("instances").remove(&id) else {
            self.order.lock().expect("order").retain(|item| item != &id);
            return Ok(());
        };
        let result = mounted.fiber.dispose_wait().await;
        if mounted.fiber.is_disposed() {
            self.order.lock().expect("order").retain(|item| item != &id);
        } else {
            self.instances
                .lock()
                .expect("instances")
                .insert(id.clone(), mounted);
        }
        result.map_err(|source| LoaderError::Lifecycle {
            instance: id.to_string(),
            source,
        })
    }

    async fn replace_instance(&self, item: PreparedInstance) -> Result<(), LoaderError> {
        let PreparedInstance {
            instance,
            factory,
            config,
            plugin,
            plugin_key,
        } = item;
        let mut mounted = self
            .instances
            .lock()
            .expect("instances")
            .remove(&instance)
            .ok_or_else(|| LoaderError::Lifecycle {
                instance: instance.to_string(),
                source: CoreError::FiberDisposed,
            })?;
        let outcome = match mounted.fiber.replace(plugin).await {
            Err(CoreError::PluginKeyMismatch { expected, actual }) => {
                Err(LoaderError::PluginKeyChanged {
                    instance: instance.to_string(),
                    expected,
                    actual,
                })
            }
            Err(source) => {
                commit_mounted(&mut mounted, factory, plugin_key, config);
                Err(lifecycle_error(&instance, source))
            }
            Ok(()) => {
                commit_mounted(&mut mounted, factory, plugin_key, config);
                failed_fiber_error(&instance, &mounted.fiber)
            }
        };
        self.instances
            .lock()
            .expect("instances")
            .insert(instance, mounted);
        outcome
    }

    async fn add_instance(&self, item: PreparedInstance) -> Result<(), LoaderError> {
        let PreparedInstance {
            instance,
            factory,
            config,
            plugin,
            plugin_key,
        } = item;
        let entry_context = self
            .context
            .extend()
            .map_err(|source| LoaderError::Runtime { source })?;
        let fiber =
            entry_context
                .plugin(plugin)
                .await
                .map_err(|source| LoaderError::Lifecycle {
                    instance: instance.to_string(),
                    source,
                })?;
        let mut instances = self.instances.lock().expect("instances");
        let mut order = self.order.lock().expect("order");
        order.push(instance.clone());
        instances.insert(
            instance.clone(),
            MountedInstance {
                factory,
                plugin_key,
                config,
                fiber,
                context: entry_context,
            },
        );
        failed_fiber_error(
            &instance,
            &instances.get(&instance).expect("inserted").fiber,
        )
    }
}

fn preflight(catalog: &ExtensionCatalog, config: &ExtensionsConfig) -> Result<(), LoaderError> {
    reconcile::prepare(catalog, &HashMap::new(), &[], config).map(|_| ())
}

fn commit_mounted(
    mounted: &mut MountedInstance,
    factory: String,
    plugin_key: PluginKey,
    config: toml::Value,
) {
    mounted.factory = factory;
    mounted.plugin_key = plugin_key;
    mounted.config = config;
}

fn failed_fiber_error(instance: &InstanceId, fiber: &Fiber) -> Result<(), LoaderError> {
    if fiber.state() == FiberState::Failed {
        Err(lifecycle_error(
            instance,
            CoreError::PluginApply(fiber.last_error().unwrap_or_default()),
        ))
    } else {
        Ok(())
    }
}

fn lifecycle_error(instance: &InstanceId, source: CoreError) -> LoaderError {
    LoaderError::Lifecycle {
        instance: instance.to_string(),
        source,
    }
}
