//! `CordisLoader`：创建 Runtime、保存实例映射并执行 reconcile。
//!
//! reconcile 由独立于调用者 Future 的协调器执行：取消 `apply()` / `reload()` 只取消等待，
//! 不中断编排，也不阻止 Loader 提交 `instances` / `order` / `config`。

use crate::{
    bootstrap::{load_bootstrap, load_extensions_source},
    catalog::ExtensionCatalog,
    config::ExtensionsConfig,
    error::LoaderError,
    loader::{LOADER, Loader},
    reconcile::{self, PreparedInstance, ReconcilePlan},
    snapshot::{InstanceId, InstanceSnapshot, LoaderSnapshot},
};
use cordis_core::{CoreError, Fiber, FiberState, PluginKey, Runtime};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use tokio::{sync::Notify, task::JoinHandle};

pub(crate) struct MountedInstance {
    pub(crate) factory: String,
    pub(crate) plugin_key: PluginKey,
    pub(crate) config: toml::Value,
    pub(crate) fiber: Fiber,
}

/// 一轮 Loader reconcile 的共享 completion：调用方只 wait，取消不 abort 协调器。
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

    fn attach_coordinator(&self, handle: JoinHandle<()>) {
        *self.retain.lock().expect("reconcile retain") = Some(handle);
    }

    fn finish(&self, result: Result<LoaderSnapshot, LoaderError>) {
        {
            let mut slot = self.result.lock().expect("reconcile completion");
            if slot.is_some() {
                return;
            }
            *slot = Some(result);
        }
        self.notify.notify_waiters();
    }

    async fn wait(self: &Arc<Self>) -> Result<LoaderSnapshot, LoaderError> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let slot = self.result.lock().expect("reconcile completion");
                if let Some(result) = slot.as_ref() {
                    return result.clone();
                }
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
    runtime: Runtime,
    pub(crate) extensions_path: Mutex<Option<PathBuf>>,
    pub(crate) desired: Mutex<ExtensionsConfig>,
    pub(crate) revision: Mutex<Option<String>>,
    instances: Mutex<HashMap<InstanceId, MountedInstance>>,
    order: Mutex<Vec<InstanceId>>,
    reconcile: Mutex<Option<Arc<ReconcileCompletion>>>,
    pub(crate) loader_operations: tokio::sync::Mutex<()>,
    loader: Loader,
}

/// 进程内静态插件 Loader。
///
/// 调用方必须在 Tokio 上下文中创建。重新加载只能通过显式 [`Self::reload`]
/// 或 [`Self::apply`]；首版不监听文件。
///
/// `apply` / `reload` 的等待可被取消，但内部 reconcile 仍会收敛并提交 Loader 映射。
#[derive(Clone)]
pub struct CordisLoader {
    pub(crate) inner: Arc<LoaderInner>,
}

impl CordisLoader {
    /// 创建空 Loader（尚无已挂载实例）。须在 Tokio 中调用。
    pub fn new(catalog: ExtensionCatalog) -> Result<Self, LoaderError> {
        let runtime = Runtime::new().map_err(|source| LoaderError::Runtime { source })?;
        let inner = Arc::new_cyclic(|weak| LoaderInner {
            catalog,
            runtime,
            extensions_path: Mutex::new(None),
            desired: Mutex::new(ExtensionsConfig {
                version: crate::config::CONFIG_VERSION,
                extensions: Vec::new(),
            }),
            revision: Mutex::new(None),
            instances: Mutex::new(HashMap::new()),
            order: Mutex::new(Vec::new()),
            reconcile: Mutex::new(None),
            loader_operations: tokio::sync::Mutex::new(()),
            loader: Loader::new(weak.clone()),
        });
        let host = Self { inner };
        host.root()
            .provide(LOADER, host.loader())
            .map_err(|source| LoaderError::Runtime { source })?;
        Ok(host)
    }

    /// 读取 bootstrap 与 extensions，预检通过后再创建 Runtime 并编排。
    pub async fn bootstrap(
        catalog: ExtensionCatalog,
        bootstrap_path: impl AsRef<Path>,
    ) -> Result<Self, LoaderError> {
        let (_, extensions_path) = load_bootstrap(bootstrap_path.as_ref())?;
        let (extensions, revision) = load_extensions_source(&extensions_path)?;
        let plan = reconcile::prepare(&catalog, &HashMap::new(), &[], &extensions)?;
        let host = Self::new(catalog)?;
        {
            *host.inner.extensions_path.lock().expect("extensions path") = Some(extensions_path);
            *host.inner.revision.lock().expect("revision") = Some(revision);
        }
        if let Err(error) = host.start_reconcile(plan, extensions).await {
            let _ = host.shutdown().await;
            return Err(error);
        }
        Ok(host)
    }

    /// 按启动时解析的 `extensions.toml` 绝对路径重新加载。
    pub async fn reload(&self) -> Result<LoaderSnapshot, LoaderError> {
        let path = self
            .inner
            .extensions_path
            .lock()
            .expect("extensions path")
            .clone()
            .ok_or(LoaderError::NoConfigSource)?;
        let (extensions, revision) = load_extensions_source(&path)?;
        self.apply_with_revision(extensions, Some(revision)).await
    }

    /// 对已解析的目标配置执行 reconcile。
    ///
    /// 调用方 Future 取消只取消 wait；协调器继续执行并提交 Loader 状态。
    /// 同一时刻已有 reconcile 时返回 [`LoaderError::ReconcileBusy`]。
    pub async fn apply(&self, config: ExtensionsConfig) -> Result<LoaderSnapshot, LoaderError> {
        self.apply_with_revision(config, None).await
    }

    pub(crate) async fn apply_with_revision(
        &self,
        config: ExtensionsConfig,
        revision: Option<String>,
    ) -> Result<LoaderSnapshot, LoaderError> {
        let completion = {
            let mut slot = self.inner.reconcile.lock().expect("reconcile slot");
            if slot.is_some() {
                return Err(LoaderError::ReconcileBusy);
            }
            let plan = {
                let instances = self.inner.instances.lock().expect("instances");
                let order = self.inner.order.lock().expect("order");
                reconcile::prepare(&self.inner.catalog, &instances, &order, &config)?
            };
            let completion = ReconcileCompletion::new();
            *slot = Some(completion.clone());
            self.spawn_reconcile(completion.clone(), plan, config, revision);
            completion
        };
        completion.wait().await
    }

    /// 返回此 Loader 的根 Context。
    ///
    /// 应用与受信任的进程内插件可通过它使用完整 Cordis 运行时能力。
    pub fn root(&self) -> cordis_core::Context {
        self.inner.runtime.root()
    }

    /// 返回此 Loader 的完整 Runtime。
    pub fn runtime(&self) -> &Runtime {
        &self.inner.runtime
    }

    /// 返回管理静态 Factory 实例并持久化 `extensions.toml` 的 Loader。
    pub fn loader(&self) -> Loader {
        self.inner.loader.clone()
    }

    pub fn snapshot(&self) -> LoaderSnapshot {
        self.inner.snapshot()
    }

    /// 先等待在途 reconcile 收敛，再关闭 Runtime。
    pub async fn shutdown(self) -> Result<(), LoaderError> {
        let pending = self.inner.reconcile.lock().expect("reconcile slot").clone();
        if let Some(completion) = pending {
            let _ = completion.wait().await;
        }
        self.inner
            .runtime
            .shutdown()
            .await
            .map_err(|source| LoaderError::Runtime { source })
    }

    async fn start_reconcile(
        &self,
        plan: ReconcilePlan,
        config: ExtensionsConfig,
    ) -> Result<LoaderSnapshot, LoaderError> {
        let completion = {
            let mut slot = self.inner.reconcile.lock().expect("reconcile slot");
            if slot.is_some() {
                return Err(LoaderError::ReconcileBusy);
            }
            let completion = ReconcileCompletion::new();
            *slot = Some(completion.clone());
            self.spawn_reconcile(completion.clone(), plan, config, None);
            completion
        };
        completion.wait().await
    }

    fn spawn_reconcile(
        &self,
        completion: Arc<ReconcileCompletion>,
        plan: ReconcilePlan,
        config: ExtensionsConfig,
        revision: Option<String>,
    ) {
        let inner = self.inner.clone();
        let completion_for_task = completion.clone();
        let supervisor = tokio::runtime::Handle::current().spawn(async move {
            let mut guard = ReconcileFinishGuard {
                inner: inner.clone(),
                completion: completion_for_task.clone(),
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
            inner.finish_reconcile(completion_for_task, result);
            guard.finished = true;
        });
        completion.attach_coordinator(supervisor);
    }
}

impl LoaderInner {
    fn finish_reconcile(
        &self,
        completion: Arc<ReconcileCompletion>,
        result: Result<LoaderSnapshot, LoaderError>,
    ) {
        {
            let mut slot = self.reconcile.lock().expect("reconcile slot");
            if slot
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &completion))
            {
                *slot = None;
            }
        }
        completion.finish(result);
    }

    fn snapshot(&self) -> LoaderSnapshot {
        let instances_map = self.instances.lock().expect("instances");
        let order = self.order.lock().expect("order");
        let instances = order
            .iter()
            .filter_map(|id| {
                let mounted = instances_map.get(id)?;
                Some(InstanceSnapshot {
                    instance: id.clone(),
                    factory: mounted.factory.clone(),
                    plugin_key: mounted.plugin_key,
                    fiber_id: mounted.fiber.id(),
                    state: mounted.fiber.state(),
                    last_error: mounted.fiber.last_error(),
                })
            })
            .collect();
        LoaderSnapshot {
            instances,
            runtime: self.runtime.diagnostics(),
        }
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
        self.runtime.settle().await;
        Ok(self.snapshot())
    }

    async fn remove_instance(&self, id: InstanceId) -> Result<(), LoaderError> {
        let Some(mut mounted) = self.instances.lock().expect("instances").remove(&id) else {
            self.order
                .lock()
                .expect("order")
                .retain(|existing| existing != &id);
            return Ok(());
        };
        let result = mounted.fiber.dispose_wait().await;
        if mounted.fiber.is_disposed() {
            self.order
                .lock()
                .expect("order")
                .retain(|existing| existing != &id);
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
        let result = mounted.fiber.replace(plugin).await;
        let outcome = match result {
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
        let fiber = match self.runtime.root().plugin(plugin).await {
            Ok(fiber) => fiber,
            Err(source) => {
                return Err(LoaderError::Lifecycle {
                    instance: instance.to_string(),
                    source,
                });
            }
        };
        {
            // 双锁顺序必须与 apply/snapshot 一致：instances → order。
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
                },
            );
            failed_fiber_error(
                &instance,
                &instances.get(&instance).expect("just inserted").fiber,
            )
        }
    }
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
