use async_trait::async_trait;
use cordis_core::{
    ConfigKey, Context, CoreError, Fiber, FiberState, Plugin, PluginKey, Runtime, ServiceKey,
};
use cordis_loader::{
    EntryOptions, EntryUpdate, ExtensionCatalog, ExtensionFactory, ExtensionsConfig, InjectConfig,
    InjectionDescriptor, IsolateValue, IsolationDescriptor, LOADER, Loader, LoaderControlError,
    LoaderError, LoaderEvent, LoaderPlugin, PreparedExtension,
};
use schemars::JsonSchema;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

static DEP: ServiceKey<()> = ServiceKey::new("loader.tree.dep@1");
static VALUE: ServiceKey<String> = ServiceKey::new("loader.tree.value@1");
static LABEL: ConfigKey<LabelConfig> = ConfigKey::new("loader.tree.label@1");

#[derive(Clone, serde::Deserialize, JsonSchema)]
struct Empty {}
#[derive(Clone, serde::Deserialize, JsonSchema)]
struct LabelConfig {
    label: String,
}
struct Factory {
    id: &'static str,
    needs_dep: bool,
    record: Option<Arc<Mutex<Vec<String>>>>,
}
struct TestPlugin {
    id: &'static str,
    needs_dep: bool,
    record: Option<Arc<Mutex<Vec<String>>>>,
}
#[async_trait]
impl Plugin for TestPlugin {
    fn key(&self) -> PluginKey {
        PluginKey::new(self.id)
    }
    fn inject(&self) -> Vec<cordis_core::ServiceId> {
        self.needs_dep.then_some(DEP.id()).into_iter().collect()
    }
    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        if self.id == "demo.value" {
            ctx.provide(VALUE, "value".into())?;
        }
        if self.id == "demo.dep" || self.id == "demo.dep2" {
            ctx.provide(DEP, ())?;
        }
        if self.id == "demo.label" {
            ctx.provide(VALUE, ctx.config(LABEL)?.label.clone())?;
        }
        if let Some(record) = &self.record {
            let id = self.id.to_string();
            let record = record.clone();
            ctx.effect()?
                .on_dispose(move || record.lock().unwrap().push(id.clone()));
        }
        Ok(())
    }
}
impl ExtensionFactory for Factory {
    type Config = Empty;
    fn id(&self) -> &'static str {
        self.id
    }
    fn prepare(&self, _: Empty) -> Result<PreparedExtension, LoaderError> {
        let id = self.id;
        let needs_dep = self.needs_dep;
        let record = self.record.clone();
        Ok(PreparedExtension::ordinary(move || {
            Ok(Arc::new(TestPlugin {
                id,
                needs_dep,
                record,
            }))
        }))
    }
}

struct GatedLoadingFactory {
    entered: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    release: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
    fail: bool,
}

struct GatedLoadingPlugin {
    entered: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    release: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
    fail: bool,
}

#[async_trait]
impl Plugin for GatedLoadingPlugin {
    fn key(&self) -> PluginKey {
        PluginKey::new("demo.gated-loading")
    }

    fn inject(&self) -> Vec<cordis_core::ServiceId> {
        vec![DEP.id()]
    }

    async fn apply(&self, _: &Context) -> Result<(), CoreError> {
        if let Some(sender) = self.entered.lock().expect("loading entered").take() {
            let _ = sender.send(());
        }
        let receiver = self.release.lock().expect("loading release").take();
        if let Some(receiver) = receiver {
            let _ = receiver.await;
        }
        if self.fail {
            Err(CoreError::PluginApply("gated apply failure".into()))
        } else {
            Ok(())
        }
    }
}

impl ExtensionFactory for GatedLoadingFactory {
    type Config = Empty;

    fn id(&self) -> &'static str {
        "demo.gated-loading"
    }

    fn prepare(&self, _: Empty) -> Result<PreparedExtension, LoaderError> {
        let entered = self.entered.clone();
        let release = self.release.clone();
        let fail = self.fail;
        Ok(PreparedExtension::ordinary(move || {
            Ok(Arc::new(GatedLoadingPlugin {
                entered,
                release,
                fail,
            }))
        }))
    }
}

struct GatedUnloadingFactory {
    entered: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    release: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
}

struct GatedUnloadingPlugin {
    entered: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    release: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
}

#[async_trait]
impl Plugin for GatedUnloadingPlugin {
    fn key(&self) -> PluginKey {
        PluginKey::new("demo.gated-unloading")
    }

    fn inject(&self) -> Vec<cordis_core::ServiceId> {
        vec![DEP.id()]
    }

    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        let entered = self.entered.clone();
        let release = self.release.clone();
        ctx.effect()?.on_dispose_async(move || async move {
            if let Some(sender) = entered.lock().expect("unloading entered").take() {
                let _ = sender.send(());
            }
            let receiver = release.lock().expect("unloading release").take();
            if let Some(receiver) = receiver {
                let _ = receiver.await;
            }
            Ok(())
        })?;
        Ok(())
    }
}

impl ExtensionFactory for GatedUnloadingFactory {
    type Config = Empty;

    fn id(&self) -> &'static str {
        "demo.gated-unloading"
    }

    fn prepare(&self, _: Empty) -> Result<PreparedExtension, LoaderError> {
        let entered = self.entered.clone();
        let release = self.release.clone();
        Ok(PreparedExtension::ordinary(move || {
            Ok(Arc::new(GatedUnloadingPlugin { entered, release }))
        }))
    }
}
fn catalog(factories: Vec<Factory>) -> ExtensionCatalog {
    let mut catalog = ExtensionCatalog::new();
    for factory in factories {
        catalog.register(factory).unwrap();
    }
    catalog
}
fn config(entries: Vec<EntryOptions>) -> ExtensionsConfig {
    ExtensionsConfig {
        version: 3,
        extensions: entries,
    }
}
async fn mount(plugin: LoaderPlugin) -> (Runtime, Fiber, Loader) {
    let runtime = Runtime::new().unwrap();
    let root = runtime.root();
    let fiber = root.plugin(Arc::new(plugin)).await.unwrap();
    let loader = (*root.get(LOADER).unwrap()).clone();
    (runtime, fiber, loader)
}

#[tokio::test]
async fn tree_uses_paths_group_contexts_and_ancestor_disable() {
    let child = EntryOptions::new("child", "demo.value");
    let mut group = EntryOptions::group("parent", vec![child]).unwrap();
    group.disabled = true;
    let (runtime, mut loader_fiber, loader) = mount(
        LoaderPlugin::new(
            catalog(vec![Factory {
                id: "demo.value",
                needs_dep: false,
                record: None,
            }]),
            config(vec![group]),
        )
        .unwrap(),
    )
    .await;
    let snapshot = loader.await_idle().await.unwrap();
    assert_eq!(snapshot.entries.len(), 2);
    assert!(snapshot.entry("parent").unwrap().state.is_some());
    assert!(!snapshot.entry("parent:child").unwrap().enabled);
    assert!(snapshot.entry("parent:child").unwrap().state.is_none());
    assert!(runtime.root().get(VALUE).is_err());
    loader_fiber.dispose_wait().await.unwrap();
    assert!(runtime.root().get(LOADER).is_err());
    assert!(matches!(
        loader.entries(),
        Err(LoaderControlError::LoaderUnavailable)
    ));
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn sibling_provider_unblocks_pending_consumer() {
    let (runtime, _loader_fiber, loader) = mount(
        LoaderPlugin::new(
            catalog(vec![
                Factory {
                    id: "demo.dep",
                    needs_dep: false,
                    record: None,
                },
                Factory {
                    id: "demo.consumer",
                    needs_dep: true,
                    record: None,
                },
            ]),
            config(vec![
                EntryOptions::new("provider", "demo.dep"),
                EntryOptions::new("consumer", "demo.consumer"),
            ]),
        )
        .unwrap(),
    )
    .await;
    let snapshot = loader.await_idle().await.unwrap();
    assert_eq!(
        snapshot.entry("provider").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(
        snapshot.entry("consumer").unwrap().state,
        Some(FiberState::Active)
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn managed_fiber_activation_after_external_provider_invalidates_snapshot() {
    let (runtime, _loader_fiber, loader) = mount(
        LoaderPlugin::new(
            catalog(vec![Factory {
                id: "demo.consumer",
                needs_dep: true,
                record: None,
            }]),
            config(vec![EntryOptions::new("consumer", "demo.consumer")]),
        )
        .unwrap(),
    )
    .await;
    assert_eq!(
        loader
            .await_idle()
            .await
            .unwrap()
            .entry("consumer")
            .unwrap()
            .state,
        Some(FiberState::Pending)
    );
    let mut events = loader.subscribe().unwrap();

    runtime
        .root()
        .plugin(Arc::new(TestPlugin {
            id: "demo.dep",
            needs_dep: false,
            record: None,
        }))
        .await
        .unwrap();

    let snapshot = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let LoaderEvent::SnapshotChanged { .. } = events.recv().await.unwrap();
            let snapshot = loader.await_idle().await.unwrap();
            if snapshot.entry("consumer").unwrap().state == Some(FiberState::Active) {
                return snapshot;
            }
        }
    })
    .await
    .expect("managed fiber became active");
    assert_eq!(
        snapshot.entry("consumer").unwrap().state,
        Some(FiberState::Active)
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn await_idle_waits_for_managed_loading_fiber() {
    assert_idle_waits_for_loading(false).await;
}

#[tokio::test]
async fn await_idle_returns_failed_snapshot_when_concurrent_control_operation_fails() {
    assert_idle_waits_for_loading(true).await;
}

async fn assert_idle_waits_for_loading(fail: bool) {
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let mut entry = EntryOptions::new("consumer", "demo.gated-loading");
    entry.disabled = true;
    let mut catalog = ExtensionCatalog::new();
    catalog
        .register(GatedLoadingFactory {
            entered: Arc::new(Mutex::new(Some(entered_tx))),
            release: Arc::new(Mutex::new(Some(release_rx))),
            fail,
        })
        .unwrap();
    let (runtime, _loader_fiber, loader) =
        mount(LoaderPlugin::new(catalog, config(vec![entry])).unwrap()).await;
    let _provider = runtime
        .root()
        .plugin(Arc::new(TestPlugin {
            id: "demo.dep",
            needs_dep: false,
            record: None,
        }))
        .await
        .unwrap();

    let update = tokio::spawn({
        let loader = loader.clone();
        async move {
            loader
                .update(
                    "consumer",
                    EntryUpdate {
                        disabled: Some(false),
                        ..Default::default()
                    },
                    None,
                    None,
                )
                .await
        }
    });
    entered_rx.await.expect("managed plugin entered apply");

    let mut idle = Box::pin(loader.await_idle());
    tokio::select! {
        result = &mut idle => panic!("await_idle returned while Fiber was Loading: {result:?}"),
        _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {}
    }

    let _ = release_tx.send(());
    let result = update.await.expect("update task");
    if fail {
        assert!(matches!(
            result,
            Err(LoaderControlError::Loader(LoaderError::Lifecycle { .. }))
        ));
        assert!(loader.entries().unwrap().extensions[0].disabled);
    } else {
        result.expect("enable entry");
    }
    let snapshot = idle.await.expect("await idle");
    assert_eq!(
        snapshot.entry("consumer").unwrap().state,
        Some(if fail {
            FiberState::Failed
        } else {
            FiberState::Active
        })
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn await_idle_waits_for_managed_unloading_fiber() {
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let mut catalog = ExtensionCatalog::new();
    catalog
        .register(GatedUnloadingFactory {
            entered: Arc::new(Mutex::new(Some(entered_tx))),
            release: Arc::new(Mutex::new(Some(release_rx))),
        })
        .unwrap();

    let runtime = Runtime::new().unwrap();
    let provider = runtime.root().effect_named("provider").unwrap();
    provider.provide(DEP, ()).unwrap();
    let root = runtime.root();
    let loader_fiber = root
        .plugin(Arc::new(
            LoaderPlugin::new(
                catalog,
                config(vec![EntryOptions::new("consumer", "demo.gated-unloading")]),
            )
            .unwrap(),
        ))
        .await
        .unwrap();
    let loader = (*root.get(LOADER).unwrap()).clone();
    assert_eq!(
        loader
            .await_idle()
            .await
            .unwrap()
            .entry("consumer")
            .unwrap()
            .state,
        Some(FiberState::Active)
    );

    provider.dispose();
    entered_rx.await.expect("managed plugin entered disposer");
    let mut idle = Box::pin(loader.await_idle());
    tokio::select! {
        result = &mut idle => panic!("await_idle returned while Fiber was Unloading: {result:?}"),
        _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {}
    }

    let _ = release_tx.send(());
    provider.dispose_wait().await.unwrap();
    let snapshot = idle.await.expect("await idle");
    assert_eq!(
        snapshot.entry("consumer").unwrap().state,
        Some(FiberState::Pending)
    );
    let mut loader_fiber = loader_fiber;
    loader_fiber.dispose_wait().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn unloading_loader_stops_events_and_releases_managed_subtree() {
    let record = Arc::new(Mutex::new(Vec::new()));
    let child = EntryOptions::new("child", "demo.value");
    let group = EntryOptions::group("group", vec![child]).unwrap();
    let (runtime, mut loader_fiber, loader) = mount(
        LoaderPlugin::new(
            catalog(vec![Factory {
                id: "demo.value",
                needs_dep: false,
                record: Some(record.clone()),
            }]),
            config(vec![group]),
        )
        .unwrap(),
    )
    .await;
    loader.await_idle().await.unwrap();
    let mut events = loader.subscribe().unwrap();

    loader_fiber.dispose_wait().await.unwrap();
    assert!(runtime.root().get(LOADER).is_err());
    assert!(matches!(
        loader.entries(),
        Err(LoaderControlError::LoaderUnavailable)
    ));
    assert_eq!(record.lock().unwrap().as_slice(), ["demo.value"]);
    assert!(runtime.root().get(VALUE).is_err());
    assert!(runtime.diagnostics().plugin_fibers.iter().all(|fiber| {
        fiber.plugin_key != "cordis.loader.group" && fiber.plugin_key != "demo.value"
    }));

    while events.try_recv().is_ok() {}
    let mut unrelated = runtime
        .root()
        .plugin(Arc::new(TestPlugin {
            id: "demo.unrelated",
            needs_dep: false,
            record: None,
        }))
        .await
        .unwrap();
    unrelated.dispose_wait().await.unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), events.recv())
            .await
            .is_err()
    );
    runtime.shutdown().await.unwrap();
}

fn dep_catalog() -> ExtensionCatalog {
    catalog(vec![
        Factory {
            id: "demo.dep",
            needs_dep: false,
            record: None,
        },
        Factory {
            id: "demo.dep2",
            needs_dep: false,
            record: None,
        },
        Factory {
            id: "demo.consumer",
            needs_dep: true,
            record: None,
        },
        Factory {
            id: "demo.label",
            needs_dep: true,
            record: None,
        },
        Factory {
            id: "demo.value",
            needs_dep: false,
            record: None,
        },
    ])
}

#[tokio::test]
async fn group_sibling_provider_unblocks_consumer() {
    let group = EntryOptions::group(
        "app",
        vec![
            EntryOptions::new("provider", "demo.dep"),
            EntryOptions::new("consumer", "demo.consumer"),
        ],
    )
    .unwrap();
    let (runtime, _loader_fiber, loader) =
        mount(LoaderPlugin::new(dep_catalog(), config(vec![group])).unwrap()).await;
    let snapshot = loader.await_idle().await.unwrap();
    assert_eq!(
        snapshot.entry("app:provider").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(
        snapshot.entry("app:consumer").unwrap().state,
        Some(FiberState::Active)
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn group_inject_config_is_visible_to_children() {
    let mut catalog = dep_catalog();
    catalog
        .register_injection(InjectionDescriptor::intercepted("dep", DEP, LABEL))
        .unwrap();
    let mut group = EntryOptions::group(
        "app",
        vec![
            EntryOptions::new("provider", "demo.dep"),
            EntryOptions::new("label", "demo.label"),
        ],
    )
    .unwrap();
    group.inject = Some(InjectConfig::Map(
        [(
            "dep".into(),
            toml::Value::Table(toml::toml! { label = "from-group" }),
        )]
        .into(),
    ));
    let (runtime, _loader_fiber, loader) =
        mount(LoaderPlugin::new(catalog, config(vec![group])).unwrap()).await;
    let snapshot = loader.await_idle().await.unwrap();
    assert_eq!(
        snapshot.entry("app:provider").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(
        snapshot.entry("app:label").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(
        &*loader
            .entry_context("app:label")
            .unwrap()
            .get(VALUE)
            .unwrap(),
        "from-group"
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn nested_group_inject_overrides_parent_config() {
    let mut catalog = dep_catalog();
    catalog
        .register_injection(InjectionDescriptor::intercepted("dep", DEP, LABEL))
        .unwrap();
    let mut inner = EntryOptions::group(
        "inner",
        vec![
            EntryOptions::new("provider", "demo.dep"),
            EntryOptions::new("label", "demo.label"),
        ],
    )
    .unwrap();
    inner.inject = Some(InjectConfig::Map(
        [(
            "dep".into(),
            toml::Value::Table(toml::toml! { label = "child" }),
        )]
        .into(),
    ));
    let mut outer = EntryOptions::group("outer", vec![inner]).unwrap();
    outer.inject = Some(InjectConfig::Map(
        [(
            "dep".into(),
            toml::Value::Table(toml::toml! { label = "parent" }),
        )]
        .into(),
    ));
    let (runtime, _loader_fiber, loader) =
        mount(LoaderPlugin::new(catalog, config(vec![outer])).unwrap()).await;
    let snapshot = loader.await_idle().await.unwrap();
    assert_eq!(
        snapshot.entry("outer:inner:label").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(
        &*loader
            .entry_context("outer:inner:label")
            .unwrap()
            .get(VALUE)
            .unwrap(),
        "child"
    );
    runtime.shutdown().await.unwrap();
}

fn isolate_dep(exclusive: bool) -> cordis_loader::IsolateConfig {
    [(
        "dep".into(),
        if exclusive {
            IsolateValue::Flag(true)
        } else {
            IsolateValue::Name("shared".into())
        },
    )]
    .into()
}

fn dep_catalog_with_isolation() -> ExtensionCatalog {
    let mut catalog = dep_catalog();
    catalog
        .register_isolation(IsolationDescriptor::new("dep", DEP))
        .unwrap();
    catalog
}

#[test]
fn isolation_catalog_rejects_duplicate_descriptor_name() {
    let mut catalog = ExtensionCatalog::new();
    catalog
        .register_isolation(IsolationDescriptor::new("dep", DEP))
        .unwrap();

    let error = catalog
        .register_isolation(IsolationDescriptor::new("dep", VALUE))
        .unwrap_err();
    assert!(matches!(error, LoaderError::DuplicateIsolation { id } if id == "dep"));
}

#[test]
fn isolation_catalog_rejects_service_key_aliases() {
    let mut catalog = ExtensionCatalog::new();
    catalog
        .register_isolation(IsolationDescriptor::new("dep", DEP))
        .unwrap();

    let error = catalog
        .register_isolation(IsolationDescriptor::new("dependency", DEP))
        .unwrap_err();
    assert!(matches!(
        error,
        LoaderError::DuplicateIsolationService {
            service,
            registered,
            attempted,
        } if service == DEP.id().as_str() && registered == "dep" && attempted == "dependency"
    ));
}

#[test]
fn isolation_catalog_accepts_distinct_service_keys() {
    let mut catalog = ExtensionCatalog::new();
    catalog
        .register_isolation(IsolationDescriptor::new("dep", DEP))
        .unwrap();
    catalog
        .register_isolation(IsolationDescriptor::new("value", VALUE))
        .unwrap();

    assert_eq!(
        catalog
            .isolations()
            .into_iter()
            .map(|descriptor| descriptor.id)
            .collect::<Vec<_>>(),
        ["dep", "value"]
    );
}

#[tokio::test]
async fn group_without_isolate_shares_root_domain_and_conflicts() {
    let group = EntryOptions::group("a", vec![EntryOptions::new("provider", "demo.dep")]).unwrap();
    let runtime = Runtime::new().unwrap();
    let fiber = runtime
        .root()
        .plugin(Arc::new(
            LoaderPlugin::new(
                dep_catalog(),
                config(vec![EntryOptions::new("root-provider", "demo.dep2"), group]),
            )
            .unwrap(),
        ))
        .await
        .expect("handle returned");
    assert_eq!(fiber.state(), FiberState::Active);
    let snapshot = runtime
        .root()
        .get(LOADER)
        .unwrap()
        .await_idle()
        .await
        .unwrap();
    assert_eq!(
        snapshot.entry("root-provider").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(
        snapshot.entry("a:provider").unwrap().state,
        Some(FiberState::Failed)
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn group_exclusive_isolate_hides_provider_from_root_and_siblings() {
    let mut group_a =
        EntryOptions::group("a", vec![EntryOptions::new("provider", "demo.dep")]).unwrap();
    group_a = group_a.with_isolate(isolate_dep(true));
    let group_b =
        EntryOptions::group("b", vec![EntryOptions::new("consumer", "demo.consumer")]).unwrap();
    let (runtime, _loader_fiber, loader) = mount(
        LoaderPlugin::new(
            dep_catalog_with_isolation(),
            config(vec![
                group_a,
                group_b,
                EntryOptions::new("root-consumer", "demo.consumer"),
            ]),
        )
        .unwrap(),
    )
    .await;
    let snapshot = loader.await_idle().await.unwrap();
    assert_eq!(
        snapshot.entry("a:provider").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(
        snapshot.entry("b:consumer").unwrap().state,
        Some(FiberState::Pending)
    );
    assert_eq!(
        snapshot.entry("root-consumer").unwrap().state,
        Some(FiberState::Pending)
    );
    assert!(loader.entry_context("a:provider").unwrap().get(DEP).is_ok());
    assert!(
        loader
            .entry_context("b:consumer")
            .unwrap()
            .get(DEP)
            .is_err()
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn different_groups_with_exclusive_isolate_may_provide_same_service_key() {
    let group_a = EntryOptions::group("a", vec![EntryOptions::new("provider", "demo.dep")])
        .unwrap()
        .with_isolate(isolate_dep(true));
    let group_b = EntryOptions::group("b", vec![EntryOptions::new("provider", "demo.dep2")])
        .unwrap()
        .with_isolate(isolate_dep(true));
    let (runtime, _loader_fiber, loader) = mount(
        LoaderPlugin::new(dep_catalog_with_isolation(), config(vec![group_a, group_b])).unwrap(),
    )
    .await;
    let snapshot = loader.await_idle().await.unwrap();
    assert_eq!(
        snapshot.entry("a:provider").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(
        snapshot.entry("b:provider").unwrap().state,
        Some(FiberState::Active)
    );
    loader
        .entry_context("a:provider")
        .unwrap()
        .get(DEP)
        .unwrap();
    loader
        .entry_context("b:provider")
        .unwrap()
        .get(DEP)
        .unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn named_isolate_label_shares_provider_across_groups() {
    let group_a = EntryOptions::group("a", vec![EntryOptions::new("provider", "demo.dep")])
        .unwrap()
        .with_isolate(isolate_dep(false));
    let group_b = EntryOptions::group("b", vec![EntryOptions::new("consumer", "demo.consumer")])
        .unwrap()
        .with_isolate(isolate_dep(false));
    let (runtime, _loader_fiber, loader) = mount(
        LoaderPlugin::new(dep_catalog_with_isolation(), config(vec![group_a, group_b])).unwrap(),
    )
    .await;
    let snapshot = loader.await_idle().await.unwrap();
    assert_eq!(
        snapshot.entry("a:provider").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(
        snapshot.entry("b:consumer").unwrap().state,
        Some(FiberState::Active)
    );
    loader
        .entry_context("b:consumer")
        .unwrap()
        .get(DEP)
        .unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn group_services_do_not_leak_to_root_or_other_groups() {
    // 保留旧名：行为改为「须显式 isolate 才隔离」。
    let mut group_a =
        EntryOptions::group("a", vec![EntryOptions::new("provider", "demo.dep")]).unwrap();
    group_a = group_a.with_isolate(isolate_dep(true));
    let group_b =
        EntryOptions::group("b", vec![EntryOptions::new("consumer", "demo.consumer")]).unwrap();
    let (runtime, _loader_fiber, loader) = mount(
        LoaderPlugin::new(
            dep_catalog_with_isolation(),
            config(vec![
                group_a,
                group_b,
                EntryOptions::new("root-consumer", "demo.consumer"),
            ]),
        )
        .unwrap(),
    )
    .await;
    let snapshot = loader.await_idle().await.unwrap();
    assert_eq!(
        snapshot.entry("a:provider").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(
        snapshot.entry("b:consumer").unwrap().state,
        Some(FiberState::Pending)
    );
    assert_eq!(
        snapshot.entry("root-consumer").unwrap().state,
        Some(FiberState::Pending)
    );
    assert!(
        loader
            .entry_context("b:consumer")
            .unwrap()
            .get(DEP)
            .is_err()
    );
    assert!(
        loader
            .entry_context("root-consumer")
            .unwrap()
            .get(DEP)
            .is_err()
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn different_groups_may_provide_same_service_key() {
    let group_a = EntryOptions::group("a", vec![EntryOptions::new("provider", "demo.dep")])
        .unwrap()
        .with_isolate(isolate_dep(true));
    let group_b = EntryOptions::group("b", vec![EntryOptions::new("provider", "demo.dep2")])
        .unwrap()
        .with_isolate(isolate_dep(true));
    let (runtime, _loader_fiber, loader) = mount(
        LoaderPlugin::new(dep_catalog_with_isolation(), config(vec![group_a, group_b])).unwrap(),
    )
    .await;
    let snapshot = loader.await_idle().await.unwrap();
    assert_eq!(
        snapshot.entry("a:provider").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(
        snapshot.entry("b:provider").unwrap().state,
        Some(FiberState::Active)
    );
    loader
        .entry_context("a:provider")
        .unwrap()
        .get(DEP)
        .unwrap();
    loader
        .entry_context("b:provider")
        .unwrap()
        .get(DEP)
        .unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn duplicate_provider_in_same_group_fails() {
    let group = EntryOptions::group(
        "app",
        vec![
            EntryOptions::new("one", "demo.dep"),
            EntryOptions::new("two", "demo.dep2"),
        ],
    )
    .unwrap();
    let runtime = Runtime::new().unwrap();
    let fiber = runtime
        .root()
        .plugin(Arc::new(
            LoaderPlugin::new(dep_catalog(), config(vec![group])).unwrap(),
        ))
        .await
        .expect("Loader Fiber handle is returned even when reconcile fails");
    assert_eq!(fiber.state(), FiberState::Active);
    let loader = runtime.root().get(LOADER).unwrap();
    let snapshot = loader.await_idle().await.unwrap();
    assert_eq!(
        snapshot.entry("app:one").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(
        snapshot.entry("app:two").unwrap().state,
        Some(FiberState::Failed)
    );
    let error = snapshot
        .entry("app:two")
        .unwrap()
        .last_error
        .clone()
        .unwrap();
    assert!(
        error.contains("Lifecycle")
            || error.contains("已在当前 Context 注册")
            || error.contains("ServiceConflict"),
        "unexpected last_error: {error:?}"
    );
    assert_eq!(loader.entries().unwrap().extensions.len(), 1);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn inject_config_does_not_isolate_sibling_services() {
    let mut catalog = dep_catalog();
    catalog
        .register_injection(InjectionDescriptor::intercepted("dep", DEP, LABEL))
        .unwrap();
    let group = EntryOptions::group(
        "app",
        vec![
            EntryOptions::new("provider", "demo.dep"),
            EntryOptions::new("label", "demo.label").with_inject(InjectConfig::Map(
                [(
                    "dep".into(),
                    toml::Value::Table(toml::toml! { label = "from-inject" }),
                )]
                .into(),
            )),
        ],
    )
    .unwrap();
    let (runtime, _loader_fiber, loader) =
        mount(LoaderPlugin::new(catalog, config(vec![group])).unwrap()).await;
    let snapshot = loader.await_idle().await.unwrap();
    assert_eq!(
        snapshot.entry("app:provider").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(
        snapshot.entry("app:label").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(
        &*loader
            .entry_context("app:label")
            .unwrap()
            .get(VALUE)
            .unwrap(),
        "from-inject"
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn configured_inject_is_typed_and_static_dependency_is_merged() {
    let mut catalog = catalog(vec![Factory {
        id: "demo.label",
        needs_dep: true,
        record: None,
    }]);
    catalog
        .register_injection(InjectionDescriptor::intercepted("dep", DEP, LABEL))
        .unwrap();
    let entry = EntryOptions::new("label", "demo.label").with_inject(InjectConfig::Map(
        [(
            "dep".into(),
            toml::Value::Table(toml::toml! { label = "configured" }),
        )]
        .into(),
    ));
    let runtime = Runtime::new().unwrap();
    runtime.root().provide(DEP, ()).unwrap();
    let fiber = runtime
        .root()
        .plugin(Arc::new(
            LoaderPlugin::new(catalog, config(vec![entry])).unwrap(),
        ))
        .await
        .unwrap();
    assert_ne!(fiber.state(), FiberState::Failed);
    let loader = runtime.root().get(LOADER).unwrap();
    assert_eq!(
        &*loader.entry_context("label").unwrap().get(VALUE).unwrap(),
        "configured"
    );
    runtime.shutdown().await.unwrap();
}

#[test]
fn rejects_unsupported_config_versions_and_invalid_groups() {
    let old = ExtensionsConfig {
        version: 1,
        extensions: vec![],
    };
    assert!(matches!(
        old.validate(&ExtensionCatalog::new()),
        Err(LoaderError::UnsupportedVersion { .. })
    ));
    let bad = config(vec![EntryOptions {
        id: "bad".into(),
        name: "demo.value".into(),
        config: toml::Value::Table(toml::Table::new()),
        group: true,
        disabled: false,
        inject: None,
        isolate: None,
    }]);
    assert!(matches!(
        bad.validate(&ExtensionCatalog::new()),
        Err(LoaderError::InvalidEntry { .. })
    ));
}

#[tokio::test]
async fn named_isolate_same_label_duplicate_provider_conflicts() {
    let group_a = EntryOptions::group("a", vec![EntryOptions::new("provider", "demo.dep")])
        .unwrap()
        .with_isolate(isolate_dep(false));
    let group_b = EntryOptions::group("b", vec![EntryOptions::new("provider", "demo.dep2")])
        .unwrap()
        .with_isolate(isolate_dep(false));
    let runtime = Runtime::new().unwrap();
    let fiber = runtime
        .root()
        .plugin(Arc::new(
            LoaderPlugin::new(dep_catalog_with_isolation(), config(vec![group_a, group_b]))
                .unwrap(),
        ))
        .await
        .expect("handle returned");
    assert_eq!(fiber.state(), FiberState::Active);
    let snapshot = runtime
        .root()
        .get(LOADER)
        .unwrap()
        .await_idle()
        .await
        .unwrap();
    assert_eq!(
        snapshot.entry("a:provider").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(
        snapshot.entry("b:provider").unwrap().state,
        Some(FiberState::Failed)
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn named_isolate_labels_do_not_cross_service_keys() {
    static OTHER: ServiceKey<&'static str> = ServiceKey::new("loader.tree.other@1");
    struct OtherPlugin;
    #[async_trait]
    impl Plugin for OtherPlugin {
        fn key(&self) -> PluginKey {
            PluginKey::new("demo.other")
        }
        async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
            ctx.provide(OTHER, "other")?;
            Ok(())
        }
    }
    struct OtherFactory;
    impl ExtensionFactory for OtherFactory {
        type Config = Empty;
        fn id(&self) -> &'static str {
            "demo.other"
        }
        fn prepare(&self, _: Empty) -> Result<PreparedExtension, LoaderError> {
            Ok(PreparedExtension::ordinary(|| Ok(Arc::new(OtherPlugin))))
        }
    }

    let mut catalog = dep_catalog_with_isolation();
    catalog.register(OtherFactory).unwrap();
    catalog
        .register_isolation(IsolationDescriptor::new("other", OTHER))
        .unwrap();

    let shared = IsolateValue::Name("team".into());
    let group_a = EntryOptions::group("a", vec![EntryOptions::new("provider", "demo.dep")])
        .unwrap()
        .with_isolate([("dep".into(), shared.clone())].into());
    let group_b = EntryOptions::group("b", vec![EntryOptions::new("other", "demo.other")])
        .unwrap()
        .with_isolate([("other".into(), shared)].into());
    let (runtime, _loader_fiber, loader) =
        mount(LoaderPlugin::new(catalog, config(vec![group_a, group_b])).unwrap()).await;
    let snapshot = loader.await_idle().await.unwrap();
    assert_eq!(
        snapshot.entry("a:provider").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(
        snapshot.entry("b:other").unwrap().state,
        Some(FiberState::Active)
    );
    assert!(
        loader
            .entry_context("a:provider")
            .unwrap()
            .get(OTHER)
            .is_err()
    );
    assert!(loader.entry_context("b:other").unwrap().get(DEP).is_err());
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn isolate_precheck_rejects_unknown_false_empty_and_v2() {
    let catalog = dep_catalog_with_isolation();
    assert!(matches!(
        LoaderPlugin::new(
            catalog.clone(),
            ExtensionsConfig {
                version: 2,
                extensions: vec![],
            },
        ),
        Err(LoaderError::UnsupportedVersion { found: 2 })
    ));
    assert!(matches!(
        LoaderPlugin::new(
            catalog.clone(),
            config(vec![EntryOptions::new("x", "demo.dep").with_isolate(
                [("missing".into(), IsolateValue::Flag(true))].into()
            )]),
        ),
        Err(LoaderError::UnknownIsolation { .. })
    ));
    assert!(matches!(
        LoaderPlugin::new(
            catalog.clone(),
            config(vec![EntryOptions::new("x", "demo.dep").with_isolate(
                [("dep".into(), IsolateValue::Flag(false))].into()
            )]),
        ),
        Err(LoaderError::InvalidEntry { .. })
    ));
    assert!(matches!(
        LoaderPlugin::new(
            catalog,
            config(vec![EntryOptions::new("x", "demo.dep").with_isolate(
                [("dep".into(), IsolateValue::Name(String::new()))].into()
            )]),
        ),
        Err(LoaderError::InvalidEntry { .. })
    ));
}
