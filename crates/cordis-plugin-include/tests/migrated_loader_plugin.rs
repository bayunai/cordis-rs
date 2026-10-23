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
use std::{
    fs,
    sync::{Arc, Mutex},
};

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
#[derive(Clone, serde::Deserialize, serde::Serialize, JsonSchema)]
struct CountConfig {
    #[serde(default)]
    n: u32,
}

struct CountFactory;

struct CountPlugin {
    n: u32,
}

#[async_trait]
impl Plugin for CountPlugin {
    fn key(&self) -> PluginKey {
        PluginKey::new("demo.count")
    }

    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        ctx.provide(VALUE, self.n.to_string())?;
        Ok(())
    }
}

impl ExtensionFactory for CountFactory {
    type Config = CountConfig;
    fn id(&self) -> &'static str {
        "demo.count"
    }
    fn prepare(&self, config: CountConfig) -> Result<PreparedExtension, LoaderError> {
        Ok(PreparedExtension::ordinary(move || {
            Ok(Arc::new(CountPlugin { n: config.n }))
        }))
    }
}

#[derive(Clone, serde::Deserialize, serde::Serialize, JsonSchema)]
struct KeySwitchConfig {
    #[serde(default)]
    alt: bool,
}

struct KeySwitchFactory;

struct KeySwitchPlugin {
    alt: bool,
}

#[async_trait]
impl Plugin for KeySwitchPlugin {
    fn key(&self) -> PluginKey {
        if self.alt {
            PluginKey::new("demo.key.alt")
        } else {
            PluginKey::new("demo.key.main")
        }
    }

    async fn apply(&self, _: &Context) -> Result<(), CoreError> {
        Ok(())
    }
}

impl ExtensionFactory for KeySwitchFactory {
    type Config = KeySwitchConfig;
    fn id(&self) -> &'static str {
        "demo.key"
    }
    fn prepare(&self, config: KeySwitchConfig) -> Result<PreparedExtension, LoaderError> {
        Ok(PreparedExtension::ordinary(move || {
            Ok(Arc::new(KeySwitchPlugin { alt: config.alt }))
        }))
    }
}

#[derive(Clone, serde::Deserialize, serde::Serialize, JsonSchema)]
struct FailConfig {
    #[serde(default)]
    fail: bool,
}

struct FailFactory;

struct FailPlugin {
    fail: bool,
}

#[async_trait]
impl Plugin for FailPlugin {
    fn key(&self) -> PluginKey {
        PluginKey::new("demo.fail")
    }

    async fn apply(&self, _: &Context) -> Result<(), CoreError> {
        if self.fail {
            Err(CoreError::ProviderCheck("intentional apply failure".into()))
        } else {
            Ok(())
        }
    }
}

impl ExtensionFactory for FailFactory {
    type Config = FailConfig;
    fn id(&self) -> &'static str {
        "demo.fail"
    }
    fn prepare(&self, config: FailConfig) -> Result<PreparedExtension, LoaderError> {
        Ok(PreparedExtension::ordinary(move || {
            Ok(Arc::new(FailPlugin { fail: config.fail }))
        }))
    }
}

fn fiber_id(snapshot: &cordis_loader::LoaderSnapshot, path: &str) -> u64 {
    snapshot
        .entry(&format!("include:{path}"))
        .unwrap()
        .fiber_id
        .unwrap()
}

async fn mount_with_file(
    catalog: ExtensionCatalog,
    entries: Vec<EntryOptions>,
) -> (
    Runtime,
    Fiber,
    Loader,
    tempfile::TempDir,
    std::path::PathBuf,
) {
    let directory = tempfile::tempdir().unwrap();
    let bootstrap = directory.path().join("bootstrap.toml");
    let extensions = directory.path().join("extensions.toml");
    fs::write(
        &bootstrap,
        "version = 3\n[config]\ndriver = \"file\"\npath = \"extensions.toml\"\n",
    )
    .unwrap();
    fs::write(&extensions, toml::to_string(&config(entries)).unwrap()).unwrap();
    let (runtime, fiber, loader) = mount(file_plugin(catalog, &bootstrap).unwrap()).await;
    (runtime, fiber, loader, directory, extensions)
}

#[tokio::test]
async fn disabling_provider_returns_consumer_to_pending_then_recovers() {
    let group = EntryOptions::group(
        "app",
        vec![
            EntryOptions::new("provider", "demo.dep"),
            EntryOptions::new("consumer", "demo.consumer"),
        ],
    )
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let bootstrap = directory.path().join("bootstrap.toml");
    let extensions = directory.path().join("extensions.toml");
    fs::write(
        &bootstrap,
        "version = 3\n[config]\ndriver = \"file\"\npath = \"extensions.toml\"\n",
    )
    .unwrap();
    fs::write(&extensions, toml::to_string(&config(vec![group])).unwrap()).unwrap();
    let (runtime, _loader_fiber, loader) =
        mount(file_plugin(dep_catalog(), &bootstrap).unwrap()).await;
    assert_eq!(
        loader
            .await_idle()
            .await
            .unwrap()
            .entry("include:app:consumer")
            .unwrap()
            .state,
        Some(FiberState::Active)
    );

    loader
        .subtree("include")
        .unwrap()
        .update(
            "app:provider",
            EntryUpdate {
                disabled: Some(true),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap();
    let snapshot = loader.await_idle().await.unwrap();
    assert!(!snapshot.entry("include:app:provider").unwrap().enabled);
    assert_eq!(
        snapshot.entry("include:app:consumer").unwrap().state,
        Some(FiberState::Pending)
    );

    loader
        .subtree("include")
        .unwrap()
        .update(
            "app:provider",
            EntryUpdate {
                disabled: Some(false),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap();
    let snapshot = loader.await_idle().await.unwrap();
    assert_eq!(
        snapshot.entry("include:app:provider").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(
        snapshot.entry("include:app:consumer").unwrap().state,
        Some(FiberState::Active)
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn persistent_create_move_and_conflict_use_paths() {
    let directory = tempfile::tempdir().unwrap();
    let bootstrap = directory.path().join("bootstrap.toml");
    let extensions = directory.path().join("extensions.toml");
    fs::write(
        &bootstrap,
        "version = 3\n[config]\ndriver = \"file\"\npath = \"extensions.toml\"\n",
    )
    .unwrap();
    fs::write(&extensions, "version = 3\nextensions = []\n").unwrap();
    let (runtime, _loader_fiber, loader) = mount(
        file_plugin(
            catalog(vec![Factory {
                id: "demo.value",
                needs_dep: false,
                record: None,
            }]),
            &bootstrap,
        )
        .unwrap(),
    )
    .await;
    loader
        .subtree("include")
        .unwrap()
        .create(EntryOptions::group("one", vec![]).unwrap(), None, None)
        .await
        .unwrap();
    loader
        .subtree("include")
        .unwrap()
        .create(EntryOptions::group("two", vec![]).unwrap(), None, None)
        .await
        .unwrap();
    loader
        .subtree("include")
        .unwrap()
        .create(EntryOptions::new("child", "demo.value"), Some("one"), None)
        .await
        .unwrap();
    loader
        .subtree("include")
        .unwrap()
        .update(
            "one",
            EntryUpdate {
                disabled: Some(true),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap();
    assert!(
        loader
            .await_idle()
            .await
            .unwrap()
            .entry("include:one:child")
            .unwrap()
            .state
            .is_none()
    );
    loader
        .subtree("include")
        .unwrap()
        .update(
            "one",
            EntryUpdate {
                disabled: Some(false),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap();
    loader
        .subtree("include")
        .unwrap()
        .update(
            "one:child",
            EntryUpdate::default(),
            Some(Some("two")),
            Some(0),
        )
        .await
        .unwrap();
    assert!(
        loader
            .await_idle()
            .await
            .unwrap()
            .entry("include:two:child")
            .is_some()
    );
    assert!(
        fs::read_to_string(&extensions)
            .unwrap()
            .contains("id = \"two\"")
    );
    fs::write(&extensions, "version = 3\nextensions = []\n# outside\n").unwrap();
    assert!(matches!(
        loader
            .subtree("include")
            .unwrap()
            .create(EntryOptions::new("nope", "demo.value"), None, None)
            .await,
        Err(LoaderControlError::ConfigConflict { .. })
    ));
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn deleting_group_disposes_children_in_postorder() {
    let record = Arc::new(Mutex::new(Vec::new()));
    let group = EntryOptions::group(
        "group",
        vec![
            EntryOptions::new("first", "demo.first"),
            EntryOptions::new("second", "demo.second"),
        ],
    )
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let bootstrap = directory.path().join("bootstrap.toml");
    let extensions = directory.path().join("extensions.toml");
    fs::write(
        &bootstrap,
        "version = 3\n[config]\ndriver = \"file\"\npath = \"extensions.toml\"\n",
    )
    .unwrap();
    fs::write(&extensions, toml::to_string(&config(vec![group])).unwrap()).unwrap();
    let (runtime, _loader_fiber, loader) = mount(
        file_plugin(
            catalog(vec![
                Factory {
                    id: "demo.first",
                    needs_dep: false,
                    record: Some(record.clone()),
                },
                Factory {
                    id: "demo.second",
                    needs_dep: false,
                    record: Some(record.clone()),
                },
            ]),
            &bootstrap,
        )
        .unwrap(),
    )
    .await;
    loader
        .subtree("include")
        .unwrap()
        .remove("group")
        .await
        .unwrap();
    assert_eq!(&*record.lock().unwrap(), &["demo.second", "demo.first"]);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn config_only_replace_keeps_fiber_ids() {
    let mut catalog = catalog(vec![
        Factory {
            id: "demo.value",
            needs_dep: false,
            record: None,
        },
        Factory {
            id: "demo.dep",
            needs_dep: false,
            record: None,
        },
    ]);
    catalog.register(CountFactory).unwrap();
    catalog
        .register_isolation(IsolationDescriptor::new("value", VALUE))
        .unwrap();
    // Group 独占 VALUE，避免与 root 的 demo.value 同域冲突。
    let group = EntryOptions::group(
        "app",
        vec![
            EntryOptions::new("provider", "demo.dep"),
            EntryOptions::new("count", "demo.count")
                .with_config(toml::Value::try_from(CountConfig { n: 1 }).unwrap()),
        ],
    )
    .unwrap()
    .with_isolate([("value".into(), IsolateValue::Flag(true))].into());
    let (runtime, _loader_fiber, loader, _dir, _) = mount_with_file(
        catalog,
        vec![EntryOptions::new("root", "demo.value"), group],
    )
    .await;
    let before = loader.await_idle().await.unwrap();
    let root_id = fiber_id(&before, "root");
    let provider_id = fiber_id(&before, "app:provider");
    let count_id = fiber_id(&before, "app:count");
    let app_id = fiber_id(&before, "app");

    loader
        .subtree("include")
        .unwrap()
        .update(
            "app:count",
            EntryUpdate {
                config: Some(toml::Value::try_from(CountConfig { n: 2 }).unwrap()),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap();
    let after = loader.await_idle().await.unwrap();
    assert_eq!(fiber_id(&after, "root"), root_id);
    assert_eq!(fiber_id(&after, "app"), app_id);
    assert_eq!(fiber_id(&after, "app:provider"), provider_id);
    assert_eq!(fiber_id(&after, "app:count"), count_id);
    assert_eq!(
        &*loader
            .entry_context("include:app:count")
            .unwrap()
            .get(VALUE)
            .unwrap(),
        "2"
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn pure_reorder_keeps_all_fiber_ids() {
    let (runtime, _loader_fiber, loader, _dir, _) = mount_with_file(
        catalog(vec![
            Factory {
                id: "demo.value",
                needs_dep: false,
                record: None,
            },
            Factory {
                id: "demo.dep",
                needs_dep: false,
                record: None,
            },
        ]),
        vec![
            EntryOptions::new("one", "demo.value"),
            EntryOptions::new("two", "demo.dep"),
        ],
    )
    .await;
    let before = loader.await_idle().await.unwrap();
    let one_id = fiber_id(&before, "one");
    let two_id = fiber_id(&before, "two");
    assert_eq!(
        before
            .entries
            .iter()
            .map(|entry| entry.path.as_str())
            .collect::<Vec<_>>(),
        vec!["include", "include:one", "include:two"]
    );

    loader
        .subtree("include")
        .unwrap()
        .update("two", EntryUpdate::default(), Some(None), Some(0))
        .await
        .unwrap();
    let after = loader.await_idle().await.unwrap();
    assert_eq!(
        after
            .entries
            .iter()
            .map(|entry| entry.path.as_str())
            .collect::<Vec<_>>(),
        vec!["include", "include:two", "include:one"]
    );
    assert_eq!(fiber_id(&after, "one"), one_id);
    assert_eq!(fiber_id(&after, "two"), two_id);
    assert_eq!(
        loader
            .subtree("include")
            .unwrap()
            .entries()
            .unwrap()
            .extensions
            .iter()
            .map(|entry| entry.id.as_str())
            .collect::<Vec<_>>(),
        vec!["two", "one"]
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn factory_changed_on_reload_is_zero_runtime_change() {
    let (runtime, _loader_fiber, loader, _dir, extensions) = mount_with_file(
        catalog(vec![
            Factory {
                id: "demo.value",
                needs_dep: false,
                record: None,
            },
            Factory {
                id: "demo.dep",
                needs_dep: false,
                record: None,
            },
        ]),
        vec![EntryOptions::new("item", "demo.value")],
    )
    .await;
    let before = loader.await_idle().await.unwrap();
    let item_id = fiber_id(&before, "item");

    fs::write(
        &extensions,
        toml::to_string(&config(vec![EntryOptions::new("item", "demo.dep")])).unwrap(),
    )
    .unwrap();
    let error = loader
        .subtree("include")
        .unwrap()
        .reload()
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            LoaderControlError::Loader(LoaderError::FactoryChanged { .. })
        ),
        "unexpected error: {error:?}"
    );
    let after = loader.await_idle().await.unwrap();
    assert_eq!(fiber_id(&after, "item"), item_id);
    assert_eq!(
        after.entry("include:item").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(
        loader
            .subtree("include")
            .unwrap()
            .entries()
            .unwrap()
            .extensions[0]
            .name,
        "demo.value"
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn plugin_key_changed_on_config_update_is_zero_runtime_change() {
    let mut catalog = ExtensionCatalog::new();
    catalog.register(KeySwitchFactory).unwrap();
    let (runtime, _loader_fiber, loader, _dir, _) = mount_with_file(
        catalog,
        vec![
            EntryOptions::new("keyed", "demo.key")
                .with_config(toml::Value::try_from(KeySwitchConfig { alt: false }).unwrap()),
        ],
    )
    .await;
    let before = loader.await_idle().await.unwrap();
    let keyed_id = fiber_id(&before, "keyed");

    let error = loader
        .subtree("include")
        .unwrap()
        .update(
            "keyed",
            EntryUpdate {
                config: Some(toml::Value::try_from(KeySwitchConfig { alt: true }).unwrap()),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            LoaderControlError::Loader(LoaderError::PluginKeyChanged { .. })
        ),
        "unexpected error: {error:?}"
    );
    let after = loader.await_idle().await.unwrap();
    assert_eq!(fiber_id(&after, "keyed"), keyed_id);
    assert_eq!(
        after.entry("include:keyed").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(
        loader
            .subtree("include")
            .unwrap()
            .entries()
            .unwrap()
            .extensions[0]
            .config,
        toml::Value::try_from(KeySwitchConfig { alt: false }).unwrap()
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn plugin_key_changed_during_group_rebuild_is_zero_runtime_change() {
    let mut catalog = ExtensionCatalog::new();
    catalog.register(KeySwitchFactory).unwrap();
    catalog
        .register_injection(InjectionDescriptor::intercepted("dep", DEP, LABEL))
        .unwrap();
    let mut initial = EntryOptions::group(
        "app",
        vec![
            EntryOptions::new("keyed", "demo.key")
                .with_config(toml::Value::try_from(KeySwitchConfig { alt: false }).unwrap()),
        ],
    )
    .unwrap();
    initial.inject = Some(InjectConfig::Map(
        [(
            "dep".into(),
            toml::Value::Table(toml::toml! { label = "before" }),
        )]
        .into(),
    ));
    let (runtime, _loader_fiber, loader, _dir, extensions) =
        mount_with_file(catalog, vec![initial]).await;
    let before = loader.await_idle().await.unwrap();
    let keyed_id = fiber_id(&before, "app:keyed");

    let mut target = EntryOptions::group(
        "app",
        vec![
            EntryOptions::new("keyed", "demo.key")
                .with_config(toml::Value::try_from(KeySwitchConfig { alt: true }).unwrap()),
        ],
    )
    .unwrap();
    target.inject = Some(InjectConfig::Map(
        [(
            "dep".into(),
            toml::Value::Table(toml::toml! { label = "after" }),
        )]
        .into(),
    ));
    fs::write(&extensions, toml::to_string(&config(vec![target])).unwrap()).unwrap();

    let error = loader
        .subtree("include")
        .unwrap()
        .reload()
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            LoaderControlError::Loader(LoaderError::PluginKeyChanged { .. })
        ),
        "unexpected error: {error:?}"
    );
    let after = loader.await_idle().await.unwrap();
    assert_eq!(fiber_id(&after, "app:keyed"), keyed_id);
    assert_eq!(
        loader
            .subtree("include")
            .unwrap()
            .entries()
            .unwrap()
            .extensions[0]
            .children()
            .unwrap()[0]
            .config,
        toml::Value::try_from(KeySwitchConfig { alt: false }).unwrap()
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn lifecycle_failure_keeps_unrelated_active_and_skips_desired_commit() {
    let mut catalog = catalog(vec![Factory {
        id: "demo.value",
        needs_dep: false,
        record: None,
    }]);
    catalog.register(FailFactory).unwrap();
    let (runtime, _loader_fiber, loader, _dir, extensions) = mount_with_file(
        catalog,
        vec![
            EntryOptions::new("stable", "demo.value"),
            EntryOptions::new("flaky", "demo.fail")
                .with_config(toml::Value::try_from(FailConfig { fail: false }).unwrap()),
        ],
    )
    .await;
    let before = loader.await_idle().await.unwrap();
    let stable_id = fiber_id(&before, "stable");
    let before_file = fs::read_to_string(&extensions).unwrap();
    let mut events = loader.subscribe().unwrap();

    let error = loader
        .subtree("include")
        .unwrap()
        .update(
            "flaky",
            EntryUpdate {
                config: Some(toml::Value::try_from(FailConfig { fail: true }).unwrap()),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            LoaderControlError::Loader(LoaderError::Lifecycle { .. })
        ),
        "unexpected error: {error:?}"
    );
    assert!(matches!(
        events.recv().await.unwrap(),
        LoaderEvent::SnapshotChanged { .. }
    ));

    let after = loader.await_idle().await.unwrap();
    assert_eq!(fiber_id(&after, "stable"), stable_id);
    assert_eq!(
        after.entry("include:stable").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(
        after.entry("include:flaky").unwrap().state,
        Some(FiberState::Failed)
    );
    assert_eq!(
        loader
            .subtree("include")
            .unwrap()
            .entries()
            .unwrap()
            .extensions[1]
            .config,
        toml::Value::try_from(FailConfig { fail: false }).unwrap()
    );
    assert_eq!(fs::read_to_string(&extensions).unwrap(), before_file);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn initial_failure_keeps_loader_and_later_entries_then_can_be_repaired() {
    let mut catalog = dep_catalog();
    catalog.register(FailFactory).unwrap();
    let (runtime, fiber, loader, _dir, _) = mount_with_file(
        catalog,
        vec![
            EntryOptions::new("broken", "demo.fail")
                .with_config(toml::Value::try_from(FailConfig { fail: true }).unwrap()),
            EntryOptions::new("provider", "demo.dep"),
            EntryOptions::new("consumer", "demo.consumer"),
        ],
    )
    .await;
    assert_eq!(fiber.state(), FiberState::Active);
    let snapshot = loader.await_idle().await.unwrap();
    assert_eq!(
        snapshot.entry("include:broken").unwrap().state,
        Some(FiberState::Failed)
    );
    assert!(
        snapshot
            .entry("include:broken")
            .unwrap()
            .last_error
            .is_some()
    );
    assert_eq!(
        snapshot.entry("include:provider").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(
        snapshot.entry("include:consumer").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(
        loader
            .subtree("include")
            .unwrap()
            .entries()
            .unwrap()
            .extensions
            .len(),
        3
    );
    let provider_id = fiber_id(&snapshot, "provider");
    loader
        .subtree("include")
        .unwrap()
        .update(
            "broken",
            EntryUpdate {
                config: Some(toml::Value::try_from(FailConfig { fail: false }).unwrap()),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap();
    let recovered = loader.await_idle().await.unwrap();
    assert_eq!(
        recovered.entry("include:broken").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(fiber_id(&recovered, "provider"), provider_id);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn initial_failed_entry_can_be_disabled_and_removed() {
    let mut catalog = dep_catalog();
    catalog.register(FailFactory).unwrap();
    let (runtime, _loader_fiber, loader, _dir, _) = mount_with_file(
        catalog,
        vec![
            EntryOptions::new("broken", "demo.fail")
                .with_config(toml::Value::try_from(FailConfig { fail: true }).unwrap()),
            EntryOptions::new("stable", "demo.dep"),
        ],
    )
    .await;
    let id = fiber_id(&loader.await_idle().await.unwrap(), "stable");
    loader
        .subtree("include")
        .unwrap()
        .update(
            "broken",
            EntryUpdate {
                disabled: Some(true),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap();
    let disabled = loader.await_idle().await.unwrap();
    assert!(!disabled.entry("include:broken").unwrap().enabled);
    assert!(disabled.entry("include:broken").unwrap().state.is_none());
    loader
        .subtree("include")
        .unwrap()
        .remove("broken")
        .await
        .unwrap();
    let removed = loader.await_idle().await.unwrap();
    assert!(removed.entry("include:broken").is_none());
    assert_eq!(fiber_id(&removed, "stable"), id);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_provider_leaves_consumer_pending_without_stopping_unrelated_entries() {
    struct BrokenProvider;
    #[async_trait]
    impl Plugin for BrokenProvider {
        fn key(&self) -> PluginKey {
            PluginKey::new("broken.provider")
        }
        async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
            ctx.provide(DEP, ())?;
            Err(CoreError::PluginApply(
                "provider failed after registration".into(),
            ))
        }
    }
    struct BrokenProviderFactory;
    impl ExtensionFactory for BrokenProviderFactory {
        type Config = Empty;
        fn id(&self) -> &'static str {
            "broken.provider"
        }
        fn prepare(&self, _: Empty) -> Result<PreparedExtension, LoaderError> {
            Ok(PreparedExtension::ordinary(|| Ok(Arc::new(BrokenProvider))))
        }
    }
    let mut catalog = dep_catalog();
    catalog.register(BrokenProviderFactory).unwrap();
    let (runtime, _fiber, loader, _dir, _) = mount_with_file(
        catalog,
        vec![
            EntryOptions::new("broken", "broken.provider"),
            EntryOptions::new("consumer", "demo.consumer"),
            EntryOptions::new("stable", "demo.value"),
        ],
    )
    .await;
    let snapshot = loader.await_idle().await.unwrap();
    assert_eq!(
        snapshot.entry("include:broken").unwrap().state,
        Some(FiberState::Failed)
    );
    assert_eq!(
        snapshot.entry("include:consumer").unwrap().state,
        Some(FiberState::Pending)
    );
    assert_eq!(
        snapshot.entry("include:stable").unwrap().state,
        Some(FiberState::Active)
    );
    assert!(runtime.root().get(DEP).is_err());
    assert!(runtime.root().get(VALUE).is_ok());
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_reload_mounts_later_entries_and_retries_without_committing_declaration() {
    let mut catalog = dep_catalog();
    catalog.register(FailFactory).unwrap();
    let (runtime, _loader_fiber, loader, _dir, file) = mount_with_file(catalog, vec![]).await;
    let target = config(vec![
        EntryOptions::new("broken", "demo.fail")
            .with_config(toml::Value::try_from(FailConfig { fail: true }).unwrap()),
        EntryOptions::new("provider", "demo.dep"),
        EntryOptions::new("consumer", "demo.consumer"),
    ]);
    let text = toml::to_string(&target).unwrap();
    fs::write(&file, &text).unwrap();
    let mut provider_id = None;
    for _ in 0..2 {
        assert!(matches!(
            loader.subtree("include").unwrap().reload().await,
            Err(LoaderControlError::Loader(LoaderError::Lifecycle { .. }))
        ));
        let snapshot = loader.await_idle().await.unwrap();
        assert_eq!(
            snapshot.entry("include:broken").unwrap().state,
            Some(FiberState::Failed)
        );
        assert_eq!(
            snapshot.entry("include:provider").unwrap().state,
            Some(FiberState::Active)
        );
        assert_eq!(
            snapshot.entry("include:consumer").unwrap().state,
            Some(FiberState::Active)
        );
        if let Some(id) = provider_id {
            assert_eq!(fiber_id(&snapshot, "provider"), id);
        }
        provider_id = Some(fiber_id(&snapshot, "provider"));
        assert!(
            loader
                .subtree("include")
                .unwrap()
                .entries()
                .unwrap()
                .extensions
                .is_empty()
        );
        assert_eq!(fs::read_to_string(&file).unwrap(), text);
    }
    // 失败 reload 未提交 revision：管理写操作仍应要求先处理外部文件变更。
    assert!(matches!(
        loader.subtree("include").unwrap().remove("provider").await,
        Err(LoaderControlError::ConfigConflict { .. })
    ));
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_external_reload_is_visible_and_retried_until_fixed() {
    let mut catalog = ExtensionCatalog::new();
    catalog.register(FailFactory).unwrap();
    let initial = EntryOptions::new("flaky", "demo.fail")
        .with_config(toml::Value::try_from(FailConfig { fail: false }).unwrap());
    let (runtime, _loader_fiber, loader, _dir, extensions) =
        mount_with_file(catalog, vec![initial.clone()]).await;

    let failed = EntryOptions::new("flaky", "demo.fail")
        .with_config(toml::Value::try_from(FailConfig { fail: true }).unwrap());
    fs::write(&extensions, toml::to_string(&config(vec![failed])).unwrap()).unwrap();
    assert!(matches!(
        loader.subtree("include").unwrap().reload().await,
        Err(LoaderControlError::Loader(LoaderError::Lifecycle { .. }))
    ));
    assert_eq!(
        loader
            .await_idle()
            .await
            .unwrap()
            .entry("include:flaky")
            .unwrap()
            .state,
        Some(FiberState::Failed)
    );
    assert_eq!(
        loader
            .subtree("include")
            .unwrap()
            .entries()
            .unwrap()
            .extensions[0]
            .config,
        initial.config
    );

    assert!(matches!(
        loader.subtree("include").unwrap().reload().await,
        Err(LoaderControlError::Loader(LoaderError::Lifecycle { .. }))
    ));
    assert_eq!(
        loader
            .await_idle()
            .await
            .unwrap()
            .entry("include:flaky")
            .unwrap()
            .state,
        Some(FiberState::Failed)
    );
    assert_eq!(
        loader
            .subtree("include")
            .unwrap()
            .entries()
            .unwrap()
            .extensions[0]
            .config,
        initial.config
    );

    fs::write(
        &extensions,
        toml::to_string(&config(vec![initial])).unwrap(),
    )
    .unwrap();
    loader.subtree("include").unwrap().reload().await.unwrap();
    assert_eq!(
        loader
            .await_idle()
            .await
            .unwrap()
            .entry("include:flaky")
            .unwrap()
            .state,
        Some(FiberState::Active)
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn reload_remounts_entry_after_runtime_unmount() {
    let (runtime, _loader_fiber, loader, _dir, _) = mount_with_file(
        catalog(vec![Factory {
            id: "demo.value",
            needs_dep: false,
            record: None,
        }]),
        vec![EntryOptions::new("value", "demo.value")],
    )
    .await;
    let before = loader.await_idle().await.unwrap();
    let before_id = fiber_id(&before, "value");

    assert_eq!(
        runtime.unmount(PluginKey::new("demo.value")).await.unwrap(),
        1
    );
    let disposed = loader.await_idle().await.unwrap();
    assert_eq!(
        disposed.entry("include:value").unwrap().state,
        Some(FiberState::Disposed)
    );
    assert!(runtime.root().get(VALUE).is_err());

    loader.subtree("include").unwrap().reload().await.unwrap();
    let remounted = loader.await_idle().await.unwrap();
    assert_eq!(
        remounted.entry("include:value").unwrap().state,
        Some(FiberState::Active)
    );
    assert_ne!(fiber_id(&remounted, "value"), before_id);
    assert_eq!(&*runtime.root().get(VALUE).unwrap(), "value");
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_create_is_visible_and_can_be_removed_before_a_later_success() {
    let mut catalog = ExtensionCatalog::new();
    catalog.register(FailFactory).unwrap();
    let (runtime, _loader_fiber, loader, _dir, _) = mount_with_file(catalog, vec![]).await;

    let failed = EntryOptions::new("flaky", "demo.fail")
        .with_config(toml::Value::try_from(FailConfig { fail: true }).unwrap());
    assert!(matches!(
        loader
            .subtree("include")
            .unwrap()
            .create(failed, None, None)
            .await,
        Err(LoaderControlError::Loader(LoaderError::Lifecycle { .. }))
    ));
    let snapshot = loader.await_idle().await.unwrap();
    assert_eq!(
        snapshot.entry("include:flaky").unwrap().state,
        Some(FiberState::Failed)
    );
    assert!(
        loader
            .subtree("include")
            .unwrap()
            .entries()
            .unwrap()
            .extensions
            .is_empty()
    );

    loader.subtree("include").unwrap().reload().await.unwrap();
    assert!(
        loader
            .await_idle()
            .await
            .unwrap()
            .entry("include:flaky")
            .is_none()
    );

    let healthy = EntryOptions::new("flaky", "demo.fail")
        .with_config(toml::Value::try_from(FailConfig { fail: false }).unwrap());
    loader
        .subtree("include")
        .unwrap()
        .create(healthy, None, None)
        .await
        .unwrap();
    assert_eq!(
        loader
            .await_idle()
            .await
            .unwrap()
            .entry("include:flaky")
            .unwrap()
            .state,
        Some(FiberState::Active)
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn group_disabled_keeps_group_fiber_and_toggles_children() {
    let record = Arc::new(Mutex::new(Vec::new()));
    let (runtime, _loader_fiber, loader, _dir, _) = mount_with_file(
        catalog(vec![Factory {
            id: "demo.value",
            needs_dep: false,
            record: Some(record.clone()),
        }]),
        vec![EntryOptions::group("group", vec![EntryOptions::new("child", "demo.value")]).unwrap()],
    )
    .await;
    let before = loader.await_idle().await.unwrap();
    let group_id = fiber_id(&before, "group");
    assert_eq!(
        before.entry("include:group:child").unwrap().state,
        Some(FiberState::Active)
    );

    loader
        .subtree("include")
        .unwrap()
        .update(
            "group",
            EntryUpdate {
                disabled: Some(true),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap();
    let disabled = loader.await_idle().await.unwrap();
    assert_eq!(fiber_id(&disabled, "group"), group_id);
    assert!(disabled.entry("include:group").unwrap().state.is_some());
    assert!(!disabled.entry("include:group:child").unwrap().enabled);
    assert!(
        disabled
            .entry("include:group:child")
            .unwrap()
            .state
            .is_none()
    );
    assert_eq!(&*record.lock().unwrap(), &["demo.value"]);

    loader
        .subtree("include")
        .unwrap()
        .update(
            "group",
            EntryUpdate {
                disabled: Some(false),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap();
    let enabled = loader.await_idle().await.unwrap();
    assert_eq!(fiber_id(&enabled, "group"), group_id);
    assert_eq!(
        enabled.entry("include:group:child").unwrap().state,
        Some(FiberState::Active)
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn group_inject_change_rebuilds_subtree_only() {
    let mut catalog = dep_catalog();
    catalog
        .register_injection(InjectionDescriptor::intercepted("dep", DEP, LABEL))
        .unwrap();
    catalog
        .register(Factory {
            id: "demo.sibling",
            needs_dep: false,
            record: None,
        })
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
            toml::Value::Table(toml::toml! { label = "before" }),
        )]
        .into(),
    ));
    let (runtime, _loader_fiber, loader, _dir, _) = mount_with_file(
        catalog,
        vec![EntryOptions::new("sibling", "demo.sibling"), group],
    )
    .await;
    let before = loader.await_idle().await.unwrap();
    let sibling_id = fiber_id(&before, "sibling");
    let provider_id = fiber_id(&before, "app:provider");
    let label_id = fiber_id(&before, "app:label");

    loader
        .subtree("include")
        .unwrap()
        .update(
            "app",
            EntryUpdate {
                inject: Some(Some(InjectConfig::Map(
                    [(
                        "dep".into(),
                        toml::Value::Table(toml::toml! { label = "after" }),
                    )]
                    .into(),
                ))),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap();
    let after = loader.await_idle().await.unwrap();
    assert_eq!(fiber_id(&after, "sibling"), sibling_id);
    assert_ne!(fiber_id(&after, "app:provider"), provider_id);
    assert_ne!(fiber_id(&after, "app:label"), label_id);
    assert_eq!(
        &*loader
            .entry_context("include:app:label")
            .unwrap()
            .get(VALUE)
            .unwrap(),
        "after"
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn isolate_change_rebuilds_affected_subtree_only() {
    let catalog = dep_catalog_with_isolation();
    let group = EntryOptions::group(
        "app",
        vec![
            EntryOptions::new("provider", "demo.dep"),
            EntryOptions::new("consumer", "demo.consumer"),
        ],
    )
    .unwrap()
    .with_isolate(isolate_dep(true));
    let (runtime, _loader_fiber, loader, _dir, _) = mount_with_file(
        catalog,
        vec![EntryOptions::new("sibling", "demo.dep2"), group],
    )
    .await;
    let before = loader.await_idle().await.unwrap();
    let sibling_id = fiber_id(&before, "sibling");
    let provider_id = fiber_id(&before, "app:provider");
    let consumer_id = fiber_id(&before, "app:consumer");
    let app_id = fiber_id(&before, "app");

    loader
        .subtree("include")
        .unwrap()
        .update(
            "app",
            EntryUpdate {
                isolate: Some(Some(isolate_dep(false))),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap();
    let after = loader.await_idle().await.unwrap();
    assert_eq!(fiber_id(&after, "sibling"), sibling_id);
    assert_ne!(fiber_id(&after, "app"), app_id);
    assert_ne!(fiber_id(&after, "app:provider"), provider_id);
    assert_ne!(fiber_id(&after, "app:consumer"), consumer_id);
    assert_eq!(
        after.entry("include:app:provider").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(
        after.entry("include:app:consumer").unwrap().state,
        Some(FiberState::Active)
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn named_isolate_labels_are_pruned_after_reload() {
    let catalog = dep_catalog_with_isolation();
    let group_a = EntryOptions::group("a", vec![EntryOptions::new("provider", "demo.dep")])
        .unwrap()
        .with_isolate(isolate_dep(false));
    let group_b = EntryOptions::group("b", vec![EntryOptions::new("consumer", "demo.consumer")])
        .unwrap()
        .with_isolate(isolate_dep(false));
    let (runtime, _loader_fiber, loader, _dir, extensions) =
        mount_with_file(catalog, vec![group_a, group_b]).await;
    loader.await_idle().await.unwrap();

    // 去掉命名 isolate：成功 reconcile 后应清理 named_labels；再挂回同名标签仍可共享。
    fs::write(
        &extensions,
        toml::to_string(&config(vec![
            EntryOptions::group("a", vec![EntryOptions::new("provider", "demo.dep")])
                .unwrap()
                .with_isolate(isolate_dep(true)),
            EntryOptions::group("b", vec![EntryOptions::new("consumer", "demo.consumer")])
                .unwrap()
                .with_isolate(isolate_dep(true)),
        ]))
        .unwrap(),
    )
    .unwrap();
    loader.subtree("include").unwrap().reload().await.unwrap();
    let mid = loader.await_idle().await.unwrap();
    assert_eq!(
        mid.entry("include:b:consumer").unwrap().state,
        Some(FiberState::Pending)
    );

    fs::write(
        &extensions,
        toml::to_string(&config(vec![
            EntryOptions::group("a", vec![EntryOptions::new("provider", "demo.dep")])
                .unwrap()
                .with_isolate(isolate_dep(false)),
            EntryOptions::group("b", vec![EntryOptions::new("consumer", "demo.consumer")])
                .unwrap()
                .with_isolate(isolate_dep(false)),
        ]))
        .unwrap(),
    )
    .unwrap();
    loader.subtree("include").unwrap().reload().await.unwrap();
    let after = loader.await_idle().await.unwrap();
    assert_eq!(
        after.entry("include:a:provider").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(
        after.entry("include:b:consumer").unwrap().state,
        Some(FiberState::Active)
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn update_enable_disable_notifies_subscribers_with_increasing_revision() {
    let (runtime, _fiber, loader, _directory, _extensions) = mount_with_file(
        catalog(vec![Factory {
            id: "demo.value",
            needs_dep: false,
            record: None,
        }]),
        vec![EntryOptions::new("value", "demo.value")],
    )
    .await;
    loader.await_idle().await.unwrap();
    let mut events = loader.subscribe().unwrap();

    loader
        .subtree("include")
        .unwrap()
        .update(
            "value",
            EntryUpdate {
                disabled: Some(true),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap();
    let LoaderEvent::SnapshotChanged { revision: disabled } = events.recv().await.unwrap();

    loader
        .subtree("include")
        .unwrap()
        .update(
            "value",
            EntryUpdate {
                disabled: Some(false),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap();
    let LoaderEvent::SnapshotChanged { revision: enabled } = events.recv().await.unwrap();
    assert!(enabled > disabled);
    runtime.shutdown().await.unwrap();
}

fn file_plugin(
    mut catalog: ExtensionCatalog,
    bootstrap: impl AsRef<std::path::Path>,
) -> Result<LoaderPlugin, LoaderError> {
    catalog.register(cordis_plugin_include::IncludeFactory)?;
    let entry = cordis_plugin_include::bootstrap_entry("include", bootstrap)
        .map_err(|error| LoaderError::invalid_config(error.to_string()))?;
    LoaderPlugin::new(
        catalog,
        ExtensionsConfig {
            version: 3,
            extensions: vec![entry],
        },
    )
}
