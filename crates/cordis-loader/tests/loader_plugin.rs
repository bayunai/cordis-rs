use async_trait::async_trait;
use cordis_core::{
    ConfigKey, Context, CoreError, Fiber, FiberState, Plugin, PluginKey, Runtime, ServiceKey,
};
use cordis_loader::{
    EntryOptions, EntryUpdate, ExtensionCatalog, ExtensionFactory, ExtensionsConfig, InjectConfig,
    InjectionDescriptor, LOADER, Loader, LoaderControlError, LoaderError, LoaderPlugin,
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
    fn build(&self, _: Empty) -> Result<Arc<dyn Plugin>, LoaderError> {
        Ok(Arc::new(TestPlugin {
            id: self.id,
            needs_dep: self.needs_dep,
            record: self.record.clone(),
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
        version: 2,
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

#[tokio::test]
async fn group_services_do_not_leak_to_root_or_other_groups() {
    let group_a =
        EntryOptions::group("a", vec![EntryOptions::new("provider", "demo.dep")]).unwrap();
    let group_b =
        EntryOptions::group("b", vec![EntryOptions::new("consumer", "demo.consumer")]).unwrap();
    let (runtime, _loader_fiber, loader) = mount(
        LoaderPlugin::new(
            dep_catalog(),
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
    let group_a =
        EntryOptions::group("a", vec![EntryOptions::new("provider", "demo.dep")]).unwrap();
    let group_b =
        EntryOptions::group("b", vec![EntryOptions::new("provider", "demo.dep2")]).unwrap();
    let (runtime, _loader_fiber, loader) =
        mount(LoaderPlugin::new(dep_catalog(), config(vec![group_a, group_b])).unwrap()).await;
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
    let (runtime, _loader_fiber, loader) =
        mount(LoaderPlugin::new(dep_catalog(), config(vec![group])).unwrap()).await;
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
        .as_deref()
        .unwrap_or("");
    assert!(
        error.contains("已在当前 Context 注册") || error.contains("ServiceConflict"),
        "unexpected last_error: {error:?}"
    );
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
        "version = 2\n[config]\ndriver = \"file\"\npath = \"extensions.toml\"\n",
    )
    .unwrap();
    fs::write(&extensions, toml::to_string(&config(vec![group])).unwrap()).unwrap();
    let (runtime, _loader_fiber, loader) =
        mount(LoaderPlugin::bootstrap(dep_catalog(), &bootstrap).unwrap()).await;
    assert_eq!(
        loader
            .await_idle()
            .await
            .unwrap()
            .entry("app:consumer")
            .unwrap()
            .state,
        Some(FiberState::Active)
    );

    loader
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
    assert!(!snapshot.entry("app:provider").unwrap().enabled);
    assert_eq!(
        snapshot.entry("app:consumer").unwrap().state,
        Some(FiberState::Pending)
    );

    loader
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
fn v2_rejects_old_flat_entries_and_bad_groups() {
    let old = ExtensionsConfig::from_toml_str("version = 1\nextensions = []\n").unwrap();
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
    }]);
    assert!(matches!(
        bad.validate(&ExtensionCatalog::new()),
        Err(LoaderError::InvalidEntry { .. })
    ));
}

#[tokio::test]
async fn persistent_create_move_and_conflict_use_paths() {
    let directory = tempfile::tempdir().unwrap();
    let bootstrap = directory.path().join("bootstrap.toml");
    let extensions = directory.path().join("extensions.toml");
    fs::write(
        &bootstrap,
        "version = 2\n[config]\ndriver = \"file\"\npath = \"extensions.toml\"\n",
    )
    .unwrap();
    fs::write(&extensions, "version = 2\nextensions = []\n").unwrap();
    let (runtime, _loader_fiber, loader) = mount(
        LoaderPlugin::bootstrap(
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
        .create(EntryOptions::group("one", vec![]).unwrap(), None, None)
        .await
        .unwrap();
    loader
        .create(EntryOptions::group("two", vec![]).unwrap(), None, None)
        .await
        .unwrap();
    loader
        .create(EntryOptions::new("child", "demo.value"), Some("one"), None)
        .await
        .unwrap();
    loader
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
            .entry("one:child")
            .unwrap()
            .state
            .is_none()
    );
    loader
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
            .entry("two:child")
            .is_some()
    );
    assert!(
        fs::read_to_string(&extensions)
            .unwrap()
            .contains("id = \"two\"")
    );
    fs::write(&extensions, "version = 2\nextensions = []\n# outside\n").unwrap();
    assert!(matches!(
        loader
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
        "version = 2\n[config]\ndriver = \"file\"\npath = \"extensions.toml\"\n",
    )
    .unwrap();
    fs::write(&extensions, toml::to_string(&config(vec![group])).unwrap()).unwrap();
    let (runtime, _loader_fiber, loader) = mount(
        LoaderPlugin::bootstrap(
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
    loader.remove("group").await.unwrap();
    assert_eq!(&*record.lock().unwrap(), &["demo.second", "demo.first"]);
    runtime.shutdown().await.unwrap();
}
