use async_trait::async_trait;
use cordis_core::{
    ConfigKey, Context, CoreError, Fiber, FiberState, Plugin, PluginKey, Runtime, ServiceKey,
};
use cordis_loader::{
    EntryOptions, EntryUpdate, ExtensionCatalog, ExtensionFactory, ExtensionsConfig, InjectConfig,
    InjectionDescriptor, IsolateValue, IsolationDescriptor, LOADER, Loader, LoaderControlError,
    LoaderError, LoaderPlugin,
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
    // 无 isolate：Root 与 Group 内同名 DEP Provider 冲突 → Loader Failed。
    assert_eq!(fiber.state(), FiberState::Failed);
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
    assert_eq!(fiber.state(), FiberState::Failed);
    let error = fiber.last_error().unwrap_or_default();
    assert!(
        error.contains("Lifecycle")
            || error.contains("已在当前 Context 注册")
            || error.contains("ServiceConflict"),
        "unexpected last_error: {error:?}"
    );
    // 生命周期失败不提交 desired；LOADER 因 Fiber Failed 而不可用。
    assert!(matches!(
        runtime.root().get(LOADER),
        Err(CoreError::ServiceUnavailable { .. })
    ));
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
        "version = 3\n[config]\ndriver = \"file\"\npath = \"extensions.toml\"\n",
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
        isolate: None,
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
        "version = 3\n[config]\ndriver = \"file\"\npath = \"extensions.toml\"\n",
    )
    .unwrap();
    fs::write(&extensions, "version = 3\nextensions = []\n").unwrap();
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
    fs::write(&extensions, "version = 3\nextensions = []\n# outside\n").unwrap();
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
        "version = 3\n[config]\ndriver = \"file\"\npath = \"extensions.toml\"\n",
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
    fn build(&self, config: CountConfig) -> Result<Arc<dyn Plugin>, LoaderError> {
        Ok(Arc::new(CountPlugin { n: config.n }))
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
    fn build(&self, config: KeySwitchConfig) -> Result<Arc<dyn Plugin>, LoaderError> {
        Ok(Arc::new(KeySwitchPlugin { alt: config.alt }))
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
    fn build(&self, config: FailConfig) -> Result<Arc<dyn Plugin>, LoaderError> {
        Ok(Arc::new(FailPlugin { fail: config.fail }))
    }
}

fn fiber_id(snapshot: &cordis_loader::LoaderSnapshot, path: &str) -> u64 {
    snapshot.entry(path).unwrap().fiber_id.unwrap()
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
    let (runtime, fiber, loader) =
        mount(LoaderPlugin::bootstrap(catalog, &bootstrap).unwrap()).await;
    (runtime, fiber, loader, directory, extensions)
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
            .entry_context("app:count")
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
        vec!["one", "two"]
    );

    loader
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
        vec!["two", "one"]
    );
    assert_eq!(fiber_id(&after, "one"), one_id);
    assert_eq!(fiber_id(&after, "two"), two_id);
    assert_eq!(
        loader
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
    let error = loader.reload().await.unwrap_err();
    assert!(
        matches!(
            error,
            LoaderControlError::Loader(LoaderError::FactoryChanged { .. })
        ),
        "unexpected error: {error:?}"
    );
    let after = loader.await_idle().await.unwrap();
    assert_eq!(fiber_id(&after, "item"), item_id);
    assert_eq!(after.entry("item").unwrap().state, Some(FiberState::Active));
    assert_eq!(loader.entries().unwrap().extensions[0].name, "demo.value");
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
        after.entry("keyed").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(
        loader.entries().unwrap().extensions[0].config,
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

    let error = loader.reload().await.unwrap_err();
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
        loader.entries().unwrap().extensions[0].children().unwrap()[0].config,
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

    let error = loader
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

    let after = loader.await_idle().await.unwrap();
    assert_eq!(fiber_id(&after, "stable"), stable_id);
    assert_eq!(
        after.entry("stable").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(
        after.entry("flaky").unwrap().state,
        Some(FiberState::Failed)
    );
    assert_eq!(
        loader.entries().unwrap().extensions[1].config,
        toml::Value::try_from(FailConfig { fail: false }).unwrap()
    );
    assert_eq!(fs::read_to_string(&extensions).unwrap(), before_file);
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
        loader.reload().await,
        Err(LoaderControlError::Loader(LoaderError::Lifecycle { .. }))
    ));
    assert_eq!(
        loader
            .await_idle()
            .await
            .unwrap()
            .entry("flaky")
            .unwrap()
            .state,
        Some(FiberState::Failed)
    );
    assert_eq!(
        loader.entries().unwrap().extensions[0].config,
        initial.config
    );

    assert!(matches!(
        loader.reload().await,
        Err(LoaderControlError::Loader(LoaderError::Lifecycle { .. }))
    ));
    assert_eq!(
        loader
            .await_idle()
            .await
            .unwrap()
            .entry("flaky")
            .unwrap()
            .state,
        Some(FiberState::Failed)
    );
    assert_eq!(
        loader.entries().unwrap().extensions[0].config,
        initial.config
    );

    fs::write(
        &extensions,
        toml::to_string(&config(vec![initial])).unwrap(),
    )
    .unwrap();
    loader.reload().await.unwrap();
    assert_eq!(
        loader
            .await_idle()
            .await
            .unwrap()
            .entry("flaky")
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
        disposed.entry("value").unwrap().state,
        Some(FiberState::Disposed)
    );
    assert!(runtime.root().get(VALUE).is_err());

    loader.reload().await.unwrap();
    let remounted = loader.await_idle().await.unwrap();
    assert_eq!(
        remounted.entry("value").unwrap().state,
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
        loader.create(failed, None, None).await,
        Err(LoaderControlError::Loader(LoaderError::Lifecycle { .. }))
    ));
    let snapshot = loader.await_idle().await.unwrap();
    assert_eq!(
        snapshot.entry("flaky").unwrap().state,
        Some(FiberState::Failed)
    );
    assert!(loader.entries().unwrap().extensions.is_empty());

    loader.reload().await.unwrap();
    assert!(loader.await_idle().await.unwrap().entry("flaky").is_none());

    let healthy = EntryOptions::new("flaky", "demo.fail")
        .with_config(toml::Value::try_from(FailConfig { fail: false }).unwrap());
    loader.create(healthy, None, None).await.unwrap();
    assert_eq!(
        loader
            .await_idle()
            .await
            .unwrap()
            .entry("flaky")
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
        before.entry("group:child").unwrap().state,
        Some(FiberState::Active)
    );

    loader
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
    assert!(disabled.entry("group").unwrap().state.is_some());
    assert!(!disabled.entry("group:child").unwrap().enabled);
    assert!(disabled.entry("group:child").unwrap().state.is_none());
    assert_eq!(&*record.lock().unwrap(), &["demo.value"]);

    loader
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
        enabled.entry("group:child").unwrap().state,
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
            .entry_context("app:label")
            .unwrap()
            .get(VALUE)
            .unwrap(),
        "after"
    );
    runtime.shutdown().await.unwrap();
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
    assert_eq!(fiber.state(), FiberState::Failed);
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
        fn build(&self, _: Empty) -> Result<Arc<dyn Plugin>, LoaderError> {
            Ok(Arc::new(OtherPlugin))
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
        after.entry("app:provider").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(
        after.entry("app:consumer").unwrap().state,
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
    loader.reload().await.unwrap();
    let mid = loader.await_idle().await.unwrap();
    assert_eq!(
        mid.entry("b:consumer").unwrap().state,
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
    loader.reload().await.unwrap();
    let after = loader.await_idle().await.unwrap();
    assert_eq!(
        after.entry("a:provider").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(
        after.entry("b:consumer").unwrap().state,
        Some(FiberState::Active)
    );
    runtime.shutdown().await.unwrap();
}
