//! 工厂目录：重复注册、未知工厂与边界 panic 隔离。

#[path = "common/mod.rs"]
mod common;
use common::*;

use async_trait::async_trait;
use cordis_core::{Context, CoreError, EventKey, FiberState, Plugin, PluginKey};
use cordis_loader::{
    CordisLoader, ExtensionCatalog, ExtensionEntry, ExtensionFactory, LoaderError, toml,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

static CONTEXT_EVENT: EventKey<u32> = EventKey::new("host.test.context-event@1");

#[test]
fn catalog_rejects_duplicate_factory_id() {
    let mut catalog = ExtensionCatalog::new();
    catalog.register(TestFactory::new("demo.dup")).unwrap();
    let error = catalog.register(TestFactory::new("demo.dup")).unwrap_err();
    assert!(matches!(error, LoaderError::DuplicateFactory { .. }));
}

#[test]
fn catalog_captures_factory_id_panic() {
    struct PanicId;
    impl ExtensionFactory for PanicId {
        type Config = serde_json::Value;

        fn id(&self) -> &'static str {
            panic!("id boom");
        }
        fn build(&self, _config: serde_json::Value) -> Result<Arc<dyn Plugin>, LoaderError> {
            unreachable!()
        }
    }

    let mut catalog = ExtensionCatalog::new();
    let error = catalog.register(PanicId).unwrap_err();
    assert!(matches!(error, LoaderError::FactoryPanic { .. }));
    assert!(error.to_string().contains("id boom"));
}

#[derive(serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct TypedConfig {
    value: u32,
}

struct TypedFactory {
    builds: Arc<AtomicUsize>,
}

impl ExtensionFactory for TypedFactory {
    type Config = TypedConfig;

    fn id(&self) -> &'static str {
        "demo.typed"
    }

    fn build(&self, config: TypedConfig) -> Result<Arc<dyn Plugin>, LoaderError> {
        assert_eq!(config.value, 7);
        self.builds.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(TypedPlugin))
    }
}

struct TypedPlugin;

#[async_trait]
impl Plugin for TypedPlugin {
    fn key(&self) -> PluginKey {
        PluginKey::new("demo.typed")
    }

    async fn apply(&self, _ctx: &Context) -> Result<(), CoreError> {
        Ok(())
    }
}

#[test]
fn catalog_generates_json_schema_for_typed_factory() {
    let builds = Arc::new(AtomicUsize::new(0));
    let mut catalog = ExtensionCatalog::new();
    catalog.register(TypedFactory { builds }).unwrap();

    let descriptor = catalog.factories().unwrap().remove(0);
    assert_eq!(descriptor.id, "demo.typed");
    assert!(descriptor.schema["properties"]["value"].is_object());
}

#[tokio::test]
async fn catalog_deserializes_typed_factory_config_before_building() {
    let builds = Arc::new(AtomicUsize::new(0));
    let host = CordisLoader::new(catalog_with(TypedFactory {
        builds: builds.clone(),
    }))
    .unwrap();

    host.apply(extensions(vec![
        enabled("typed", "demo.typed").with_config(table(&[("value", toml::Value::Integer(7))])),
    ]))
    .await
    .unwrap();

    assert_eq!(builds.load(Ordering::SeqCst), 1);
    assert_eq!(host.snapshot().instances.len(), 1);
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_typed_config_fails_before_runtime_mutation() {
    let builds = Arc::new(AtomicUsize::new(0));
    let host = CordisLoader::new(catalog_with(TypedFactory {
        builds: builds.clone(),
    }))
    .unwrap();

    let error = host
        .apply(extensions(vec![
            enabled("typed", "demo.typed").with_config(table(&[(
                "value",
                toml::Value::String("not a number".into()),
            )])),
        ]))
        .await
        .unwrap_err();

    assert!(matches!(error, LoaderError::PluginBuild { factory, .. } if factory == "demo.typed"));
    assert_eq!(builds.load(Ordering::SeqCst), 0);
    assert!(host.snapshot().instances.is_empty());
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn missing_typed_config_field_fails_before_runtime_mutation() {
    let builds = Arc::new(AtomicUsize::new(0));
    let host = CordisLoader::new(catalog_with(TypedFactory {
        builds: builds.clone(),
    }))
    .unwrap();

    let error = host
        .apply(extensions(vec![enabled("typed", "demo.typed")]))
        .await
        .unwrap_err();

    assert!(matches!(error, LoaderError::PluginBuild { factory, .. } if factory == "demo.typed"));
    assert_eq!(builds.load(Ordering::SeqCst), 0);
    assert!(host.snapshot().instances.is_empty());
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn disabled_typed_config_is_not_deserialized_or_built() {
    let builds = Arc::new(AtomicUsize::new(0));
    let host = CordisLoader::new(catalog_with(TypedFactory {
        builds: builds.clone(),
    }))
    .unwrap();

    host.apply(extensions(vec![
        ExtensionEntry::new("typed", "demo.typed", false)
            .with_config(table(&[("value", toml::Value::String("invalid".into()))])),
    ]))
    .await
    .unwrap();

    assert_eq!(builds.load(Ordering::SeqCst), 0);
    assert!(host.snapshot().instances.is_empty());
    host.shutdown().await.unwrap();
}

struct EventFactory {
    hits: Arc<AtomicUsize>,
}

impl ExtensionFactory for EventFactory {
    type Config = serde_json::Value;

    fn id(&self) -> &'static str {
        "demo.events"
    }

    fn build(&self, _config: serde_json::Value) -> Result<Arc<dyn Plugin>, LoaderError> {
        Ok(Arc::new(EventPlugin {
            hits: self.hits.clone(),
        }))
    }
}

struct EventPlugin {
    hits: Arc<AtomicUsize>,
}

#[async_trait]
impl Plugin for EventPlugin {
    fn key(&self) -> PluginKey {
        PluginKey::new("demo.events")
    }

    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        let hits = self.hits.clone();
        ctx.on(CONTEXT_EVENT, move |_| {
            hits.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })?;
        Ok(())
    }
}

#[tokio::test]
async fn host_root_reads_services_and_dispatches_events() {
    let provider = TestFactory::new("demo.provider").on_apply(|ctx, _| {
        ctx.provide(NUMBER, 9)?;
        Ok(())
    });
    let hits = Arc::new(AtomicUsize::new(0));
    let mut catalog = ExtensionCatalog::new();
    catalog.register(provider).unwrap();
    catalog
        .register(EventFactory { hits: hits.clone() })
        .unwrap();
    let host = CordisLoader::new(catalog).unwrap();
    host.apply(extensions(vec![
        enabled("provider", "demo.provider"),
        enabled("events", "demo.events"),
    ]))
    .await
    .unwrap();

    let root = host.root();
    assert_eq!(*root.get(NUMBER).unwrap(), 9);
    root.emit(CONTEXT_EVENT, &1).unwrap();
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn unknown_factory_fails_before_mutation() {
    let factory = TestFactory::new("demo.known");
    let applies = factory.applies();
    let host = CordisLoader::new(catalog_with(factory)).unwrap();
    host.apply(extensions(vec![enabled("ok", "demo.known")]))
        .await
        .unwrap();
    assert_eq!(applies.load(std::sync::atomic::Ordering::SeqCst), 1);

    let error = host
        .apply(parse_extensions(
            r#"
version = 1
[[extensions]]
instance = "ok"
factory = "demo.known"
enabled = true
[[extensions]]
instance = "ghost"
factory = "demo.missing"
enabled = true
"#,
        ))
        .await
        .unwrap_err();
    assert!(matches!(error, LoaderError::UnknownFactory { .. }));
    let snapshot = host.snapshot();
    assert_eq!(snapshot.instances.len(), 1);
    assert_eq!(snapshot.instance("ok").unwrap().state, FiberState::Active);
    assert_eq!(applies.load(std::sync::atomic::Ordering::SeqCst), 1);
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn disabled_entry_still_requires_registered_factory() {
    let host = CordisLoader::new(catalog_with(TestFactory::new("demo.known"))).unwrap();
    let error = host
        .apply(parse_extensions(
            r#"
version = 1
[[extensions]]
instance = "idle"
factory = "demo.missing"
enabled = false
"#,
        ))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        LoaderError::UnknownFactory {
            instance,
            factory
        } if instance == "idle" && factory == "demo.missing"
    ));
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn disabled_entry_is_never_built_or_mounted() {
    let factory = TestFactory::new("demo.disabled");
    let builds = factory.builds();
    let applies = factory.applies();
    let host = CordisLoader::new(catalog_with(factory)).unwrap();

    host.apply(parse_extensions(
        r#"
version = 1
[[extensions]]
instance = "disabled"
factory = "demo.disabled"
enabled = false
"#,
    ))
    .await
    .unwrap();

    assert_eq!(builds.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(applies.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert!(host.snapshot().instances.is_empty());
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn factory_build_panic_leaves_active_instance() {
    struct PanicBuild {
        armed: Arc<std::sync::Mutex<bool>>,
    }
    impl ExtensionFactory for PanicBuild {
        type Config = serde_json::Value;

        fn id(&self) -> &'static str {
            "demo.panic-build"
        }
        fn build(&self, config: serde_json::Value) -> Result<Arc<dyn Plugin>, LoaderError> {
            if *self.armed.lock().unwrap() {
                panic!("build boom");
            }
            TestFactory::new("demo.panic-build").build(config)
        }
    }

    let armed = Arc::new(std::sync::Mutex::new(false));
    let host = CordisLoader::new(catalog_with(PanicBuild {
        armed: armed.clone(),
    }))
    .unwrap();
    host.apply(extensions(vec![enabled("db", "demo.panic-build")]))
        .await
        .unwrap();
    assert_eq!(
        host.snapshot().instance("db").unwrap().state,
        FiberState::Active
    );

    *armed.lock().unwrap() = true;
    let error = host
        .apply(extensions(vec![
            enabled("db", "demo.panic-build").with_config(table(&[("n", toml::Value::Integer(1))])),
        ]))
        .await
        .unwrap_err();
    assert!(matches!(error, LoaderError::FactoryPanic { .. }));
    assert!(error.to_string().contains("build boom"));
    assert_eq!(
        host.snapshot().instance("db").unwrap().state,
        FiberState::Active
    );
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn plugin_key_panic_leaves_active_instance() {
    struct KeyPanicPlugin;
    #[async_trait]
    impl Plugin for KeyPanicPlugin {
        fn key(&self) -> PluginKey {
            panic!("key boom");
        }
        async fn apply(&self, _ctx: &Context) -> Result<(), CoreError> {
            Ok(())
        }
    }

    struct KeyPanicFactory {
        armed: Arc<std::sync::Mutex<bool>>,
    }
    impl ExtensionFactory for KeyPanicFactory {
        type Config = serde_json::Value;

        fn id(&self) -> &'static str {
            "demo.panic-key"
        }
        fn build(&self, config: serde_json::Value) -> Result<Arc<dyn Plugin>, LoaderError> {
            if *self.armed.lock().unwrap() {
                return Ok(Arc::new(KeyPanicPlugin));
            }
            TestFactory::new("demo.panic-key").build(config)
        }
    }

    let armed = Arc::new(std::sync::Mutex::new(false));
    let host = CordisLoader::new(catalog_with(KeyPanicFactory {
        armed: armed.clone(),
    }))
    .unwrap();
    host.apply(extensions(vec![enabled("db", "demo.panic-key")]))
        .await
        .unwrap();

    *armed.lock().unwrap() = true;
    let error = host
        .apply(extensions(vec![
            enabled("db", "demo.panic-key").with_config(table(&[("n", toml::Value::Integer(1))])),
        ]))
        .await
        .unwrap_err();
    assert!(matches!(error, LoaderError::PluginPanic { .. }));
    assert!(error.to_string().contains("key boom"));
    assert_eq!(
        host.snapshot().instance("db").unwrap().state,
        FiberState::Active
    );
    host.shutdown().await.unwrap();
}
