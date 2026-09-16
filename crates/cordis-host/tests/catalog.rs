//! 工厂目录：重复注册、未知工厂与边界 panic 隔离。

#[path = "common/mod.rs"]
mod common;
use common::*;

use async_trait::async_trait;
use cordis_core::{Context, CoreError, FiberState, Plugin, PluginKey};
use cordis_host::{CordisHost, ExtensionCatalog, ExtensionFactory, HostError, toml};
use std::sync::Arc;

#[test]
fn catalog_rejects_duplicate_factory_id() {
    let mut catalog = ExtensionCatalog::new();
    catalog
        .register(TestFactory::new("demo.dup").into_arc())
        .unwrap();
    let error = catalog
        .register(TestFactory::new("demo.dup").into_arc())
        .unwrap_err();
    assert!(matches!(error, HostError::DuplicateFactory { .. }));
}

#[test]
fn catalog_captures_factory_id_panic() {
    struct PanicId;
    impl ExtensionFactory for PanicId {
        fn id(&self) -> &'static str {
            panic!("id boom");
        }
        fn build(&self, _config: &toml::Value) -> Result<Arc<dyn Plugin>, HostError> {
            unreachable!()
        }
    }

    let mut catalog = ExtensionCatalog::new();
    let error = catalog.register(Arc::new(PanicId)).unwrap_err();
    assert!(matches!(error, HostError::FactoryPanic { .. }));
    assert!(error.to_string().contains("id boom"));
}

#[tokio::test]
async fn unknown_factory_fails_before_mutation() {
    let factory = TestFactory::new("demo.known");
    let applies = factory.applies();
    let host = CordisHost::new(catalog_with(factory.into_arc())).unwrap();
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
    assert!(matches!(error, HostError::UnknownFactory { .. }));
    let snapshot = host.snapshot();
    assert_eq!(snapshot.instances.len(), 1);
    assert_eq!(snapshot.instance("ok").unwrap().state, FiberState::Active);
    assert_eq!(applies.load(std::sync::atomic::Ordering::SeqCst), 1);
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn disabled_entry_still_requires_registered_factory() {
    let host = CordisHost::new(catalog_with(TestFactory::new("demo.known").into_arc())).unwrap();
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
        HostError::UnknownFactory {
            instance,
            factory
        } if instance == "idle" && factory == "demo.missing"
    ));
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn factory_build_panic_leaves_active_instance() {
    struct PanicBuild {
        armed: Arc<std::sync::Mutex<bool>>,
    }
    impl ExtensionFactory for PanicBuild {
        fn id(&self) -> &'static str {
            "demo.panic-build"
        }
        fn build(&self, config: &toml::Value) -> Result<Arc<dyn Plugin>, HostError> {
            if *self.armed.lock().unwrap() {
                panic!("build boom");
            }
            TestFactory::new("demo.panic-build").build(config)
        }
    }

    let armed = Arc::new(std::sync::Mutex::new(false));
    let host = CordisHost::new(catalog_with(Arc::new(PanicBuild {
        armed: armed.clone(),
    })))
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
    assert!(matches!(error, HostError::FactoryPanic { .. }));
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
        fn id(&self) -> &'static str {
            "demo.panic-key"
        }
        fn build(&self, config: &toml::Value) -> Result<Arc<dyn Plugin>, HostError> {
            if *self.armed.lock().unwrap() {
                return Ok(Arc::new(KeyPanicPlugin));
            }
            TestFactory::new("demo.panic-key").build(config)
        }
    }

    let armed = Arc::new(std::sync::Mutex::new(false));
    let host = CordisHost::new(catalog_with(Arc::new(KeyPanicFactory {
        armed: armed.clone(),
    })))
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
    assert!(matches!(error, HostError::PluginPanic { .. }));
    assert!(error.to_string().contains("key boom"));
    assert_eq!(
        host.snapshot().instance("db").unwrap().state,
        FiberState::Active
    );
    host.shutdown().await.unwrap();
}
