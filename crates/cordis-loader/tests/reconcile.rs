//! Reconcile：预检零修改、按差异 replace / 卸载、不静默换工厂。

#[path = "common/mod.rs"]
mod common;
use common::*;

use cordis_core::FiberState;
use cordis_loader::{CordisLoader, ExtensionCatalog, LoaderError, toml};
use std::sync::atomic::Ordering;

#[tokio::test]
async fn duplicate_instance_fails_before_mutation() {
    let factory = TestFactory::new("demo.noop");
    let applies = factory.applies();
    let host = CordisLoader::new(catalog_with(factory)).unwrap();
    let error = host
        .apply(parse_extensions(
            r#"
version = 1
[[extensions]]
instance = "dup"
factory = "demo.noop"
enabled = true
[[extensions]]
instance = "dup"
factory = "demo.noop"
enabled = true
"#,
        ))
        .await
        .unwrap_err();
    assert!(matches!(error, LoaderError::DuplicateInstance { instance } if instance == "dup"));
    assert!(host.snapshot().instances.is_empty());
    assert_eq!(applies.load(Ordering::SeqCst), 0);
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn plugin_build_error_does_not_touch_active_instance() {
    let factory = TestFactory::new("demo.value").on_apply(|ctx, _| {
        ctx.provide(NUMBER, 1)?;
        Ok(())
    });
    let fail_build = factory.fail_build();
    let applies = factory.applies();
    let host = CordisLoader::new(catalog_with(factory)).unwrap();
    host.apply(extensions(vec![enabled("db", "demo.value")]))
        .await
        .unwrap();
    assert_eq!(*host.root().get(NUMBER).unwrap(), 1);

    fail_build.lock().unwrap().replace("bad schema".into());
    let error = host
        .apply(extensions(vec![
            enabled("db", "demo.value").with_config(table(&[("n", toml::Value::Integer(2))])),
        ]))
        .await
        .unwrap_err();
    assert!(matches!(error, LoaderError::PluginBuild { .. }));
    assert_eq!(
        host.snapshot().instance("db").unwrap().state,
        FiberState::Active
    );
    assert_eq!(*host.root().get(NUMBER).unwrap(), 1);
    assert_eq!(applies.load(Ordering::SeqCst), 1);
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn identical_reload_does_not_restart() {
    let factory = TestFactory::new("demo.noop");
    let builds = factory.builds();
    let applies = factory.applies();
    let host = CordisLoader::new(catalog_with(factory)).unwrap();
    let config = extensions(vec![enabled("a", "demo.noop")]);
    host.apply(config.clone()).await.unwrap();
    assert_eq!(builds.load(Ordering::SeqCst), 1);
    assert_eq!(applies.load(Ordering::SeqCst), 1);

    host.apply(config).await.unwrap();
    assert_eq!(builds.load(Ordering::SeqCst), 1);
    assert_eq!(applies.load(Ordering::SeqCst), 1);
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn config_change_replaces_only_that_instance() {
    let alpha = TestFactory::new("demo.alpha");
    let beta = TestFactory::new("demo.beta");
    let alpha_applies = alpha.applies();
    let beta_applies = beta.applies();
    let mut catalog = ExtensionCatalog::new();
    catalog.register(alpha).unwrap();
    catalog.register(beta).unwrap();
    let host = CordisLoader::new(catalog).unwrap();
    host.apply(extensions(vec![
        enabled("a", "demo.alpha"),
        enabled("b", "demo.beta"),
    ]))
    .await
    .unwrap();
    assert_eq!(alpha_applies.load(Ordering::SeqCst), 1);
    assert_eq!(beta_applies.load(Ordering::SeqCst), 1);

    host.apply(extensions(vec![
        enabled("a", "demo.alpha").with_config(table(&[("n", toml::Value::Integer(1))])),
        enabled("b", "demo.beta"),
    ]))
    .await
    .unwrap();
    assert_eq!(alpha_applies.load(Ordering::SeqCst), 2);
    assert_eq!(beta_applies.load(Ordering::SeqCst), 1);
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn factory_change_on_same_instance_is_rejected() {
    let mut catalog = ExtensionCatalog::new();
    catalog.register(TestFactory::new("demo.one")).unwrap();
    catalog.register(TestFactory::new("demo.two")).unwrap();
    let host = CordisLoader::new(catalog).unwrap();
    host.apply(extensions(vec![enabled("db", "demo.one")]))
        .await
        .unwrap();
    let fiber_id = host.snapshot().instance("db").unwrap().fiber_id;

    let error = host
        .apply(extensions(vec![enabled("db", "demo.two")]))
        .await
        .unwrap_err();
    assert!(matches!(error, LoaderError::FactoryChanged { .. }));
    let snapshot = host.snapshot();
    let instance = snapshot.instance("db").unwrap();
    assert_eq!(instance.factory, "demo.one");
    assert_eq!(instance.fiber_id, fiber_id);
    assert_eq!(instance.state, FiberState::Active);
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn plugin_key_change_on_replace_is_rejected() {
    struct DualKey;
    impl cordis_loader::ExtensionFactory for DualKey {
        type Config = serde_json::Value;

        fn id(&self) -> &'static str {
            "demo.dual"
        }
        fn build(
            &self,
            config: serde_json::Value,
        ) -> Result<std::sync::Arc<dyn cordis_core::Plugin>, LoaderError> {
            let alt = config.get("alt").and_then(|value| value.as_bool()) == Some(true);
            TestFactory::new("demo.dual")
                .plugin_key(if alt {
                    "demo.dual.alt"
                } else {
                    "demo.dual.main"
                })
                .build(config)
        }
    }

    let mut catalog = ExtensionCatalog::new();
    catalog.register(DualKey).unwrap();
    let host = CordisLoader::new(catalog).unwrap();
    host.apply(extensions(vec![enabled("db", "demo.dual")]))
        .await
        .unwrap();
    assert_eq!(
        host.snapshot().instance("db").unwrap().plugin_key.as_str(),
        "demo.dual.main"
    );

    let error = host
        .apply(extensions(vec![
            enabled("db", "demo.dual").with_config(table(&[("alt", toml::Value::Boolean(true))])),
        ]))
        .await
        .unwrap_err();
    assert!(matches!(error, LoaderError::PluginKeyChanged { .. }));
    assert_eq!(
        host.snapshot().instance("db").unwrap().state,
        FiberState::Active
    );
    assert_eq!(
        host.snapshot().instance("db").unwrap().plugin_key.as_str(),
        "demo.dual.main"
    );
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn removing_instance_waits_for_async_disposer() {
    let (gate, started_rx, release_tx) = DisposeGate::new();
    let host = CordisLoader::new(catalog_with(
        TestFactory::new("demo.gate").with_disposer(gate),
    ))
    .unwrap();
    host.apply(extensions(vec![enabled("db", "demo.gate")]))
        .await
        .unwrap();

    {
        let mut remove = std::pin::pin!(host.apply(empty_config()));
        tokio::select! {
            _ = &mut remove => panic!("remove finished before async disposer"),
            _ = started_rx => {}
        }
        release_tx.send(()).unwrap();
        remove.await.unwrap();
    }
    assert!(host.snapshot().instances.is_empty());
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn file_reload_rereads_extensions() {
    let dir = tempfile::tempdir().unwrap();
    let bootstrap = write_pair(
        dir.path(),
        r#"
version = 1
[config]
driver = "file"
path = "extensions.toml"
"#,
        r#"
version = 1
[[extensions]]
instance = "a"
factory = "demo.noop"
enabled = true
"#,
    );
    let host = CordisLoader::bootstrap(catalog_with(TestFactory::new("demo.noop")), &bootstrap)
        .await
        .unwrap();
    assert_eq!(host.snapshot().instances.len(), 1);

    std::fs::write(
        dir.path().join("extensions.toml"),
        r#"
version = 1
extensions = []
"#,
    )
    .unwrap();
    host.reload().await.unwrap();
    assert!(host.snapshot().instances.is_empty());
    host.shutdown().await.unwrap();
}

#[test]
fn missing_extensions_field_fails_parse() {
    let error = cordis_loader::ExtensionsConfig::from_toml_str("version = 1\n").unwrap_err();
    assert!(matches!(error, LoaderError::Toml { .. }));
    assert!(error.to_string().contains("extensions"));
}

#[tokio::test]
async fn reload_missing_extensions_field_leaves_active_instances() {
    let dir = tempfile::tempdir().unwrap();
    let bootstrap = write_pair(
        dir.path(),
        r#"
version = 1
[config]
driver = "file"
path = "extensions.toml"
"#,
        r#"
version = 1
[[extensions]]
instance = "a"
factory = "demo.noop"
enabled = true
"#,
    );
    let host = CordisLoader::bootstrap(catalog_with(TestFactory::new("demo.noop")), &bootstrap)
        .await
        .unwrap();
    assert_eq!(
        host.snapshot().instance("a").unwrap().state,
        FiberState::Active
    );

    std::fs::write(dir.path().join("extensions.toml"), "version = 1\n").unwrap();
    let error = host.reload().await.unwrap_err();
    assert!(matches!(error, LoaderError::Toml { .. }));
    assert_eq!(
        host.snapshot().instance("a").unwrap().state,
        FiberState::Active
    );
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn unknown_extension_field_fails_parse() {
    let error = cordis_loader::ExtensionsConfig::from_toml_str(
        r#"
version = 1
[[extensions]]
instance = "a"
factory = "demo.noop"
enabled = true
extra = true
"#,
    )
    .unwrap_err();
    assert!(matches!(error, LoaderError::Toml { .. }));
    assert!(error.to_string().contains("extra"));
}
