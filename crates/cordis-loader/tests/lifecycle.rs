//! 依赖收敛、replace 失败、shutdown 与快照字段。

#[path = "common/mod.rs"]
mod common;
use common::*;

use cordis_core::FiberState;
use cordis_loader::{CordisLoader, ExtensionCatalog, LoaderError};
use cordis_testkit::wait_until;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn missing_dependency_stays_pending_until_provider_arrives() {
    let provider = TestFactory::new("demo.provider").on_apply(|ctx, _| {
        ctx.provide(NUMBER, 7)?;
        Ok(())
    });
    let consumer = TestFactory::new("demo.consumer").inject(vec![NUMBER.id()]);
    let mut catalog = ExtensionCatalog::new();
    catalog.register(provider).unwrap();
    catalog.register(consumer).unwrap();
    let host = CordisLoader::new(catalog).unwrap();

    host.apply(extensions(vec![
        enabled("consumer", "demo.consumer"),
        enabled("provider", "demo.provider"),
    ]))
    .await
    .unwrap();

    wait_until(|| {
        host.snapshot().instance("consumer").map(|item| item.state) == Some(FiberState::Active)
    })
    .await;
    assert_eq!(
        host.snapshot().instance("provider").unwrap().state,
        FiberState::Active
    );
    assert_eq!(*host.root().get(NUMBER).unwrap(), 7);
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn replace_apply_failure_enters_failed_without_rollback() {
    let factory = TestFactory::new("demo.value").on_apply(|ctx, config| {
        let value = config.get("n").and_then(|item| item.as_i64()).unwrap_or(0) as u32;
        ctx.provide(NUMBER, value)?;
        Ok(())
    });
    let fail_apply = factory.fail_apply();
    let host = CordisLoader::new(catalog_with(factory)).unwrap();
    host.apply(extensions(vec![
        enabled("db", "demo.value").with_config(table(&[("n", toml_int(1))])),
    ]))
    .await
    .unwrap();
    assert_eq!(*host.root().get(NUMBER).unwrap(), 1);

    fail_apply.lock().unwrap().replace("boom".into());
    let error = host
        .apply(extensions(vec![
            enabled("db", "demo.value").with_config(table(&[("n", toml_int(2))])),
        ]))
        .await
        .unwrap_err();
    assert!(matches!(error, LoaderError::Lifecycle { .. }));

    let snapshot = host.snapshot().instance("db").unwrap().clone();
    assert_eq!(snapshot.state, FiberState::Failed);
    assert!(
        snapshot
            .last_error
            .as_deref()
            .is_some_and(|text| text.contains("boom"))
    );
    assert!(host.root().get(NUMBER).is_err());
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_releases_plugins_and_stops_scheduler() {
    let factory = TestFactory::new("demo.noop");
    let applies = factory.applies();
    let host = CordisLoader::new(catalog_with(factory)).unwrap();
    host.apply(extensions(vec![enabled("a", "demo.noop")]))
        .await
        .unwrap();
    assert_eq!(applies.load(Ordering::SeqCst), 1);
    let runtime = host.runtime().clone();
    host.shutdown().await.unwrap();
    assert!(runtime.scheduler_stopped());
}

#[tokio::test]
async fn snapshot_links_instance_plugin_key_fiber_and_state() {
    let factory = TestFactory::new("demo.snap").plugin_key("demo.snap.key");
    let host = CordisLoader::new(catalog_with(factory)).unwrap();
    let snapshot = host
        .apply(extensions(vec![enabled("primary", "demo.snap")]))
        .await
        .unwrap();
    let instance = snapshot.instance("primary").unwrap();
    assert_eq!(instance.instance.as_str(), "primary");
    assert_eq!(instance.factory, "demo.snap");
    assert_eq!(instance.plugin_key.as_str(), "demo.snap.key");
    assert_eq!(instance.state, FiberState::Active);
    assert!(instance.last_error.is_none());
    assert!(
        snapshot
            .runtime
            .plugin_fibers
            .iter()
            .any(|fiber| fiber.id == instance.fiber_id && fiber.plugin_key == "demo.snap.key")
    );
    host.shutdown().await.unwrap();
}

fn toml_int(value: i64) -> cordis_loader::toml::Value {
    cordis_loader::toml::Value::Integer(value)
}
