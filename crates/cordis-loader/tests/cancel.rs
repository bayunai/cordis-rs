//! apply/reload 取消安全：协调器继续收敛，Host 映射与 Core 一致。

#[path = "common/mod.rs"]
mod common;
use common::*;

use cordis_core::FiberState;
use cordis_loader::{CordisLoader, LoaderError, toml};
use cordis_testkit::wait_until;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn cancel_apply_during_replace_still_commits_new_config() {
    let (gate, started_rx, release_tx) = DisposeGate::new();
    let factory = TestFactory::new("demo.value")
        .with_disposer(gate)
        .on_apply(|ctx, config| {
            let value = config.get("n").and_then(|item| item.as_i64()).unwrap_or(0) as u32;
            ctx.provide(NUMBER, value)?;
            Ok(())
        });
    let applies = factory.applies();
    let host = CordisLoader::new(catalog_with(factory)).unwrap();
    let initial = extensions(vec![
        enabled("db", "demo.value").with_config(table(&[("n", toml::Value::Integer(1))])),
    ]);
    host.apply(initial).await.unwrap();
    assert_eq!(*host.root().get(NUMBER).unwrap(), 1);
    assert_eq!(applies.load(Ordering::SeqCst), 1);

    let updated = extensions(vec![
        enabled("db", "demo.value").with_config(table(&[("n", toml::Value::Integer(2))])),
    ]);
    {
        let mut pending = std::pin::pin!(host.apply(updated.clone()));
        tokio::select! {
            _ = &mut pending => panic!("replace finished before async disposer"),
            _ = started_rx => {}
        }
        // 离开作用域即取消调用方 wait；协调器必须继续。
    }
    release_tx.send(()).unwrap();
    wait_until(|| {
        applies.load(Ordering::SeqCst) >= 2
            && host.snapshot().instance("db").map(|item| item.state) == Some(FiberState::Active)
    })
    .await;
    assert_eq!(*host.root().get(NUMBER).unwrap(), 2);
    assert_eq!(applies.load(Ordering::SeqCst), 2);

    // 相同目标配置再次 apply 不得二次 replace。
    host.apply(updated).await.unwrap();
    assert_eq!(applies.load(Ordering::SeqCst), 2);
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancel_apply_during_remove_clears_instance_mapping() {
    let (gate, started_rx, release_tx) = DisposeGate::new();
    let host = CordisLoader::new(catalog_with(
        TestFactory::new("demo.gate").with_disposer(gate),
    ))
    .unwrap();
    host.apply(extensions(vec![enabled("db", "demo.gate")]))
        .await
        .unwrap();
    assert!(host.snapshot().instance("db").is_some());

    {
        let mut pending = std::pin::pin!(host.apply(empty_config()));
        tokio::select! {
            _ = &mut pending => panic!("remove finished before async disposer"),
            _ = started_rx => {}
        }
    }
    release_tx.send(()).unwrap();
    wait_until(|| host.snapshot().instances.is_empty()).await;
    assert!(host.snapshot().instance("db").is_none());
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn concurrent_apply_while_reconcile_returns_busy() {
    let (gate, started_rx, release_tx) = DisposeGate::new();
    let host = CordisLoader::new(catalog_with(
        TestFactory::new("demo.gate").with_disposer(gate),
    ))
    .unwrap();
    host.apply(extensions(vec![enabled("db", "demo.gate")]))
        .await
        .unwrap();

    {
        let mut pending = std::pin::pin!(host.apply(empty_config()));
        tokio::select! {
            _ = &mut pending => panic!("remove finished before async disposer"),
            _ = started_rx => {}
        }
        let busy = host
            .apply(extensions(vec![enabled("db", "demo.gate")]))
            .await
            .unwrap_err();
        assert!(matches!(busy, LoaderError::ReconcileBusy));
        release_tx.send(()).unwrap();
        pending.await.unwrap();
    }
    assert!(host.snapshot().instances.is_empty());
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_during_reconcile_converges_and_stops_scheduler() {
    let (gate, started_rx, release_tx) = DisposeGate::new();
    let host = CordisLoader::new(catalog_with(
        TestFactory::new("demo.gate").with_disposer(gate),
    ))
    .unwrap();
    host.apply(extensions(vec![enabled("db", "demo.gate")]))
        .await
        .unwrap();
    let runtime = host.runtime().clone();

    {
        let mut pending = std::pin::pin!(host.apply(empty_config()));
        tokio::select! {
            _ = &mut pending => panic!("remove finished before async disposer"),
            _ = started_rx => {}
        }
    }

    let shutdown = tokio::spawn(async move { host.shutdown().await });
    tokio::task::yield_now().await;
    release_tx.send(()).unwrap();
    shutdown.await.unwrap().unwrap();
    assert!(runtime.scheduler_stopped());
}
