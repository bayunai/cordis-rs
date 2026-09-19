//! snapshot 与 reconcile 新增实例的锁顺序回归。

#[path = "common/mod.rs"]
mod common;
use common::*;

use async_trait::async_trait;
use cordis_core::{Context, CoreError, FiberState, Plugin, PluginKey};
use cordis_loader::{CordisLoader, ExtensionFactory, LoaderError};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::timeout;

struct GatedAddFactory {
    entered: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    release: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
}

impl ExtensionFactory for GatedAddFactory {
    type Config = serde_json::Value;

    fn id(&self) -> &'static str {
        "demo.concurrent"
    }

    fn build(&self, _config: serde_json::Value) -> Result<Arc<dyn Plugin>, LoaderError> {
        Ok(Arc::new(GatedAddPlugin {
            entered: self.entered.clone(),
            release: self.release.clone(),
        }))
    }
}

struct GatedAddPlugin {
    entered: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    release: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
}

#[async_trait]
impl Plugin for GatedAddPlugin {
    fn key(&self) -> PluginKey {
        PluginKey::new("demo.concurrent")
    }

    async fn apply(&self, _ctx: &Context) -> Result<(), CoreError> {
        if let Some(sender) = self.entered.lock().expect("entered").take() {
            let _ = sender.send(());
        }
        let receiver = self.release.lock().expect("release").take();
        if let Some(receiver) = receiver {
            let _ = receiver.await;
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_concurrent_with_add_instance_does_not_deadlock() {
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let factory = GatedAddFactory {
        entered: Arc::new(Mutex::new(Some(entered_tx))),
        release: Arc::new(Mutex::new(Some(release_rx))),
    };

    let host = CordisLoader::new(catalog_with(factory)).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let snapshot_host = host.clone();
    let snapshot_stop = stop.clone();
    let snapshots = tokio::spawn(async move {
        while !snapshot_stop.load(Ordering::SeqCst) {
            let _ = snapshot_host.snapshot();
            tokio::task::yield_now().await;
        }
        snapshot_host.snapshot()
    });

    let apply = tokio::spawn({
        let host = host.clone();
        async move {
            host.apply(extensions(vec![enabled("new", "demo.concurrent")]))
                .await
        }
    });

    timeout(Duration::from_secs(2), entered_rx)
        .await
        .expect("apply should enter gate")
        .expect("gate sender dropped");

    // 给 snapshot 任务机会在 add_instance 双锁提交前后交叉执行。
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
    release_tx.send(()).unwrap();

    let apply_result = timeout(Duration::from_secs(2), apply)
        .await
        .expect("apply should finish without deadlock")
        .expect("apply join")
        .expect("apply ok");
    assert!(
        apply_result
            .instance("new")
            .is_some_and(|item| item.state == FiberState::Active)
    );

    stop.store(true, Ordering::SeqCst);
    let final_snapshot = timeout(Duration::from_secs(2), snapshots)
        .await
        .expect("snapshot loop should finish without deadlock")
        .expect("snapshot join");
    assert!(
        final_snapshot
            .instance("new")
            .is_some_and(|item| item.state == FiberState::Active)
    );

    host.shutdown().await.unwrap();
}
