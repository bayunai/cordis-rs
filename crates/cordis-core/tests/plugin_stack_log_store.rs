//! plugin_stack LogStore / HTTP 日志脱敏回归。

#[path = "../../../examples/plugin_stack/db.rs"]
mod db;
#[path = "../../../examples/plugin_stack/log_store.rs"]
mod log_store;
#[path = "../../../examples/plugin_stack/url_redact.rs"]
mod url_redact;

mod keys {
    use cordis_core::{PluginKey, ServiceKey};

    use crate::db::Db;

    pub static DB: ServiceKey<Db> = ServiceKey::new("demo.stack.db@1");
    pub static KEY_DB: PluginKey = PluginKey::new("demo.stack.db");
    pub static KEY_LOG_STORE: PluginKey = PluginKey::new("demo.stack.log-store");
}

use cordis_core::{FiberState, Runtime};
use db::DbPlugin;
use keys::DB;
use log_store::LogStorePlugin;
use std::sync::Arc;
use tokio::time::{Duration, sleep};
use url_redact::redact_url;

#[test]
fn http_redact_strips_query_and_fragment() {
    assert_eq!(
        redact_url("https://api.example/v1/items?token=secret#frag"),
        "https://api.example/v1/items?[redacted]"
    );
    assert_eq!(
        redact_url("http://localhost:8080/path"),
        "http://localhost:8080/path"
    );
    assert_eq!(redact_url("not a url"), "(invalid-url)");
}

#[tokio::test]
async fn log_store_writes_runtime_logs_into_db() {
    let runtime = Runtime::new().unwrap();
    let mut db_fiber = runtime.root().plugin(Arc::new(DbPlugin)).await.unwrap();
    let mut store = runtime
        .root()
        .plugin(Arc::new(LogStorePlugin))
        .await
        .unwrap();
    runtime.settle().await;
    assert_eq!(store.state(), FiberState::Active);

    runtime.root().logger().unwrap().info("persist-me");
    sleep(Duration::from_millis(50)).await;

    let lines = runtime.root().get(DB).unwrap().recent(20);
    assert!(
        lines.iter().any(|l| l.contains("persist-me")),
        "expected DB to contain persist-me, got {lines:?}"
    );

    store.dispose_wait().await.unwrap();
    runtime.settle().await;
    runtime.root().get(DB).unwrap().clear();
    runtime.root().logger().unwrap().info("after-dispose");
    sleep(Duration::from_millis(50)).await;
    let after = runtime.root().get(DB).unwrap().recent(20);
    assert!(
        after.iter().all(|l| !l.contains("after-dispose")),
        "log-store should stop writing after dispose: {after:?}"
    );

    db_fiber.dispose_wait().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn log_store_pending_without_db() {
    let runtime = Runtime::new().unwrap();
    let mut store = runtime
        .root()
        .plugin(Arc::new(LogStorePlugin))
        .await
        .unwrap();
    runtime.settle().await;
    assert_eq!(store.state(), FiberState::Pending);
    assert!(
        store
            .missing_dependencies()
            .iter()
            .any(|id| id.as_str() == DB.id().as_str())
    );
    store.dispose_wait().await.unwrap();
    runtime.shutdown().await.unwrap();
}
