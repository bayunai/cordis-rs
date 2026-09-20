//! 共享 SSE LogExporter 回归。

#[path = "../../../examples/log_stream.rs"]
mod log_stream;

use cordis_core::{LogLevel, Runtime};
use log_stream::SseLogExporter;
use std::sync::Arc;
use tokio::time::{Duration, timeout};

#[tokio::test]
async fn sse_exporter_receives_runtime_logger_records() {
    let runtime = Runtime::new().unwrap();
    let exporter = Arc::new(SseLogExporter::new(16));
    let mut rx = exporter.subscribe();
    runtime
        .root()
        .register_log_exporter(exporter.clone())
        .unwrap();

    runtime.root().logger().unwrap().warn("hello-sse");

    let line = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("recv")
        .expect("message");
    assert!(line.contains("WARN"));
    assert!(line.contains("root"));
    assert!(line.contains("hello-sse"));
    assert_eq!(
        SseLogExporter::format_line(&runtime.logger_service().buffer_snapshot()[0]),
        line
    );
    let snap = runtime.logger_service().buffer_snapshot();
    assert_eq!(snap[0].level, LogLevel::Warn);
    assert_eq!(snap[0].target, "root");
    assert_eq!(snap[0].message, "hello-sse");
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn no_subscribers_still_buffers_records() {
    let runtime = Runtime::new().unwrap();
    let exporter = Arc::new(SseLogExporter::new(8));
    // subscribe then drop — channel has no live receivers
    drop(exporter.subscribe());
    runtime.root().register_log_exporter(exporter).unwrap();
    runtime.root().logger().unwrap().info("orphan");
    let snap = runtime.logger_service().buffer_snapshot();
    assert_eq!(snap.len(), 1);
    assert_eq!(snap[0].message, "orphan");
    runtime.shutdown().await.unwrap();
}
