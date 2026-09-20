//! Runtime 全局 Logger 回归。

use async_trait::async_trait;
use cordis_core::{
    ConfigKey, Context, CoreError, LOG_BUFFER_CAPACITY, LOGGER_CONFIG, LogExporter, LogLevel,
    LogRecord, LoggerConfig, Plugin, PluginKey, Runtime, ServiceKey,
};
use std::sync::{Arc, Mutex};

struct CaptureExporter {
    records: Arc<Mutex<Vec<LogRecord>>>,
    min_level: LogLevel,
}

impl CaptureExporter {
    fn all(records: Arc<Mutex<Vec<LogRecord>>>) -> Self {
        Self {
            records,
            min_level: LogLevel::Debug,
        }
    }

    fn threshold(records: Arc<Mutex<Vec<LogRecord>>>, min_level: LogLevel) -> Self {
        Self { records, min_level }
    }
}

impl LogExporter for CaptureExporter {
    fn export(&self, record: &LogRecord) {
        if record.level < self.min_level {
            return;
        }
        self.records.lock().unwrap().push(record.clone());
    }
}

#[test]
fn log_level_as_str_uses_stable_lowercase_labels() {
    assert_eq!(LogLevel::Debug.as_str(), "debug");
    assert_eq!(LogLevel::Info.as_str(), "info");
    assert_eq!(LogLevel::Warn.as_str(), "warn");
    assert_eq!(LogLevel::Error.as_str(), "error");
}

#[tokio::test]
async fn isolate_and_extend_share_same_logger_service() {
    static KEY: ServiceKey<()> = ServiceKey::new("logger.test.isolate@1");
    let runtime = Runtime::new().unwrap();
    let root = runtime.root();
    let extended = root.extend().unwrap();
    let (isolated, _) = root.isolate(KEY).unwrap();
    assert!(Arc::ptr_eq(
        &runtime.logger_service(),
        root.logger().unwrap().service()
    ));
    assert!(Arc::ptr_eq(
        root.logger().unwrap().service(),
        extended.logger().unwrap().service()
    ));
    assert!(Arc::ptr_eq(
        root.logger().unwrap().service(),
        isolated.logger().unwrap().service()
    ));
    let _ = isolated;
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn buffer_keeps_records_without_exporter_and_caps_at_1000() {
    let runtime = Runtime::new().unwrap();
    let logger = runtime.root().logger().unwrap();
    for i in 0..LOG_BUFFER_CAPACITY + 50 {
        logger.info(format!("msg-{i}"));
    }
    let snap = runtime.logger_service().buffer_snapshot();
    assert_eq!(snap.len(), LOG_BUFFER_CAPACITY);
    assert_eq!(snap.first().unwrap().message, "msg-50");
    assert_eq!(
        snap.last().unwrap().message,
        format!("msg-{}", LOG_BUFFER_CAPACITY + 49)
    );
    assert!(snap[0].seq < snap[1].seq);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn debug_enters_buffer_under_default_info_context() {
    let runtime = Runtime::new().unwrap();
    let records = Arc::new(Mutex::new(Vec::new()));
    runtime
        .root()
        .register_log_exporter(Arc::new(CaptureExporter::all(records.clone())))
        .unwrap();
    let logger = runtime.root().logger().unwrap();
    assert_eq!(logger.default_level(), LogLevel::Info);
    logger.debug("dbg");
    let snap = runtime.logger_service().buffer_snapshot();
    assert_eq!(snap.len(), 1);
    assert_eq!(snap[0].level, LogLevel::Debug);
    assert_eq!(snap[0].default_level, LogLevel::Info);
    assert_eq!(records.lock().unwrap().len(), 1);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn exporters_filter_independently_by_threshold() {
    let runtime = Runtime::new().unwrap();
    let loose = Arc::new(Mutex::new(Vec::new()));
    let strict = Arc::new(Mutex::new(Vec::new()));
    runtime
        .root()
        .register_log_exporter(Arc::new(CaptureExporter::threshold(
            loose.clone(),
            LogLevel::Debug,
        )))
        .unwrap();
    let child = runtime.root().effect().unwrap();
    child
        .register_log_exporter(Arc::new(CaptureExporter::threshold(
            strict.clone(),
            LogLevel::Warn,
        )))
        .unwrap();
    runtime.root().logger().unwrap().info("info-only");
    assert_eq!(loose.lock().unwrap().len(), 1);
    assert_eq!(strict.lock().unwrap().len(), 0);
    runtime.root().logger().unwrap().warn("both");
    assert_eq!(loose.lock().unwrap().len(), 2);
    assert_eq!(strict.lock().unwrap().len(), 1);
    child.dispose_wait().await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn exporter_receives_records_and_unregisters_on_effect_dispose() {
    let runtime = Runtime::new().unwrap();
    let records = Arc::new(Mutex::new(Vec::new()));
    let effect = runtime.root().effect().unwrap();
    effect
        .register_log_exporter(Arc::new(CaptureExporter::all(records.clone())))
        .unwrap();
    runtime.root().logger().unwrap().info("hello");
    assert_eq!(records.lock().unwrap().len(), 1);
    effect.dispose_wait().await.unwrap();
    runtime.root().logger().unwrap().info("after");
    assert_eq!(records.lock().unwrap().len(), 1);
    assert_eq!(runtime.logger_service().exporter_count(), 0);
    runtime.shutdown().await.unwrap();
}

struct LoggingPlugin {
    records: Arc<Mutex<Vec<LogRecord>>>,
}

#[async_trait]
impl Plugin for LoggingPlugin {
    fn key(&self) -> PluginKey {
        PluginKey::new("demo.logger.plugin")
    }

    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        ctx.register_log_exporter(Arc::new(CaptureExporter::all(self.records.clone())))?;
        ctx.logger()?.info("from-apply");
        let effect = ctx.effect()?;
        effect.logger()?.warn("from-effect");
        Ok(())
    }
}

#[tokio::test]
async fn plugin_apply_and_effect_attach_fiber_and_plugin_key() {
    let runtime = Runtime::new().unwrap();
    let records = Arc::new(Mutex::new(Vec::new()));
    let mut fiber = runtime
        .root()
        .plugin(Arc::new(LoggingPlugin {
            records: records.clone(),
        }))
        .await
        .unwrap();
    let fiber_id = fiber.id();
    let captured = records.lock().unwrap().clone();
    assert!(captured.len() >= 2);
    for record in &captured {
        assert_eq!(record.fiber_id, Some(fiber_id));
        assert_eq!(
            record.plugin_key,
            Some(PluginKey::new("demo.logger.plugin"))
        );
        assert_eq!(record.target, "demo.logger.plugin");
    }
    fiber.dispose_wait().await.unwrap();
    assert_eq!(runtime.logger_service().exporter_count(), 0);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn logger_config_sets_default_level_without_filtering() {
    let runtime = Runtime::new().unwrap();
    let records = Arc::new(Mutex::new(Vec::new()));
    let view = runtime
        .root()
        .intercept(
            LOGGER_CONFIG,
            LoggerConfig {
                name: Some("custom".into()),
                level: Some(LogLevel::Warn),
            },
        )
        .unwrap();
    view.register_log_exporter(Arc::new(CaptureExporter::all(records.clone())))
        .unwrap();
    let logger = view.logger().unwrap();
    assert_eq!(logger.target(), "custom");
    assert_eq!(logger.default_level(), LogLevel::Warn);
    logger.info("still-buffered");
    logger.warn("kept");
    let captured = records.lock().unwrap().clone();
    assert_eq!(captured.len(), 2);
    assert_eq!(captured[0].message, "still-buffered");
    assert_eq!(captured[0].default_level, LogLevel::Warn);
    assert_eq!(captured[1].message, "kept");
    assert_eq!(captured[1].target, "custom");
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn logger_fails_on_disposed_context_and_config_type_mismatch() {
    let runtime = Runtime::new().unwrap();
    let effect = runtime.root().effect().unwrap();
    effect.dispose_wait().await.unwrap();
    assert!(matches!(effect.logger(), Err(CoreError::ContextDisposed)));

    #[derive(Debug)]
    struct Other;
    static LOGGER_AS_OTHER: ConfigKey<Other> = ConfigKey::new("cordis.logger.config@1");
    let wrong = runtime.root().intercept(LOGGER_AS_OTHER, Other).unwrap();
    assert!(matches!(
        wrong.logger(),
        Err(CoreError::ConfigTypeMismatch { .. })
    ));
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn multiple_exporters_receive_independently() {
    let runtime = Runtime::new().unwrap();
    let a = Arc::new(Mutex::new(Vec::new()));
    let b = Arc::new(Mutex::new(Vec::new()));
    runtime
        .root()
        .register_log_exporter(Arc::new(CaptureExporter::all(a.clone())))
        .unwrap();
    let child = runtime.root().effect().unwrap();
    child
        .register_log_exporter(Arc::new(CaptureExporter::all(b.clone())))
        .unwrap();
    runtime.root().logger().unwrap().error("both");
    assert_eq!(a.lock().unwrap().len(), 1);
    assert_eq!(b.lock().unwrap().len(), 1);
    child.dispose_wait().await.unwrap();
    runtime.root().logger().unwrap().error("only-a");
    assert_eq!(a.lock().unwrap().len(), 2);
    assert_eq!(b.lock().unwrap().len(), 1);
    runtime.shutdown().await.unwrap();
}
