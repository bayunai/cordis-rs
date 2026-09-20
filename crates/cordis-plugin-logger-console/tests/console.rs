//! ConsoleExporter 渲染与挂载回归。

use cordis_core::{LogLevel, LogRecord, PluginKey, Runtime};
use cordis_plugin_logger_console::{
    ColorMode, ConsoleExporter, ConsoleLoggerConfig, ConsoleLoggerPlugin, LabelAlign,
};
use std::{collections::BTreeMap, sync::Arc, time::SystemTime};

fn record(level: LogLevel, target: &str, message: &str, default_level: LogLevel) -> LogRecord {
    LogRecord {
        seq: 1,
        timestamp: SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000),
        level,
        target: target.into(),
        message: message.into(),
        fiber_id: Some(7),
        plugin_key: Some(PluginKey::new("demo.app")),
        default_level,
    }
}

fn config(
    levels: BTreeMap<String, LogLevel>,
    label_width: usize,
    max_length: Option<usize>,
) -> ConsoleLoggerConfig {
    ConsoleLoggerConfig {
        colors: ColorMode::Never,
        levels,
        show_time: false,
        show_diff: false,
        max_length,
        label_width,
        label_margin: 1,
        label_align: LabelAlign::Right,
    }
}

#[test]
fn render_filters_by_target_default_and_record_fallback() {
    let mut levels = BTreeMap::new();
    levels.insert("app".into(), LogLevel::Debug);
    levels.insert("default".into(), LogLevel::Warn);
    let exporter = ConsoleExporter::new(config(levels, 8, None));

    // target-specific: Debug allowed for "app"
    let app_debug = exporter.render(&record(LogLevel::Debug, "app", "a", LogLevel::Error));
    assert!(app_debug.contains("[D]"));
    assert!(app_debug.contains("app"));
    assert!(app_debug.contains("a"));

    // other target uses "default" → Warn
    assert!(
        exporter
            .render(&record(LogLevel::Info, "other", "nope", LogLevel::Debug))
            .is_empty()
    );
    let other_warn = exporter.render(&record(LogLevel::Warn, "other", "yes", LogLevel::Debug));
    assert!(other_warn.contains("[W]"));
    assert!(other_warn.contains("yes"));

    // empty map → record.default_level
    let fallback = ConsoleExporter::new(config(BTreeMap::new(), 8, None));
    assert!(
        fallback
            .render(&record(LogLevel::Debug, "x", "no", LogLevel::Info))
            .is_empty()
    );
    let ok = fallback.render(&record(LogLevel::Info, "x", "ok", LogLevel::Info));
    assert!(ok.contains("[I]"));
    assert!(ok.contains("ok"));
    assert!(!ok.contains('\u{001b}'));
}

#[test]
fn render_zero_width_label_and_level_prefix() {
    let exporter = ConsoleExporter::new(config(BTreeMap::new(), 0, None));
    let line = exporter.render(&record(LogLevel::Error, "verylong", "msg", LogLevel::Debug));
    assert!(line.starts_with("[E] "));
    assert!(!line.contains('…'));
    assert!(!line.contains("verylong"));
    assert!(line.contains("msg"));
}

#[test]
fn render_truncates_and_indents_multiline() {
    let exporter = ConsoleExporter::new(ConsoleLoggerConfig {
        colors: ColorMode::Never,
        levels: BTreeMap::new(),
        show_time: false,
        show_diff: false,
        max_length: Some(5),
        label_width: 4,
        label_margin: 1,
        label_align: LabelAlign::Left,
    });
    let line = exporter.render(&record(
        LogLevel::Info,
        "svc",
        "abcdef\nsecond",
        LogLevel::Debug,
    ));
    let lines: Vec<_> = line.lines().collect();
    assert_eq!(lines.len(), 2);
    assert!(lines[0].contains("[I]"));
    assert!(lines[0].contains("abcd…"));
    // "[I] "(4) + label(4) + margin(1) = 9
    assert!(lines[1].starts_with("         "));
    assert!(lines[1].trim_start().starts_with("seco…") || lines[1].contains("seco…"));
}

#[test]
fn render_show_diff_placeholder() {
    let exporter = ConsoleExporter::new(ConsoleLoggerConfig {
        colors: ColorMode::Never,
        levels: BTreeMap::new(),
        show_time: false,
        show_diff: true,
        max_length: None,
        label_width: 3,
        label_margin: 1,
        label_align: LabelAlign::Right,
    });
    let with_diff = exporter.render_with_diff(
        &record(LogLevel::Info, "x", "m", LogLevel::Debug),
        Some(std::time::Duration::from_millis(42)),
    );
    assert!(with_diff.contains("+42ms"));
    assert!(with_diff.contains("[I]"));
}

#[tokio::test]
async fn plugin_mount_and_unmount_clears_exporter() {
    let runtime = Runtime::new().unwrap();
    assert_eq!(runtime.logger_service().exporter_count(), 0);
    let mut fiber = runtime
        .root()
        .plugin(Arc::new(ConsoleLoggerPlugin::new(ConsoleLoggerConfig {
            colors: ColorMode::Never,
            levels: BTreeMap::from([("default".into(), LogLevel::Debug)]),
            show_time: false,
            show_diff: false,
            max_length: None,
            label_width: 8,
            label_margin: 1,
            label_align: LabelAlign::Right,
        })))
        .await
        .unwrap();
    assert_eq!(runtime.logger_service().exporter_count(), 1);
    runtime.root().logger().unwrap().info("console-ok");
    fiber.dispose_wait().await.unwrap();
    assert_eq!(runtime.logger_service().exporter_count(), 0);
    runtime.shutdown().await.unwrap();
}
