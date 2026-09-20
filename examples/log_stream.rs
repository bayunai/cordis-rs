//! 网页 Demo 共享：将 Runtime [`LogRecord`] 桥接到 SSE（仅传输层，非日志事实源）。

use axum::response::sse::Event;
use cordis_core::{LogExporter, LogLevel, LogRecord};
use futures_util::stream::{Stream, unfold};
use std::convert::Infallible;
use tokio::sync::broadcast;

/// 默认 broadcast 容量。
pub const SSE_LOG_CAPACITY: usize = 256;

/// 将 Runtime Logger 记录转发到 `broadcast`，供 EventSource 消费。
#[derive(Clone)]
pub struct SseLogExporter {
    tx: broadcast::Sender<String>,
}

impl SseLogExporter {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self { tx }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<String> {
        self.tx.subscribe()
    }

    /// 渲染为 SSE 文本行：`LEVEL target message`。
    pub fn format_line(record: &LogRecord) -> String {
        format!(
            "{} {} {}",
            level_tag(record.level),
            record.target,
            record.message
        )
    }
}

impl Default for SseLogExporter {
    fn default() -> Self {
        Self::new(SSE_LOG_CAPACITY)
    }
}

impl LogExporter for SseLogExporter {
    fn export(&self, record: &LogRecord) {
        let _ = self.tx.send(Self::format_line(record));
    }
}

/// 将 broadcast 接收端转为 Axum SSE 流（连接后增量；不回放缓冲）。
#[allow(dead_code)] // 由 plugin_web / plugin_stack 使用；单测模块不引用。
pub fn sse_stream(
    rx: broadcast::Receiver<String>,
) -> impl Stream<Item = Result<Event, Infallible>> {
    unfold(rx, |mut rx| async move {
        match rx.recv().await {
            Ok(msg) => Some((Ok(Event::default().data(msg)), rx)),
            Err(broadcast::error::RecvError::Closed) => None,
            Err(broadcast::error::RecvError::Lagged(_)) => Some((
                Ok(Event::default().data("(log lagged; skipped some lines)")),
                rx,
            )),
        }
    })
}

fn level_tag(level: LogLevel) -> &'static str {
    match level {
        LogLevel::Debug => "DEBUG",
        LogLevel::Info => "INFO",
        LogLevel::Warn => "WARN",
        LogLevel::Error => "ERROR",
    }
}
