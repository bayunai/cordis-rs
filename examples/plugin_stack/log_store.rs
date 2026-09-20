//! Log Store：依赖 DB，将 Runtime Logger 记录异步落库（不提供 ServiceKey）。
//!
//! **队列契约**：exporter 侧 `try_send`（容量 256）。队列满或已关闭时丢弃该条并累加
//! 原子计数；后台任务在写入间隙把累计丢弃数聚合成一条 WARN 诊断写入 DB
//! （不经 Runtime Logger，避免 exporter 重入再次占满队列）。

use async_trait::async_trait;
use cordis_core::{
    Context, CoreError, LogExporter, LogLevel, LogRecord, Plugin, PluginKey, ServiceId,
};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::mpsc;

use crate::keys::{DB, KEY_LOG_STORE};

struct DbLogExporter {
    tx: mpsc::Sender<String>,
    dropped: Arc<AtomicU64>,
}

impl LogExporter for DbLogExporter {
    fn export(&self, record: &LogRecord) {
        let line = format_record(record);
        match self.tx.try_send(line) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) | Err(mpsc::error::TrySendError::Closed(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

fn format_record(record: &LogRecord) -> String {
    format!(
        "{} {} {}",
        level_tag(record.level),
        record.target,
        record.message
    )
}

fn level_tag(level: LogLevel) -> &'static str {
    match level {
        LogLevel::Debug => "DEBUG",
        LogLevel::Info => "INFO",
        LogLevel::Warn => "WARN",
        LogLevel::Error => "ERROR",
    }
}

fn flush_drop_diagnostics(db: &crate::db::Db, dropped: &AtomicU64) {
    let n = dropped.swap(0, Ordering::Relaxed);
    if n > 0 {
        db.insert_log(format!(
            "WARN log-store dropped {n} record(s) (queue full or closed)"
        ));
    }
}

pub struct LogStorePlugin;

#[async_trait]
impl Plugin for LogStorePlugin {
    fn key(&self) -> PluginKey {
        KEY_LOG_STORE
    }

    fn inject(&self) -> Vec<ServiceId> {
        vec![DB.id()]
    }

    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        let db = ctx.get(DB)?.clone();
        let logger = ctx.logger()?;
        let (tx, mut rx) = mpsc::channel::<String>(256);
        let dropped = Arc::new(AtomicU64::new(0));
        ctx.register_log_exporter(Arc::new(DbLogExporter {
            tx,
            dropped: dropped.clone(),
        }))?;
        logger.info("sys: log-store apply");

        let effect = ctx.effect()?;
        let dropped_bg = dropped.clone();
        effect.spawn(move |cancel| async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    maybe = rx.recv() => {
                        let Some(line) = maybe else { break };
                        db.insert_log(line);
                        flush_drop_diagnostics(&db, &dropped_bg);
                    }
                }
            }
            while let Ok(line) = rx.try_recv() {
                db.insert_log(line);
            }
            flush_drop_diagnostics(&db, &dropped_bg);
        })?;

        let logger_d = logger.clone();
        effect.on_dispose(move || {
            logger_d.info("sys: log-store disposed");
        });
        Ok(())
    }
}

#[cfg(test)]
mod queue_tests {
    use super::*;
    use std::time::SystemTime;

    fn sample(message: &str) -> LogRecord {
        LogRecord {
            seq: 1,
            timestamp: SystemTime::UNIX_EPOCH,
            level: LogLevel::Info,
            target: "test".into(),
            message: message.into(),
            fiber_id: None,
            plugin_key: None,
            default_level: LogLevel::Info,
        }
    }

    #[test]
    fn full_queue_increments_dropped_counter() {
        let (tx, _rx) = mpsc::channel(1);
        let dropped = Arc::new(AtomicU64::new(0));
        let exporter = DbLogExporter {
            tx,
            dropped: dropped.clone(),
        };
        exporter.export(&sample("first"));
        exporter.export(&sample("second"));
        exporter.export(&sample("third"));
        assert!(
            dropped.load(Ordering::Relaxed) >= 1,
            "expected drops when capacity=1 and receiver idle"
        );
    }
}
