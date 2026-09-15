//! 日志插件：对外 `Logger::info`，对内依赖 DB，后台异步落库（不堵调用方）。

use async_trait::async_trait;
use cordis_core::{Context, CoreError, Plugin, PluginKey, ServiceId};
use tokio::sync::mpsc;

use crate::db::LogTx;
use crate::keys::{DB, KEY_LOGGER, LOGGER};

fn bus(tx: &LogTx, msg: impl Into<String>) {
    let _ = tx.send(msg.into());
}

/// 对外契约：业务只调用这些方法，不关心 SQLite / PG。
#[derive(Clone, Debug)]
pub struct Logger {
    tx: mpsc::Sender<String>,
}

impl Logger {
    pub fn info(&self, msg: impl Into<String>) {
        let line = msg.into();
        let _ = self.tx.try_send(line);
    }
}

pub struct LoggerPlugin {
    pub bus: LogTx,
}

#[async_trait]
impl Plugin for LoggerPlugin {
    fn key(&self) -> PluginKey {
        KEY_LOGGER
    }

    fn inject(&self) -> Vec<ServiceId> {
        vec![DB.id()]
    }

    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        let db = ctx.get(DB)?.clone();
        let (tx, mut rx) = mpsc::channel::<String>(256);
        ctx.provide(LOGGER, Logger { tx })?;
        bus(&self.bus, "sys: logger apply");

        let bus_tx = self.bus.clone();
        let effect = ctx.effect()?;
        effect.spawn(move |cancel| async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    maybe = rx.recv() => {
                        let Some(line) = maybe else { break };
                        db.insert_log(line.clone());
                        bus(&bus_tx, format!("log: {line}"));
                    }
                }
            }
            while let Ok(line) = rx.try_recv() {
                db.insert_log(line.clone());
                bus(&bus_tx, format!("log: {line}"));
            }
        })?;

        let bus_d = self.bus.clone();
        effect.on_dispose(move || {
            bus(&bus_d, "sys: logger disposed");
        });
        Ok(())
    }
}
