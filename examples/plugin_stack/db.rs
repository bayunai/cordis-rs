//! DB 插件：内存库，模拟后续可换成 SQLite / PostgreSQL 的存储面。

use async_trait::async_trait;
use cordis_core::{Context, CoreError, Plugin, PluginKey};
use std::sync::{Arc, Mutex};

use crate::keys::{DB, KEY_DB};

/// 对外契约：日志落库 / 查询。换 SQLite/PG 时尽量保持这些方法稳定。
#[derive(Clone, Debug)]
pub struct Db {
    lines: Arc<Mutex<Vec<String>>>,
}

impl Db {
    pub fn new() -> Self {
        Self {
            lines: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn insert_log(&self, line: impl Into<String>) {
        let line = line.into();
        if let Ok(mut guard) = self.lines.lock() {
            guard.push(line);
            if guard.len() > 500 {
                let overflow = guard.len() - 500;
                guard.drain(0..overflow);
            }
        }
    }

    pub fn recent(&self, limit: usize) -> Vec<String> {
        self.lines
            .lock()
            .map(|guard| {
                let start = guard.len().saturating_sub(limit);
                guard[start..].to_vec()
            })
            .unwrap_or_default()
    }

    pub fn clear(&self) {
        if let Ok(mut guard) = self.lines.lock() {
            guard.clear();
        }
    }
}

pub struct DbPlugin;

#[async_trait]
impl Plugin for DbPlugin {
    fn key(&self) -> PluginKey {
        KEY_DB
    }

    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        let logger = ctx.logger()?;
        logger.info("sys: db apply (memory)");
        ctx.provide(DB, Db::new())?;
        let logger_d = logger.clone();
        ctx.effect()?.on_dispose(move || {
            logger_d.info("sys: db disposed");
        });
        Ok(())
    }
}
