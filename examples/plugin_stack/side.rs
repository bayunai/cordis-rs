//! 根级旁路条目：用于观察 `app` 子树 reconcile 时 Fiber ID 是否保持。

use async_trait::async_trait;
use cordis_core::{Context, CoreError, Plugin, PluginKey};

use crate::db::LogTx;
use crate::keys::KEY_SIDE;

fn bus(tx: &LogTx, msg: impl Into<String>) {
    let _ = tx.send(msg.into());
}

pub struct SidePlugin {
    pub bus: LogTx,
}

#[async_trait]
impl Plugin for SidePlugin {
    fn key(&self) -> PluginKey {
        KEY_SIDE
    }

    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        bus(&self.bus, "sys: side apply (root sibling)");
        let bus_tx = self.bus.clone();
        ctx.effect()?.on_dispose(move || {
            bus(&bus_tx, "sys: side disposed");
        });
        Ok(())
    }
}
