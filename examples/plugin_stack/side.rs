//! 根级旁路条目：用于观察 `app` 子树 reconcile 时 Fiber ID 是否保持。

use async_trait::async_trait;
use cordis_core::{Context, CoreError, Plugin};

pub struct SidePlugin;

#[async_trait]
impl Plugin for SidePlugin {
    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        let logger = ctx.logger()?;
        logger.info("sys: side apply (root sibling)");
        let logger_d = logger.clone();
        ctx.effect()?.on_dispose(move || {
            logger_d.info("sys: side disposed");
        });
        Ok(())
    }
}
