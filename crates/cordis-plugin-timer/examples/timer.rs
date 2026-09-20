//! Host 显式挂载 TimerPlugin，业务插件通过 `inject([TIMER.id()])` 使用 interval/timeout。
//!
//! ```bash
//! cargo run -p cordis-plugin-timer --example timer
//! ```

use async_trait::async_trait;
use cordis_core::{Context, CoreError, Plugin, PluginKey, ServiceId};
use cordis_host::CordisHost;
use cordis_plugin_logger_console::ConsoleLoggerPlugin;
use cordis_plugin_timer::{TIMER, TimerExt, TimerPlugin};
use std::{sync::Arc, time::Duration};

struct HeartbeatPlugin;

#[async_trait]
impl Plugin for HeartbeatPlugin {
    fn key(&self) -> PluginKey {
        PluginKey::new("example.heartbeat")
    }

    fn inject(&self) -> Vec<ServiceId> {
        vec![TIMER.id()]
    }

    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        let effect = ctx.effect_named("heartbeat")?;
        let logger = effect.logger()?;
        let tick_logger = logger.clone();
        let handle = effect
            .interval(
                move || {
                    tick_logger.info("heartbeat tick");
                },
                Duration::from_millis(50),
            )
            .map_err(|err| CoreError::PluginApply(err.to_string()))?;
        effect.on_dispose(move || {
            drop(handle);
        });

        let timeout_logger = logger.clone();
        let timeout = effect
            .timeout(
                move || {
                    timeout_logger.info("timeout fired once");
                },
                Duration::from_millis(120),
            )
            .map_err(|err| CoreError::PluginApply(err.to_string()))?;
        effect.on_dispose(move || {
            drop(timeout);
        });
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host = CordisHost::new()?;
    let _console = host
        .root()
        .plugin(Arc::new(ConsoleLoggerPlugin::default()))
        .await?;
    let _timer = host.root().plugin(Arc::new(TimerPlugin)).await?;
    let _heartbeat = host.root().plugin(Arc::new(HeartbeatPlugin)).await?;

    tokio::time::sleep(Duration::from_millis(200)).await;
    host.shutdown().await?;
    Ok(())
}
