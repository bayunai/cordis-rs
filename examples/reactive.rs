//! 最小 Plugin + Service + inject + Event 示例。

use async_trait::async_trait;
use cordis_core::{Context, CoreError, EventKey, Plugin, Runtime, ServiceKey};
use cordis_plugin_logger_console::ConsoleLoggerPlugin;
use std::sync::Arc;

#[derive(Debug)]
struct Clock;

#[derive(Debug, Clone)]
struct Tick(u64);

static CLOCK: ServiceKey<Clock> = ServiceKey::new("example.clock@1");
static TICK: EventKey<Tick> = EventKey::new("example.tick@1");

struct ClockPlugin;

#[async_trait]
impl Plugin for ClockPlugin {
    fn key(&self) -> cordis_core::PluginKey {
        cordis_core::PluginKey::new("example.clock")
    }
    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        ctx.provide(CLOCK, Clock)?;
        let logger = ctx.logger()?;
        ctx.on(TICK, move |tick| {
            logger.info(format!("tick {}", tick.0));
            Ok(())
        })?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = Runtime::new()?;
    let root = runtime.root();
    let _console = root
        .plugin(Arc::new(ConsoleLoggerPlugin::default()))
        .await?;
    let logger = root.logger()?;

    let handle = root.inject([CLOCK.id()], |services, effect| async move {
        let _clock = services.get(CLOCK)?;
        let logger = effect.logger()?;
        let logger_d = logger.clone();
        effect.on_dispose(move || logger_d.info("clock consumer disposed"));
        logger.info("clock consumer active");
        Ok(())
    })?;

    logger.info(format!("before plugin: {:?}", handle.state()));
    let mut plugin = root.plugin(Arc::new(ClockPlugin)).await?;
    runtime.settle().await;
    logger.info(format!("after plugin: {:?}", handle.state()));

    root.emit(TICK, &Tick(1))?;
    plugin.dispose_wait().await.expect("dispose_wait");
    runtime.settle().await;
    runtime.shutdown().await.expect("shutdown");
    Ok(())
}
