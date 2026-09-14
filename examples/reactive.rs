//! 最小 Plugin + Service + inject + Event 示例。

use async_trait::async_trait;
use cordis_core::{Context, CoreError, EventKey, Plugin, Runtime, ServiceKey};
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
    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        ctx.provide(CLOCK, Clock)?;
        ctx.on(TICK, |tick| {
            println!("tick {}", tick.0);
            Ok(())
        })?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = Runtime::new()?;
    let root = runtime.root();

    let handle = root.inject([CLOCK.id()], |services, effect| async move {
        let _clock = services.get(CLOCK)?;
        effect.on_dispose(|| println!("clock consumer disposed"));
        println!("clock consumer active");
        Ok(())
    })?;

    println!("before plugin: {:?}", handle.state());
    let mut plugin = root.plugin(Arc::new(ClockPlugin)).await?;
    runtime.settle().await;
    println!("after plugin: {:?}", handle.state());

    root.emit(TICK, &Tick(1))?;
    plugin.dispose();
    runtime.settle().await;
    runtime.shutdown().await;
    Ok(())
}
