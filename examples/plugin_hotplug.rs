//! 插件插拔 Demo：多个独立 PluginKey，演示挂载、依赖激活、热替换与统一卸载。
//!
//! ```bash
//! cargo run -p cordis-core --example plugin_hotplug
//! ```

use async_trait::async_trait;
use cordis_core::{
    Context, CoreError, EventKey, FiberState, Plugin, PluginKey, Runtime, ServiceKey,
};
use cordis_plugin_logger_console::ConsoleLoggerPlugin;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

// --- Services / Events -------------------------------------------------------

#[derive(Debug)]
struct NoteService;

#[derive(Debug, Clone)]
struct Greeting(String);

#[derive(Debug, Clone)]
struct Counter;

static NOTE_SERVICE: ServiceKey<NoteService> = ServiceKey::new("demo.note@1");
static GREETING: ServiceKey<Greeting> = ServiceKey::new("demo.greeting@1");
static COUNTER: ServiceKey<Counter> = ServiceKey::new("demo.counter@1");
static NOTE: EventKey<String> = EventKey::new("demo.note@1");

static KEY_NOTE: PluginKey = PluginKey::new("demo.note");
static KEY_GREETER: PluginKey = PluginKey::new("demo.greeter");
static KEY_COUNTER: PluginKey = PluginKey::new("demo.counter");

// --- Plugins -----------------------------------------------------------------

/// 基础能力：提供 NoteService，并订阅 NOTE 事件。
struct NotePlugin {
    label: &'static str,
}

#[async_trait]
impl Plugin for NotePlugin {
    fn key(&self) -> PluginKey {
        KEY_NOTE
    }

    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        let logger = ctx.logger()?;
        logger.info(format!("[{}] note apply", self.label));
        ctx.provide(NOTE_SERVICE, NoteService)?;
        let label = self.label;
        let logger_on = logger.clone();
        ctx.on(NOTE, move |msg| {
            logger_on.info(format!("[{label}] note: {msg}"));
            Ok(())
        })?;
        let effect = ctx.effect()?;
        let logger_d = logger.clone();
        effect.on_dispose(move || logger_d.info(format!("[{label}] note disposed")));
        Ok(())
    }
}

/// 问候语插件：提供 Greeting，并向 NOTE 发一条上线消息。
struct GreeterPlugin {
    name: String,
}

#[async_trait]
impl Plugin for GreeterPlugin {
    fn key(&self) -> PluginKey {
        KEY_GREETER
    }

    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        let logger = ctx.logger()?;
        let text = format!("hello from {}", self.name);
        logger.info(format!("[greeter] apply → {text}"));
        ctx.provide(GREETING, Greeting(text.clone()))?;
        ctx.emit(NOTE, &format!("greeter online: {}", self.name))?;
        let name = self.name.clone();
        let effect = ctx.effect()?;
        let logger_d = logger.clone();
        effect.on_dispose(move || logger_d.info(format!("[greeter:{name}] disposed")));
        Ok(())
    }
}

/// 计数器：依赖 NoteService + Greeting，后台 tick，提供 Counter。
struct CounterPlugin {
    ticks: Arc<AtomicU64>,
}

#[async_trait]
impl Plugin for CounterPlugin {
    fn key(&self) -> PluginKey {
        KEY_COUNTER
    }

    fn inject(&self) -> Vec<cordis_core::ServiceId> {
        vec![NOTE_SERVICE.id(), GREETING.id()]
    }

    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        let _note = ctx.get(NOTE_SERVICE)?;
        let greeting = ctx.get(GREETING)?;
        let logger = ctx.logger()?;
        logger.info(format!("[counter] apply (seen greeting: {})", greeting.0));

        let ticks = self.ticks.clone();
        ctx.provide(COUNTER, Counter)?;

        let effect = ctx.effect()?;
        let ticks_bg = ticks.clone();
        let logger_bg = logger.clone();
        effect.spawn(move |cancel| async move {
            let mut n = 0u64;
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        logger_bg.info(format!("[counter] tick loop cancelled at {n}"));
                        break;
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_millis(80)) => {
                        n += 1;
                        ticks_bg.store(n, Ordering::SeqCst);
                        logger_bg.info(format!("[counter] tick {n}"));
                        if n >= 3 {
                            // 演示：有限次后自停等待取消（真实长等须始终响应 cancel）
                            cancel.cancelled().await;
                            break;
                        }
                    }
                }
            }
        })?;
        let logger_d = logger.clone();
        effect.on_dispose(move || logger_d.info("[counter] disposed"));
        Ok(())
    }
}

// --- Demo --------------------------------------------------------------------

fn log_registry(runtime: &Runtime, title: &str) {
    let logger = runtime.root().logger().expect("logger");
    logger.info(format!("--- {title} ---"));
    for group in &runtime.diagnostics().plugin_registry {
        let states: Vec<_> = group
            .fibers
            .iter()
            .map(|f| format!("{}:{:?}", f.id, f.state))
            .collect();
        logger.info(format!("  {} → [{}]", group.plugin_key, states.join(", ")));
    }
    if runtime.diagnostics().plugin_registry.is_empty() {
        logger.info("  (empty)");
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
    let ticks = Arc::new(AtomicU64::new(0));

    // 1) 先挂 Note；Counter 依赖未齐，保持 Pending。
    let mut note = root.plugin(Arc::new(NotePlugin { label: "v1" })).await?;
    logger.info(format!("note state: {:?}", note.state()));

    let mut counter = root
        .plugin(Arc::new(CounterPlugin {
            ticks: ticks.clone(),
        }))
        .await?;
    runtime.settle().await;
    logger.info(format!(
        "counter before greeter: {:?} missing={:?}",
        counter.state(),
        counter.missing_dependencies()
    ));
    log_registry(&runtime, "after note+counter");

    // 2) 挂 Greeter → Counter 依赖齐，应变为 Active。
    let mut greeter = root
        .plugin(Arc::new(GreeterPlugin {
            name: "Alice".into(),
        }))
        .await?;
    runtime.settle().await;
    logger.info(format!("counter after greeter: {:?}", counter.state()));
    assert_eq!(counter.state(), FiberState::Active);
    log_registry(&runtime, "all three mounted");

    root.emit(NOTE, &"manual ping".into())?;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // 3) 同 Key 热替换 Note（v1 → v2）。
    note.replace(Arc::new(NotePlugin { label: "v2" })).await?;
    runtime.settle().await;
    root.emit(NOTE, &"after note replace".into())?;
    logger.info(format!(
        "ticks so far: {}; counter fiber: {:?}",
        ticks.load(Ordering::SeqCst),
        counter.state()
    ));
    log_registry(&runtime, "note replaced");

    // 4) 卸载 Greeter：Counter 失去 Greeting，应变回 Pending。
    greeter.dispose_wait().await?;
    runtime.settle().await;
    logger.info(format!(
        "counter after greeter unplug: {:?} missing={:?}",
        counter.state(),
        counter.missing_dependencies()
    ));
    log_registry(&runtime, "greeter unplugged");

    // 5) 再插一版 Greeter，Counter 重新 Active。
    greeter = root
        .plugin(Arc::new(GreeterPlugin { name: "Bob".into() }))
        .await?;
    runtime.settle().await;
    logger.info(format!(
        "counter after greeter replug: {:?}",
        counter.state()
    ));
    assert_eq!(counter.state(), FiberState::Active);

    // 6) Runtime 级按 Key 卸载 Counter（与 dispose_wait 等价收口）。
    let n = runtime.unmount(KEY_COUNTER).await?;
    logger.info(format!("unmount counter fibers: {n}"));
    log_registry(&runtime, "counter unmounted");

    // 7) 收尾：卸掉剩余插件并 shutdown。
    greeter.dispose_wait().await?;
    note.dispose_wait().await?;
    let _ = counter.dispose_wait().await;
    log_registry(&runtime, "before shutdown");
    runtime.shutdown().await?;
    logger.info(format!(
        "done. total ticks seen: {}",
        ticks.load(Ordering::SeqCst)
    ));
    Ok(())
}
