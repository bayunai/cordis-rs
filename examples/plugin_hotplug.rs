//! 插件插拔 Demo：多个独立 PluginKey，演示挂载、依赖激活、热替换与统一卸载。
//!
//! ```bash
//! cargo run -p cordis-core --example plugin_hotplug
//! ```

use async_trait::async_trait;
use cordis_core::{
    Context, CoreError, EventKey, FiberState, Plugin, PluginKey, Runtime, ServiceKey,
};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

// --- Services / Events -------------------------------------------------------

#[derive(Debug)]
struct Logger;

#[derive(Debug, Clone)]
struct Greeting(String);

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct Counter(u64);

static LOGGER: ServiceKey<Logger> = ServiceKey::new("demo.logger@1");
static GREETING: ServiceKey<Greeting> = ServiceKey::new("demo.greeting@1");
static COUNTER: ServiceKey<Counter> = ServiceKey::new("demo.counter@1");
static NOTE: EventKey<String> = EventKey::new("demo.note@1");

static KEY_LOGGER: PluginKey = PluginKey::new("demo.logger");
static KEY_GREETER: PluginKey = PluginKey::new("demo.greeter");
static KEY_COUNTER: PluginKey = PluginKey::new("demo.counter");

// --- Plugins -----------------------------------------------------------------

/// 基础能力：提供 Logger，并订阅 NOTE 事件。
struct LoggerPlugin {
    label: &'static str,
}

#[async_trait]
impl Plugin for LoggerPlugin {
    fn key(&self) -> PluginKey {
        KEY_LOGGER
    }

    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        println!("[{}] logger apply", self.label);
        ctx.provide(LOGGER, Logger)?;
        let label = self.label;
        ctx.on(NOTE, move |msg| {
            println!("[{label}] note: {msg}");
            Ok(())
        })?;
        let effect = ctx.effect()?;
        effect.on_dispose(move || println!("[{label}] logger disposed"));
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
        let text = format!("hello from {}", self.name);
        println!("[greeter] apply → {text}");
        ctx.provide(GREETING, Greeting(text.clone()))?;
        ctx.emit(NOTE, &format!("greeter online: {}", self.name))?;
        let name = self.name.clone();
        let effect = ctx.effect()?;
        effect.on_dispose(move || println!("[greeter:{name}] disposed"));
        Ok(())
    }
}

/// 计数器：依赖 Logger + Greeting，后台 tick，提供 Counter。
struct CounterPlugin {
    ticks: Arc<AtomicU64>,
}

#[async_trait]
impl Plugin for CounterPlugin {
    fn key(&self) -> PluginKey {
        KEY_COUNTER
    }

    fn inject(&self) -> Vec<cordis_core::ServiceId> {
        vec![LOGGER.id(), GREETING.id()]
    }

    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        let _logger = ctx.get(LOGGER)?;
        let greeting = ctx.get(GREETING)?;
        println!("[counter] apply (seen greeting: {})", greeting.0);

        let ticks = self.ticks.clone();
        ctx.provide(COUNTER, Counter(ticks.load(Ordering::SeqCst)))?;

        let effect = ctx.effect()?;
        let ticks_bg = ticks.clone();
        effect.spawn(move |cancel| async move {
            let mut n = 0u64;
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        println!("[counter] tick loop cancelled at {n}");
                        break;
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_millis(80)) => {
                        n += 1;
                        ticks_bg.store(n, Ordering::SeqCst);
                        println!("[counter] tick {n}");
                        if n >= 3 {
                            // 演示：有限次后自停等待取消（真实长等须始终响应 cancel）
                            cancel.cancelled().await;
                            break;
                        }
                    }
                }
            }
        })?;
        effect.on_dispose(|| println!("[counter] disposed"));
        Ok(())
    }
}

// --- Demo --------------------------------------------------------------------

fn print_registry(runtime: &Runtime, title: &str) {
    println!("--- {title} ---");
    for group in &runtime.diagnostics().plugin_registry {
        let states: Vec<_> = group
            .fibers
            .iter()
            .map(|f| format!("{}:{:?}", f.id, f.state))
            .collect();
        println!("  {} → [{}]", group.plugin_key, states.join(", "));
    }
    if runtime.diagnostics().plugin_registry.is_empty() {
        println!("  (empty)");
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = Runtime::new()?;
    let root = runtime.root();
    let ticks = Arc::new(AtomicU64::new(0));

    // 1) 先挂 Logger；Counter 依赖未齐，保持 Pending。
    let mut logger = root
        .plugin(Arc::new(LoggerPlugin { label: "v1" }))
        .await?;
    println!("logger state: {:?}", logger.state());

    let mut counter = root
        .plugin(Arc::new(CounterPlugin {
            ticks: ticks.clone(),
        }))
        .await?;
    runtime.settle().await;
    println!(
        "counter before greeter: {:?} missing={:?}",
        counter.state(),
        counter.missing_dependencies()
    );
    print_registry(&runtime, "after logger+counter");

    // 2) 挂 Greeter → Counter 依赖齐，应变为 Active。
    let mut greeter = root
        .plugin(Arc::new(GreeterPlugin {
            name: "Alice".into(),
        }))
        .await?;
    runtime.settle().await;
    println!("counter after greeter: {:?}", counter.state());
    assert_eq!(counter.state(), FiberState::Active);
    print_registry(&runtime, "all three mounted");

    root.emit(NOTE, &"manual ping".into())?;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // 3) 同 Key 热替换 Logger（v1 → v2）。
    //    Logger 短暂撤销会使依赖它的 Counter 进入 Unloading→Pending，再随 Logger 恢复而重激活。
    logger
        .replace(Arc::new(LoggerPlugin { label: "v2" }))
        .await?;
    runtime.settle().await;
    root.emit(NOTE, &"after logger replace".into())?;
    println!(
        "ticks so far: {}; counter fiber: {:?}",
        ticks.load(Ordering::SeqCst),
        counter.state()
    );
    print_registry(&runtime, "logger replaced");

    // 4) 卸载 Greeter：Counter 失去 Greeting，应变回 Pending。
    greeter.dispose_wait().await?;
    runtime.settle().await;
    println!(
        "counter after greeter unplug: {:?} missing={:?}",
        counter.state(),
        counter.missing_dependencies()
    );
    print_registry(&runtime, "greeter unplugged");

    // 5) 再插一版 Greeter，Counter 重新 Active。
    greeter = root
        .plugin(Arc::new(GreeterPlugin {
            name: "Bob".into(),
        }))
        .await?;
    runtime.settle().await;
    println!("counter after greeter replug: {:?}", counter.state());
    assert_eq!(counter.state(), FiberState::Active);

    // 6) Runtime 级按 Key 卸载 Counter（与 dispose_wait 等价收口）。
    let n = runtime.unmount(KEY_COUNTER).await?;
    println!("unmount counter fibers: {n}");
    print_registry(&runtime, "counter unmounted");

    // 7) 收尾：卸掉剩余插件并 shutdown。
    greeter.dispose_wait().await?;
    logger.dispose_wait().await?;
    // counter Fiber 已在 unmount 中释放；句柄可能已 Disposed。
    let _ = counter.dispose_wait().await;
    print_registry(&runtime, "before shutdown");
    runtime.shutdown().await?;
    println!("done. total ticks seen: {}", ticks.load(Ordering::SeqCst));
    Ok(())
}
