# cordis-core workspace

最小 Cordis 风格 Runtime：**Context / Service / inject / Effect / Plugin / Event / Diagnostics**。

不含 HTTP、数据库、缓存、JSON、扩展包加载或网关概念。宿主应用自行组装这些能力。

## 包结构

| Crate | 用途 |
| --- | --- |
| [`crates/cordis-core`](crates/cordis-core) | 生产可用的 Runtime 内核 |
| [`crates/cordis-testkit`](crates/cordis-testkit) | 测试辅助（`TestPlugin`、`wait_injection`、事件记录） |

文档：

- [架构与边界](docs/architecture.md)
- [扩展编写指南](docs/extension-authoring.md)

## 快速开始

```rust
use async_trait::async_trait;
use cordis_core::{Context, CoreError, Plugin, Runtime, ServiceKey};
use std::sync::Arc;

#[derive(Debug)]
struct Clock;

static CLOCK: ServiceKey<Clock> = ServiceKey::new("example.clock@1");

struct ClockPlugin;

#[async_trait]
impl Plugin for ClockPlugin {
    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        ctx.provide(CLOCK, Clock)?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = Runtime::new()?;
    let root = runtime.root();

    root.inject([CLOCK.id()], |services, effect| async move {
        let _clock = services.get(CLOCK)?;
        effect.on_dispose(|| eprintln!("clock consumer disposed"));
        Ok(())
    })?;

    let mut plugin = root.plugin(Arc::new(ClockPlugin)).await?;
    runtime.settle().await;
    // 热卸载：await 插件任务；或 fiber.replace(新实例)
    plugin.dispose_wait().await;
    runtime.shutdown().await;
    Ok(())
}
```

运行示例：

```bash
cd backend-cordis
cargo run -p cordis-core --example reactive
```

## 语义摘要

- `Runtime::new()` 须在 Tokio 中调用；专用调度器处理 dirty 重算。
- `isolate` / `isolate_with`：按 ServiceKey 共享隔离标签（不可跨 Runtime）。
- `Context::plugin` 返回 `Fiber`（`restart` / `replace` / `dispose_wait`）。
- 具名 Effect + 诊断树（plugin_fibers / inject_fibers / effects）。
- 事件四模式：Observe / Waterfall / Serial / Parallel。
- **受控关闭必须** `Runtime::shutdown()`。
- `Runtime::diagnostics()` 只暴露 ID、状态与标签，无业务载荷。

## 开发验证

```bash
cd backend-cordis
cargo fmt --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```
