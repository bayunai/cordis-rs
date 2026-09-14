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
    // 热卸载：await 插件任务后再挂新实例；普通路径也可用同步 dispose()
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
- 子 Context 覆盖父 Provider；子 dispose 后回退；Node 随 Scope 从 Registry 移除。
- 同 Context 同 Key 唯一 Provider；同 Runtime 内 Service/Event ID 全局类型（及事件模式）唯一。
- 事件四模式：Observe（同步）/ Waterfall / Serial / Parallel（后三者 async）；同 ID 不可混模式。
- 同步 `dispose`：取消与清理，已归属任务上收到父 Scope；无法入队的任务显式 `abort`。
- `PluginHandle::dispose_wait`：取消并等待本插件任务（不上收）；**热加载必须用它**，再 `plugin(新实例)`。
- **受控关闭必须** `Runtime::shutdown()`：Root `dispose_wait` → `settle` → 等待调度器退出。
- 仅 `drop(Runtime)` 会尽力 abort 调度器，**不**等待业务任务。
- `Runtime::diagnostics()` 只暴露 ID 与 phase，无伪造循环边。

## 开发验证

```bash
cd backend-cordis
cargo fmt --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```
