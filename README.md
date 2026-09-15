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

启动约束：宿主从本地 `bootstrap.toml` 读取配置存储与扩展目录等最小启动锚点；SQLite
或 PostgreSQL 中的运行期主配置由配置扩展读取。详见[Bootstrap 配置边界](docs/architecture.md#bootstrap-配置边界)。

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
    fn key(&self) -> cordis_core::PluginKey {
        cordis_core::PluginKey::new("example.clock")
    }
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
    // 热卸载：await 插件任务与异步 disposer；或 fiber.replace(新实例)
    plugin.dispose_wait().await?;
    runtime.shutdown().await?;
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
- `Context::extend()`：创建不拥有独立生命周期的派生视图。
- `isolate` / `isolate_with`：创建仅对派生视图生效的 ServiceKey 隔离标签（不可跨 Runtime）。
- Context 不可释放；资源由 `Runtime`、`EffectContext` 或 `Fiber` 持有和释放。Fiber 在重启、替换或依赖变更时会先进入 `Unloading`，旧任务退出后才重新激活。
- `Context::plugin` 返回 `Fiber`（`restart` / 同 Key `replace` / `dispose_wait`）；跨 Key 用 `Runtime::unmount`。
- 一次性异步收尾用 `EffectContext::on_dispose_async`（串行 LIFO）；长期后台用 `spawn`。`dispose()` 后再 `dispose_wait()` 仍等待同一轮结果；释放失败由 `dispose_wait` / `unmount` / `shutdown` 观察。
- `Runtime::subscribe_fiber_states()` 提供 Fiber 只读状态广播；订阅者滞后时使用 `Runtime::diagnostics()` 重建快照。
- 具名 Effect + 诊断树（plugin_fibers / plugin_registry / inject_fibers / effects）。
- 事件四模式：Observe / Waterfall / Serial / Parallel；支持 `ListenOptions`（once / prepend / global / filter）。
- `ConfigKey` + `intercept` / `config`：派生配置覆盖，不影响 Service。
- Plugin 须声明 `PluginKey`。
- **受控关闭必须** `Runtime::shutdown()`；仅 Drop 不启动未执行的 async disposer。
- `Runtime::diagnostics()` 只暴露 ID、状态与标签，无业务载荷。

## 开发验证

```bash
cd backend-cordis
cargo fmt --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```
