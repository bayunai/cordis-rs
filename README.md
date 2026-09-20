# cordis-rs

一个面向 Rust 的轻量级、进程内响应式运行时，提供作用域服务、依赖注入、插件生命周期、
资源归属、事件与运行时诊断。它不是 HTTP 框架、数据库抽象或动态插件加载器；这些能力应由
应用或独立扩展实现。

> 当前版本为 [`0.2.0`](CHANGELOG.md#020---2026-09-20)，公共 API 尚未稳定。升级 `0.x` 版本前请阅读变更说明并自行评估破坏性变更。

## 适用场景

- 在一个进程内按服务依赖关系编排模块与插件。
- 将后台任务、监听器、异步清理等资源绑定到插件生命周期。
- 在应用宿主中根据明确工厂目录和 TOML 配置启停扩展实例。

不适用：需要跨进程 RPC、数据库/缓存连接管理、动态库发现或浏览器端插件加载的场景；这些不属于
`cordis-core` 的职责边界。

## Crate

| Crate | 用途 |
| --- | --- |
| [`cordis-core`](crates/cordis-core) | Runtime 内核：`Context`、`Service`、`inject`、`Effect`、`Plugin`、`Event` 与诊断。 |
| [`cordis-loader`](crates/cordis-loader) | 可挂载的静态 Catalog EntryTree LoaderPlugin：v3 TOML 树、多树 reconcile、管理服务与原子持久化。 |
| [`cordis-host`](crates/cordis-host) | 应用进程薄外壳：创建 Runtime、开放完整权限并受控关闭。 |
| [`cordis-plugin-timer`](crates/cordis-plugin-timer) | Host 显式挂载的 Effect 作用域计时器能力（timeout / interval / throttle / debounce）。 |
| [`cordis-plugin-logger-console`](crates/cordis-plugin-logger-console) | Host 显式挂载的控制台日志 exporter；不经 ServiceKey / isolate。 |
| [`cordis-plugin-include`](crates/cordis-plugin-include) | 可选：TOML 文件驱动的嵌套 EntryTree（`cordis:include`）；须 Catalog 显式注册。 |
| [`cordis-testkit`](crates/cordis-testkit) | 测试辅助；不应用于生产宿主。 |

## 快速开始

在应用的 `Cargo.toml` 中以 Git 依赖接入当前未发布版本：

```toml
[dependencies]
async-trait = "0.1"
cordis-core = { git = "https://github.com/bayunai/cordis-rs", tag = "v0.2.0" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

> `tag` 应替换为你实际审计并固定的发布标签；不建议依赖浮动分支。

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
    plugin.dispose_wait().await?;
    runtime.shutdown().await?;
    Ok(())
}
```

## 示例与文档

- [`reactive`](examples/reactive.rs)：最小服务注入与响应式重算。
- [`plugin_hotplug`](examples/plugin_hotplug.rs)：插件替换与卸载。
- [`plugin_web`](examples/plugin_web.rs)：将运行时能力接入 HTTP 示例。
- [`plugin_stack`](examples/plugin_stack/main.rs)：Loader EntryTree 子树差分 reconcile 网页 Demo（`side` + `app/{db,logger,http}`）。
- [`host_bootstrap`](crates/cordis-host/examples/host_bootstrap/main.rs)：可复制的 Host 模板，包含入口、静态 Catalog、插件目录与 TOML 扩展清单。
- [架构与边界](docs/architecture.md)
- [Host 编排](docs/host.md)
- [扩展编写指南](docs/extension-authoring.md)

运行示例：

```bash
cargo run -p cordis-core --example reactive
cargo run -p cordis-core --example plugin_stack
cargo run -p cordis-host --example host_bootstrap
```

应用入口通过 `root.plugin(Arc::new(LoaderPlugin::bootstrap(...)?))` 显式挂载 LoaderPlugin；
`bootstrap.toml` 当前仅支持 `file` 配置源。应用与受信任插件通过 `ctx.get(LOADER)` 取得管理
服务；条目业务服务则通过 `loader.entry_context("父:子")` 读取。Loader 的 Factory 与可配置 inject
均为静态、强类型目录，并公开 JSON Schema 供管理界面生成表单。详情见[Host 文档](docs/host.md)。

## 关键语义

- `Runtime::new()` 必须在 Tokio Runtime 内调用；专用调度器处理依赖变更后的重算。
- `Context::extend()` 是不拥有独立生命周期的派生视图，并创建新的服务域；`isolate` /
  `isolate_with` 复用父服务 identity，仅隔离指定 `ServiceKey` 的可见性。
- `Context::logger()?` 使用 Runtime 全局日志总线（不过滤等级），不受 `isolate` 影响；控制台
  输出由 `cordis-plugin-logger-console` 显式挂载，按 target / `"default"` / `default_level` 过滤。
- `Context::intercept()` 只覆盖 `ConfigKey`，**复用父视图的 Service identity**，不会隔离服务。
- Loader EntryTree 中：顶层条目共享 Loader 根服务域；服务隔离由条目 `isolate` 声明驱动（见 Host 文档）；
  普通 Entry 默认不是服务边界。嵌套文件树由可选的 `cordis-plugin-include` 附着为独立
  `RuntimeTree`（须 Catalog 显式注册）。详见[Host 文档](docs/host.md)。
- `Plugin` 用 `PluginKey` 标识，并由 Fiber 管理加载、替换、卸载与依赖变更后的重启。
- `Effect` 统一归属插件的任务、监听器和清理回调；长期任务使用 `spawn`，一次性异步清理使用
  `on_dispose_async`。
- 受控退出必须调用 `Runtime::shutdown()`；仅 Drop 不会启动尚未执行的异步清理。
- `Runtime::diagnostics()` 只输出标识、状态和标签，不暴露服务业务载荷。

## 开发

需要 Rust `1.98.1` 或更高版本：

```bash
cargo fmt --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

贡献方式见[贡献指南](CONTRIBUTING.md)。安全问题请按[安全策略](SECURITY.md)私下披露，不要直接公开
敏感细节。

## 许可证

本项目采用双许可证：你可以任选 [MIT](LICENSE-MIT) 或 [Apache-2.0](LICENSE-APACHE) 的条款使用。
