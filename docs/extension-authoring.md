# 扩展编写指南

面向在宿主中挂载的 `Plugin` 作者。Core 只提供挂载与 Fiber 生命周期；宿主负责构造插件实例与配置校验。

## 最小 Plugin

```rust
use async_trait::async_trait;
use cordis_core::{Context, CoreError, Plugin, Runtime, ServiceKey};
use std::sync::Arc;

struct Greeter;
static GREETER: ServiceKey<&'static str> = ServiceKey::new("demo.greeter@1");

#[async_trait]
impl Plugin for Greeter {
    fn key(&self) -> cordis_core::PluginKey {
        cordis_core::PluginKey::new("demo.greeter")
    }

    fn inject(&self) -> Vec<cordis_core::ServiceId> {
        // 可选：插件级依赖；未齐时 Fiber 保持 Pending
        Vec::new()
    }

    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        ctx.provide(GREETER, "hello")?;
        ctx.inject([GREETER.id()], |services, effect| async move {
            let _ = services.get(GREETER)?;
            effect.on_dispose(|| {});
            effect.on_dispose_async(|| async move { Ok(()) })?;
            Ok(())
        })?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), CoreError> {
    let runtime = Runtime::new()?;
    let root = runtime.root();
    let mut fiber = root.plugin(Arc::new(Greeter)).await?;
    fiber.dispose_wait().await?;
    runtime.shutdown().await?;
    Ok(())
}
```

## Isolation

- `let (isolated, label) = ctx.isolate(KEY)?` 创建带新标签的派生 Context；原 `ctx` 不变。
- `ctx.isolate_with(KEY, label)?` 创建加入既有标签的派生 Context。
- 同 label 的兄弟 Context 共享该 Key；未加入 label 的 Context 不可见。
- 标签不可跨 Runtime。

## Context 与资源生命周期

- `Context::extend()`、`isolate()` 返回的都是派生视图，没有 `dispose()`。
- 需要可卸载的 Provider、订阅或任务时，先通过 `ctx.effect()` 获取 `EffectContext`，再在其上 `extend()` 或创建资源。
- 资源只由 `EffectContext`、`Fiber` 或 `Runtime::shutdown()` 释放；不要把 Context 句柄当作资源所有者。

## Fiber

- `plugin()` 返回 `Fiber`（非旧 PluginHandle）。
- `restart()` 保留 Plugin 对象重跑；`replace(new)` 要求相同 `PluginKey`，先等旧任务结束再挂新实例。
- `apply` 失败 → `FiberState::Failed`，句柄仍返回；查 `last_error()`。
- 热更新：同 Key → `fiber.replace(new)`；跨 Key → `Runtime::unmount(old)` 后再 `plugin(new)`。
- `restart()`、`replace()` 和依赖 Provider 变更会先进入 `Unloading`，等待旧 Effect 的受控任务退出，再重新激活；不要在任务取消后继续使用旧实例资源。
- 宿主可通过 `Runtime::subscribe_fiber_states()` 观察状态；这是可能丢失的广播流，收到 `Lagged` 后应使用 `Runtime::diagnostics()` 重建当前状态。

## 配置更新

插件实例应持有已经由宿主校验完成的强类型、不可变配置。`cordis-core` 不接收 JSON、
不执行 Schema 校验，也不提供 `update(config)`。

```text
宿主读取配置 → 插件 Schema 校验/反序列化 → 构造新实例 → fiber.replace(new_instance)
```

- 校验或反序列化失败时，宿主不得调用 `replace`；旧 Active 实例保持运行。
- `replace` 开始后旧实例会被释放；新实例的 `apply` 失败使 Fiber 进入 `Failed`，不会自动回滚旧实例。
- 不得修改已挂载实例的内部配置后调用 `restart()`；`restart()` 仅用于配置未变的重新执行。

## ServiceKey / Event Key

- 稳定 `&'static str`；同 Runtime 内 ID 永久绑定类型（事件另绑模式）。
- Observe / Waterfall / Serial / Parallel 分型 Key，勿混用同 ID。

## 任务与取消

- 长期监听、循环和后台刷新用 `EffectContext::spawn`，并响应 `CancellationToken`。
- 连接关闭、flush、注销等一次性收尾用 `on_dispose_async`（Scope 已进入释放阶段，不接收取消令牌）。
- 需要确认资源真实释放或获取失败时必须 `await dispose_wait()` / `unmount()` / `shutdown()`。
- 单纯 `dispose()` 是 fire-and-forget；异步释放失败只能由后续父 Scope 或 Runtime 的等待路径返回。
- `dispose` 上收托管任务与异步 disposer；热路径用 `dispose_wait` / `replace`。

## 禁止事项

- 不要依赖宿主 AppState / HTTP / DB / Redis / 配置文件。
- 不要实现 Manifest、Loader/HMR、JSON Schema——属宿主层。
- `intercept` 仅派生配置（`ConfigKey`），不可替代 `provide` / Service。
