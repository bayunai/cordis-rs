# 扩展编写指南

面向在宿主中挂载的 `Plugin` 作者。Core 只提供挂载与释放；宿主负责构造插件实例。

## 最小 Plugin

```rust
use async_trait::async_trait;
use cordis_core::{Context, CoreError, Plugin, Runtime, ServiceKey};
use std::sync::Arc;

struct Greeter;

static GREETER: ServiceKey<&'static str> = ServiceKey::new("demo.greeter@1");

#[async_trait]
impl Plugin for Greeter {
    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        ctx.provide(GREETER, "hello")?;
        ctx.inject([GREETER.id()], |services, effect| async move {
            let _ = services.get(GREETER)?;
            effect.on_dispose(|| {});
            Ok(())
        })?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), CoreError> {
    let runtime = Runtime::new()?; // 必须在 Tokio 内
    let root = runtime.root();
    let mut handle = root.plugin(Arc::new(Greeter)).await?;
    // 热卸载或需要确认任务结束后再继续时，用 dispose_wait：
    handle.dispose_wait().await;
    runtime.shutdown().await;
    Ok(())
}
```

## ServiceKey / Event Key 约定

- ID 使用稳定的 `&'static str`，建议 `domain.name@version`。
- **同一 Runtime 内，同一字符串 ID 永久绑定一种分发模式与 Rust 类型**（Serial 另锁答案类型）；冲突立即返回对应错误。
- Observe 用 `EventKey<T>`；策略链用 `WaterfallKey`；决策用 `SerialKey<T, R>`；扇出用 `ParallelKey`。
- 扩展之间只通过公开的 Key 协作，不要依赖对方内部类型布局。

## 事件分发

- Observe：`on` / `emit`（同步）。
- Waterfall：外层先执行；必须 `next.call(value).await` 才会进入下游；直接返回即短路。
- Serial：按注册顺序 await，返回第一个 `Some(R)`。
- Parallel：并发执行；任一失败 → `ParallelDispatchFailed`（其余仍会跑完）。
- async 监听器若捕获 `&T`，请在闭包内拷贝所需字段后再 `async move`（`Fut: 'static`）。

## 任务与取消

- 长生命周期任务必须用 `EffectContext::spawn`，并在任务内 `cancel.cancelled().await`（或等价检查）。
- `PluginHandle::dispose()` / 同步 `dispose`：只发取消并把任务上收到父 Scope，不强制中断。
- `PluginHandle::dispose_wait()`：取消并**等待本插件受控任务结束**（不上收）；不停机热加载必须用它。
- `Runtime::shutdown()`：Root 级 `dispose_wait` + settle + 等待调度器。
- Core **不**内置超时；宿主需要时用 `tokio::time::timeout` 包一层。
- **不要**假设存在循环依赖诊断；环只表现为持续 `Pending` 与 `missing_dependencies`。

## 热加载

```text
旧 PluginHandle.dispose_wait().await
→ Context::plugin(新实例)
```

在旧实例 `dispose_wait` 完成前，不要对同 ServiceId 再次 `provide`（会 `ServiceConflict`）。更换类型须换 ID。

## 推荐做法

- 在 `apply` 中先 `inject` 再 `provide` 派生 Service，或先 `provide` 基础依赖。
- 观察事件用 `EventKey` + `on`/`emit`；策略改写用 `WaterfallKey`；需要提前退订时调用 `Unsubscribe::dispose`。
- 测试使用 `cordis-testkit`（`wait_injection`、`TestPlugin`、`EventRecorder`），不要依赖生产宿主。

## 禁止事项

- 不要依赖或修改宿主的 `AppState`、HTTP 路由、数据库连接、Redis、配置文件。
- 不要在 Core 层假设网关 path、凭证、安全策略等业务概念。
- 不要把 Service 实例写入日志或诊断输出；诊断只暴露 ID。
- 不要实现 Manifest / JSON config / 优先级 / HookStage——那些属于宿主扩展协议，不是本 Core。
- 不要依赖已删除的 `cycles` 诊断字段。
