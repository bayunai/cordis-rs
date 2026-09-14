# Cordis Runtime Core 架构

## 三层边界

| 层 | 职责 | 本仓库位置 |
| --- | --- | --- |
| **Runtime 内核** | Context、Service、inject、EffectScope、Plugin 挂载、Event、诊断 | `crates/cordis-core` |
| **宿主** | 组装 Runtime、构造插件对象、接入网络/配置/持久化 | 应用仓库（例如网关） |
| **扩展** | 实现 `Plugin`，`provide` / `inject` / `on`，不触碰宿主内部类型 | 插件 crate |

`cordis-core` **不包含** HTTP、数据库、Redis、JSON 配置、Manifest、WASM 或网关语义。

## Runtime 与 Tokio

- `Runtime::new()` **必须**在 Tokio Runtime 上下文中调用，否则返回 `SchedulerUnavailable`。
- 创建时启动**唯一**的响应式重算调度器；Provider / 注入撤销通过 `Notify` 唤醒，不依赖调用方是否仍在 Tokio 线程。
- 调度器持有取消令牌与 `JoinHandle`；关闭后不再处理 dirty，也不会被静默重启。

## 响应式注入

1. `Context::inject(deps, callback)` 登记 Fiber；依赖未齐时为 `Pending`。
2. `provide` / Provider 释放 / 覆盖会标记 dirty，由专用调度器**串行**重算。
3. 子 Context 覆盖父 Provider；子释放后回退父 Provider，并按 resolved `provider_id` 重建 inject。
4. 同 Context 同 Key 仅一个 Provider；冲突返回 `ServiceConflict`。
5. 环依赖保持 `Pending`；诊断暴露**缺失依赖**与最后失败原因，**不**伪造循环边。

### `settle()` 一致性边界

- `Runtime::settle()` 等待**调用时已排队**的 dirty 重算收敛。
- 先登记 `Notify` 等待者再检查 idle，避免丢失 `notify_waiters` 导致悬挂。
- **不**承诺阻止 `settle` 返回之后的并发 `provide` / dispose。

## Service / Event 类型合同

- 同一 Runtime 内，`ServiceId` / 事件 ID 首次使用后永久绑定 `TypeId`。
- 不同 Context、不同插件、或 Provider 撤销后，均不得将同名 ID 绑到另一 Rust 类型；立即返回 `ServiceKeyTypeConflict` / `EventKeyTypeConflict`。

## Plugin 生命周期

```text
Host constructs Plugin
        │
        ▼
Context::plugin(Arc<dyn Plugin>)
        │
        ├─ 创建子 EffectScope
        ├─ Plugin::apply(&Context)
        │     ├─ provide / inject / on / spawn
        │     └─ 资源挂在该 Scope
        ├─ apply 返回后若 Scope 已释放 → 清理并返回错误（不返回 Handle）
        └─ 返回 PluginHandle（持有该 Scope）
                │
                ├─ dispose / Drop（同步，幂等）：任务上收到父 Scope
                ├─ dispose_wait（异步，幂等）：任务留在本 Scope 并 await 完毕（热卸载）
                └─ Scope 释放 → 撤销 provide / inject / 订阅；任务收到取消令牌
```

子 Scope 释放后会从父级 `children` 集合移除，避免反复挂载/动态 Context 累积空引用。

### 热加载约定

不停机替换插件时，宿主应：

```text
旧 PluginHandle.dispose_wait().await
→ 再 Context::plugin(新实例)
```

同步 `dispose()` 会把任务上收到父 Scope，旧任务可能与新实例重叠，**不能**作为热替换的唯一步骤。同 ServiceId 须在旧实例 `dispose_wait` 完成后再对新实例 `provide`（否则 `ServiceConflict`）。类型合同仍全局锁定，热更换类型须换 ID。

## Event

同 `&'static str` ID 永久绑定 **分发模式 + payload TypeId**（Serial 另锁 answer TypeId）；混用返回 `EventModeMismatch` / `EventKeyTypeConflict` / `EventAnswerTypeConflict`。

| 模式 | Key | 订阅 / 派发 | 行为 |
| --- | --- | --- | --- |
| Observe | `EventKey<T>` | 同步 `on` / `emit` | 顺序调用；遇错即停上抛 |
| Waterfall | `WaterfallKey<T>` | async `on_waterfall` / `waterfall` | 洋葱链；`Next::call` 委托下游；不 call = 短路 |
| Serial | `SerialKey<T, R>` | async `on_serial` / `serial` | 顺序 await；首个 `Some(R)`；全 `None` → `Ok(None)` |
| Parallel | `ParallelKey<T>` | async `on_parallel` / `parallel` | 真并发 join；错误聚合为 `ParallelDispatchFailed` |

- 订阅归属当前 EffectScope；Scope 释放自动退订；`Unsubscribe::dispose` 可提前退订（丢弃句柄不退订）。
- Waterfall / Serial / Parallel **无内置超时**；宿主可用 `tokio::time::timeout`。
- 无 HookStage、isolate 过滤、`internal/*` intercept。

## 诊断

`Runtime::diagnostics() -> RuntimeSnapshot` 仅含：

- Context 节点 id + parent（Scope 释放后节点从 Registry 移除）
- Service **ID 字符串**（无实例）
- Plugin / Fiber id、phase、依赖 ServiceId、缺失依赖、last_error

禁止快照中出现 Service 值、业务数据、配置或伪造的循环依赖列表。

## 任务 Handle 所有权

| 路径 | 行为 |
| --- | --- |
| 成功 `spawn` / 入队 | `JoinHandle` 归属当前 EffectScope |
| 同步 `dispose` | 取消令牌；子任务 Handle **上收到父 Scope**；不 abort 已归属任务 |
| `dispose_wait` | 取消令牌；任务**留在本 Scope**并 await；不上收；再从父集合摘除 |
| 无法入队（Scope 已释放等） | 显式 `abort()`，不依赖 JoinHandle Drop 契约 |
| `Runtime::shutdown` | Root `dispose_wait` → 等待协作任务与调度器退出 |

## 关闭：dispose / dispose_wait / shutdown

| API | 行为 |
| --- | --- |
| `Context` / `EffectScope` / `PluginHandle` **dispose**（同步） | 取消令牌、释放子 Scope、执行 cleanup、从父集合摘除；已归属任务上收到父 Scope |
| `PluginHandle` / Scope **dispose_wait**（异步） | 同上清理，但任务留在本 Scope 并 await 完毕（不上收）；**无超时**（宿主可用 `tokio::time::timeout`） |
| `Runtime::shutdown`（异步） | ① Root `dispose_wait` ② `settle` ③ 取消并 **await** 调度器退出；**无超时** |
| `drop(Runtime)` | **尽力**同步关闭：Root `dispose` + abort 调度器任务以释放 Registry；**不等待**业务受控任务 |

宿主正常路径必须调用 `shutdown()`。插件热替换必须用 `dispose_wait`，不能只靠同步 `dispose`。仅依赖 `Drop` 不足以保证协作任务结束。

后台任务必须响应 `CancellationToken`；忽略取消会导致 `dispose_wait` / `shutdown` 一直等待。
