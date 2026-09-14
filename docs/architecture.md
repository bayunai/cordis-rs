# Cordis Runtime Core 架构

## 三层边界

| 层 | 职责 | 本仓库位置 |
| --- | --- | --- |
| **Runtime 内核** | Context、Service、inject、Isolation、Fiber、Effect、Event、诊断 | `crates/cordis-core` |
| **宿主** | 组装 Runtime、构造插件对象、接入网络/配置/持久化 | 应用仓库（例如网关） |
| **扩展** | 实现 `Plugin`，`provide` / `inject` / `on`，不触碰宿主内部类型 | 插件 crate |

`cordis-core` **不包含** HTTP、数据库、Redis、JSON 配置、Manifest、WASM 或网关语义。

## Runtime 与 Tokio

- `Runtime::new()` **必须**在 Tokio Runtime 上下文中调用，否则返回 `SchedulerUnavailable`。
- 创建时启动**唯一**的响应式重算调度器；Provider / 注入撤销通过 `Notify` 唤醒。
- 调度器持有取消令牌与 `JoinHandle`；关闭后不再处理 dirty。

## Isolation

- `IsolationLabel` 由 Runtime 分配，跨 Runtime 使用 → `IsolationRuntimeMismatch`。
- `Context::isolate(key)` 为指定 Service 新建标签；`isolate_with(key, label)` 加入既有标签。
- 解析：若当前 Context 谱系对该 Key 有隔离覆盖，则只看同 label 的 Provider；否则走父子 Local 覆盖。
- 隔离**只影响声明的 Key**；其他 Service 仍按父子链解析。

## 响应式注入

1. `Context::inject(deps, callback)` 登记派生 inject fiber；依赖未齐时为 `Pending`。
2. `provide` / Provider 释放会标记 dirty，调度器串行重算。
3. 子 Context 覆盖父 Provider（未隔离 Key）；子释放后回退。
4. 同槽冲突返回 `ServiceConflict`；Runtime 内 ID↔TypeId 永久锁定。

## Fiber（Plugin 生命周期）

```text
Context::plugin(Arc<dyn Plugin>)
        │
        ├─ 登记 Fiber（Plugin::inject 声明依赖）
        ├─ 依赖未齐 → Pending；齐则 Loading → apply → Active / Failed
        └─ 返回 Fiber
                ├─ dispose / dispose_wait / Drop
                ├─ restart()：同 Plugin 强制重跑
                └─ replace(plugin)：dispose_wait 旧任务后再挂新实例
```

Provider 变化时，依赖该 Key 的 Plugin Fiber 自动 `Active → Pending → Active`。配置更新 = 宿主构造新 Plugin + `Fiber::replace`（Core **无** `update(json)`）。

`Context::inject` 仍返回 `InjectionHandle`（派生依赖），与 Plugin Fiber 分开诊断。

## Effect

- 具名 `EffectHandle`（`effect()` / `effect_named`）；记录父子、取消状态与资源计数。
- Provider / 订阅 / 任务 / 子 Fiber 挂在创建它们的 Effect 上。
- `dispose` 上收任务；`dispose_wait` 本地 await（热卸载）。

## Event

同 ID 锁定 **模式 + TypeId**。Observe / Waterfall / Serial / Parallel 四模式；无 isolate 事件过滤、无 intercept。

## 诊断

`Runtime::diagnostics()` 含：contexts（含 isolation 覆盖）、isolations、providers（isolation/effect_id）、plugin_fibers、inject_fibers、effects。无 Service 值与配置。

## 关闭

宿主须 `Runtime::shutdown()`。插件热替换优先 `fiber.replace` 或 `dispose_wait` 后再挂载。
