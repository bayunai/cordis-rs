# Cordis Runtime Core 架构

## 三层边界

| 层 | 职责 | 本仓库位置 |
| --- | --- | --- |
| **Runtime 内核** | Context、Service、inject、Isolation、Fiber、Effect、Event、诊断 | `crates/cordis-core` |
| **宿主** | 组装 Runtime、构造插件对象、接入网络/配置/持久化 | 应用仓库（例如网关） |
| **扩展** | 实现 `Plugin`，`provide` / `inject` / `on`，不触碰宿主内部类型 | 插件 crate |

`cordis-core` **不包含** HTTP、数据库、Redis、JSON 配置、Manifest、WASM 或网关语义。

## Bootstrap 配置边界

宿主必须从本地、可人工恢复的**极小启动配置文件**启动；默认约定为
`bootstrap.toml`。它是 Runtime 之外唯一不可由扩展自身替代的配置来源，目的是避免
“配置扩展需要先读取自身配置”的启动循环。

启动文件只能包含启动锚点：配置存储位置与类型、扩展包目录、信任公钥位置、基础日志
设置，以及密钥的环境变量或外部引用；不得承载应用、路由、策略或任意插件业务配置，
也不得保存凭证明文。

```text
bootstrap.toml
  → 宿主创建 Runtime 并挂载配置存储扩展
  → 配置扩展读取运行期主配置
  → 宿主校验配置、构造不可变 Plugin 实例
  → 挂载或 replace 已启用扩展
```

- 单机一体化部署可用本地 SQLite 作为运行期主配置存储。
- 多节点部署必须使用 PostgreSQL 等网络数据库；禁止将 SQLite 放在 NFS、SMB 或其他
  网络文件系统上作为共享配置库。
- SQLite/PostgreSQL 配置存储、文件热更新和配置发布均是宿主或扩展能力，不进入
  `cordis-core`。

## Runtime 与 Tokio

- `Runtime::new()` **必须**在 Tokio Runtime 上下文中调用，否则返回 `SchedulerUnavailable`。
- 创建时启动**唯一**的响应式重算调度器；Provider / 注入撤销通过 `Notify` 唤醒。
- 调度器持有取消令牌与 `JoinHandle`；关闭后不再处理 dirty。

## Isolation

- `IsolationLabel` 由 Runtime 分配，跨 Runtime 使用 → `IsolationRuntimeMismatch`。
- `Context::extend()` 创建派生 Context 节点，不创建 Scope；父 Context 不会被修改。
- `Context::isolate(key)` 返回 `(派生 Context, 新标签)`；`isolate_with(key, label)` 返回加入既有标签的派生 Context。
- 解析：若当前 Context 谱系对该 Key 有隔离覆盖，则只看同 label 的 Provider；否则走父子 Local 覆盖。
- 隔离**只影响声明的 Key**；其他 Service 仍按父子链解析。

## 响应式注入

1. `Context::inject(deps, callback)` 登记派生 inject fiber；依赖未齐时为 `Pending`。
2. `provide` / Provider 释放会标记 dirty，调度器串行重算。
3. 派生 Context 覆盖父 Provider（未隔离 Key）；Effect/Fiber 释放后 Provider 自动回退。
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
                └─ replace(plugin)：同 PluginKey；dispose_wait 旧任务后再挂
                跨 Key：Runtime::unmount(key) 后再 plugin(new)
```

Provider 变化、`restart()` 与 `replace()` 都先经历 `Active → Unloading → Pending → Loading → Active`：Core 取消并等待旧 Effect 的受控任务退出后，才允许下一次 `apply()`。因此不会出现旧任务与新 Plugin 实例并行运行的窗口。配置更新固定为：宿主读取并校验配置 → 构造新的不可变 Plugin 实例 → `Fiber::replace`（Core **无** `update(json)`）。

- 配置无效：宿主记录配置错误，**不得调用** `replace`；旧 Active 实例继续运行。
- 新实例 `apply` 失败：旧实例已释放，Fiber 进入 `Failed`；Core 不回滚配置或恢复旧实例。
- `restart()` 只用于配置未变时的重新执行、依赖变化或人工恢复，禁止作为可变配置更新入口。

`Context::inject` 仍返回 `InjectionHandle`（派生依赖），与 Plugin Fiber 分开诊断。

`Runtime::subscribe_fiber_states()` 提供容量为 1024 的只读广播流，依次报告 `None → Pending` 以及后续状态转换。监听滞后会收到 `broadcast::error::RecvError::Lagged`；宿主应调用 `Runtime::diagnostics()` 重建当前快照。状态通知不能阻塞、拒绝或回滚 Fiber 生命周期。

## Effect

- 具名 `EffectHandle`（`effect()` / `effect_named`）；记录父子、取消状态与资源计数。
- Provider / 订阅 / 任务 / 子 Fiber 挂在创建它们的 Effect 或 Fiber 上；Context 只是视图，不能单独释放。
- `dispose`：取消 Scope，立即执行同步 `on_dispose`（LIFO），再启动 `on_dispose_async`（LIFO）并把 JoinHandle 上收至父/Root，不等待。
- `dispose_wait`：同样同步 cleanup 后，本地 await 受控任务与异步 disposer；失败聚合为 `DisposeFailed`。
- 长期后台工作用 `spawn`（带取消令牌）；一次性收尾用 `on_dispose_async`（无取消令牌）。

## Event

同 ID 锁定 **模式 + TypeId**。Observe / Waterfall / Serial / Parallel 四模式。

监听选项（`*_with_options` / `ListenOptions`）：

- `once`：真正调用前原子注销，并发派发最多执行一次
- `prepend`：同事件列表内优先于普通监听器；多个 prepend 最新注册优先
- `global`：Runtime 内同 ID 广播（先于祖先链本地监听器）；无 isolate 事件过滤
- `filter`：同步 `Fn(&T) -> Result<bool, CoreError>`；`false` 跳过，`Err` 中止（Parallel 计入聚合）

无选项的 `on*` 保持默认行为。配置覆盖见 `intercept`（独立于 Event）。

## 诊断

`Runtime::diagnostics()` 含：contexts（isolation 覆盖 + `config_keys` 标识）、isolations、providers（isolation/effect_id）、plugin_fibers（含 `plugin_key`）、plugin_registry（Key + Fiber id/state）、inject_fibers、effects。无 Service / Config 值。

## Config / intercept

`ConfigKey<T>` 独立于 `ServiceKey`。`Context::intercept(key, value)` 创建共享 Scope 的派生节点并写入覆盖；`config(key)` 向父爬最近值。不影响 Provider / inject 生命周期。

## Plugin Registry

`Plugin::key()` 声明稳定身份。Runtime 按 Key 归组 Fiber；`Runtime::unmount(key)` 标记卸载中、拒绝同 Key 新挂载、`dispose_wait` 全部实例后清分组；释放错误聚合返回，但仍完成分组清理。`Fiber::replace` 仅允许同 Key；旧 Effect 释放失败时进入 `Failed`，不启动新 `apply`。

## 关闭

宿主须 `await Runtime::shutdown()`（返回 `Result`）。插件热替换优先 `fiber.replace` 或 `dispose_wait` 后再挂载。单纯 `dispose()` 为 fire-and-forget，释放失败只能由父 Scope / Fiber / Runtime 的等待路径观察。
