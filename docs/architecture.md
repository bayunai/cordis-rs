# Cordis Runtime Core 架构

## 三层边界

| 层 | 职责 | 本仓库位置 |
| --- | --- | --- |
| **Runtime 内核** | Context、Service、inject、Isolation、Fiber、Effect、Event、诊断 | `crates/cordis-core` |
| **Loader** | 静态 Catalog、严格配置、reconcile 与管理服务 | `crates/cordis-loader` |
| **应用 Host** | 组装 Loader、运行应用事件循环并受控关闭 | `crates/cordis-host`；应用再接入网络/持久化 |
| **扩展** | 实现 `Plugin`，`provide` / `inject` / `on`，不触碰宿主内部类型 | 插件 crate |

`cordis-core` **不包含** HTTP、数据库、Redis、JSON 配置、Manifest、WASM 或网关语义。
`cordis-loader` 首版只做进程内编排与文件配置源，不加载动态库、不提供管理后台。

## Bootstrap 配置边界

宿主必须从本地、可人工恢复的**极小启动配置文件**启动；默认约定为
`bootstrap.toml`。它是 Runtime 之外唯一不可由扩展自身替代的配置来源，目的是避免
“配置扩展需要先读取自身配置”的启动循环。

启动文件只能包含已实现的启动锚点，不得承载应用、路由、策略或任意插件业务配置，
也不得保存凭证明文。

当前 `cordis-loader` 实现的锚点只有文件配置源：

```toml
version = 1

[config]
driver = "file"
path = "extensions.toml"
```

```text
bootstrap.toml
  → 宿主读取文件配置源
  → 校验 extensions.toml、经 ExtensionFactory 构造不可变 Plugin 实例
  → 挂载或 replace 已启用扩展
```

- 相对 `path` 相对于 `bootstrap.toml` 所在目录解析；重新加载只能显式 `reload()`。
- 扩展包目录、信任公钥、基础日志、SQLite/PostgreSQL 配置存储和文件热更新仍是未来
  宿主或扩展能力，不进入 `cordis-core`。首版 Host 遇到这些字段会因
  `deny_unknown_fields` 直接失败。
- 多节点部署若改用网络数据库，禁止将 SQLite 放在 NFS、SMB 或其他网络文件系统上作为
  共享配置库。

详见 [Host 编排](host.md)。

## Runtime 与 Tokio

- `Runtime::new()` **必须**在 Tokio Runtime 上下文中调用，否则返回 `SchedulerUnavailable`。
- 创建时启动**唯一**的响应式重算调度器；Provider / 注入撤销通过 `Notify` 唤醒。
- 调度器持有取消令牌与 `JoinHandle`；关闭后不再处理 dirty。

## Isolation

- `IsolationLabel` 由 Runtime 分配，跨 Runtime 使用 → `IsolationRuntimeMismatch`。
- `Context::extend()` 创建不可变派生 Context 视图，不创建 Registry 节点或 Scope；父 Context 不会被修改，最后一个引用释放后视图自然回收。
- `Context::isolate(key)` 返回 `(派生 Context, 新标签)`；`isolate_with(key, label)` 返回加入既有标签的派生 Context。
- 解析：若当前 Context 谱系对该 Key 有隔离覆盖，则只看同 label 的 Provider；否则走父子 Local 覆盖。最近槽位即使未就绪也会遮蔽父级，严格解析绝不静默回退。
- 隔离**只影响声明的 Key**；其他 Service 仍按父子链解析。

## 响应式注入

1. `Context::inject(deps, callback)` 登记派生 inject fiber；依赖未齐时为 `Pending`。
2. `provide`、`provide_checked` 的就绪状态变化、Provider 释放都会标记 dirty，调度器串行重算。
3. 派生 Context 覆盖父 Provider（未隔离 Key）；Effect/Fiber 释放后 Provider 自动回退。
4. 同槽冲突返回 `ServiceConflict`；Runtime 内 ID↔TypeId 永久锁定。

## Fiber（Plugin 生命周期）

```text
Context::plugin(Arc<dyn Plugin>)
        │
        ├─ 登记 Fiber（Plugin::inject 声明依赖）
        ├─ 依赖未齐 → Pending；齐则 Loading → apply → Active / Failed
        └─ 返回 Fiber（首次激活由内部协调器完成）
                ├─ dispose / dispose_wait / Drop
                ├─ restart()：同 Plugin 强制重跑
                └─ replace(plugin)：同 PluginKey；dispose_wait 旧任务后再挂
                跨 Key：Runtime::unmount(key) 后再 plugin(new)
```

`Loading` / `Unloading` 始终由 **Fiber 内部生命周期协调器** 收敛：调用方 Future 取消只取消 wait，不 abort 协调器。因此不会因取消而残留 Busy、Loading 或 Unloading。

- **首次 `plugin()`**：调用方在返回 Handle 前取消 wait → 协调器撤销临时 Scope、注销 Fiber，视为从未成功挂载。
- **`restart()` / `replace()`**：一旦开始（`replace` 在候选 `key()`/`inject()` 预检成功后即提交），调用方取消不回滚；协调器继续完成卸载与后续重激活。
- Provider 变化、`restart()` 与 `replace()` 都先经历 `Active → Unloading → Pending → Loading → Active`：Core 取消并等待旧 Effect 的受控任务退出后，才允许下一次 `apply()`。
- 配置更新固定为：宿主读取并校验配置 → 构造新的不可变 Plugin 实例 → `Fiber::replace`（Core **无** `update(json)`）。
- 配置无效：宿主记录配置错误，**不得调用** `replace`；旧 Active 实例继续运行。
- 新实例 `apply` 失败：旧实例已释放，Fiber 进入 `Failed`；Core 不回滚配置或恢复旧实例。
- `restart()` 只用于配置未变时的重新执行、依赖变化或人工恢复，禁止作为可变配置更新入口。

`Context::inject` 仍返回 `InjectionHandle`（派生依赖），与 Plugin Fiber 分开诊断。

### Provider 就绪状态

普通 `provide()` 注册的 Provider 默认 `Ready`。`provide_checked(key, value, check)` 会保存一个同步、无阻塞的状态检查函数，并返回 `ProviderHandle`；Provider 自己的健康任务在本地状态变化后调用 `refresh()`。Core 不轮询、不发网络请求。

公开 `get()`、`inject()` 与 Plugin `inject()` 一律要求 Provider 的检查结果为 `Ready`，且其所属 Plugin Fiber 已 `Active`。Core 不提供读取未就绪实例的公开旁路；诊断通过 Registry 内部元数据区分“未注册”与“已注册未就绪”。可用性翻转会递增 Provider revision，确保短暂的失效/恢复也会使消费者重新验证。

`Runtime::subscribe_fiber_states()` 提供容量为 1024 的只读广播流，依次报告 `None → Pending` 以及后续状态转换。监听滞后会收到 `broadcast::error::RecvError::Lagged`；宿主应调用 `Runtime::diagnostics()` 重建当前快照。状态通知不能阻塞、拒绝或回滚 Fiber 生命周期。

## Effect

- 具名 `EffectHandle`（`effect()` / `effect_named`）；记录父子、取消状态与资源计数。
- Provider / 订阅 / 任务 / 子 Fiber 挂在创建它们的 Effect 或 Fiber 上；Context 只是视图，不能单独释放。
- 每次 Scope 释放共享一个 `DisposeCompletion`：`dispose()` 后再 `dispose_wait()`（可多次）等待同一轮结果与同一聚合错误。
- `dispose`：取消 Scope，立即执行同步 `on_dispose`（LIFO），再经 Runtime 捕获的 Tokio Handle 启动释放协调任务；协调器先等待子 Scope 完成，再执行当前 Scope 的 async disposer（串行 LIFO），最后收敛当前 Scope 的后台任务；协调等待上收至父/Root。
- `dispose_wait`：等待同一 `DisposeCompletion`；失败聚合为 `DisposeFailed`。
- 长期后台工作用 `spawn`（带取消令牌）；一次性收尾用 `on_dispose_async`（无取消令牌）。
- `Drop` 为非受控关闭：同步 cleanup + 终止已启动工作，**不**启动尚未执行的 async disposer；完整异步释放须 `dispose_wait` / `shutdown`。

## Event

同 ID 锁定 **模式 + TypeId**。Observe / Waterfall / Serial / Parallel 四模式。

监听选项（`*_with_options` / `ListenOptions`）：

- `once`：真正调用前原子注销，并发派发最多执行一次
- `prepend`：同事件列表内优先于普通监听器；多个 prepend 最新注册优先
- `global`：Runtime 内同 ID 广播（先于祖先链本地监听器）；无 isolate 事件过滤
- `filter`：同步 `Fn(&T) -> Result<bool, CoreError>`；`false` 跳过，`Err` 中止（Parallel 计入聚合）

无选项的 `on*` 保持默认行为。配置覆盖见 `intercept`（独立于 Event）。

## 诊断

`Runtime::diagnostics()` 含：isolations、providers、plugin_fibers（含 `plugin_key`）、plugin_registry（Key + Fiber id/state）、inject_fibers、effects。Provider 与依赖 Snapshot 会显示受控的未就绪原因；业务 `get()` 错误不会携带原因。普通 Context 是不可枚举的短生命周期视图，不进入诊断；无 Service / Config 值。

## Config / intercept

`ConfigKey<T>` 独立于 `ServiceKey`。`Context::intercept(key, value)` 创建共享 Scope 的派生视图并写入覆盖；`config(key)` 向父链查找最近值。不影响 Provider / inject 生命周期。

## Plugin Registry

`Plugin::key()` 声明稳定身份。Runtime 按 Key 归组 Fiber；`Runtime::unmount(key)` 标记卸载中、拒绝同 Key 新挂载、`dispose_wait` 全部实例后清分组；释放错误聚合返回，但仍完成分组清理。

`unmount` 的重入保护按 **Effect Scope 树**判定，而非粗暴禁止所有生命周期回调：若当前用户回调（`apply` / inject / spawn / async disposer）所属 Scope 与任一待卸载 Fiber 将等待的 Scope 同树，立即返回 `UnmountReentrant`，不 cancel、不等待。卸载无关插件的 Key 仍允许。同树判定使用创建时冻结的共享祖先链（`detach` 后仍有效）。`shutdown` 的全局 `ShutdownReentrant` 约束不变。

卸载 / `dispose_now` 在 Effect `DisposeCompletion` 完成前保留 `pending_wait` 与 plugin 索引，使 async disposer / 并发 `unmount` 仍能看到归属；完成后才 `unregister`。

首次挂载：`Context::plugin` 的 wait 被取消时向本轮协调器请求放弃 Handle——若 `apply` 尚未开始则直接撤销；若已开始则**不** abort 用户 Future，等 `apply` 返回后不提交 Active、释放临时 Scope 并注销 Fiber。因此 `Plugin::apply` **不得无限阻塞**；长等须用 Effect `spawn` + 取消令牌。

`Fiber::replace` 仅允许同 Key；旧 Effect 释放失败时进入 `Failed`，不启动新 `apply`。

## 关闭

宿主须 `await Runtime::shutdown()`（返回 `Result`）。插件热替换优先 `fiber.replace` 或 `dispose_wait` 后再挂载。单纯 `dispose()` 为 fire-and-forget；之后仍可 `dispose_wait()` 观察同一轮释放结果。仅 `drop` Runtime **不**保证执行未启动的 async disposer。
