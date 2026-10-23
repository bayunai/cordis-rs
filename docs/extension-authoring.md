# 扩展编写指南

面向在宿主中挂载的 `Plugin` 作者。Core 只提供挂载与 Fiber 生命周期；
[`cordis-loader`](host.md) 负责显式 Factory 注册、EntryTree 配置校验与生命周期编排。

## 类型化热配置

所有插件统一实现 `ExtensionFactory::prepare()`，并由 Catalog 调用 `register(factory)`。
`Config` 是正常的 `Deserialize + JsonSchema` 输入类型；`prepare()` 校验全部字段并返回
延迟构造的 `PreparedExtension`。普通插件使用 `PreparedExtension::ordinary(build)`，无需
空 Live、配置引用或额外划分方法。

需要保留连接、服务和 Effect 而动态调整参数时，返回
`PreparedExtension::volatile(settings, live, build)`。`settings` 需要 `Clone + PartialEq`，
以同一份已校验值构造插件并保存比较基准；`live` 只需
`PartialEq`。两者还须满足 `Send + Sync + 'static`。

热配置构造闭包接收 `(settings, ConfigRef<Live>)`，仅需要新实例时执行。
闭包必须拥有配置及依赖，共享数据通过 `Arc` 捕获，不得借用 Factory。
每次请求先调用一次 `get()`，
使用返回快照中的全部字段，避免在一次操作中混用不同版本。引用本身不可写；热值由 Loader
校验后整体原子替换。配置类型应使用普通不可变数据，不在其中放置业务可变状态。

主动响应更新时，在自身 `ctx.effect()?.spawn(...)` 受管任务中等待
`reference.subscribe().changed()`，同时观察任务的 cancellation token。
订阅允许合并更新，`current()`/`get()` 返回最新值；Loader 不等待业务调整完成。
Entry 释放后订阅返回 `ConfigClosed`，旧引用仍可读取最后快照。依赖暂时不可用导致的
卸载/恢复不创建新引用，但非 Active 实例的配置更新会通过替换处理。

普通配置变化必须重建插件；混合变化将完整候选值交给新插件，不先修改旧插件热值。
需要关闭连接、修改服务依赖或重建 Context 的参数应归普通配置，而不是 `Live`。
完整示例见 [`volatile_config`](../crates/cordis-plugin-include/examples/volatile_config.rs)：

```bash
cargo run -p cordis-plugin-include --example volatile_config
```

## 配置表单与秘密字段

Factory 的 `Config: Deserialize + JsonSchema` 同时提供输入类型与表单描述。
字段的 Rust 文档注释可提供说明；秘密字段使用标准 Schema 标记，不在另一份目录中登记：

```rust
#[derive(serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct UploadConfig {
    /// 上传服务地址。
    endpoint: String,
    #[serde(default)]
    #[schemars(extend("writeOnly" = true))]
    bearer_token: Option<String>,
}
```

`writeOnly` 仅控制管理投影，不自动加密磁盘文件，也不意味着热更新。
`prepare()` 必须校验完整配置，不挂载资源；通过 `ordinary()` / `volatile()` 表达实际更新能力。
禁用实例的表单编辑也会调用 prepare，因此不要将启用期资源访问放到该阶段。
不要在错误、panic 或日志中拼接 Token 或其他秘密。

应用使用 `describe_config()` 展示表单，再通过 `mutate_config()` 提交有序字段操作和版本。
未修改的秘密不要从脱敏表单补为空字符串；只有用户明确替换或删除时才提交相应操作。
管理端无需依赖插件自身的客户端入口，未启用或失败的实例同样能修复配置。
详细并发、持久化及 Schema 边界见 [Host 配置控制面](host.md#面向管理界面的配置控制面)。

## 文件来源与树载体

文件读取、路径、编解码、patches 与文件写回属于可选 `cordis-plugin-include`，不是 Factory
或 Core 的通用职责。Host 显式注册 IncludeFactory，并用 `bootstrap_entry("include", path).await?` 构造
根文件条目；文件中的实例完整路径因此带 `include:` 前缀。原始文件内的局部 ID 不需要增加前缀。

业务服务通过 `loader.entry_context("include:插件")` 读取；文件树 CRUD 使用
`loader.subtree("include")` 的局部路径。`describe_config` / `mutate_config` 可以直接使用完整路径，
由 Loader 定位来源。Loader 内存根 CRUD 不持久化，不能把文件修改发到根控制面。

需要文件目录时读取 Include 提供的 `INCLUDE_LOCATION`，不要假定 `EntryLocation` 包含文件信息。
该元数据只在 Include 来源的上下文中存在；内存根中的 Include 应使用绝对路径。
`PreparedExtension::tree()` 为来源树载体保留局部更新语义，不应通过普通 volatile 订阅模拟
来源更新完成。仅 patches、initial 或日志策略改变时，Include 的控制调用会等待子树协调。
文件路径或父上下文改变则重新挂载来源树。详见 [Include 文档](include.md)。

## 最小 Plugin 示例

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

- `let (isolated, label) = ctx.isolate(KEY)?` 创建带新标签的派生 Context；**复用父服务 identity**，原 `ctx` 不变。
- `ctx.isolate_with(KEY, label)?` 创建加入既有标签的派生 Context。
- 同 label 的兄弟 Context 共享该 Key；未加入 label 的 Context 不可见。
- 标签不可跨 Runtime。
- Loader 侧通过 Catalog `IsolationDescriptor` + 条目 `isolate` 声明驱动上述 API（见 Host 文档）。

## Logger

- `ctx.logger()?` / `ctx.logger_named("x")?` 使用 Runtime 唯一的 `LoggerService`；`isolate` / Group **不能**换成另一总线。
- 全局总线**不过滤**：`Debug` 也会进入缓冲并送达全部 exporter。
- 默认目标名：`LOGGER_CONFIG.name` → 当前 PluginKey → `root`；`LOGGER_CONFIG.level`（缺省 `Info`）写入 `LogRecord.default_level`，供 exporter 回退，不决定是否创建记录。
- `ConfigUnavailable`（无 `LOGGER_CONFIG`）时使用上述默认；`ContextDisposed`、配置类型冲突等其他错误由 `logger()` 原样返回。
- `ctx.register_log_exporter(exporter)` 挂到当前 Effect Scope；Scope/Fiber 卸载后自动移除。
- 控制台输出用独立 crate `cordis-plugin-logger-console`（Host 显式挂载，先于 Loader）：

```rust
let _console = host.root().plugin(Arc::new(ConsoleLoggerPlugin::default())).await?;
ctx.logger()?.info("hello");
```

- Console `levels` 为 `BTreeMap`：完整 `target` → 键 `"default"` → `record.default_level`。
- 若需经 Loader 管理 Console，应用自建 Factory；勿向 Core/Loader 引入反向依赖。
- 首版无 JSON fields、文件/远程 exporter、HTTP 订阅或动态改配置。

## Context 与资源生命周期

- `Context::extend()`、`isolate()` 返回的都是不可变派生视图，不登记 Runtime 节点，也没有 `dispose()`；无引用后自动回收。
- 需要可卸载的 Provider、订阅或任务时，先通过 `ctx.effect()` 获取 `EffectContext`，再在其上 `extend()` 或创建资源。
- 连接器等“实例已创建、暂不可服务”的 Provider 使用 `provide_checked(key, value, check)`。`check` 只能读取本地健康状态，不能执行 I/O；健康任务状态变化后调用返回的 `ProviderHandle::refresh()`。业务代码只能使用严格 `get()`；未就绪 Provider 的状态和原因由 `Runtime::diagnostics()` 暴露，而非读取服务实例。
- 资源只由 `EffectContext`、`Fiber` 或 `Runtime::shutdown()` 释放；不要把 Context 句柄当作资源所有者。

## Fiber

- `plugin()` 返回 `Fiber`（非旧 PluginHandle）。首次激活由内部协调器完成；调用方在返回前取消 wait 会撤销挂载（无残留 Fiber/诊断/服务）。若取消时 `apply` 已开始，协调器仍等 `apply` 结束再撤销，**不会** abort 用户 Future。
- **`Plugin::apply` 不得无限阻塞**；长等、监听或后台刷新须用 `EffectContext::spawn` 并响应取消令牌，以便挂载取消、卸载与 `shutdown` 能收敛。
- `restart()` 保留 Plugin 对象重跑；`replace(new)` 要求相同 `PluginKey`，先等旧任务结束再挂新实例。二者一旦开始，调用方取消不中断协调器，无需再手动 restart。
- `Loading` / `Unloading` 由内部协调器收敛，不会因取消而粘滞。
- `apply` 失败 → `FiberState::Failed`，句柄仍返回；查 `last_error()`。
- 热更新：同 Key → `fiber.replace(new)`；跨 Key → `Runtime::unmount(old)` 后再 `plugin(new)`。
- `unmount(自身 Key)` 不可在该插件的 `apply` / inject / spawn / async disposer 内调用（`UnmountReentrant`）；卸载无关 Key 可以。`dispose_now` 与 `dispose_wait` 在释放完成前均保留归属，两条路径下重入判定一致。
- `restart()`、`replace()` 和依赖 Provider 变更会先进入 `Unloading`，等待旧 Effect 的受控任务退出，再重新激活；不要在任务取消后继续使用旧实例资源。
- 宿主可通过 `Runtime::subscribe_fiber_states()` 观察状态；这是可能丢失的广播流，收到 `Lagged` 后应使用 `Runtime::diagnostics()` 重建当前状态。

## 配置更新

插件实例应持有已经由宿主校验完成的强类型、不可变配置。`cordis-core` 不接收 JSON、
不执行 Schema 校验，也不提供 `update(config)`。

进程内宿主通过显式 `ExtensionFactory` 构造实例：

```rust
use cordis_core::Plugin;
use cordis_loader::{ExtensionFactory, LoaderError, PreparedExtension};
use serde::Deserialize;
use schemars::JsonSchema;
use std::sync::Arc;

struct GreeterFactory;

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GreeterConfig {
    name: String,
}

impl ExtensionFactory for GreeterFactory {
    type Config = GreeterConfig;

    fn id(&self) -> &'static str {
        "demo.greeter"
    }

    fn prepare(&self, config: GreeterConfig) -> Result<PreparedExtension, LoaderError> {
        Ok(PreparedExtension::ordinary(move || {
            Ok(Arc::new(Greeter { name: config.name }))
        }))
    }
}
```

```text
来源读取声明 → Loader 反序列化为 `Factory::Config` → factory.prepare(config) → 选择热提交、树载体更新或延迟构造 → 挂载到所属服务域 Context
```

- `prepare` 与构造闭包必须无业务副作用；I/O 与任务只出现在 `Plugin::apply`。
- 工厂由应用调用 `ExtensionCatalog::register` 显式登记，不使用自动注册宏。`JsonSchema` 会被
  `Loader` 公开给管理界面生成配置表单。
- 校验或反序列化失败时，Loader 不执行 reconcile，也不写回配置文件。
- Loader 按 Entry/子树差分更新：仅受影响节点参与生命周期；纯排序不重启 Fiber。
  `inject`/`isolate`/父 Context 不变的普通配置更新走 `Fiber::replace`；Group 的 `inject` 或
  `isolate` 变化重建该子树。
- 插件不得自行保存可变 Loader 配置。
- 不得修改已挂载实例的内部配置后调用 `restart()`；`restart()` 仅用于配置未变的重新执行。
- **服务域**：顶层与 Group 默认共享父服务域（Group **不再** `extend()`）。服务隔离唯一手段是
  条目上的 `isolate`（Catalog 须先 `register_isolation`）：`true` 独占运行期标签，非空字符串
  按 `(ServiceId, 标签名)` 在 Loader 内复用。`inject = { ... }` 只覆盖 `ConfigKey`。未隔离的
  Key 与父同槽，可共享也可 `ServiceConflict`。
- **Include**：`cordis-plugin-include` 的 `cordis:include` 将独立 v3 TOML／JSON／YAML 文档附着为
  Loader 来源树，并拥有文件读取、patches、版本检查与写回；须 Catalog 显式注册。
  文件管理用 `loader.subtree(path)` 与局部路径，外部改文件后须显式 `reload()`。

## ServiceKey / Event Key

- 稳定 `&'static str`；同 Runtime 内 ID 永久绑定类型（事件另绑模式）。
- Observe / Waterfall / Serial / Parallel 分型 Key，勿混用同 ID。

## 任务与取消

- 长期监听、循环和后台刷新用 `EffectContext::spawn`，并响应 `CancellationToken`。
- 连接关闭、flush、注销等一次性收尾用 `on_dispose_async`（Scope 已进入释放阶段，不接收取消令牌；多个 disposer **严格串行 LIFO**）。
- 需要确认资源真实释放或获取失败时必须 `await dispose_wait()` / `unmount()` / `shutdown()`。
- 单纯 `dispose()` 是 fire-and-forget；之后仍可 `dispose_wait()` 等待同一轮完成并读取相同错误。非 Tokio 线程也可调用 `dispose()`（由 Runtime 创建时捕获的 Handle 调度）。
- `Drop` / 未 `shutdown` 的进程退出不会启动尚未执行的 async disposer；受控关闭路径才会完整执行。
- `dispose` 上收释放协调；热路径用 `dispose_wait` / `replace`。

## Timer（`cordis-plugin-timer`）

计时器是独立能力插件，不在 Core，也不在 Loader builtin。Host 须先挂载并保留 Fiber：

```rust
let _timer = host.root().plugin(Arc::new(TimerPlugin)).await?;
```

业务插件必须静态依赖 `TIMER`，并只在 `EffectContext` 上调用 `TimerExt`
（`timeout` / `sleep` / `interval` / `ticks` / `throttle` / `debounce`）：

```rust
fn inject(&self) -> Vec<ServiceId> {
    vec![TIMER.id()]
}

async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
    let effect = ctx.effect_named("refresh")?;
    let handle = effect.interval(|| { /* ... */ }, Duration::from_secs(5))?;
    // TimerHandle Drop 会取消；挂到 Effect 上以跟随 Fiber 生命周期。
    effect.on_dispose(move || {
        drop(handle);
    });
    Ok(())
}
```

取消语义：手动 `cancel` / Handle Drop、所属 Effect 或 Fiber 释放、Timer 依赖撤销、
Runtime shutdown 都会停止未触发回调；等待中的 `sleep` / `ticks` 得到 `TimerError::Disposed`。

## 禁止事项

- 不要依赖宿主 AppState / HTTP / DB / Redis / 配置文件。
- 不要实现 Manifest、Loader/HMR——属 Loader 层。插件配置 Schema 由 Factory 的 `Config` 类型声明，并由 Loader 在调用 `prepare` 前进行类型化反序列化；业务约束由 `prepare` 校验。
- `intercept` 仅派生配置（`ConfigKey`），不可替代 `provide` / Service。
- 不要给 `Context` 做 Timer mixin；未挂载 `TimerPlugin` 时不得假定计时器可用。
