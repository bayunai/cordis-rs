# Definition 与 Fiber 接入迁移

插件身份由挂载方显式提供，执行对象只负责依赖声明和 `apply`。Core 不包含配置工厂；Loader Catalog 负责将配置工厂绑定到定义。

## 定义与实例

```rust
use cordis_core::PluginDefinition;

cordis_core::plugin_definition!(pub static PLUGIN_DEFINITION = "app.storage";);

let shared = PLUGIN_DEFINITION.clone();
assert_eq!(shared.id(), PLUGIN_DEFINITION.id());
let another = PluginDefinition::named("app.storage");
assert_ne!(another.id(), PLUGIN_DEFINITION.id());
let anonymous = PluginDefinition::anonymous();
assert!(anonymous.name().is_none());
```

静态共享定义推荐使用 `cordis_core::plugin_definition!`，可保留文档、属性和可见性，调用方不必导入 `LazyLock` 或 `PluginDefinition`。宏只封装已有静态初始化方式：每个声明身份独立，Clone 共享身份。运行时名称继续使用 `PluginDefinition::named()`，匿名注入仍由 Core 自动创建定义，不使用声明宏。

`name()` 仅用于展示；同名独立定义不是同一身份。Definition 可以跨 Runtime 复用，`Runtime::unmount(definition.id())` 只影响调用它的 Runtime。`PluginDefinitionId` 支持 Copy、相等比较、排序、Hash、Display；需要数值传输时调用 `as_u64()`。名称和进程内 Definition ID 均不应被当作配置文件中的插件寻址协议。

移除插件实现中的身份方法，并由创建方调用 `ctx.plugin(&definition, Arc::new(plugin)).await`。`Fiber::replace(plugin)` 使用新执行对象，但保留该 Fiber 的 Definition、实例身份与归属。

## 句柄与释放

| 入口 | 返回值 | Drop 行为 | 支持的生命周期操作 |
| --- | --- | --- | --- |
| `ctx.plugin(&definition, plugin).await` | `Fiber` | 请求卸载 | restart、replace、dispose、dispose_wait |
| `fiber.handle()` | `FiberHandle` | 无动作 | restart、dispose、dispose_wait |
| `ctx.inject(deps, callback)` | `FiberHandle` | 无动作 | restart、dispose、dispose_wait |

拥有型 `Fiber` 需要保存在宿主或拥有者中；临时值被丢弃会卸载插件。`FiberHandle` 可克隆用于观察和显式操作，生命周期仍由原有 Scope 管理，保留或丢弃观察句柄都不会改变资源归属。两者使用相同 `FiberState` 与统一快照；非拥有型句柄不提供 replace。

插件与派生注入的 Failed 均不会因服务变化自动重试。修复外部条件后显式调用 restart，或由插件拥有者提供新对象调用 replace。Pending 仍会在全部依赖齐备后自动激活；Active 在依赖变化时先完整清理旧执行再按依赖重新激活。

## Loader、诊断与日志

Catalog 登记改为 `catalog.register(&definition, factory)`；`PreparedExtension::tree(...)` 的身份来自 Catalog 定义，不再由来源构造结果传入。内置插件 crate 统一导出 `PLUGIN_DEFINITION`，Loader 另导出 `GROUP_DEFINITION`。

Runtime 快照统一提供 `fibers`、`definitions`、`providers`、`effects`、`isolations`。Fiber 快照包含 Definition ID、父 Fiber ID、可选名称及 `FiberState`；Definition 条目列出当前 `fiber_ids`，包含归组及卸载状态。插件实例和派生注入使用同一份 Fiber 清单。

生命周期事件使用 `definition_id`、`parent_fiber_id`，日志使用可选 `definition_id`、`plugin_name`。通过身份关联记录，通过可选名称生成展示文字；匿名定义同样具备完整身份和诊断能力。
