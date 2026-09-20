# `cordis-loader`：静态 EntryTree Host

`cordis-host` 只创建和关闭 Runtime。`cordis-loader` 的 `LoaderPlugin` 是应用显式挂载的
普通插件：它从静态 Catalog 构造业务插件，并把 `extensions.toml` 收敛为一棵可管理的 EntryTree。

```rust
let host = CordisHost::new()?;
let plugin = LoaderPlugin::bootstrap(build_catalog()?, "bootstrap.toml")?;
let _loader_fiber = host.root().plugin(Arc::new(plugin)).await?;
let loader = host.root().get(LOADER)?;
```

保留 `_loader_fiber` 直到应用关闭；释放它会撤销 `LOADER` 并后序释放整棵条目树。

## 配置树

`bootstrap.toml` 与扩展清单都使用 v2：

```toml
# bootstrap.toml
version = 2
[config]
driver = "file"
path = "extensions.toml"
```

```toml
# extensions.toml
version = 2

[[extensions]]
id = "infrastructure"
name = "cordis:group"
group = true

[[extensions.config]]
id = "database"
name = "postgres.connector"

[extensions.config.config]
url_env = "DATABASE_URL"

[[extensions.config]]
id = "reports"
name = "cordis:group"
group = true

[[extensions.config.config]]
id = "exporter"
name = "report.exporter"
disabled = true
```

普通条目使用 `id`、`name`、`config`、`disabled` 与可选 `inject`。Group 必须使用保留
`name = "cordis:group"` 和 `group = true`，其 `config` 是子条目数组。路径以 `:` 拼接，以上
示例的导出器路径为 `infrastructure:reports:exporter`。

旧平面 v1 的 `instance/factory/enabled` 不再被读取；需将配置显式迁移到 v2。

## Factory 与配置化 inject

`name` 只匹配应用在 `host/catalog.rs` 编译期注册的 `ExtensionFactory`，不会加载动态库或第三方模块。
`Loader::factories()` 返回业务 Factory 的 JSON Schema。

除 Factory 外，宿主可注册可配置注入项：

```rust
catalog.register_injection(InjectionDescriptor::intercepted(
    "database", DATABASE, DATABASE_OPTIONS,
))?;
```

条目中 `inject = ["database"]` 仅声明必需服务；映射形式在声明依赖的同时为该条目 Context
写入强类型 `ConfigKey` 覆盖：

```toml
[extensions.inject]
database = { read_only = true }
```

未知服务、不可配置服务的映射值或不符合 Schema 的配置都会在 reconcile 前失败。

`inject = { ... }` **只覆盖 ConfigKey，不会隔离服务**：映射写入的 `intercept` 与所属
服务域共享同一 Service identity；同 Group（或顶层 Loader 根域）内的兄弟条目仍可互相
`get` 到对方的 `provide`。

Group 上的 `inject = { ... }` 会下传给整棵子树：子条目（及嵌套 Group）沿父链读取
Config 时可见该覆盖；嵌套 Group 再写同名 ConfigKey 时，子覆盖优先。

## 服务域边界

EntryTree 的服务可见性由 **父 Group（或 Loader 根）** 决定，而不是由单个 Entry：

| 位置 | 服务域 |
| --- | --- |
| 顶层普通 Entry | 共享 Loader 根服务域 |
| 每个 Group | 挂载时恰好 `extend()` 一次，得到该 Group 独立服务域 |
| Group 的直接子项 | 共享所属 Group 的服务域 |
| 嵌套 Group | 再派生下一层服务域 |

因此：

- 同级依赖（同顶层或同 Group）可以正常 `provide` → `inject` 激活。
- Group A 内的服务对根级条目与 Group B **不可见**。
- 不同 Group 可各自 `provide` 同一 `ServiceKey`，互不冲突。
- 同一服务域内重复 `provide` 同一 Key 会得到明确的 `ServiceConflict`。

普通 Entry **不是**默认服务边界；不要假设每个条目有独立服务域。

## 管理 API 与可见性

`Loader` 提供 `entries()`、`factories()`、`injections()`、`create()`、`update()`、`remove()`、
`reload()` 和 `await_idle()`。`create` / `update` 接收父 Group 路径与位置；`update` 可以移动条目，
移动按「旧路径后序释放 → 新路径前序挂载」处理（不做隐式原地迁移）。

Loader reconcile 是**局部**的：仅创建、删除、移动、启停或配置/`inject` 变化的 Entry/子树
参与生命周期；无关 Entry 保留原 Fiber ID、Context 与服务。普通 Entry 仅 `config` 变化且
`inject`/父 Context 不变时走 `Fiber::replace`。Group 的 `inject` 变化会重建该 Group 整棵子树；
仅 `disabled` 变化时保留 Group Fiber/Context，按祖先禁用规则启停后代。纯排序只更新
`LoaderSnapshot.entries` 前序与持久化文件顺序，**不**重启任何 Fiber。依赖关系仍须通过
`inject` 声明，排序不是业务依赖表达方式。

跨 Group 的服务不会泄漏。应用读取某一条目视图上的服务时使用路径：

```rust
let database = loader.entry_context("infrastructure:database")?.get(DATABASE)?;
```

同一 Group（或顶层）内的兄弟条目共享服务域，因此也可以从任一同域 `entry_context` 解析到
该域内已 `Active` 的 Provider。

文件来源的变更先完成树预检与 reconcile，再原子写回完整 TOML。预检失败（未知 Factory、
`FactoryChanged`、`PluginKeyChanged`、无效 inject 等）零运行时变更。生命周期失败不回滚
已触及节点、不提交 `desired`/revision、不由 Loader 发起写回；无关 Entry 继续运行。
失败后的 `LoaderSnapshot` 仍包含实际已插入的 Failed/Pending 节点，即使该次 reconcile
尚未来得及提交目标排序；对同一外部配置再次 `reload()` 会重试失败的启用 Entry。
文件被外部修改时返回 `ConfigConflict`；写回失败返回带已生效 `LoaderSnapshot` 的 `Persist`。
`await_idle()` 只等待 Loader 自己的本次 reconcile，不等待 Runtime 的无关工作。
