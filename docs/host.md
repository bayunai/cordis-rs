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

## 管理 API 与可见性

`Loader` 提供 `entries()`、`factories()`、`injections()`、`create()`、`update()`、`remove()`、
`reload()` 和 `await_idle()`。`create` / `update` 接收父 Group 路径与位置；`update` 可以移动条目，
移动会重建该节点和后代的 Context 链。

条目服务不泄漏到 Root 或兄弟条目。应用读取服务时必须使用路径：

```rust
let database = loader.entry_context("infrastructure:database")?.get(DATABASE)?;
```

文件来源的变更先完成树预检与 reconcile，再原子写回完整 TOML。文件被外部修改时返回
`ConfigConflict`；写回失败返回带已生效 `LoaderSnapshot` 的 `Persist`。`await_idle()` 只等待
Loader 自己的树稳定，不等待 Runtime 的无关工作。
