# cordis-loader 与 cordis-host

`cordis-loader` 提供可挂载的 `LoaderPlugin`：它管理静态 Factory Catalog、
`extensions.toml`、实例 reconcile 与 `Loader` 服务。`cordis-host` 只创建、暴露和关闭应用的
唯一 Runtime，不假定应用一定使用 Loader。

## 应用启动

`LoaderPlugin` 是 Bootstrap 固定挂载的基础插件，不能写入它自己管理的
`extensions.toml`，以避免“先读取配置才能决定是否读取配置”的启动循环。

```rust
use cordis_core::Plugin;
use cordis_host::CordisHost;
use cordis_loader::{LOADER, LoaderPlugin};
use std::sync::Arc;

let host = CordisHost::new()?;
let plugin = LoaderPlugin::bootstrap(build_catalog()?, "bootstrap.toml")?;
let _loader_fiber = host.root().plugin(Arc::new(plugin)).await?;

let loader = host.root().get(LOADER)?;
// 在此启动 HTTP、桌面窗口或其他应用事件循环。
host.shutdown().await?;
```

`LoaderPlugin::new(catalog, config)` 用于没有文件来源的内存配置；它不允许持久化管理操作。

## Context 与服务可见性

LoaderPlugin 自身把 `LOADER` 提供给它的挂载 Context。每个 `extensions.toml` 条目则从 Loader
根 Context 派生独立子 Context 后挂载，因此条目服务不会泄漏给兄弟条目或应用 Root Context。

应用需要读取某实例提供的服务时，使用该实例 Context：

```rust
let loader = host.root().get(LOADER)?;
let database = loader.entry_context("primary-database")?.get(DATABASE)?;
```

卸载 LoaderPlugin Fiber 会撤销 `LOADER` 服务，并释放全部受管实例及其 Effect。

## 静态 Factory 与配置

`host/catalog.rs` 只登记当前可执行程序编译进来的 Factory。它是可用类型目录，不是启动列表；
实际启停只由 `extensions.toml` 的 `enabled` 字段决定。

```rust
pub trait ExtensionFactory: Send + Sync + 'static {
    type Config: DeserializeOwned + JsonSchema + Send + Sync + 'static;

    fn id(&self) -> &'static str;
    fn build(&self, config: Self::Config) -> Result<Arc<dyn Plugin>, LoaderError>;
}
```

Factory 配置在构造前由 Loader 反序列化；每个 Factory 的 JSON Schema 可由管理界面从
`Loader::factories()` 读取。

```toml
# bootstrap.toml
version = 1
[config]
driver = "file"
path = "extensions.toml"
```

```toml
# extensions.toml
version = 1

[[extensions]]
instance = "primary-database"
factory = "postgres.connector"
enabled = true

[extensions.config]
url_env = "DATABASE_URL"
```

## Loader 管理服务

受信任应用代码或插件通过 `ctx.get(LOADER)` 获得 `Loader`。它支持 `entries`、`factories`、
`create`、`update`、`set_enabled`、`remove`、`reload` 和 `await_idle`。

- 文件来源下，变更先 reconcile，成功后原子写回完整 `extensions.toml`。
- 外部文件变更但未先 `reload()` 时返回 `LoaderControlError::ConfigConflict`。
- 写回失败返回 `LoaderControlError::Persist`，其中保留已生效的 `LoaderSnapshot`。
- `await_idle()` 只等待 Loader 当前 reconcile，不等待 Runtime 中无关插件或注入任务。
- 不支持动态库、第三方安装、Group、Include、HMR 或自动文件监听。
