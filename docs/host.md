# cordis-loader 与 cordis-host

`cordis-loader` 是静态 Catalog 的插件管理库：读取严格 TOML 配置、构造插件实例、执行
reconcile，并把 `Loader` 管理服务注册到根 Context。`cordis-host` 是应用进程的薄外壳：它只持有
一个 `CordisLoader`，供 `main.rs` 启动、访问根 Context 与受控关闭。

`cordis-core` 只负责 Plugin、Fiber、Service、Effect 与生命周期。Loader 不加载动态库，不提供
HTTP 管理接口或业务协议；管理界面通过 `Loader` 服务自行接入。

## 应用 Host 约定

一个独立运行的可执行程序只创建一个 `CordisHost`；它持有该进程唯一的
`cordis_core::Runtime`。插件不是 Host，不能自行创建 Runtime、读取扩展清单或决定其他
插件是否启用。

每个应用按下列边界组织代码：

| 位置 | 推荐职责 | 不推荐事项 |
| --- | --- | --- |
| `main.rs` | 组装工厂目录、调用 `CordisHost::bootstrap`、等待应用退出、调用 `shutdown` | 逐个 `Context::plugin(...)` 挂载业务插件 |
| `host/catalog.rs` | 显式登记该可执行程序编译进来的全部 `ExtensionFactory` | 根据配置决定启停或在这里执行 I/O |
| `bootstrap.toml` | 指向运行期扩展清单 | 保存插件业务配置、路由或凭证 |
| `extensions.toml` | 按 `instance` 选择启用的工厂，并保存该实例配置 | 引用目录外、未编译进程序的 factory |
| 插件 crate | 实现 `Plugin`，声明依赖并在 `apply` 中注册资源 | 创建 Host / Runtime，或读取 `extensions.toml` |

这里的“显式登记”是**编译期可用目录**，不是启动命令：Host 必须知道 Rust 类型如何被构造；
是否实际运行由 `extensions.toml` 的 `enabled` 决定。业务插件由 `CordisLoader` 的 reconcile
进入 Runtime；直接调用 `Context::plugin` 属于受信任应用代码的显式运行时操作。

可执行且可整体复制的模板见
[`host_bootstrap`](../crates/cordis-host/examples/host_bootstrap/main.rs)：

```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host = CordisHost::bootstrap(build_catalog(), "bootstrap.toml").await?;

    // 应用自己的进程级工作；业务插件已经由 extensions.toml 挂载。
    run_application(&host).await?;

    host.shutdown().await?;
    Ok(())
}
```

同一产品中的 HTTP、桌面窗口、数据库、侧边栏等能力应登记在同一个 Host 目录并由同一份
扩展清单编排。另一个可执行程序即使复用某些插件，也应创建自己的 Catalog、`bootstrap.toml`
与 `CordisHost`，因为它拥有独立进程生命周期和运行期配置。

复制模板时只做三类修改：在 `host/catalog.rs` 增删 Factory 注册，在 `plugins/` 增删业务
插件 crate 的接入，在 `extensions.toml` 调整实例启停和参数。通常不应把业务插件挂载逻辑移回
`main.rs`，也不应让插件自行读取扩展清单；需要手动编排时，应用可直接使用完整 Context。

应用层通过 `host.root()` 获取完整的 `Context`，并可通过 `host.runtime()` 获取完整 Runtime。
这与 Cordis 的进程内信任模型一致：拿到 Host 的代码可以读取或提供服务、注册或派发事件、
创建派生 Context，以及直接挂载、替换或卸载插件。`extensions.toml` 与 `reconcile` 仍是应用
推荐的声明式管理路径；调用方若直接操作运行时，须自行保证与 Host 配置和实例映射的一致性。

## 启动链

```text
应用入口
  → 读取 bootstrap.toml
  → 创建 ExtensionCatalog
  → CordisHost 创建 CordisLoader / Runtime
  → 读取 extensions.toml
  → 全量校验并构造插件
  → reconcile：挂载 / replace / 卸载
  → diagnostics
  → shutdown
```

相对路径相对于 `bootstrap.toml` 所在目录解析。重新加载只能由显式 `reload()` 触发。

## 身份

Host 区分三个身份，Core 只认最后一个：

| 身份 | 含义 | 例子 |
| --- | --- | --- |
| `factory` | 插件类型 | `postgres.connector` |
| `instance` | 配置实例（Host 主键） | `primary-database` |
| `PluginKey` | 构造后由 `Plugin::key()` 给出的运行时身份 | 常与 factory 相同 |

同一 factory 的多个 instance 通常共享 `PluginKey`。卸载单个 instance 使用
`Fiber::dispose_wait`，不会调用 `Runtime::unmount(key)`（后者会拆掉该 Key 下全部 Fiber）。

## 显式工厂

```rust
pub trait ExtensionFactory: Send + Sync + 'static {
    type Config: DeserializeOwned + JsonSchema + Send + Sync + 'static;

    fn id(&self) -> &'static str;

    fn build(&self, config: Self::Config) -> Result<Arc<dyn Plugin>, LoaderError>;
}
```

- Loader 在调用 `build` 前将 `extensions.config` 反序列化为 `Config`；字段缺失、类型不符和
  未知字段都会在挂载前失败。
- `Config` 同时生成 JSON Schema，管理界面可通过 `Loader::factories()` 自动生成表单。
- 通过 `ExtensionCatalog::register(MyFactory)` 显式登记；重复 `id` 失败且不覆盖。
- 不使用自动注册宏或 `inventory`。
- `build` 必须无副作用（禁止 I/O / spawn）；副作用只属于 `Plugin::apply`。
- 确有自由结构配置需求时，Factory 可显式使用 `type Config = serde_json::Value`；它仍会生成
  可表达任意 JSON 值的 Schema，TOML 输入由 Loader 反序列化后传入。

## Loader 服务

`Loader` 是静态 Catalog 的运行期管理控制面。`CordisLoader` 创建时将它注册为根服务；应用可用
`host.loader()` 取得它，进程内插件可通过 `ctx.get(LOADER)` 取得同一控制面。若应用不需要
`CordisHost` 外壳，也可以直接创建 `CordisLoader` 并调用其 `apply`、`reload` 与 `shutdown`。

```rust
let loader = host.loader();
let factories = loader.factories()?; // Factory id + JSON Schema
loader.create(ExtensionEntry::new("primary-db", "app.sqlite", true)).await?;
loader.set_enabled("primary-db", false).await?;
```

- `entries`、`factories` 用于管理界面读取当前条目与可创建类型。
- `create`、`update`、`set_enabled`、`remove` 和 `reload` 是唯一的 Loader 管理操作；更新不可更换 Factory。
- Loader 仅管理编译进静态 Catalog 的 Factory；不加载动态库、第三方模块或嵌套插件树。
- 变更先经过完整 reconcile，成功后才原子写回 `extensions.toml`。写入失败会返回
  `LoaderControlError::Persist`，其中携带已经生效的 `LoaderSnapshot`。
- Loader 会拒绝未先 `reload` 的外部文件修改，避免管理界面静默覆盖人工编辑。

## 配置合同

全部结构 `deny_unknown_fields`。必填字段缺失直接失败；禁止默认值掩盖配置错误。

`bootstrap.toml` 只保留已实现的启动锚点：

```toml
version = 1

[config]
driver = "file"
path = "extensions.toml"
```

- `version` 必须为 `1`
- `driver` 仅接受 `file`
- `path` 相对 bootstrap 文件目录解析

运行配置：

```toml
version = 1

[[extensions]]
instance = "primary-database"
factory = "postgres.connector"
enabled = true

[extensions.config]
url_env = "DATABASE_URL"
max_connections = 20
```

规则：

- `extensions` 字段必填；缺少该字段解析失败，不得默认为空数组
- 有意清空全部实例时必须显式写 `extensions = []`
- `instance` / `factory` / `enabled` 必填；`enabled` 无默认
- `config` 缺省视为空表，由 factory 决定字段是否必填
- `instance` 必须唯一
- `factory` 必须已注册（含 `enabled = false` 的条目）
- 整份配置及全部新增 / 变更插件必须预构造成功，之后才能进入变更阶段
- 同一 `instance` 不允许更换 `factory`
- 同一 `instance` 预构造后的 `PluginKey` 若与已挂载 Fiber 不同，视为预检失败

## Reconcile

固定顺序：

1. 解析并验证完整目标配置
2. 计算 unchanged / remove / replace / add
3. 构造全部新增和变更插件
4. 逆序卸载已删除实例（`dispose_wait`）
5. 对同实例、同 `PluginKey` 执行 `Fiber::replace`
6. 按配置顺序挂载新增实例
7. `Runtime::settle`
8. 生成结果快照

编排由独立于 `apply()` / `reload()` 调用者 Future 的协调器执行：调用方取消只取消等待，
不中断收敛；Loader 的 `instances` / `order` / `config` 在每步完成后提交。同一时刻只允许
一个 reconcile；并发 `apply` / `reload` 返回 `ReconcileBusy`。`shutdown()` 会先等待在途
reconcile 收敛，再关闭 Runtime。

约束：

- 预检失败：零修改
- 生命周期操作失败：立即返回明确错误
- 不自动回滚旧版本
- 不自动重试
- 相同配置 `reload` 不触发 restart / replace
- `enabled = false` 或从文件中删除：卸载并等待 async disposer
- `replace` / 首次挂载后 Fiber 进入 `Failed`：视为已提交，旧实例不恢复
- 缺依赖插件保持 `Pending`，依赖出现后由 Core 调度为 `Active`

## 诊断

`LoaderSnapshot` 将 `InstanceId`、`factory`、`PluginKey`、Fiber ID 与 `FiberState` 关联起来，
并嵌入 `Runtime::diagnostics()`。

## 暂不实现

- 原生动态库加载
- WASM / 子进程插件
- 扩展包签名和信任公钥
- SQLite / PostgreSQL 配置源
- 文件监听和自动热更新
- HTTP 管理接口
- Plugin 宏
- 自动恢复、回滚和兼容配置
