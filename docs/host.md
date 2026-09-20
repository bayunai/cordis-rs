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

`bootstrap.toml` 与扩展清单都使用 v3：

```toml
# bootstrap.toml
version = 3
[config]
driver = "file"
path = "extensions.toml"
```

```toml
# extensions.toml
version = 3

[[extensions]]
id = "infrastructure"
name = "cordis:group"
group = true

[extensions.isolate]
database = true

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

普通条目使用 `id`、`name`、`config`、`disabled` 与可选 `inject` / `isolate`。Group 必须使用保留
`name = "cordis:group"` 和 `group = true`，其 `config` 是子条目数组。路径以 `:` 拼接，以上
示例的导出器路径为 `infrastructure:reports:exporter`。

**Group vs Include：**

| | Group | Include（`cordis-plugin-include`） |
| --- | --- | --- |
| 子项存放 | 内联在父条目 `config` 数组 | 独立 v3 `ExtensionsConfig` TOML 文件 |
| 注册 | Loader 内建 | Catalog 显式 `register(IncludeFactory)` |
| 持久化 | 写回父树文件 | 只写回 Include 自己的文件 |
| 重载 | 根 `Loader::reload()` | `loader.subtree("path")?.reload()` |

旧平面 v1 与 v2 不再被读取；需将配置显式迁移到 v3。

## Factory、inject 与 isolate

`name` 只匹配应用在 `host/catalog.rs` 编译期注册的 `ExtensionFactory`，不会加载动态库或第三方模块。
`Loader::factories()` 返回业务 Factory 的 JSON Schema。

除 Factory 外，宿主可注册可配置注入项与可隔离服务：

```rust
catalog.register_injection(InjectionDescriptor::intercepted(
    "database", DATABASE, DATABASE_OPTIONS,
))?;
catalog.register_isolation(IsolationDescriptor::new("database", DATABASE))?;
catalog.register_isolation(IsolationDescriptor::new("logger", LOGGER))?;
```

条目中 `inject = ["database"]` 仅声明必需服务；映射形式在声明依赖的同时为该条目 Context
写入强类型 `ConfigKey` 覆盖：

```toml
[extensions.inject]
database = { read_only = true }
```

未知服务、不可配置服务的映射值或不符合 Schema 的配置都会在 reconcile 前失败。

`inject = { ... }` **只覆盖 ConfigKey，不会隔离服务**：映射写入的 `intercept` 与所属
服务域共享同一 Service identity；同域内兄弟条目仍可互相 `get` 到对方的 `provide`。

Group 上的 `inject = { ... }` 会下传给整棵子树：子条目（及嵌套 Group）沿父链读取
Config 时可见该覆盖；嵌套 Group 再写同名 ConfigKey 时，子覆盖优先。

服务隔离只用 `isolate`（须先在 Catalog 登记 `IsolationDescriptor`）：

```toml
[extensions.isolate]
logger = true            # 本路径独占运行期标签
database = "team-ab"     # Loader 内按「ServiceId + 标签名」复用
```

- `true`：该路径对该服务独占隔离槽。
- 非空字符串：同 `(ServiceId, 标签名)` 跨条目/跨 Group 共享同一 `IsolationLabel`。
- `false`、空串、未登记服务名 → 预检失败，零生命周期变更。

挂载顺序：父 Context → 按条目 `isolate` 链式 overlay → 既有 `inject` intercept → 挂 Fiber。

## 服务域边界

Group **只**组织 EntryTree / 生命周期 / 父子 Context，**不再** `extend()` 造独立服务域。
未声明 `isolate` 的服务与父同槽：可共享，也可因重复 `provide` 得到 `ServiceConflict`。

| 位置 | 服务可见性 |
| --- | --- |
| 顶层普通 Entry | 默认共享 Loader 根服务域 |
| Group | 默认与父同域；仅声明的 `isolate` Key 被隔离 |
| 同域兄弟 | 互相可见对方的 `provide`；同 Key 重复 provide → `ServiceConflict` |
| 经 `isolate` 隔离的 Key | 仅同隔离槽内可见；跨槽互不可见 |

因此：

- 同级依赖（同顶层或同 Group、且未隔离该 Key）可以正常 `provide` → `inject` 激活。
- 需要跨 Group 隐藏或并存同一 `ServiceKey` 时，显式写 `isolate`。
- 两 Group 使用同一命名标签字符串时，跨组共享该服务槽；同标签重复 Provider 冲突。
- 同名标签绑定不同服务键时互不串槽。

普通 Entry **不是**默认服务边界；不要假设每个条目或每个 Group 有独立服务域。

## 管理 API 与可见性

`Loader` 提供 `entries()`、`factories()`、`injections()`、`isolations()`、`subtree()`、
`attach_file_subtree()`、`create()`、`update()`、`remove()`、`reload()` 和 `await_idle()`。
根控制面的 `create` / `update` / `remove` / `reload` 只操作根树；Include 内部用
`LoaderSubtree`（`loader.subtree("reports")`）。`create` / `update` 接收父 Group 路径与位置；
`update` 可以移动条目，移动按「旧路径后序释放 → 新路径前序挂载」处理（不做隐式原地迁移）。
**跨树移动一律拒绝**（`CrossTreeMove`）。

Loader reconcile 是**局部**的：仅创建、删除、移动、启停或配置/`inject`/`isolate` 变化的 Entry/子树
参与生命周期；无关 Entry 保留原 Fiber ID、Context 与服务。普通 Entry 仅 `config` 变化且
`inject`/`isolate`/父 Context 不变时走 `Fiber::replace`。Group 的 `inject` 或 `isolate` 变化会
重建该 Group 整棵子树；仅 `disabled` 变化时保留 Group Fiber/Context，按祖先禁用规则启停后代。
纯排序只更新 `LoaderSnapshot.entries` 前序与持久化文件顺序，**不**重启任何 Fiber。依赖关系仍须
通过 `inject` 声明，排序不是业务依赖表达方式。成功 reconcile 后会清理目标树不再引用的命名隔离标签。
全局 `await_idle()` / 快照按根树前序聚合，并在 Include 路径后插入对应子树。

## Include（可选 crate）

`cordis-plugin-include` 不是 Loader builtin。应用须：

```rust
catalog.register(IncludeFactory)?;
```

```toml
[[extensions]]
id = "reports"
name = "cordis:include"
[extensions.config]
path = "reports.toml"   # 相对父配置文件目录；或绝对路径
```

`reports.toml` 必须是完整 v3 `ExtensionsConfig`。子条目完整路径形如 `reports:importer`。
外部改动 Include 文件后必须 `loader.subtree("reports")?.reload()`；首版无文件监听、YAML/JSON、
patches 或 HMR。重复附着同一规范化文件路径、Include 祖先循环引用会在挂载前失败。

应用读取某一条目视图上的服务时使用路径：

```rust
let database = loader.entry_context("infrastructure:database")?.get(DATABASE)?;
```

同一服务域（或同一命名 isolate 槽）内的兄弟条目共享可见性，因此也可以从任一同域
`entry_context` 解析到该域内已 `Active` 的 Provider。

文件来源的变更先完成树预检与 reconcile，再原子写回完整 TOML。预检失败（未知 Factory、
`FactoryChanged`、`PluginKeyChanged`、无效 inject/isolate、非 v3 等）零运行时变更。生命周期失败
不回滚已触及节点、不提交 `desired`/revision、不由 Loader 发起写回；无关 Entry 继续运行。
失败后的 `LoaderSnapshot` 仍包含实际已插入的 Failed/Pending 节点，即使该次 reconcile
尚未来得及提交目标排序；对同一外部配置再次 `reload()` 会重试失败的启用 Entry。
文件被外部修改时返回 `ConfigConflict`；写回失败返回带已生效 `LoaderSnapshot` 的 `Persist`。
`await_idle()` 只等待 Loader 自己的本次 reconcile，不等待 Runtime 的无关工作。
