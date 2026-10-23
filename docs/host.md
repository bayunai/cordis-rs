# `cordis-loader`：静态 EntryTree Host

`cordis-host` 只创建和关闭 Runtime。`cordis-loader` 的 `LoaderPlugin` 是应用显式挂载的
普通插件：它从静态 Catalog 构造业务插件，并把配置声明收敛为一棵可管理的 EntryTree。
Loader 根树只在内存中；根文件和嵌套文件均由显式登记的 Include 来源树承载。

```rust
let host = CordisHost::new()?;
let mut catalog = build_catalog()?;
catalog.register(cordis_plugin_include::IncludeFactory)?;
let plugin = LoaderPlugin::new(catalog);
let _loader_fiber = host.root().plugin(Arc::new(plugin)).await?;
let loader = host.root().get(LOADER)?;
loader.create(cordis_plugin_include::include_root_entry("include", "cordis.toml")?, None, None).await?;
```

保留 `_loader_fiber` 直到应用关闭；释放它会撤销 `LOADER` 并后序释放整棵条目树。
Loader 挂载后即为空树 Active；启动条目与运行期条目统一使用 `create()`。配置预检或业务
挂载失败由该调用返回，不升级为 Loader 自身挂载失败。内存根保留已通过预检但业务挂载失败的
声明供修复；来源树仍沿用生命周期失败不写回的规则。应用入口自行决定遇到必需条目失败时
退出，或明确报告错误并继续提供管理界面，不得把 Failed/Pending 快照表述为全部启动成功。

## 配置树

Host 用 `include_root_entry` 直挂根 v3 文档（约定名 `cordis.toml`）。产品路径常为空根，
业务 Entry 由 config-editor 的 deploy ∘ user ∘ home 经 Include `patches` 同步；测试与示例也可把
条目直接写进该文件。文档结构：

```toml
# cordis.toml
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
示例的导出器路径为 `include:infrastructure:reports:exporter`；文件内 ID 仍是原来的局部 ID。

**Group vs Include：**

| | Group | Include（`cordis-plugin-include`） |
| --- | --- | --- |
| 子项存放 | 内联在父条目 `config` 数组 | 独立 v3 TOML／JSON／YAML 文档 |
| 注册 | Loader 内建 | Catalog 显式 `register(IncludeFactory)` |
| 持久化 | 由所属树来源决定；内存树不落盘 | 只写回 Include 自己的文件 |
| 重载 | 重载所属来源树 | `loader.subtree("include:reports")?.reload()` |

旧平面 v1 与 v2 不再被读取；需将配置显式迁移到 v3。

## Factory、inject 与 isolate

`name` 只匹配应用在 `host/catalog.rs` 编译期注册的 `ExtensionFactory`，不会加载动态库或第三方模块。
`Loader::factories()` 返回业务 Factory 的 JSON Schema。

所有包统一使用 `register()`，Factory 的 `prepare()` 返回普通或热配置预备结果。
Include 使用专用的 `PreparedExtension::tree()` 来源树载体结果，不修改 Core 生命周期接口。
完整输入 Schema 供管理界面使用；`ConfigRef`、revision 和写入端不进入 TOML 或 Schema。
普通插件不声明空热配置；只有 `PreparedExtension::volatile()` 创建热绑定。
所有根树和 Include 子树的 `update/reload` 都经过相同的预检与局部 reconcile。
仅普通配置相同、热配置变化且原 Fiber 仍 Active、上下文未变化时，才原地提交；
所有生命周期操作成功后再提交热值，提交前再次核对 Fiber 身份与绑定。
失败时目标配置与文件 revision 不提交；已经发生的生命周期操作或热提交不回滚。
写回失败沿用 `Persist`，明确报告已生效、未保存。

热配置 revision 从零开始，仅实际值变化递增。替换、禁用恢复、移动或 Group 重建为新实例
创建新引用；无关条目不受影响。`await_idle()` 仅等待 Loader 完成提交，不等待订阅者。
不自动监听文件；将来监听/HMR 插件可复用显式 reload 路径。

本次 Factory API 直接收敛为 `prepare()` 与 `PreparedExtension`；工作区内调用方已迁移，
外部应用须同步修改 Factory 实现与登记入口。TOML 格式不变，不提供旧接口兼容层。

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
`attach_subtree()`、`create()`、`update()`、`remove()`、`reload()` 和 `await_idle()`。
`attach_subtree()` 接收通用 `EntryTreeSource`，不接收文件路径。根控制面的 CRUD 只操作内存根树，
不写文件；内存根没有来源可 reload。Include 文件内部用 `LoaderSubtree`
（`loader.subtree("include")` 或 `loader.subtree("include:reports")`），并传入该文件内的局部路径。
来源的读取、版本检查、写入预检及持久化均为异步操作；Loader 在所属树的操作锁内等待完成。
Include 将文件事务放入阻塞任务，文件 I/O 不占用 Tokio 异步工作线程。
`create` / `update` 接收父 Group 路径与位置；
`update` 可以移动条目，移动按「旧路径后序释放 → 新路径前序挂载」处理（不做隐式原地迁移）。
**跨树移动一律拒绝**（`CrossTreeMove`）。

Loader reconcile 是**局部**的：仅创建、删除、移动、启停或配置/`inject`/`isolate` 变化的 Entry/子树
参与生命周期；无关 Entry 保留原 Fiber ID、Context 与服务。普通 Entry 仅 `config` 变化且
`inject`/`isolate`/父 Context 不变时走 `Fiber::replace`。Group 的 `inject` 或 `isolate` 变化会
重建该 Group 整棵子树；仅 `disabled` 变化时保留 Group Fiber/Context，按祖先禁用规则启停后代。
纯排序只更新 `LoaderSnapshot.entries` 前序与持久化文件顺序，**不**重启任何 Fiber。依赖关系仍须
通过 `inject` 声明，排序不是业务依赖表达方式。成功 reconcile 后会清理目标树不再引用的命名隔离标签。
全局 `await_idle()` / 快照按根树前序聚合，并在 Include 路径后插入对应子树。

### 面向管理界面的配置控制面

Host 在 Loader（及 Include 根）之后显式挂载
[`ConfigEditorPlugin`](../crates/cordis-plugin-config-editor) 与
[`SettingsPlugin`](../crates/cordis-plugin-settings)。分层见 [配置控制面拆分](config-surface.md)。
Loader **不**提供 `describe_config` / `mutate_config`；表单只走 `SETTINGS` → config-editor
**user** 层。运维整表写回走 `CONFIG_EDITOR.commit_source`（其下为 `commit_entry_config`）。

`settings.describe(path).await` 返回 `EntryConfigView`：完整路径、Factory ID、管理 Schema、
脱敏 `base` / `user` / `value`（effective）、秘密字段存在状态与 `ConfigRevision`。
`base` 为有序 deploy patch 组成结果；`user` 为本机覆盖；`value` = deploy ∘ user ∘ home。
Group 仅是树结构，不提供配置表单。Schema 来自静态 Catalog，因此禁用、Pending、Failed 条目
都能描述；读取不会反序列化或构造插件，也不会要求缺失必填字段的禁用条目先启用。

`settings.mutate(path, expected_revision, operations).await` 在 **user** 上拼装有序字段操作，
委托 config-editor 写入本机 user patch 并 reconcile；**不** bake 空根或各插件 deploy。
若 home 盖住导致 effective ≠ 目标则拒绝。`Set` / `Unset` 的路径是配置表的字符串键序列，不含
Entry 控制字段；空路径、穿过标量或数组、删除不存在的键明确报 `InvalidConfigPath`。
即使条目禁用，也执行反序列化与 `prepare()`，但不执行延迟构造。嵌套 Include 子树首版不可经此路径编辑。

版本令牌包含随机树实例身份与协调代次，支持 `Display` / `FromStr`，不代表授权。
应用负责自己的 HTTP DTO、身份验证和权限检查。版本检查失败返回
`ConfigRevisionConflict`。所属树的协调一旦启动，即使生命周期失败，也会让旧令牌失效；
同树无关条目的修改也会导致保守冲突。重新读取描述后再编辑，不自动重试写操作。
配置编辑一经提交便由独立任务完成；取消调用方的等待不会取消已接受的编辑或跳过
user 文档写回。Host 须注入有序 `deploy_patch_paths`、`user_patch_path`、`home_patch_path` 与
`config_dir`。

表单路径约定为「先原子写 user，再 reconcile」；校验或应用失败回滚 user 文档。
`commit_source` 仍沿用「运行时先应用，再写回来源」并可能产生 `Persist`。操作返回
错误后应重新读取描述和诊断快照，不能将原来的表单认定为仍然有效。

Config 字段通过 `#[schemars(extend("writeOnly" = true))]` 标记秘密。
描述中删除秘密值，`secrets` 的路径与 `is_set` 仅表示原始配置中是否存在该字段。
未提交的秘密保留；Set 明确替换；Unset 明确删除并重新校验完整配置。替换父对象或数组
就是替换整个值，不自动恢复被删除的秘密。数组元素含秘密时，整个数组被隐藏；根配置
整体标记 writeOnly 时整份配置隐藏，但空路径仍不可操作，建议将标记放到实际字段上。

管理 Schema 保守合并组合分支的秘密标记，展开本地 JSON Pointer 引用，并移除秘密及其
祖先相关的 `default`、`examples`、`const`、`enum` 载荷。它是安全表单描述，不是可替代
Factory 校验的完整验证器。远程、动态、循环引用、改变作用域的 `$id`，以及无法安全投影
的秘密属性模式等返回 `SecretSchema`，不静默忽略。无需另建秘密字段清单或配置存储。

`entries()`、`factories()`、`update()` 以及 config-editor 的 `read` / Loader
`snapshot_entry_config` 是受信任接口，可能包含明文配置或原始 Schema，不能原样发送到浏览器。
表单路径不会把反序列化、prepare、构造和比较中的原始错误文本作为校验错误返回；插件仍不得在
生命周期错误、panic、日志或 Schema 文字说明中拼接秘密。Loader 无法替业务代码清洗任意日志或
进程 panic hook 输出。

可运行示例：`cargo run -p cordis-plugin-include --example config_control`。示例使用临时文件，
演示禁用实例先配置、Token 脱敏与应用层版本字符串往返，不启动 HTTP 服务。

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

`reports.toml` 必须是完整 v3 `ExtensionsConfig`。若上述条目位于根 include 文件树，子条目完整路径为
`include:reports:importer`。外部改动文件后使用 `loader.subtree("include:reports")?.reload()`。
Include 支持 TOML／JSON／YAML、显式 initial 和 patches，不提供自动监听或代码 HMR。
规范化路径确定来源身份，符号链接别名也视为同一文件；重复来源与祖先循环明确失败。
文件路径、初始文件创建、0600 原子写回及完整迁移说明见 [Include 文件配置树](include.md)。

应用读取某一条目视图上的服务时使用路径：

```rust
let database = loader.entry_context("include:infrastructure:database")?.get(DATABASE)?;
```

同一服务域（或同一命名 isolate 槽）内的兄弟条目共享可见性，因此也可以从任一同域
`entry_context` 解析到该域内已 `Active` 的 Provider。

文件来源的变更先完成文件版本及写回往返检查、树预检与 reconcile，再委托 Include 原子写回。
不把有效树反向拆成基础层，也不另存用户 patch。预检失败（未知 Factory、
`FactoryChanged`、`PluginKeyChanged`、无效 inject/isolate、非 v3 等）零运行时变更。生命周期失败
不回滚已触及节点、不提交 `desired`/revision、不由 Loader 发起写回；无关 Entry 继续运行。
本轮继续尝试其他独立条目，逐项记录业务生命周期错误，控制调用返回首个错误；有失败时不提交待更新热值。
内存根通过 `create()` 登记且完整预检通过的声明会保留，业务挂载失败由调用返回并标记为 Failed。
Include 子树初次加载同样保留通过预检的初始声明，不撤销树控制面或无关插件。
根 `entries()` 返回通过 `create()` 登记的声明或后续成功更新的声明；失败更新不提交新目标。
来源子树 `entries()` 返回初始加载或后续成功提交的声明，
两者都不代表全部条目已 Active。
配置、Factory 预检和内部协调错误仍明确报错；父挂载上下文无效时停止其后代挂载，不转挂到根节点。
失败后的 `LoaderSnapshot` 仍包含实际已插入的 Failed/Pending 节点，即使该次 reconcile
尚未来得及提交目标排序；对同一外部配置再次 `reload()` 会重试失败的启用 Entry。
来源被外部修改时返回明确冲突；写回失败返回带已生效 `LoaderSnapshot` 的 `Persist`。
`await_idle()` 等待 Loader 自己的本次 reconcile，以及 EntryTree 受管 Fiber 的 Loading / Unloading 收敛；不等待 Runtime 上与本 Loader 无关的工作，也不等待仍缺依赖的 Pending。
它返回实际状态，包括 Failed/Pending；业务生命周期错误由发起变更的返回值、条目诊断及日志报告。
Loader 关闭及内部协调错误仍返回错误。应用可另外检查自身必需条目的 Active 状态，决定是否继续启动。
