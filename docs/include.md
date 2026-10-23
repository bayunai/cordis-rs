# Include 文件配置树

`cordis-plugin-include` 是可选的文件来源插件，不是 Loader 内建文件驱动。
它负责路径与格式解析、initial、patches、来源版本检查和原子写回。
`cordis-loader` 只处理内存条目树、Factory 预检、生命周期协调及热配置提交，通过
`EntryTreeSource` 委托实际来源操作；`cordis-core` 不感知文件配置。

## 显式挂载根文件

```rust
use cordis_loader::{ExtensionCatalog, LOADER, LoaderPlugin};
use cordis_plugin_include::{IncludeFactory, include_root_entry};

let mut catalog = ExtensionCatalog::new();
catalog.register(IncludeFactory)?;
// 继续登记本应用的业务 Factory。
let plugin = LoaderPlugin::new(catalog);
let _loader_fiber = host.root().plugin(std::sync::Arc::new(plugin)).await?;
let loader = host.root().get(LOADER)?;
loader.create(include_root_entry("include", "cordis.toml")?, None, None).await?;
```

根文档是 v3 `version` / `extensions` 清单（约定名 `cordis.toml`），由 Include 直接读取，
不经独立 bootstrap 配方、也不交给 Loader 解析。产品路径常为空根，业务 Entry 见
[config-surface.md](config-surface.md)。

`include_root_entry` 的 ID 必须由 Host 显式传入。示例统一使用 `include`，因此文件中的 `worker`
完整路径为 `include:worker`；嵌套文件 `reports` 中的 `importer` 为 `include:reports:importer`。
这是实际路径变化，不提供无前缀别名。文件内部保存的局部 ID 不增加前缀。
`include` 是应用启动层选择的稳定 ID，不是 Loader 保留名称；应用传入其他 ID 时，
完整路径使用该 ID 作为前缀，Loader 不特殊识别或省略根 Include。

`include_root_entry` 将路径解析为绝对路径。内存根本身没有文件目录上下文，
因此直接构造根 IncludeConfig 时必须使用绝对路径。嵌套 Include 的相对路径则以承载它的
父配置文件目录解析，不以进程工作目录解析。

## 文档与配置

Include 按扩展名选择格式：`.toml`、`.json`、`.yaml`、`.yml`。每种格式都使用同一份 v3
`version` / `extensions` 文档结构；不会在解析失败后尝试另一种格式。JSON 和 YAML 不是
另一套配置模型：字段值必须能无损表示为当前 Entry 使用的 TOML 值。无法表示的 null、
键类型、数值和不支持的 YAML 标签明确报错，不执行 JavaScript 或动态模块。

下例是父文件中的一个 Include 条目：

```toml
[[extensions]]
id = "reports"
name = "cordis:include"

[extensions.config]
path = "reports.yaml"
initial = []
enable_logs = true
```

IncludeConfig 的字段为：

| 字段 | 含义 |
| --- | --- |
| `path` | 文件路径；根使用绝对路径，嵌套相对所属文件目录 |
| `initial` | 可选初始条目列表，仅在文件确实不存在时创建完整 v3 文档 |
| `patches` | 按顺序执行的插入或覆盖列表，默认为空 |
| `enable_logs` | 显式设置日志策略；未设置时继承上级 Include，根默认关闭 |

`initial = []` 表示显式允许创建空配置文件，不等于未提供 initial。已有文件为空、格式非法、
无法访问时不会被 initial 覆盖。初始文件的父目录必须已经存在；并发创建时不得覆盖另一方
已经创建的文件。既有目录权限不会被插件修改。

解析后的来源上下文通过 `INCLUDE_LOCATION` 提供 `directory` 与 `enable_logs`。
Loader 的 `ENTRY_LOCATION` 只提供条目与所属树的位置，不再携带文件目录。

## 顺序 patches

每个 patch 可包含 `id`、`insert`、`name`、`config`、`disabled`、`group`、`inject`、`isolate`。
`id` 使用当前文件内的完整局部路径，例如 `workers:sender`，不带外层 `include:` 前缀。

- 含 insert：无 id 时插入文件根；有 id 时必须定位到 Group。不能同时包含字段覆盖。
- 不含 insert：必须有 id，覆盖该条目的指定字段；未指定字段保持原声明。
- name 若存在，只校验目标 Factory 身份，不用来改名。
- patch 依次执行；后面的 patch 可以引用前面插入的条目。
- 缺失目标、Factory 名称不符、非 Group 插入、重复 ID、非法覆盖和未知字段均明确失败。

例如，父文件里为 reports 来源中的 worker 覆盖配置：

```toml
[[extensions.config.patches]]
id = "worker"
name = "acme.worker"

[extensions.config.patches.config]
timeout_ms = 1000
```

patches 是 Include 自身的配置，不是在被读取文档中增加第二个顶层区域，也不是产品级
profile 继承模型。插件状态与来源更新失败会明确返回，不能把警告后跳过当作成功。
`initial` 和 `patches` 可嵌入任意业务插件的秘密配置，因此它们在管理 Schema 中整体标记
writeOnly；安全配置描述仅报告是否设置，不回传原值。受信任后端的原始声明不受此投影影响。

## 编辑、重载与写回

```rust
let loader = host.root().get(cordis_loader::LOADER)?;
let file_tree = loader.subtree("include")?;
let declarations = file_tree.entries()?;
let snapshot = file_tree.reload().await?;
let settings = host.root().get(cordis_plugin_settings::SETTINGS)?;
let form = settings.describe("include:worker").await?;
```

根 Loader 的 `create` / `update` / `remove` 接受完整路径，按路径定位所属来源树并写回该文件；
纯内存根条目的变更不落盘。也可用 `LoaderSubtree` 与文件内局部路径。配置描述和安全字段编辑经
settings / config-editor，同样使用完整路径。根 Include 自身仍是应用提供的内存
启动声明；需要持久化该启动组合时应由应用明确保存。

Include 写回的是**当前有效树**。它不反向恢复原始基础层、不自动清空 patches，也不另存
用户覆盖文件。已有覆盖可能因此固化进原文件。独立保存用户 patch 与整表提交同属
config-editor 路径，不能与受信任的明文 `update` 混用。

持久化前先检查来源版本与写回往返：将目标声明序列化，再读取并应用同一组 patches，
必须得到相同且合法的目标声明。若插入项重复或固定覆盖使目标不能保持，则明确拒绝，
不会静默去重、抹掉 patches 或自动改父文件。

同路径只改 patches、initial 或日志策略时，控制调用等待当前子树局部协调；Include Fiber
和无关子实例保留。修改文件路径或父上下文变化时重新挂载来源树，旧句柄和旧版本失效。
reload 显式重新读取文件，不启动自动监听；未来监听／HMR 插件应调用同一 reload 路径。
来源版本和有效声明均未变化，且实际树已收敛时，reload 只读取验证，不再次调用 Factory、
推进配置 revision 或发布生命周期变更通知。Failed、手动卸载、缺失实例或未完成来源绑定
仍由 reload 重试；正常等待依赖的 Pending 不单独触发重建。来源读取不缓存，文件错误仍明确返回。
多个来源定义更新分别协调；业务生命周期失败不会阻止其他独立来源定义更新，但父声明
不会整体提交。不提供跨来源事务，已经生效的局部更新不回滚，失败来源的相同目标可重试。

## 提交与释放边界

来源接口的 `read`、`check_revision`、`validate_write`、`write` 均为异步方法。
`identity` 与 `context` 只访问已解析的内存元数据，不执行文件 I/O。来源构造函数接收拥有的
`Context` 并返回 `Send + 'static` Future，Loader 在异步预检和协调中显式等待来源准备完成。
Include 将路径规范化、读取与解析、版本检查、写回验证及原子文件事务放入 `spawn_blocking`，
避免阻塞 Tokio 异步工作线程；每次处理完整工作单元，不逐个系统调用切换任务。

来源的同步 `enable_logs()` 只读取内存策略。Include 的 `enable_logs` 同时控制读取／写回和
所属树普通条目的挂载、重载、卸载信息日志，包含依赖变化后的卸载与再激活；Group 不输出
普通插件生命周期日志，无变化 reload 不输出生命周期操作。日志不包含配置正文；失败诊断
不受开关屏蔽。嵌套 Include 继续继承父策略，显式值覆盖继承值，策略仅在成功提交后更新。
生命周期事件流是有损通道：发生滞后时无条件警告丢失数量，丢弃过时积压并按实际 Fiber
重建日志去重基线，同时刷新诊断快照和稳定等待者。不补造丢失历史，不宣称日志完整。

每棵树使用同一操作锁完成版本检查、预检、协调与来源写回。Include 在实际写入前再次检查
来源版本，使用同目录临时文件、同步和原子替换；Unix 创建与替换文件为 0600，不生成备份。
符号链接规范化后共享同一来源身份，重复挂载和祖先循环会明确拒绝。
临时文件只有在本次 `create_new` 成功后才可清理；重名打开失败不删除已有文件。
操作与清理同时失败时保留原始错误分类，并报告两种原因，不扫描删除其他临时文件。
对外 `await_idle()` 也等待已进入操作锁的来源预检与写回，不在异步读取期间提前返回稳定快照。

管理写入在生命周期操作前实时预判权限：尊重文件只读标记，以不截断的写打开检查文件权限，
并在同目录创建和清理唯一的 0600 探测文件。不修改目标或父目录权限，不永久缓存只读状态；
已知权限拒绝返回 `ReadOnly`，其他文件异常明确报错。只读来源仍允许读取、挂载和 reload。
实际写回前会再次检查，但预判不能保证之后的 ACL、权限、文件占用或文件系统状态不变；
协调后才发生的写回失败仍返回 `Persist` 和实际运行快照，不回滚运行时。

运行时先应用，再写文件。生命周期失败不写回，但已经触及的实例不会回滚；写回失败返回
`Persist` 与实际运行快照，不能解释为从未执行。初始业务子条目失败保留声明和 Failed 状态，
不撤销 Include 控制面或无关插件；文件、格式、来源登记等错误落在对应 Include 条目。

取消调用方等待不会中断已接受提交。已经开始的阻塞任务不可强制取消，不会另起无人结算的
后台写回；阻塞任务异常退出会明确返回脱敏的来源错误，不自动重试。停用 Include 时拒绝新操作，等待已执行提交结算，
后序释放嵌套树并注销来源。释放按树实例及实际父子关系认领，不按路径前缀寻找后代；
同一实例的重复或并发 `detach()` 共享释放结果，取消等待不取消清理。即使同路径已重建，
旧句柄也只返回旧实例的结果，不操作新实例。释放失败仍尝试其余目标，重复调用保留原失败。
秘密字段的安全描述仍由配置
控制面处理；文件权限不是加密，业务插件不得把配置正文或凭据放入错误、日志与 panic。

## 破坏性 API 迁移

- `LoaderPlugin::new(catalog)` 返回空 Loader 插件，不再接收初始声明，也不返回 `Result`。
  挂载后显式调用 `loader.create(entry, None, None).await`；启动与运行期只有这一创建入口。
- 删除 `LoaderPlugin::bootstrap` 与 Include 的 `bootstrap.toml` / `bootstrap_entry` / `load_bootstrap`；
  根文件条目由同步 `include_root_entry("include", "cordis.toml")?` 生成后交给 `create()`。
- 文件 read 辅助函数改从 `cordis_plugin_include` 导入，不保留 Loader 重导出。
- `FileSource::open` / `from_config`、`load_extensions` 为异步，调用方须使用 `.await`；
  `include_root_entry` 为同步，不提供旧 bootstrap API。
- 自定义 `EntryTreeSource` 使用 `async_trait` 实现异步读写方法；`PreparedExtension::tree` 的来源构造闭包改为接收拥有的 `Context` 并返回异步结果。
- 自定义来源必须实现纯内存的 `enable_logs()`，明确是否输出所属条目的生命周期信息日志。
- 删除 `attach_file_subtree`；自定义来源使用通用 `attach_subtree`，不把文件路径交给 Loader。
- 删除 `EntryLocation.source_dir`；文件目录从 Include 的 `INCLUDE_LOCATION` 读取。
- 所有文件条目服务查询、路由参数与配置描述增加实际 Include 前缀；文件 CRUD 可用完整路径
  （Loader 自动路由）或 `LoaderSubtree` 局部路径。
- 外部工作台和网关需要独立迁移；本仓库不替它们增加兼容入口或旧路径别名。

运行示例：

```bash
cargo run -p cordis-host --example host_bootstrap
cargo run -p cordis-plugin-include --example volatile_config
cargo run -p cordis-plugin-include --example config_control
cargo run -p cordis-core --example plugin_stack
```

Host 与 Core 的这些示例仅通过 dev-dependencies 使用 Include；它们的生产依赖边界不变。
