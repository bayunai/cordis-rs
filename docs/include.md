# Include 文件配置树

`cordis-plugin-include` 是可选的文件来源插件，不是 Loader 内建文件驱动。
它负责 bootstrap 读取、路径与格式解析、initial、patches、来源版本检查和原子写回。
`cordis-loader` 只处理内存条目树、Factory 预检、生命周期协调及热配置提交，通过
`EntryTreeSource` 委托实际来源操作；`cordis-core` 不感知文件配置。

## 显式挂载根文件

```rust
use cordis_loader::{CONFIG_VERSION, ExtensionCatalog, ExtensionsConfig, LoaderPlugin};
use cordis_plugin_include::{IncludeFactory, bootstrap_entry};

let mut catalog = ExtensionCatalog::new();
catalog.register(IncludeFactory)?;
// 继续登记本应用的业务 Factory。
let plugin = LoaderPlugin::new(catalog, ExtensionsConfig {
    version: CONFIG_VERSION,
    extensions: vec![bootstrap_entry("include", "bootstrap.toml").await?],
})?;
let _loader_fiber = host.root().plugin(std::sync::Arc::new(plugin)).await?;
```

启动文件仍是极小 v3 文档，由 Include 读取，不交给 Loader 解析：

```toml
version = 3
[config]
driver = "file"
path = "extensions.toml"
```

`bootstrap_entry` 的 ID 必须由 Host 显式传入。示例统一使用 `include`，因此文件中的 `worker`
完整路径为 `include:worker`；嵌套文件 `reports` 中的 `importer` 为 `include:reports:importer`。
这是实际路径变化，不提供无前缀别名。文件内部保存的局部 ID 不增加前缀。
`include` 是应用启动层选择的稳定 ID，不是 Loader 保留名称；应用传入其他 ID 时，
完整路径使用该 ID 作为前缀，Loader 不特殊识别或省略根 Include。

`bootstrap_entry` 将启动文件的相对 path 转为绝对路径。内存根本身没有文件目录上下文，
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
let form = loader.describe_config("include:worker").await?;
```

根 Loader 的 create/update/remove 只修改内存声明，不会保存 bootstrap 或任何文件。
文件树 CRUD 使用对应 `LoaderSubtree` 与文件内局部路径；配置描述和安全字段编辑可以使用
Loader 的完整路径，由控制面定位来源。include Include 自身仍是应用提供的内存启动声明，
修改它的配置也不代表已修改 bootstrap；需要持久化该启动组合时应由应用明确保存。

Include 写回的是**当前有效树**。它不反向恢复原始基础层、不自动清空 patches，也不另存
用户覆盖文件。已有覆盖可能因此固化进原文件。独立保存用户 patch 属于后续 ConfigEditor，
不能把两种写回路径混在一起。

持久化前先检查来源版本与写回往返：将目标声明序列化，再读取并应用同一组 patches，
必须得到相同且合法的目标声明。若插入项重复或固定覆盖使目标不能保持，则明确拒绝，
不会静默去重、抹掉 patches 或自动改父文件。

同路径只改 patches、initial 或日志策略时，控制调用等待当前子树局部协调；Include Fiber
和无关子实例保留。修改文件路径或父上下文变化时重新挂载来源树，旧句柄和旧版本失效。
reload 显式重新读取文件，不启动自动监听；未来监听／HMR 插件应调用同一 reload 路径。
多个来源定义更新分别协调；业务生命周期失败不会阻止其他独立来源定义更新，但父声明
不会整体提交。不提供跨来源事务，已经生效的局部更新不回滚，失败来源的相同目标可重试。

## 提交与释放边界

来源接口的 `read`、`check_revision`、`validate_write`、`write` 均为异步方法。
`identity` 与 `context` 只访问已解析的内存元数据，不执行文件 I/O。来源构造函数接收拥有的
`Context` 并返回 `Send + 'static` Future，Loader 在异步预检和协调中显式等待来源准备完成。
Include 将路径规范化、读取与解析、版本检查、写回验证及原子文件事务放入 `spawn_blocking`，
避免阻塞 Tokio 异步工作线程；每次处理完整工作单元，不逐个系统调用切换任务。

每棵树使用同一操作锁完成版本检查、预检、协调与来源写回。Include 在实际写入前再次检查
来源版本，使用同目录临时文件、同步和原子替换；Unix 创建与替换文件为 0600，不生成备份。
符号链接规范化后共享同一来源身份，重复挂载和祖先循环会明确拒绝。
对外 `await_idle()` 也等待已进入操作锁的来源预检与写回，不在异步读取期间提前返回稳定快照。

运行时先应用，再写文件。生命周期失败不写回，但已经触及的实例不会回滚；写回失败返回
`Persist` 与实际运行快照，不能解释为从未执行。初始业务子条目失败保留声明和 Failed 状态，
不撤销 Include 控制面或无关插件；文件、格式、来源登记等错误落在对应 Include 条目。

取消调用方等待不会中断已接受提交。已经开始的阻塞任务不可强制取消，不会另起无人结算的
后台写回；阻塞任务异常退出会明确返回脱敏的来源错误，不自动重试。停用 Include 时拒绝新操作，等待已执行提交结算，
后序释放嵌套树并注销来源；保留的旧控制句柄不可用于新实例。秘密字段的安全描述仍由配置
控制面处理；文件权限不是加密，业务插件不得把配置正文或凭据放入错误、日志与 panic。

## 破坏性 API 迁移

- 删除 `LoaderPlugin::bootstrap`；改为 `LoaderPlugin::new` 加显式 `bootstrap_entry("include", …).await?`。
- 文件 bootstrap/read 辅助函数改从 `cordis_plugin_include` 导入，不保留 Loader 重导出。
- `FileSource::open` / `from_config`、`load_bootstrap`、`bootstrap_entry`、`load_extensions` 统一改为异步，调用方须使用 `.await`，不提供同步兼容入口。
- 自定义 `EntryTreeSource` 使用 `async_trait` 实现异步读写方法；`PreparedExtension::tree` 的来源构造闭包改为接收拥有的 `Context` 并返回异步结果。
- 删除 `attach_file_subtree`；自定义来源使用通用 `attach_subtree`，不把文件路径交给 Loader。
- 删除 `EntryLocation.source_dir`；文件目录从 Include 的 `INCLUDE_LOCATION` 读取。
- 所有文件条目服务查询、路由参数与配置描述增加实际 Include 前缀；文件 CRUD 改用来源树局部路径。
- 外部工作台和网关需要独立迁移；本仓库不替它们增加兼容入口或旧路径别名。

运行示例：

```bash
cargo run -p cordis-host --example host_bootstrap
cargo run -p cordis-plugin-include --example volatile_config
cargo run -p cordis-plugin-include --example config_control
cargo run -p cordis-core --example plugin_stack
```

Host 与 Core 的这些示例仅通过 dev-dependencies 使用 Include；它们的生产依赖边界不变。
