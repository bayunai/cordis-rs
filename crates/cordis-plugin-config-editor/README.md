# cordis-plugin-config-editor

空根上的 **deploy ∘ user ∘ home** 组成，并将 effective 推进 Loader reconcile。  
对照 DeepSeek `@deepseek-ai/dsh-config-editor`：持久化与应用，不做表单投影、HTTP 或鉴权。

分层见 [配置控制面拆分](../../docs/config-surface.md)。

## 职责

| 做 | 不做 |
| --- | --- |
| `layers`：base=deploy∘home（可含解析默认）/ user / effective=deploy∘user∘home | Schema 表单投影、秘密脱敏 |
| `edit` / `set_enabled`：只写 user patch 并全量 reconcile | 写空根、deploy、home |
| home 盖住时拒绝 `edit` | 页面组件、HTTP、鉴权 |
| `read` / `commit_source`：受信任整表（运维） | 用户偏好文档（非 Entry Config） |

## API

```rust
let config = ConfigEditorConfig::new(
    deploy_patch_paths,
    user_patch_path,
    home_patch_path,
);
let entry = prepare_root_include("cordis.toml", &config).await?;
loader.create(entry, None, None).await?;
ctx.plugin(Arc::new(ConfigEditorPlugin::new(config))).await?;
```

`prepare_root_include` 是独立的异步启动组合函数，不依赖运行中的 ConfigEditor 服务，
不访问 Loader、不挂载插件、不创建根文件。它与运行期编辑共用 deploy → user → home 的
读取顺序；根 ID 来自 `config.include_root`，必须是单段 ID，默认 `include`。
不使用分层的应用仍直接使用 Include 的 `include_root_entry`。

- `layers(path) -> LayeredEntryConfig`（raw `effective` 与 Active 的可选 `resolved` 分开）
- `edit(path, expected, effective) -> LayeredEntryConfig`
- `set_enabled(path, enabled)`
- `can_edit(path)` / `editability(path)`：按 deploy 所有权判断是否支持编辑，并给出
  `ConfigReadOnlyReason`（`Group` / `NestedInclude` / `UnmanagedEntry`）；嵌套 Include 后代不冒充可编辑。
- `read` / `commit_source` — 后者可能 bake Include path；表单不得使用
- `reload()` — 显式重读各层并协调，外部修改冲突后由调用方刷新，不自动重试编辑

## 失败与文件安全

`edit` / `set_enabled` 必须报告本次生命周期失败，不将 Failed 条目视为编辑成功。
失败后恢复此前 user 文档；恢复失败会同时报告，不能宣称持久化已恢复。
Loader 保留可能已经发生的局部生命周期结果，不回滚业务副作用，但不提交完整候选声明、来源版本或待提交热值。

初次业务声明由启动入口调用 `prepare_root_include` 组合，随根 Include 的 `patches` 一起挂载。
Include 初始化保留 Failed 条目供修复；普通业务生命周期失败不会使配置控制面失效。
ConfigEditor 挂载只注册服务，不读取配置、不协调或重试业务条目。
非法配置文件、未知 Factory 与预检错误仍明确阻止相应准备/加载操作，不伪装成业务启动成功。

user 文件通过同目录唯一临时文件独占创建并原子替换；只清理本次拥有的临时文件。
Unix 文件创建权限为 `0600`，新建目录为 `0700`，不修改已有父目录权限。

所有文件读取、解析与完整写入事务在 `spawn_blocking` 中执行。`PatchFileDocument::load`
及 `load_ordered_patches` 是异步接口，调用方必须 `.await`；不保留同步入口。
已接受的 user 写入即使调用方取消等待，也会持有存储锁直至结算。

完整编辑在同目录持久锁锚点 `.<user文件名>.lock` 上持有 OS 排他锁，跨 Store、Loader 和
进程串行，覆盖读取、校验、写回、协调与失败恢复。锁锚点不会删除，不是备份；进程退出后
OS 自动释放锁。锁等待也在阻塞任务内，不阻塞异步工作线程，不用 PID 猜测清理“陈旧锁”。
`UserPatchStore::begin_edit()` 返回 `UserPatchEdit`，多步读改写必须使用同一 guard；
单独 `load()` / `store()` 仅保证各自操作受锁保护。
检测到磁盘层与本 Loader 已加载 patches 不一致时拒绝写入并返回 `ConfigConflict`；
先显式 `reload()`、重新读取描述再编辑，不能覆盖另一进程提交的值。
所有协作写入者必须遵守锁协议；直接编辑文件的工具不受 OS 咨询锁强制约束。
读取 user 层可能创建私有目录和锁锚点，但不创建缺失的 user 配置文件。

分层读取和编辑使用 Loader 的 `config_transaction`，在同一 Loader 内与直接 CRUD、
配置提交、reload 串行。revision 检查、user 写入、Include 协调及失败恢复位于同一事务，
不再依赖编辑器私有锁；`ConfigEditorConfig` 已删除无效的 `config_dir` 参数。

浏览器安全描述与字段操作由 [`cordis-plugin-settings`](../cordis-plugin-settings) 提供。

PluginKey：`cordis.config-editor`。
