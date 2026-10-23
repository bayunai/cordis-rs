# cordis-plugin-config-editor

空根上的 **deploy ∘ user ∘ home** 组成，并将 effective 推进 Loader reconcile。  
对照 DeepSeek `@deepseek-ai/dsh-config-editor`：持久化与应用，不做表单投影、HTTP 或鉴权。

分层见 [配置控制面拆分](../../docs/config-surface.md)。

## 职责

| 做 | 不做 |
| --- | --- |
| `layers`：base=deploy / user / effective=deploy∘user∘home | Schema 表单投影、秘密脱敏 |
| `edit` / `set_enabled`：只写 user patch 并全量 reconcile | 写空根、deploy、home |
| home 盖住时拒绝 `edit` | 页面组件、HTTP、鉴权 |
| `read` / `commit_source`：受信任整表（运维） | 用户偏好文档（非 Entry Config） |

## API

```rust
ConfigEditorPlugin::new(ConfigEditorConfig::new(
    deploy_patch_paths,
    user_patch_path,
    home_patch_path,
))
```

- `layers(path) -> LayeredEntryConfig`
- `edit(path, expected, effective) -> LayeredEntryConfig`
- `set_enabled(path, enabled)`
- `can_edit(path)`：按 deploy 所有权判断是否支持编辑；嵌套 Include 后代不冒充可编辑。
- `read` / `commit_source` — 后者可能 bake Include path；表单不得使用

## 失败与文件安全

`edit` / `set_enabled` 必须报告本次生命周期失败，不将 Failed 条目视为编辑成功。
失败后恢复此前 user 文档；恢复失败会同时报告，不能宣称持久化已恢复。
Loader 保留可能已经发生的局部生命周期结果，不回滚业务副作用，但不提交完整候选声明、来源版本或待提交热值。

初次业务声明应随根 Include 的 `patches` 一起挂载，由 Include 初始化保留 Failed 条目供修复。
编辑器启动时若 patches 已一致，不主动重试这些失败条目；若需要同步新的 patches，协调错误明确导致启动失败。

user 文件通过同目录唯一临时文件独占创建并原子替换；只清理本次拥有的临时文件。
Unix 文件创建权限为 `0600`，新建目录为 `0700`，不修改已有父目录权限。

所有文件读取、解析与完整写入事务在 `spawn_blocking` 中执行。`PatchFileDocument::load`
及 `load_ordered_patches` 是异步接口，调用方必须 `.await`；不保留同步入口。
已接受的 user 写入即使调用方取消等待，也会持有存储锁直至结算。

分层读取和编辑使用 Loader 的 `config_transaction`，在同一 Loader 内与直接 CRUD、
配置提交、reload 串行。revision 检查、user 写入、Include 协调及失败恢复位于同一事务，
不再依赖编辑器私有锁；`ConfigEditorConfig` 已删除无效的 `config_dir` 参数。

浏览器安全描述与字段操作由 [`cordis-plugin-settings`](../cordis-plugin-settings) 提供。

PluginKey：`cordis.config-editor`。
