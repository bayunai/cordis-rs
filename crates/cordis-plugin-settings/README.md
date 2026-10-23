# cordis-plugin-settings

为管理界面投影 Entry 配置的 Schema 表单视图：分层脱敏、秘密存在状态、有序字段操作与版本往返。  
对照 DeepSeek `@deepseek-ai/dsh-settings`：表单投影与安全编辑；写回委托 [`cordis-plugin-config-editor`](../cordis-plugin-config-editor) 的 **user** 层。

分层见 [配置控制面拆分](../../docs/config-surface.md)。

## 职责

| 做 | 不做 |
| --- | --- |
| `describe`：安全 Schema、脱敏 `base` / `user` / `value`、`secrets`、revision | 文件 I/O、原子写回 |
| `mutate`：字段 Set/Unset 拼装后委托 config-editor `edit`（只写 user） | 直接调用 Loader 明文 `update`、bake Include path |
| 禁用 / Pending / Failed 条目的可描述性 | 页面组件、HTTP、鉴权 |
| writeOnly Schema 保守投影 | 用户偏好 `settings.toml`（属应用 Host） |

## API

- `describe(path) -> EntryConfigView`（含 `base`、`user`、`value`）
- `mutate(path, expected, operations) -> EntryConfigView`

公开类型：`ConfigOperation`、`EntryConfigView`、`SecretFieldState`；`ConfigRevision` 从 loader re-export。

## 挂载

须在已配置路径的 `ConfigEditorPlugin` 之后：

```rust
root.plugin(Arc::new(SettingsPlugin)).await?;
let settings = root.get(SETTINGS)?;
```

PluginKey：`cordis.settings`。
