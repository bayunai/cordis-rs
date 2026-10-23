# cordis-plugin-settings

为管理界面投影 Entry 配置的 Schema 表单视图：分层脱敏、秘密存在状态、有序字段操作与版本往返。  
对照 DeepSeek `@deepseek-ai/dsh-settings`：表单投影与安全编辑；写回委托 [`cordis-plugin-config-editor`](../cordis-plugin-config-editor) 的 **user** 层。

分层见 [配置控制面拆分](../../docs/config-surface.md)。

## 职责

| 做 | 不做 |
| --- | --- |
| `describe`：安全 Schema、脱敏 `base` / `user` / `config` / `value`、`secrets`、revision | 文件 I/O、原子写回 |
| `mutate`：字段 Set/Unset 拼装后委托 config-editor `edit`（只写 user） | 直接调用 Loader 明文 `update`、bake Include path |
| 禁用 / Pending / Failed 条目的可描述性 | 页面组件、HTTP、鉴权 |
| writeOnly Schema 保守投影 | 用户偏好 `settings.toml`（属应用 Host） |

## API

- `describe(path) -> EntryConfigView`（含 `base`、`user`、`config`、`value`）
- `mutate(path, expected, operations) -> EntryConfigView`

公开类型：`ConfigOperation`、`EntryConfigView`、`SecretFieldState`；`ConfigRevision` 从 loader re-export。

`config` 是 deploy ∘ user ∘ home 合成后的原始 TOML 配置，编辑只以此为基础。
`value: Option<serde_json::Value>` 是 Active 实例真正使用的 Factory 类型解析快照，
包含 Serde 默认值、嵌套默认值与 JSON null；非 Active 为 `None`，不伪装成运行值。
查询不会重新执行默认函数、`prepare()` 或构造函数；普通替换和热提交成功后才更新相应快照。
`secrets.is_set` 仍表示原始配置是否显式设置，不把默认秘密当作用户已填写。
原始配置按输入 Schema 脱敏，运行值按序列化 Schema 脱敏，覆盖非对称 serde 重命名。
默认值仅供展示，不自动写入 user 层；不存在从 Schema `default` 推测 Rust 默认值的逻辑。

这是直接 API 调整：原先读取 `value` 原始配置的调用方改用 `config`，不保留旧含义。

## 挂载

须在已配置路径的 `ConfigEditorPlugin` 之后：

```rust
root.plugin(Arc::new(SettingsPlugin)).await?;
let settings = root.get(SETTINGS)?;
```

PluginKey：`cordis.settings`。
