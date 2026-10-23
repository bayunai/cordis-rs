# cordis-plugin-settings

为管理界面投影 Entry 配置的 Schema 表单视图：目录、分层脱敏、可编辑性、秘密存在状态、有序字段操作与失效订阅。  
对照 DeepSeek `@deepseek-ai/dsh-settings`：表单投影与安全编辑；写回委托 [`cordis-plugin-config-editor`](../cordis-plugin-config-editor) 的 **user** 层。

分层见 [配置控制面拆分](../../docs/config-surface.md)。

## 职责

| 做 | 不做 |
| --- | --- |
| `list`：可配置条目目录（非 Group）与可编辑性 | 启停库存（属 PluginManager） |
| `describe`：安全 Schema、脱敏分层、`editable` / `read_only_reason`、secrets、revision | 文件 I/O、原子写回 |
| `mutate`：字段 Set/Unset（Unset=恢复继承）后委托 config-editor `edit`；只读预检 | 直接 Loader 明文 `update`、bake Include path；硬删下层已有值 |
| `subscribe`：Loader 快照失效转发 | 页面组件、HTTP、鉴权、`autoGenerate` |
| 禁用 / Pending / Failed 条目的可描述性 | 用户偏好 `settings.toml`（属应用 Host） |

## API

- `list() -> Vec<SettingsCatalogEntry>`
- `describe(path) -> EntryConfigView`（含 `base`、`user`、`config`、`value`、`editable`）
- `mutate(path, expected, operations) -> EntryConfigView`
- `subscribe() -> Receiver<SettingsEvent>`（`Changed { revision }` 或落后时的 `Resync`）

公开类型：`ConfigOperation`、`EntryConfigView`、`SettingsCatalogEntry`、`SettingsEvent`、
`SecretFieldState`、`SettingsConfig`；`ConfigReadOnlyReason` / `ConfigRevision` re-export。

**能 describe ≠ 允许 mutate。** `editable` / `read_only_reason` 与 ConfigEditor `editability` 同源。

`base` 是去除 user 后的继承配置（deploy ∘ home）；能通过 Factory 解析时含 Serde 默认值
（对齐 DSH `inheritedConfig`，解析失败则退回 raw）。
`config` 是 deploy ∘ user ∘ home 合成后的原始 TOML，字段编辑以此为基础。
`value: Option<serde_json::Value>` 是 Active 实例真正使用的类型解析快照；非 Active 为 `None`。
`Unset` 对齐 DSH：对象字段从 `base` 恢复继承（有则写入该值，无则删键）；数组下标删除元素。
管理 UI 应标「恢复继承」，不要把 `Unset` 标成「删除字段」。不能经 Settings 硬删下层已提供的值。
`Unset` 恢复时可把 `base` 中的继承/默认值写入 user（与 DSH 相同）；不存在从 Schema `default`
键推测 Rust 默认值的逻辑。`secrets.is_set` 仍表示原始配置是否显式设置。

## 挂载

须在已配置路径的 `ConfigEditorPlugin` 之后：

```rust
root.plugin(Arc::new(SettingsPlugin::default())).await?;
let settings = root.get(SETTINGS)?;
```

PluginKey：`cordis.settings`。
