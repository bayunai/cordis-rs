# cordis-plugin-settings

为管理界面投影 Entry 配置的 Schema 表单视图：目录、分层脱敏、可编辑性、字段操作与失效订阅。
对照 DeepSeek `@deepseek-ai/dsh-settings`：表单投影与安全编辑；写回委托 [`cordis-plugin-config-editor`](../cordis-plugin-config-editor) 的 **user** 层。

分层见 [配置控制面拆分](../../docs/config-surface.md)。

## 职责

| 做 | 不做 |
| --- | --- |
| `list`：可配置条目目录（非 Group）；根路径来自 ConfigEditor | 启停库存（属 PluginManager）；另存 `include_root` |
| `describe`：安全 Schema、raw `base`、可选 `inherited` 投影、`editable` | 文件 I/O、原子写回 |
| `mutate`：Set/Unset（Unset 只恢复 **raw** 继承，不 bake Serde 默认） | 直接 Loader `update`、bake Include path |
| `subscribe() -> watch::Receiver<u64>`：实例失效代次 | 页面组件、HTTP、鉴权、`autoGenerate` |

## API

- `list() -> Vec<SettingsCatalogEntry>`
- `describe(path) -> EntryConfigView`（`base` raw / `inherited` 可选 JSON / `value` Active）
- `mutate(path, expected, operations) -> EntryConfigView`
- `subscribe() -> watch::Receiver<u64>`（`changed().await` 后 `*borrow()`；无 `Lagged`）

订阅值是本服务实例独立的失效代次，不是 Loader revision 或配置并发令牌。
初值为零；上游变更或事件缺失都会推进，无接收者时仍保存最新值。
页面首次加载应直接查询目录／描述，随后每次代次变化完整刷新；不按代次差值推算事件数量。
慢消费者只读取最新值。插件卸载后旧接收端关闭，新实例独立重新计数。

**能 describe ≠ 允许 mutate。** `editable` 与 ConfigEditor `editability` 同源。

`base` 是 deploy ∘ home 的 **raw TOML**（输入 Schema 脱敏），也是 Unset 恢复源。
`inherited` 是对 raw 的 Catalog **类型投影**（含 Serde 默认，序列化 Schema 脱敏）。仅输入无法独立反序列化时为 `None`；投影 panic、序列化故障、未知 Factory 或 Loader 关闭均明确报错，不退回 raw 混用。
`value` 是 Active 快照。Unset 有 raw 键则写入该值，无键则删键——不把展示默认写进 user。

描述使用一次捕获并核对过的层数据；磁盘层与所属 Include 已加载 patches 不一致时返回
`ConfigConflict`，须先显式刷新。不会把新文件内容拼接到旧 Factory 的 Schema 上返回。

## 挂载

须在已配置路径的 `ConfigEditorPlugin` 之后（编辑范围只配 ConfigEditor）：

```rust
root.plugin(Arc::new(SettingsPlugin)).await?;
let settings = root.get(SETTINGS)?;
```

PluginKey：`cordis.settings`。
