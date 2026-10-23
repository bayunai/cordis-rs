# cordis-plugin-plugin-manager

根 Include 下的 Entry **库存与启停**门面。对照 DeepSeek `@deepseek-ai/dsh-plugin-manager` 的 `listPlugins` / `setPluginEnabled` 角色；不做 pnpm、bundle 安装或 HTTP。

启停写回委托 [`cordis-plugin-config-editor`](../cordis-plugin-config-editor) 的 user 层。分层见 [配置控制面拆分](../../docs/config-surface.md)。

## 职责

| 做 | 不做 |
| --- | --- |
| `list_plugins`：根 Include 下 Entry 库存 | Schema 表单、秘密脱敏 |
| `set_plugin_enabled` → `CONFIG_EDITOR.set_enabled` | 写空根 / deploy / home |
| `subscribe`：转发 Loader `SnapshotChanged` | HTTP、鉴权、pnpm、bundle 选层 |

## 挂载

须在 `ConfigEditorPlugin` 之后：

```rust
ctx.plugin(Arc::new(PluginManagerPlugin::new(PluginManagerConfig::default())))
```

PluginKey：`cordis.plugin-manager`。

## 生命周期与只读状态

每个插件实例仅有一个 Effect 托管的 Loader 事件转发任务。`subscribe()` 只创建共享广播接收端，
页面退出丢弃接收端即可；插件卸载后通道关闭，旧服务句柄不能重新订阅或发起启停。
重新挂载建立独立通道，旧订阅不会收到新实例事件。

列表与启停入口共同使用 ConfigEditor 的 `can_edit()` 能力判断。
Group、嵌套 Include 载体，以及不属于可编辑 deploy 层的条目均明确标为只读；
嵌套 Include 内的业务条目不会被误报为可启停。
