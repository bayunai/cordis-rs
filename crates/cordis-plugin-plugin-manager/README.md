# cordis-plugin-plugin-manager

根 Include 下的 Entry **库存与启停**门面。对照 DeepSeek `@deepseek-ai/dsh-plugin-manager` 的 `listPlugins` / `setPluginEnabled` 角色；不做 pnpm、bundle 安装或 HTTP。

启停写回委托 [`cordis-plugin-config-editor`](../cordis-plugin-config-editor) 的 user 层。分层见 [配置控制面拆分](../../docs/config-surface.md)。

## 职责

| 做 | 不做 |
| --- | --- |
| `list_plugins`：根 Include 下 Entry 库存（投影自 `CONFIG_EDITOR.managed_entries`） | Schema 表单、秘密脱敏 |
| `set_plugin_enabled` → `CONFIG_EDITOR.set_enabled` | 写空根 / deploy / home；另存 `include_root` |
| `subscribe() -> watch::Receiver<u64>` | HTTP、鉴权、pnpm、bundle 选层 |

## 挂载

须在 `ConfigEditorPlugin` 之后（`include_root` 只来自 ConfigEditor）：

```rust
ctx.plugin(Arc::new(PluginManagerPlugin))
```

PluginKey：`cordis.plugin-manager`。

## 生命周期与只读状态

每个插件实例仅有一个 Effect，将 Loader `SnapshotChanged` 或上游事件缺失转为本地失效代次并写入 `watch`。`subscribe()` 返回
共享接收端；页面退出丢弃即可。插件卸载后 `subscribe` / `list_plugins` 失败。
代次初值为零，与 Loader revision 和配置并发令牌无关；没有接收者时也保存最新值。
页面首次加载应直接查询列表，随后 `changed().await` 后读取 `*borrow()` 并完整刷新；
落后只保留最新代次，不按差值推算事件数量，无哨兵 revision。
插件卸载后旧接收端关闭；重新挂载的新实例独立重新计数。

列表与启停使用 ConfigEditor 的 `editability`（`ConfigReadOnlyReason`）。
