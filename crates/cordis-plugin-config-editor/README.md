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
    config_dir,
))
```

- `layers(path) -> LayeredEntryConfig`
- `edit(path, expected, effective) -> LayeredEntryConfig`
- `set_enabled(path, enabled)`
- `read` / `commit_source` — 后者可能 bake Include path；表单不得使用

浏览器安全描述与字段操作由 [`cordis-plugin-settings`](../cordis-plugin-settings) 提供。

PluginKey：`cordis.config-editor`。
