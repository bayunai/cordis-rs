# 配置控制面拆分

管理界面配置能力由两个 Host 显式挂载插件承接；Loader 只提供受信任的 snapshot/commit 原语。无 Loader 兼容层或 deprecated 转发（对齐 DeepSeek）。

相关包：

- [`cordis-plugin-config-editor`](../crates/cordis-plugin-config-editor) — 多层 patch 组成与 reconcile；可选整表 `commit_source`
- [`cordis-plugin-settings`](../crates/cordis-plugin-settings) — Schema 表单投影与秘密安全编辑

对照 DeepSeek：`@deepseek-ai/dsh-config-editor`（持久化/应用）与 `@deepseek-ai/dsh-settings`（表单投影）。  
行为细则见 [Host 配置控制面](host.md#面向管理界面的配置控制面)。

## 分层

```text
浏览器 / 管理 UI
        │  HTTP · 鉴权 · DTO（应用 Host）
        ▼
cordis-plugin-settings          ← describe / 字段操作 / 脱敏（base · user · value）
        │  委托 user 层提交
        ▼
cordis-plugin-config-editor     ← deploy ∘ user ∘ home → Include.patches
        │
        ├─► 各插件 deploy patch（只读于 UI）
        ├─► 本机 user patch（唯一产品写目标）
        ├─► 本机 home patch（只读叠加，优先于 user）
        ├─► cordis-loader       ← snapshot / update(include) / reconcile
        └─► cordis-plugin-include ← 空根 path；patches 为运行时全量载体
```

| 层 | 拥有 | 不拥有 |
| --- | --- | --- |
| **settings** | 安全 Schema、脱敏 `base` / `user` / `value`、secrets、revision、有序 Set/Unset | 文件 I/O、直接明文 `update` |
| **config-editor** | 有序 deploy 路径、user/home 文档、组成与 reconcile；运维用整表 `commit_source` | 表单投影、HTTP |
| **Loader** | Catalog、EntryTree、reconcile、`EntryTreeSource`、snapshot/commit 原语 | 表单 DTO、文件格式、产品 user 语义 |
| **Include** | 路径、编解码、文件事务；`patches` 为组成载体 | 管理界面语义、本机 user 权威 |
| **应用 Host** | HTTP、鉴权、页面；注入 deploy 路径列表与 user/home 绝对路径 | 不得把偏好文档冒充 Entry Config |

## 空根 / deploy / user / home / effective

| 层 | 含义 | 持久化 |
| --- | --- | --- |
| **空根** | Include `path` 指向的空条目列表 | 如 `cordis.toml`（Host 代码挂载，对齐 DSH `cordis.yml`）；UI 不写 |
| **deploy** | 各插件有序 `cordis.patch.toml`（通常根 `insert`） | 包内文件；UI 不写 |
| **user** | 本机覆盖：Entry `config` 与启停 `disabled` | Host 注入（如 `user.patches.toml`） |
| **home** | 更高优先本机覆盖 | Host 注入（如 `home.patches.toml`）；UI 不写 |
| **value / effective** | deploy ∘ user ∘ home | 不单独落盘；经 Include `patches` 同步 |

产品 **user ≠** Include 配置里的 `patches` 字段语义：后者是调和载体；权威在独立文档与各插件 deploy 文件。  
**user ≠** 应用偏好（例如工作台 `settings.toml` / `workbench-settings`）：那是第三命名空间。

表单 `mutate` / 启停：**只写 user 文档**，再 reconcile；禁止 bake 空根或 deploy。`edit` 若被 home 盖住导致 effective ≠ 目标则拒绝。嵌套 Include 子树首版可 `describe`，`mutate` 返回 `ConfigNotEditable`。

Host 在代码中挂根 Include（空根 `cordis.toml`），不使用独立 bootstrap 配方文件，也不把空根拆进业务插件。

## PluginKey

| 包 | PluginKey |
| --- | --- |
| config-editor | `cordis.config-editor` |
| settings | `cordis.settings` |

挂载顺序：Loader（及根 Include）→ `ConfigEditorPlugin::new(ConfigEditorConfig { deploy_patch_paths, user_patch_path, home_patch_path, … })` → `SettingsPlugin`。

## 非目标

- 不引入独立 Settings 持久化存储替代 Entry 声明。
- 不把 HTTP、鉴权或前端组件放进 cordis-rs。
- 首版不做 XDG / CLI `--patch` 更高优先级层。
- 不在 Loader 上保留 `describe_config` / `mutate_config` 兼容入口。
