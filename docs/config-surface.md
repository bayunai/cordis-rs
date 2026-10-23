# 配置控制面拆分

管理界面配置与插件库存由 Host 显式挂载插件承接；Loader 只提供受信任的 snapshot/commit 原语。无 Loader 兼容层或 deprecated 转发（对齐 DeepSeek）。

相关包：

- [`cordis-plugin-config-editor`](../crates/cordis-plugin-config-editor) — 多层 patch 组成与 reconcile；可选整表 `commit_source`
- [`cordis-plugin-settings`](../crates/cordis-plugin-settings) — Schema 表单投影与秘密安全编辑
- [`cordis-plugin-plugin-manager`](../crates/cordis-plugin-plugin-manager) — Entry 库存与启停门面

对照 DeepSeek：`@deepseek-ai/dsh-config-editor`、`@deepseek-ai/dsh-settings`、`@deepseek-ai/dsh-plugin-manager`（`listPlugins` / `setPluginEnabled`；不含 pnpm/安装）。  
行为细则见 [Host 配置控制面](host.md#面向管理界面的配置控制面)。

## 分层

```text
浏览器 / 管理 UI
        │  HTTP · 鉴权 · DTO（应用 Host）
        ├─► cordis-plugin-plugin-manager  ← list_plugins / set_plugin_enabled / subscribe
        │         │ 启停委托
        ▼         ▼
cordis-plugin-settings          ← describe / 字段操作 / 脱敏（base · user · config · value）
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
| **plugin-manager** | 根 Include 下 Entry 库存、`set_plugin_enabled`、快照失效订阅 | 表单字段、HTTP、pnpm、bundle 安装/选层 |
| **settings** | 安全 Schema、脱敏 `base` / `user` / `config` / `value`、secrets、revision、有序 Set/Unset | 文件 I/O、直接明文 `update` |
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
| **config / effective** | deploy ∘ user ∘ home 的 raw 配置 | 不单独落盘；经 Include `patches` 同步 |
| **value / resolved** | Active 实例的类型解析快照（含真实 Serde 默认值）；其他状态为 None | 仅展示，不写回 user |

产品 **user ≠** Include 配置里的 `patches` 字段语义：后者是调和载体；权威在独立文档与各插件 deploy 文件。  
**user ≠** 应用偏好（例如工作台 `settings.toml` / `workbench-settings`）：那是第三命名空间。

表单 `mutate` / 启停：**只写 user 文档**，再 reconcile；禁止 bake 空根或 deploy。`edit` 若被 home 盖住导致 effective ≠ 目标则拒绝。嵌套 Include 子树首版可 `describe`，`mutate` 返回 `ConfigNotEditable`。产品启停经 `PLUGIN_MANAGER.set_plugin_enabled`（内部仍调 config-editor）。

Host 在代码中挂根 Include（空根 `cordis.toml`），不使用独立 bootstrap 配方文件，也不把空根拆进业务插件。

## PluginKey

| 包 | PluginKey |
| --- | --- |
| config-editor | `cordis.config-editor` |
| settings | `cordis.settings` |
| plugin-manager | `cordis.plugin-manager` |

挂载顺序：Loader → `prepare_root_include(path, &config).await` 组合完整 patches →
`loader.create(entry, None, None).await` 初次挂载根 Include → `ConfigEditorPlugin::new(config)` →
`SettingsPlugin` → `PluginManagerPlugin`。启动组合函数与运行期编辑共用分层读取规则；
ConfigEditor 挂载仅注册服务，不读取或协调业务声明。初次业务 Failed 不会拖掉配置控制面，
后续编辑失败仍必须通过操作返回值报告。

## 非目标

- 不引入独立 Settings 持久化存储替代 Entry 声明。
- 不把 HTTP、鉴权或前端组件放进 cordis-rs。
- PluginManager 首版不做 bundle 安装、选层、pnpm 或 profile `package.json`。
- 首版不做 XDG / CLI `--patch` 更高优先级层。
- 不在 Loader 上保留 `describe_config` / `mutate_config` 兼容入口。
