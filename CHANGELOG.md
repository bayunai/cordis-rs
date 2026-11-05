# Changelog

本项目遵循语义化版本；在 `0.x` 阶段，minor 版本可能包含破坏性公共 API 与配置变更。

## Unreleased

### 破坏性变更

- `dispose()` 立即禁止 Scope 树新增工作，任务取消及同步、异步清理由协调器按依赖顺序执行；`on_dispose` 不再保证返回前运行。子 Scope 使用独立取消令牌。
- `PluginDefinition` 身份与 Plugin 对象分离；挂载和 Catalog 登记显式传 Definition。插件与派生注入统一为 Fiber，状态直接使用 `FiberState`。
- `plugin()` 返回拥有型 `Fiber`，Drop 卸载；`inject()` 和 `Fiber::handle()` 返回可克隆的非拥有型 `FiberHandle`，Drop 不卸载。
- 插件与派生注入的 Failed 均只通过显式 restart 或插件 replace 恢复；依赖变化不自动重试。
- Runtime 诊断统一为 fibers / definitions / providers / effects / isolations；事件与日志使用 Definition 身份及可选名称。
- Loader Group 不再参与地址前缀，同一 EntryTree 内所有条目 ID 唯一；ID 拒绝空值和冒号。旧 Group 层级地址及 patch 目标必须显式迁移，不保留别名；Include 边界前缀仍保留。

迁移步骤见 [Definition 与 Fiber 接入迁移](docs/migration-plugin-definitions.md)。

### 新增

- 增加 `cordis_core::plugin_definition!` 声明宏，统一静态具名定义的写法，支持文档、属性、可见性与函数内声明。宏展开为既有 `LazyLock<PluginDefinition>`，身份、挂载、依赖及生命周期语义不变；动态与匿名定义继续使用现有构造接口。

- 显式注入按 Provider 实例与消费者执行 Scope 建立生命周期绑定，覆盖 Loading、Active、失败清理与在途卸载；提供者等待消费者全部清理完成后才取消任务和释放资源，适用于普通 Effect、插件重启替换、批量卸载与 Runtime shutdown。
- 新增 `DependencyCycle { scopes }` 与 `LifecycleReentrant { scope }` 错误；`unmount` 的专用重入检查覆盖跨树服务依赖。
- Loader 支持跨 Include 移动条目及 Group 同树成员。两侧全量预检后协调运行时，按目标、源顺序写回；目标写入失败不写源，`LoaderControlError::Move` 与 `MovePersistence` 分别报告实际快照、生命周期错误和两侧持久化状态；同树移动也使用该错误且只写所属来源一次。不回滚已发生的变更，取消等待不取消协调。
- 同树换 Group 保持条目地址，完整重挂载仍可改变 Fiber ID；仅同父项排序不重挂载。

### 改进与修复

- 依赖解析与绑定登记同锁完成，retiring Provider 拒绝新绑定并保留遮蔽槽位，完成撤销后才恢复父级解析。
- 同一释放错误经父子与依赖等待路径汇合时按来源去重；取消调用方等待不取消内部协调器。
- 严格 `get()` 规则保持不变；清理阶段使用显式注入后捕获的服务引用。仅 Drop Runtime 不提供受控依赖释放顺序。

## 0.2.0 - 2026-09-20

### 破坏性变更

- Loader 配置统一为 v3 EntryTree；旧平面 v1 与 v2 配置不再读取。
- Loader 改为由应用显式挂载的 `LoaderPlugin`；`cordis-host` 只负责 Runtime 的创建与关闭。
- 运行日志改为 Runtime 全局 `LoggerService`；不再通过可注入、可隔离的 `LOGGER` 服务键传递。

### 新增

- `cordis-loader` 提供静态 Catalog 驱动的 EntryTree、局部 reconcile、可配置 `inject`、按服务键的 `isolate`、管理快照与原子 TOML 持久化。
- 新增 `cordis-plugin-include`，用于将独立 v3 TOML 文件附着为 Loader 子树。
- 新增 `cordis-plugin-timer`，提供由 Effect 生命周期管理的 timeout、interval、throttle 与 debounce。
- 新增 `cordis-plugin-logger-console`，作为 Runtime 全局日志的控制台 exporter。

### 改进与修复

- Fiber 生命周期 worker 中止后会收敛释放 Scope、Provider 与所有权，不再遗留可见资源。
- Loader 同域条目可按声明共享服务；失败条目可观察、可重试，且局部 reconcile 不影响无关条目。
- 完善 Include 子树注册、循环诊断、快照父关系，以及隔离服务描述符校验。
- 补充 Effect、日志、生命周期、Loader、Include 与 Timer 的回归测试和架构文档。

[0.2.0]: https://github.com/bayunai/cordis-rs/releases/tag/v0.2.0
