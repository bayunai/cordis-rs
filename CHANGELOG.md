# Changelog

本项目遵循语义化版本；在 `0.x` 阶段，minor 版本可能包含破坏性公共 API 与配置变更。

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
