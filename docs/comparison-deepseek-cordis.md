# cordis-core 与 deepseek Cordis 核心对比

对照范围：

| 侧 | 来源 | 说明 |
| --- | --- | --- |
| **TS 核心** | [deepseek-harness](https://github.com/deepseek-ai/deepseek-harness) `vendor/cordis`（`@deepseek-ai/cordis`） | Context / Fiber / Registry / Reflect / Events / Service / Logger |
| **Rust 核心** | 本仓库 `crates/cordis-core` | Runtime / Context / Fiber / Effect / Event / Service / Isolation |

**不含** loader、HMR、include、timer 等扩展包（两边都放在核心外）。

相关文档：[architecture.md](./architecture.md)、[extension-authoring.md](./extension-authoring.md)。

---

## 1. 包边界（两边一致）

deepseek `vendor/` 大致为：

```text
vendor/
  cordis/            ← 核心
  loader/            ← 配置树、按 entry 挂插件
  hmr/               ← 热重载（依赖 loader）
  include/ group/ timer/ logger-console/ ...
```

`vendor/cordis/src` 只有内核文件；loader / HMR 是独立包（核心里多为 optional peer）。

本仓库同样约定：

| 层 | Cordis 侧 | 本仓库 |
| --- | --- | --- |
| Runtime 内核 | `cordis` | `cordis-core` |
| 宿主 / 扩展 | loader、hmr、timer… | 网关宿主、配置扩展、日后 HMR 等 |

热重载、配置加载、脚手架**不进核心**；核心只提供 `plugin` / `replace` / Fiber dispose，由宿主或扩展编排。

---

## 2. 功能对照总表

### 2.1 两边都有（可当同一套模型）

| 能力 | TS | Rust |
| --- | --- | --- |
| Context 派生 / isolate / intercept | ✅ | ✅ |
| provide / get / inject（AND 全齐才就绪） | ✅ | ✅ |
| Plugin → Fiber 状态机（Pending / Loading / Active / Failed / Unloading / Disposed） | ✅ | ✅ |
| Effect + dispose（可异步） | ✅ | ✅ |
| 事件：emit / parallel / serial / waterfall；prepend / global / once | ✅ | ✅ |
| 同插件多实例归组与批量卸载 | `registry.delete` | `Runtime::unmount` |
| 依赖变化时 unload → 再激活 | epoch / inertia | provider id 集合 + scheduler |

### 2.2 TS 有、Rust 更薄或没有

| 能力 | TS 位置 / 行为 | Rust 现状 | 建议 |
| --- | --- | --- | --- |
| Proxy + Reflect | `reflect.ts`：`ctx.foo` 穿透、`mixin` / `accessor`、`internal/get\|set` | 无；显式 `get` / `provide` | **不进核心**（刻意） |
| 内置 Logger | `logger.ts` 整模块 | 无 | 宿主或扩展接 tracing/log |
| Service 基类 | `service.ts`：构造时自动 provide、可选 `check`、可调用 Service | `ServiceKey` / `Services` + `provide_checked` / `ProviderHandle` | 不进 Core；扩展层有重复样板后再封装 |
| 多形态 Plugin | fn / class / `{apply}`；`@Inject`；`Config` StandardSchema | 单一 `Plugin` trait + `PluginKey` | 当前不做宏；待真实插件重复样板稳定后，宏只生成 key / inject |
| `fiber.update(config)` | 校验后 restart + `internal/update` | 无；宿主校验后 **`Fiber::replace`** | 保持现状 |
| 事件 `bail` | 同步短路：首个非 `null` / `false` / `undefined` 结果获胜 | 未提供；现有 `serial()` 已提供异步逐个等待、`Some` 短路的等价主路径 | **预留槽位，不实现**；仅出现“必须在同步函数中做首个结果选择”的真实场景时再增加 |
| provide 的 `check` 谓词 | `check` 在 Fiber 依赖刷新时决定是否可注入 | `provide_checked` + `ProviderHandle::refresh`；支持 Ready / Unavailable | Rust 的就绪语义更统一、显式 |
| `get(strict)` | `get(name)` 只检查 Provider Fiber 为 ACTIVE；`get(name, false)` 绕过该检查，且不运行 `check` | 公开 `get(key)` 同时要求 Fiber Active 与 Provider Ready；无公开旁路读取 | 刻意收紧，诊断走元数据而非服务实例 |
| Generator effect | yield 多个 disposer | `on_dispose` / `on_dispose_async` / `spawn` | 手写即可 |
| Fiber thenable | `await fiber` 等待当前 inertia，不保证依赖最终出现 | `plugin().await`、`restart/replace/dispose_wait().await`、`Runtime::settle()` | API 风格差异；暂不实现 `IntoFuture` |

### 2.3 Rust 有、TS 核心更弱或没有

| 能力 | Rust | 相对 TS |
| --- | --- | --- |
| 显式 `Runtime` | `new` / `root` / `settle` / `shutdown` / `unmount` / `diagnostics` / `subscribe_fiber_states` | 根即 `new Context()`，无独立关闭协议 |
| 类型化 Key | `ServiceKey` / `ConfigKey` / `EventKey` / … + `TypeId` 锁定 | 字符串名 + 类型增强 |
| `Fiber::replace` | 同 Key 换不可变插件实例 | 靠 `update` + restart |
| Config ≠ Service | `ConfigKey` 独立表 | intercept 挂在服务名上 |
| inject ≠ Plugin Fiber | `InjectionHandle` 与 `Fiber` 分诊断 | `inject` 是 `plugin({inject, apply})` 语法糖 |
| 跨 Runtime 隔离令牌 | `IsolationLabel` 绑 Runtime | Symbol label，无 runtime 令牌校验 |
| 结构化 diagnostics | providers / plugin_registry / inject_fibers / effects 快照 | 主要是 `getEffects()` label 树 |
| 取消安全协调器 | Loading / Unloading 不因 wait cancel 卡死 | 靠 fiber inertia / epoch |
| 插件 panic 隔离 | `key` / `inject` / `apply` 边界 catch | JS 异常路径不同 |

---

## 3. 关键路径细比

### 3.1 inject / get / provide

**inject（AND）**

- **TS**：`Inject.resolve` → `fiber.inject`；`notify` → `_checkImpl`（strict：提供方须 ACTIVE，可选 `impl.check`）→ `_refresh` 拼 epoch（依赖方 fiber.uid）；缺任一 → `INACTIVE` → unload。
- **Rust**：`Plugin::inject() -> Vec<ServiceId>`；`mark_dirty` → scheduler；就绪 = 每个依赖均解析到 **Ready** Provider；Provider ID 或可用性 revision 变化都会触发 Active → unload → Pending → 再激活。

两边都**没有 AnyOf / OR**。可选依赖不可写入 `inject`；未来数据库等多后端场景需要专用 facade / 选择器扩展自行处理状态变化，不能把 MySQL 与 PostgreSQL 同时写入 `inject` 并期待 OR 语义。

**get**

- **TS**：`reflect.get(name, strict=true)`，不入依赖图；只要求提供方 Fiber `ACTIVE`，不执行 `impl.check`。`reflect.get(name, false)` 仅绕过 Active 检查，主要供 Reflect 内部使用。
- **Rust**：`Context::get(key)`，不入依赖图；只有提供方 Fiber `Active` 且 Provider `Ready` 才返回，否则为 `ServiceUnavailable`。Core 不公开 raw / non-strict 读取；诊断读取 Provider 元数据，不读取服务载荷。

**provide**

- **TS**：包在 `fiber.effect`；写入 `reflect.store[isolateKey]`；ACTIVE 时 `notify`；dispose 删 store 并 `await` 下游 Fiber。
- **Rust**：`Registry::provide` 挂在 EffectScope；`provide_checked` 会保存无阻塞状态检查，并由 `ProviderHandle::refresh()` 在健康状态变化时标脏；`on_dispose` → `remove_provider` → `mark_dirty`。下游由 scheduler 驱动，不在 disposer 内直接 await 依赖方。

### 3.2 Fiber 生命周期

| | TS | Rust |
| --- | --- | --- |
| 激活 | `_setEpoch` → `_reload` → 跑 plugin callback | `claim_activation` → `apply` → Active |
| 依赖掉了 | epoch=`INACTIVE` → `_unload` → Pending | Active 且 providers 变 → unload → Pending |
| 并发串行 | 同 Fiber 的 `inertia` | 全局 scheduler + `EffectOwnership` / handoff |
| 配置更新 | `update(config)` | 宿主构造新实例 → `replace`（Core 无 `update(json)`） |

### 3.3 事件 / 配置 / 隔离

| 主题 | TS | Rust |
| --- | --- | --- |
| 事件模式 | 同一 name，调用方选 dispatch；`serial` 为异步短路，另有同步 `bail` | 模式分 Key（`EventKey` / `WaterfallKey` / `SerialKey` / …）；`serial` 为异步短路；同步 `bail` 仅预留 |
| 过滤 | thisArg + `Context.filter`；内置 `internal/*` | `ListenOptions::filter`；无内置框架事件集 |
| 配置 | `intercept(serviceName, config)` 进服务 `resolveConfig` | `intercept(ConfigKey<T>, T)` + 祖先链 `config(key)` |
| 隔离 | `isolate(name, label?: symbol)` | `isolate(key)` / `isolate_with(key, label)` + Runtime 令牌 |

### 3.4 API 形态一览

| 主题 | TS | Rust |
| --- | --- | --- |
| Context | Proxy；mixin `events`/`logger`/`reflect`/`registry`；每 ctx 绑 `fiber`；Reflect 内部可 non-strict 读取 | 显式方法；Context **不可 dispose**，资源归 Runtime / Effect / Fiber；公开读取始终严格 |
| Plugin | 多形态 + 可选 schema + inject map | `async apply` + `inject() -> Vec<ServiceId>` + `PluginKey` |
| Effect | `fiber.effect(execute, label)` 收集 disposer | `effect()` → `EffectContext`；`on_dispose` / `on_dispose_async` / `spawn`；`dispose_wait` |
| Fiber | `restart` / `update`；无 `replace` | `restart` / **`replace`**；`dispose` / `dispose_wait` |

---

## 4. 运行效率（架构对照，非 benchmark）

无同机压测数据；按下表理解相对成本。

### 4.1 可确认的结构差异

1. **TS Proxy 与 Rust 显式调用**：TS 服务读取可经过 Proxy / Reflect；Rust 必经类型化 `get` 与 Registry 锁。两者谁更快不能只由“有无 Proxy”判断。
2. **TS 同步 notify 与 Rust 脏标记批处理**：TS 变更时同步扫描相关 Fiber；Rust 通过单一 scheduler 合并变化后重算。后者更利于控制抖动期的生命周期顺序。
3. **并发模型不同**：TS 通常运行于 Node 事件循环；Rust 插件业务可在 Tokio 上并行，但 Core scheduler 为保证确定性仍串行。
4. **资源成本不同**：Rust 有 `Mutex`、`Arc` 与 async 协调成本；TS 有 Proxy、动态属性与字符串 / Symbol 解析成本。

### 4.2 基准原则

没有同机 benchmark 前，不宣称任一实现“热路径更快”。真正瓶颈通常是插件业务 I/O。若需量化，应分别测量 `get` 热路径、Provider 状态翻转、批量挂卸载和依赖抖动下的收敛时间。

---

## 5. 缺口与演进建议

### 不必为对齐而塞进 core

- Proxy / mixin / accessor
- 内置 Logger
- Config StandardSchema、`@Inject`、多形态 Plugin 入口
- Generator effect
- loader / HMR（与 deepseek 一样放扩展层）

### 值得评估的薄语义缺口

1. 同步事件 `bail`（**已预留，暂不实现**）：现有 `serial()` 已覆盖异步“首个结果获胜”。仅在同步函数中必须完成首个结果选择时，新增独立 `BailKey` / `bail()`；不得用它替代异步插件、生命周期或服务依赖处理。
2. `AnyOf` / 替代依赖表达式（需要独立设计，不把多个候选直接塞进 `Plugin::inject()`）
3. 可选 `cordis-macros`：仅在多个真实插件出现稳定重复样板后生成 `PluginKey` / `inject` / `ServiceKey`，**不**把 Fiber、scheduler、配置或健康检查宏化

### 跨项目复用方式

```text
cordis-core          ← 库：Runtime 内核（必选）
cordis-macros        ← 可选糖：声明样板
宿主 / 扩展          ← loader、日志、HMR、业务插件
```

核心以 crate 复用；宏只减轻已稳定的样板。热插拔、Provider 就绪语义与依赖重算必须留在库里。

---

## 6. 一句话

概念层（Context 树、isolate/intercept、provide/get/inject、Plugin Fiber、事件多模式、Effect 清理）对齐。TS 强在 Proxy / Reflect、Logger、多形态插件与 `update(config)`；Rust 强在显式 Runtime、类型化 Key、`replace`、隔离令牌、结构化 diagnostics、Provider Ready 语义与 async 协调关闭。性能结论必须以同机基准为准，不能只凭抽象结构下定论。
