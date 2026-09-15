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
| Service 基类 | `service.ts`：自动 provide、`check`、`invoke` | 仅 `ServiceKey` / `Services` | 扩展层按需封装 |
| 多形态 Plugin | fn / class / `{apply}`；`@Inject`；`Config` StandardSchema | 单一 `Plugin` trait + `PluginKey` | 可选宏只生成样板 |
| `fiber.update(config)` | 校验后 restart + `internal/update` | 无；宿主校验后 **`Fiber::replace`** | 保持现状 |
| 事件 `bail` | 同步短路 | 无 | 有需求再评估 |
| provide 的 `check` 谓词 | 实现「有了但不就绪」 | 只有解析到 / 解析不到 | 语义缺口，可评估 |
| `get(strict)` | strict 时要求提供方 Fiber 为 ACTIVE | `get` 解析到即返回 | 行为略松 |
| Generator effect | yield 多个 disposer | `on_dispose` / `on_dispose_async` / `spawn` | 手写即可 |
| Fiber thenable | `await fiber` | `settle` / 显式句柄 | API 风格差异 |

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
- **Rust**：`Plugin::inject() -> Vec<ServiceId>`；`mark_dirty` → scheduler；就绪 = `provider_ids.len() == deps.len()`；Active 时 provider id 集合变化 → unload → Pending → 再激活。

两边都**没有 AnyOf / OR**。可选依赖用 `get`，不写进 `inject`。数据库等多后端场景用 facade 插件（如 DbPlugin）`get` 选后端再 `provide(DATABASE)`。

**get**

- **TS**：`reflect.get(name, strict=true)`，不入依赖图；未就绪返回 `undefined`。
- **Rust**：`Context::get(key)`，不入依赖图；解析不到 → `ServiceUnavailable`。不强制提供方 Fiber 为 Active。

**provide**

- **TS**：包在 `fiber.effect`；写入 `reflect.store[isolateKey]`；ACTIVE 时 `notify`；dispose 删 store 并 `await` 下游 Fiber。
- **Rust**：`Registry::provide` 挂在 EffectScope；`on_dispose` → `remove_provider` → `mark_dirty`；由 scheduler 驱动下游，不在 provide disposer 里直接 await 依赖方。

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
| 事件模式 | 同一 name，调用方选 dispatch；另有 `bail` | 模式分 Key（`EventKey` / `WaterfallKey` / …），无 bail |
| 过滤 | thisArg + `Context.filter`；内置 `internal/*` | `ListenOptions::filter`；无内置框架事件集 |
| 配置 | `intercept(serviceName, config)` 进服务 `resolveConfig` | `intercept(ConfigKey<T>, T)` + 祖先链 `config(key)` |
| 隔离 | `isolate(name, label?: symbol)` | `isolate(key)` / `isolate_with(key, label)` + Runtime 令牌 |

### 3.4 API 形态一览

| 主题 | TS | Rust |
| --- | --- | --- |
| Context | Proxy；mixin `events`/`logger`/`reflect`/`registry`；每 ctx 绑 `fiber` | 显式方法；Context **不可 dispose**，资源归 Runtime / Effect / Fiber |
| Plugin | 多形态 + 可选 schema + inject map | `async apply` + `inject() -> Vec<ServiceId>` + `PluginKey` |
| Effect | `fiber.effect(execute, label)` 收集 disposer | `effect()` → `EffectContext`；`on_dispose` / `on_dispose_async` / `spawn`；`dispose_wait` |
| Fiber | `restart` / `update`；无 `replace` | `restart` / **`replace`**；`dispose` / `dispose_wait` |

---

## 4. 运行效率（架构对照，非 benchmark）

无同机压测数据；按下表理解相对成本。

### 4.1 Rust 相对更省 / 更可控

1. **无 Proxy**：TS 每次 `ctx.xxx` 走 Reflect trap + isolate 链；Rust 显式 `get`/`provide`。
2. **类型化 HashMap 解析**：TS 为 string + Symbol；Rust 为 `ProviderKey` + `ServiceId`，少字符串比较，类型冲突提前拦。
3. **脏标记批处理**：TS 同步 `notify` 扫全部 fiber，可能立刻 unload/reload；Rust `mark_dirty` → 单一 scheduler 取批，突发变更可合并。
4. **真并行**：TS 绑 Node 单线程；Rust 业务可在 Tokio 上并行（核心 scheduler 仍串行重算以保证正确性）。

### 4.2 TS 相对更轻的地方

1. **无锁单线程**：Rust Registry / Fiber 状态需 `Mutex`；极高频 provide/get 时锁是额外成本。
2. **同步激活延迟**：TS 同线程 `_reload`；Rust 经 async `apply` + 协调器，单次挂载调度延迟通常更高（微秒～毫秒级，一般不是吞吐数量级差距）。
3. **`Arc` 克隆**：Rust 服务以 `Arc<T>` 共享。

### 4.3 场景对照

| 场景 | 相对更优 |
| --- | --- |
| 极高频读服务（类 `ctx.prop`） | Rust（无 Proxy） |
| 单线程、插件少、变更少 | TS（无锁、同步刷新） |
| 大量插件同时上下线 / 依赖抖动 | Rust（dirty 批处理） |
| CPU 密集业务挂在插件里 | Rust（真并行） |
| 嵌入长期运行的网关进程 | Rust（shutdown / 诊断 / 类型边界） |

就绪判定两边都是约 O(依赖数 × 解析深度)；Rust 多一次锁内快照。真正瓶颈通常在插件业务 I/O，不在核心 HashMap/锁。若要量化，可对 `provide` 风暴、`get` 热路径、`plugin` 挂卸载做微基准。

---

## 5. 缺口与演进建议

### 不必为对齐而塞进 core

- Proxy / mixin / accessor
- 内置 Logger
- Config StandardSchema、`@Inject`、多形态 Plugin 入口
- Generator effect
- loader / HMR（与 deepseek 一样放扩展层）

### 值得评估的薄语义缺口

1. `provide` 可选 `check`（或等价「可见但不就绪」）
2. 事件 `bail`（若确有同步短路需求）
3. 可选 `cordis-macros`：只生成 `PluginKey` / `inject` / `ServiceKey` 样板，**不**把 Fiber/scheduler 宏化

### 跨项目复用方式

```text
cordis-core          ← 库：Runtime 内核（必选）
cordis-macros        ← 可选糖：声明样板
宿主 / 扩展          ← loader、日志、HMR、业务插件
```

核心以 crate 复用；宏只减轻样板。热插拔就绪语义必须留在库里。

---

## 6. 一句话

概念层（Context 树、isolate/intercept、provide/get/inject、Plugin Fiber、事件多模式、Effect 清理）对齐；TS 强在 Proxy/Reflect/Logger/多形态插件与 `update(config)`；Rust 强在显式 Runtime、类型化 Key、`replace`、隔离令牌、结构化 diagnostics 与 async 协调关闭。效率上无结构性「慢一个数量级」问题；抖动场景 Rust 批调度更稳，单线程微操作 TS 更轻。
