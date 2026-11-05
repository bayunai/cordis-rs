# Cordis Runtime Core 架构

## 运行时与可选能力边界

| 层 | 职责 | 本仓库位置 |
| --- | --- | --- |
| **Runtime 内核** | Context、Service、inject、Isolation、Fiber、Effect、Event、诊断 | `crates/cordis-core` |
| **Loader** | 静态 Catalog、内存条目树、预检、reconcile、热提交与通用来源接口 | `crates/cordis-loader` |
| **应用 Host** | 创建 Runtime、显式挂载 LoaderPlugin、运行应用事件循环并受控关闭 | `crates/cordis-host`；应用再接入网络/持久化 |
| **生命周期能力** | Timer / Console Logger 等须由 Host 显式挂载；不属于 Loader builtin | `cordis-plugin-timer`、`cordis-plugin-logger-console` |
| **文件配置树** | 可选 Include：根路径、TOML／JSON／YAML、patches、文件检查与写回；须 Catalog 显式注册 | `crates/cordis-plugin-include` |
| **配置控制面** | Host 显式挂载：settings（表单）→ config-editor（写回）；plugin-manager（库存/启停） | `cordis-plugin-settings`、`cordis-plugin-config-editor`、`cordis-plugin-plugin-manager`；见 [config-surface.md](config-surface.md) |
| **扩展** | 实现 `Plugin`，`provide` / `inject` / `on`，不触碰宿主内部类型 | 插件 crate |

`cordis-core` **不包含** HTTP、数据库、Redis、JSON 配置、Manifest、WASM、网关或计时器语义。
`cordis-loader` 只做进程内编排，不直接读写文件或解析文件格式，不加载动态库、不提供管理后台。
`EntryTreeSource` 让树来源自行完成读取、来源版本检查和写回；Loader 只在统一提交边界调用它。
文件实现全部由 Include 提供，根文件与嵌套文件使用相同来源模型，没有 Loader 特殊文件根。
计时器由独立的 `cordis-plugin-timer` 提供：Host 在 Root 显式 `plugin(TimerPlugin)` 并保留 Fiber；业务插件通过静态 `inject([TIMER.id()])` 依赖，并在 `EffectContext` 上使用 `TimerExt`。

结构化日志由 Core 内置的 Runtime 全局 `LoggerService` 提供（`ctx.logger()?`），**不经** `ServiceKey` / `isolate`，且**不过滤**等级——全部记录进入环形缓冲并分发给 exporter。等级阈值由各 exporter 自行解释（可参考记录上的 `default_level`，来自 `LOGGER_CONFIG.level`）。控制台输出由独立的 `cordis-plugin-logger-console` 挂载；无 Console 时 Logger 仍缓冲。

## 根配置挂载

宿主在代码中显式构造根 Include，指向本地可人工恢复的根文档（约定 `cordis.toml`）。
不再经独立 bootstrap 配方文件间接指向扩展清单。产品路径常挂空根，业务 Entry 由
独立启动组合函数的 deploy ∘ user ∘ home 组成；见 [config-surface.md](config-surface.md)。

```text
应用入口构造 LoaderPlugin::new(catalog)
  → root.plugin(&cordis_loader::PLUGIN_DEFINITION, LoaderPlugin)
  → 分层应用 await prepare_root_include(cordis.toml, &config)，或直接 include_root_entry(id, path)
  → await loader.create(entry, None, None)
  → include Include 读取文件、提供来源上下文与文件版本
  → Loader 预检文件条目并收敛为 include:* 子树
  → 显式挂载 ConfigEditor / Settings / PluginManager；服务挂载不再协调业务树
```

- 根 Include 的 `path` 须为绝对路径（`include_root_entry` 负责解析）；嵌套 Include 相对于所属文件目录。
  文件重载使用 `loader.subtree("include")?.reload()`，没有自动监听。
- Loader 地址由所属来源树前缀与树内 ID 构成；Group 不增加前缀，同树所有 Group 的成员 ID 也必须唯一。Include 边界增加前缀，同树换 Group 保持地址。
- Loader 的内存根 CRUD 不写任何文件；普通 Include 来源树 CRUD 与配置编辑写所属文件。跨来源移动先全量预检，运行时提交后先写目标、成功后写源；目标失败不写源，错误报告两侧状态，不回滚已发生的运行时或文件变更。
  `EntryLocation` 只描述条目位置；文件目录与继承日志策略由 Include 的 `INCLUDE_LOCATION` 提供。
- 扩展包目录、信任公钥、基础日志、SQLite/PostgreSQL 配置存储和文件热更新仍是未来
  宿主或扩展能力，不进入 `cordis-core`。首版 Host 遇到这些字段会因
  `deny_unknown_fields` 直接失败。
- 多节点部署若改用网络数据库，禁止将 SQLite 放在 NFS、SMB 或其他网络文件系统上作为
  共享配置库。

详见 [Host 编排](host.md)。

## Runtime 与 Tokio

- `Runtime::new()` **必须**在 Tokio Runtime 上下文中调用，否则返回 `SchedulerUnavailable`。
- 创建时启动**唯一**的响应式重算调度器；Provider / 注入撤销通过 `Notify` 唤醒。
- 调度器持有取消令牌与 `JoinHandle`；关闭后不再处理 dirty。

## Isolation

- `IsolationLabel` 由 Runtime 分配，跨 Runtime 使用 → `IsolationRuntimeMismatch`。
- `Context::extend()` 创建不可变派生 Context 视图，不创建 Registry 节点或 Scope；父 Context 不会被修改，最后一个引用释放后视图自然回收。
- `Context::isolate(key)` 返回 `(派生 Context, 新标签)`；`isolate_with(key, label)` 返回加入既有标签的派生 Context。二者**复用父 Service identity**，只覆盖声明的 Key。
- 解析：若当前 Context 谱系对该 Key 有隔离覆盖，则只看同 label 的 Provider；否则走父子 Local 覆盖。最近槽位即使未就绪也会遮蔽父级，严格解析绝不静默回退。
- 隔离**只影响声明的 Key**；其他 Service 仍按父子链解析，且与父同槽（可共享或 `ServiceConflict`）。

## 响应式注入

1. `Context::inject(deps, callback)` 登记派生 inject fiber；依赖未齐时为 `Pending`。
2. `provide`、`provide_checked` 的就绪状态变化、Provider 释放都会标记 dirty，调度器串行选批，独立协调器推进卸载与激活，无关服务图不互相等待。
3. 派生 Context 覆盖父 Provider（未隔离 Key）；retiring Provider 保留槽位并遮蔽父级，完成撤销后才恢复父级解析。
4. 同槽冲突返回 `ServiceConflict`；Runtime 内 ID↔TypeId 永久锁定。

## Definition 与 Fiber 生命周期

`PluginDefinition` 是显式身份句柄，与 `Plugin` 对象分离。`named(name)` 创建带名称的定义，`anonymous()` 创建匿名定义；`id()` 返回 `PluginDefinitionId`，`name()` 只提供可选展示元数据。Clone 共享身份，独立创建的同名定义具有不同身份；同一 Definition 可跨 Runtime 使用，实例状态仍由各 Runtime 独立维护。

```text
Context::plugin(&definition, Arc<dyn Plugin>)
        │
        ├─ 登记 Fiber（Plugin::inject 声明依赖）
        ├─ 依赖未齐 → Pending；齐则 Loading → apply → Active / Failed
        └─ 返回拥有型 Fiber
                ├─ dispose / dispose_wait / Drop：卸载
                ├─ restart()：保留执行逻辑，显式重新激活
                ├─ replace(plugin)：保留 Definition 身份，替换执行对象
                └─ handle()：取得可克隆的非拥有型 FiberHandle
Context::inject(deps, callback)
        └─ 返回 FiberHandle；Drop 不卸载，生命周期归属创建它的 Scope
Runtime::unmount(definition.id())
        └─ 卸载当前 Runtime 中该定义的全部实例
```

`Plugin` 只定义 `inject()` 与 `apply()`，不自行产生或校验身份。`FiberHandle` 可查询状态、快照和依赖，并调用 `restart()`、`dispose()`、`dispose_wait()`；只有拥有型插件 `Fiber` 提供 `replace(plugin)`。

`Loading` / `Unloading` 由每个 Fiber 的内部协调器收敛。调用方取消已提交操作的等待，不取消协调器。首次 `plugin()` 在拥有型句柄交付前被取消时撤销该次挂载，不遗留实例。旧执行、子 Scope、清理和受控任务完整结束后才允许下一轮激活。

普通配置更新由宿主校验后构造新的不可变 Plugin 对象，再调用 `Fiber::replace`；身份由挂载时传入的 Definition 决定。热配置由 Loader 的 `PreparedExtension::volatile()` 与 `ConfigRef<Live>` 提交，Core 不保存可变插件配置或配置工厂。

配置候选无效时不调用 `replace`。新对象 `apply` 失败或清理失败进入 `Failed`，不回滚已释放对象。插件与 `inject` 统一采用显式恢复：依赖变化不会重试 Failed；只有 `restart()` 或插件 `replace()` 会启动下一次尝试。

### Factory 统一构造边界

Factory 是静态 Loader 的必要构造边界：Catalog 根据条目 `name` 找到 Factory，将配置
解析、校验并构造为 `Plugin`；Core 只负责挂载和生命周期，不认识业务配置或插件发现。
因此 Factory 本身不是冗余，也不应将这部分逻辑散回应用入口。

`ExtensionFactory::prepare()` 是唯一预备入口，`register(&definition, factory)` 是唯一登记入口；Catalog 将工厂与显式 Definition 关联。
`PreparedExtension::ordinary()` 表达普通插件，`PreparedExtension::volatile()` 表达包含
普通配置与热配置的插件；两者均延迟构造。普通插件无需空 Live 或划分方法，只有热配置
结果创建配置引用。`PreparedExtension::tree()` 表达 Include 等来源树载体，在同来源配置更新时
等待子树局部协调，不用普通 volatile 订阅假装提交已经完成。Loader 内部统一比较、构造与提交，
插件作者不操作类型擦除或写入端。

普通/热配置能力切换或绑定类型变化会完整替换实例，使用 Catalog 登记的 Definition 并关闭旧引用。
该 API 为直接迁移，外部应用需同步更新；不保留双入口或旧构造接口。

### 配置控制面

由两个 Host 插件承接（Loader 无表单兼容 API）：

- **settings**：安全 Schema、脱敏 value、秘密存在状态与字段操作拼装。
- **config-editor**：deploy ∘ user ∘ home 组成；表单 `edit` / `set_enabled` 只写 user，
  再经根 Include `patches` reconcile。运维整表才走 `commit_source` → `commit_entry_config`。

不建立独立 Settings 存储。普通/热配置都可管理，禁用或失败实例也可修复。Schema 的
`writeOnly` 是秘密投影标记，不参与普通/热配置划分。HTTP、鉴权、表单组件与 DTO 属于应用 Host。
持久化失败和局部生命周期失败继续遵循现有不回滚语义。

分层见 [配置控制面拆分](config-surface.md)；行为细则见 [Host 文档](host.md#面向管理界面的配置控制面)。

### Provider 就绪状态

普通 `provide()` 注册的 Provider 默认 `Ready`。`provide_checked(key, value, check)` 会保存一个同步、无阻塞的状态检查函数，并返回 `ProviderHandle`；Provider 自己的健康任务在本地状态变化后调用 `refresh()`。Core 不轮询、不发网络请求。

公开 `get()`、`inject()` 与 Plugin `inject()` 一律要求 Provider 的检查结果为 `Ready`，且其所属 Plugin Fiber 已 `Active`。Core 不提供读取未就绪实例的公开旁路；诊断通过 Registry 内部元数据区分“未注册”与“已注册未就绪”。可用性翻转会递增 Provider revision，确保短暂的失效/恢复也会使消费者重新验证。

`Runtime::subscribe_fiber_states()` 提供容量为 1024 的只读广播流，依次报告 `None → Pending` 以及后续状态转换。监听滞后会收到 `broadcast::error::RecvError::Lagged`；宿主应调用 `Runtime::diagnostics()` 重建当前快照。状态通知不能阻塞、拒绝或回滚 Fiber 生命周期。

## 服务依赖与卸载顺序

只有 `Plugin::inject` 与 `Context::inject` 的显式声明建立生命周期绑定。Registry 在同一次锁内解析依赖并绑定 Provider 实例 ID 与本轮消费者执行 Scope；普通 `get()`、`Services` 克隆和 `Arc` 克隆不建立或延长绑定。绑定从用户回调开始前持续到回调、子 Scope、清理和受控任务全部结束，覆盖 Loading、失败及卸载中的执行。

Provider 开始释放时先标记 retiring，拒绝新绑定并请求已有消费者卸载。提供者等待所有消费者完成后，才取消自身任务并运行资源清理。这个顺序适用于普通 Effect、插件 restart / replace / unmount 及 Runtime shutdown；兄弟插件也按声明的服务依赖排序。资源由实际注册 Provider 的 Scope 保护，不由 Definition ID 或 ServiceKey 代替实例归属。

消费者清理使用激活时捕获的服务引用；`Context::get()` 仍严格要求 Provider Ready 且所属 Fiber Active。该保证保护 Core 管理的资源释放，不保证外部故障中的服务继续可用。服务等待关系与 Effect 父子等待关系形成环时返回 `DependencyCycle { scopes }`，不执行候选回调；依赖尚未满足的循环注入保持 Pending。

## Effect

- 具名 `EffectHandle`（`effect()` / `effect_named`）；记录父子、取消状态与资源计数。
- Provider / 订阅 / 任务 / 子 Fiber 挂在创建它们的 Effect 或 Fiber 上；Context 只是视图，不能单独释放。
- 每次 Scope 释放共享一个 `DisposeCompletion`：`dispose()` 后再 `dispose_wait()`（可多次）等待同一轮结果与同一聚合错误。
- `dispose`：立即禁止目标 Scope 树新增工作并发布唯一 Completion，先关闭 Provider 新绑定、请求消费者卸载，再按依赖顺序进入实际清理。消费者完整结束后才取消本 Scope 任务；等待本轮用户回调和子 Scope 结束后，执行同步 `on_dispose`（LIFO）、async disposer（严格串行 LIFO）并收敛受控任务。同步清理不再承诺在 `dispose()` 返回前运行。
- `dispose_wait`：等待同一 `DisposeCompletion`；取消调用方等待不取消内部释放协调器。失败仍完成其他必要清理，聚合为 `DisposeFailed`；同一 Completion 的错误经依赖和父子路径汇合时按来源去重。
- 长期后台工作用 `spawn`（带取消令牌）；一次性收尾用 `on_dispose_async`（无取消令牌）。子 Scope 使用独立令牌，由释放协调器按依赖顺序取消，不通过父令牌立即传播取消。
- 仅 Drop Runtime 为非受控关闭：同步 cleanup + 终止已启动工作，**不**启动尚未执行的 async disposer，也不保证依赖卸载顺序；完整受控释放须 `dispose_wait` / `shutdown`。

## Event

同 ID 锁定 **模式 + TypeId**。Observe / Waterfall / Serial / Parallel 四模式。

监听选项（`*_with_options` / `ListenOptions`）：

- `once`：真正调用前原子注销，并发派发最多执行一次
- `prepend`：同事件列表内优先于普通监听器；多个 prepend 最新注册优先
- `global`：Runtime 内同 ID 广播（先于祖先链本地监听器）；无 isolate 事件过滤
- `filter`：同步 `Fn(&T) -> Result<bool, CoreError>`；`false` 跳过，`Err` 中止（Parallel 计入聚合）

无选项的 `on*` 保持默认行为。配置覆盖见 `intercept`（独立于 Event）。

## 诊断

`Runtime::diagnostics()` 统一包含 `fibers`、`definitions`、`providers`、`effects`、`isolations`。每个 `FiberSnapshot` 使用 `FiberState`，并报告 `id`、`definition_id`、`parent_fiber_id`、可选 `name`、依赖、缺失/不可用原因、最近错误及根 Effect。Definition 条目报告 `definition_id`、可选名称、`unmounting` 和 `fiber_ids`。事件 `FiberStateChange` 同样包含 Definition 与父 Fiber 身份；日志记录使用可选 `definition_id` 与 `plugin_name`。诊断不含 Service / Config 业务值。

## Config / intercept

`ConfigKey<T>` 独立于 `ServiceKey`。`Context::intercept(key, value)` 创建**共享父视图 Service
identity** 的派生配置视图并写入覆盖；`config(key)` 向父链查找最近值。`provide` / `get` /
`inject` 的服务域不因配置覆盖而改变。需要新服务域时使用 `extend()`；按 Key 隔离可见性使用 `isolate` / `isolate_with`。

## Definition 分组

Runtime 按显式 Definition ID 归组 Fiber；`Runtime::unmount(definition.id())` 标记卸载中、拒绝该定义的新挂载，等待全部实例完整释放后清理分组。释放错误聚合返回，但仍完成分组清理。

生命周期等待同时检查服务依赖与 Effect 父子等待关系：用户回调中的 `dispose_wait`、`restart`、`replace` 若会反向等待当前 Scope，立即返回 `LifecycleReentrant { scope }`，不启动该操作。`unmount` 保留同树检查，并增加跨树服务依赖检查，拒绝时返回 `UnmountReentrant`；卸载不形成反向等待的无关插件仍允许。同步清理、异步清理、`apply`、inject 和受控任务均携带所属 Scope。同树判定使用创建时冻结的共享祖先链（`detach` 后仍有效）。`shutdown` 的全局 `ShutdownReentrant` 约束不变。

卸载 / `dispose_now` 在 Effect `DisposeCompletion` 完成前保留 `pending_wait` 与 plugin 索引，使 async disposer / 并发 `unmount` 仍能看到归属；完成后才 `unregister`。

首次挂载：`Context::plugin` 的 wait 被取消时向本轮协调器请求放弃 Handle——若 `apply` 尚未开始则直接撤销；若已开始则**不** abort 用户 Future，等 `apply` 返回后不提交 Active、释放临时 Scope 并注销 Fiber。因此 `Plugin::apply` **不得无限阻塞**；长等须用 Effect `spawn` + 取消令牌。

`Fiber::replace` 保留挂载时的 Definition 身份；旧 Effect 释放失败时进入 `Failed`，不启动新 `apply`。

## 关闭

宿主须 `await Runtime::shutdown()`（返回 `Result`）。插件热替换优先 `fiber.replace` 或 `dispose_wait` 后再挂载。单纯 `dispose()` 为 fire-and-forget；之后仍可 `dispose_wait()` 观察同一轮释放结果。仅 `drop` Runtime **不**保证执行未启动的 async disposer。

### 服务资源的 Scope 归属

Provider 的保护范围包含实际注册 Scope 的非消费资源子树；直接及传递消费者分支先完成清理，再释放提供者资源。把服务注册到 Root 等过宽 Scope，可能使其他服务的资源后代与消费者互相等待。Core 在绑定候选依赖时检查完整等待图并返回 `DependencyCycle`，不会等到 shutdown 才发现挂死。只有服务自身资源应归入其 Provider Scope；无全局资源归属要求的宿主服务应放在独立 Effect，而不是将整个 Runtime 子树归为它的资源。
