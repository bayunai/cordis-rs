//! Runtime、根 Context 与全局关闭入口。
//!
//! 拥有根 EffectScope / Registry，并对外暴露挂载与诊断；
//! 不是业务宿主，不承担应用协议或外部基础设施职责。

use crate::{
    Context, CoreError, PluginKey,
    context::ContextInner,
    diagnostics::RuntimeSnapshot,
    effect::EffectScope,
    fiber::{Fiber, FiberStateChange},
    registry::Registry,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use tokio::{runtime::Handle, sync::Notify, task::JoinHandle};

/// Runtime 的所有 Context 与 Effect 的根所有者。
///
/// 必须在 Tokio Runtime 上下文中创建，以便启动唯一的响应式重算调度器。
///
/// 正常关闭请调用 [`Runtime::shutdown`]。仅 `drop` 时会尽力停止调度器（abort），
/// **不**等待业务受控任务退出。
#[derive(Clone)]
pub struct Runtime {
    inner: Arc<RuntimeInner>,
}

struct RuntimeInner {
    registry: Arc<Registry>,
    root: Context,
    handle: Handle,
    shutdown: Mutex<Option<Arc<ShutdownCompletion>>>,
    /// 受控 shutdown 已完成 `stop_scheduler().await`；供 Drop 跳过 abort。
    scheduler_awaited: AtomicBool,
}

struct ShutdownCompletion {
    notify: Notify,
    result: Mutex<Option<Result<(), CoreError>>>,
    /// 保持关闭协调器独立于任一调用者 future；首个 shutdown() 调用者取消后，
    /// 其余等待者仍能取得同一轮结果。
    retain: Mutex<Option<JoinHandle<()>>>,
}

impl ShutdownCompletion {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            notify: Notify::new(),
            result: Mutex::new(None),
            retain: Mutex::new(None),
        })
    }

    fn attach_coordinator(&self, handle: JoinHandle<()>) {
        *self.retain.lock().expect("shutdown retain") = Some(handle);
    }

    fn finish(&self, result: Result<(), CoreError>) {
        {
            let mut slot = self.result.lock().expect("shutdown completion");
            if slot.is_some() {
                return;
            }
            *slot = Some(result);
        }
        self.notify.notify_waiters();
    }

    async fn wait(self: &Arc<Self>) -> Result<(), CoreError> {
        loop {
            let notified = self.notify.notified();
            {
                let slot = self.result.lock().expect("shutdown completion");
                if let Some(result) = slot.as_ref() {
                    return result.clone();
                }
            }
            notified.await;
        }
    }
}

impl Runtime {
    pub fn new() -> Result<Self, CoreError> {
        let registry = Registry::new();
        registry.start_scheduler()?;
        let root_id = registry.allocate_id();
        let handle = tokio::runtime::Handle::current();
        let root_scope = EffectScope::root(handle.clone());
        registry.add_node(
            root_id,
            None,
            std::collections::HashMap::new(),
            std::collections::HashMap::new(),
        )?;
        registry.bind_node_lifecycle(root_id, &root_scope);
        Ok(Self {
            inner: Arc::new(RuntimeInner {
                registry: registry.clone(),
                root: Context {
                    inner: Arc::new(ContextInner {
                        id: root_id,
                        registry,
                        scope: root_scope,
                    }),
                },
                handle,
                shutdown: Mutex::new(None),
                scheduler_awaited: AtomicBool::new(false),
            }),
        })
    }

    pub fn root(&self) -> Context {
        self.inner.root.clone()
    }

    /// 等待当前已排队的响应式依赖图收敛。
    ///
    /// 只保证调用时已排队变更收敛，不阻止之后的并发写入。
    pub async fn settle(&self) {
        self.inner.registry.settle().await;
    }

    /// 受控关闭：Root dispose_wait → settle → 等待调度器退出。
    ///
    /// 这是宿主应使用的正常关闭路径。释放错误仍会完成 settle 与停调度器后返回。
    /// 并发与后续调用共享同一轮 completion 与同一结果。
    pub async fn shutdown(&self) -> Result<(), CoreError> {
        let (completion, start_coordinator) = {
            let mut slot = self.inner.shutdown.lock().expect("shutdown slot");
            if let Some(existing) = slot.as_ref() {
                (existing.clone(), false)
            } else {
                let completion = ShutdownCompletion::new();
                *slot = Some(completion.clone());
                (completion, true)
            }
        };

        if start_coordinator {
            let inner = self.inner.clone();
            let completion_for_task = completion.clone();
            let coordinator = self.inner.handle.spawn(async move {
                let dispose_result = inner.root.inner.scope.dispose_wait().await;
                inner.registry.settle().await;
                inner.registry.stop_scheduler().await;
                inner.scheduler_awaited.store(true, Ordering::Release);
                completion_for_task.finish(dispose_result);
            });
            completion.attach_coordinator(coordinator);
        }

        completion.wait().await
    }

    /// 调度器是否已停止（测试/诊断）。
    pub fn scheduler_stopped(&self) -> bool {
        self.inner.registry.scheduler_stopped()
    }

    /// 只读诊断快照：不含 Service 实例或业务数据。
    pub fn diagnostics(&self) -> RuntimeSnapshot {
        self.inner.registry.diagnostics()
    }

    /// 订阅 Plugin Fiber 生命周期转换；接收滞后时调用 `diagnostics()` 重建快照。
    pub fn subscribe_fiber_states(&self) -> tokio::sync::broadcast::Receiver<FiberStateChange> {
        self.inner.registry.subscribe_fiber_states()
    }

    /// 按 [`PluginKey`] 统一卸载：拒绝并发新挂载，等待该组全部 Fiber `dispose_wait`。
    ///
    /// 即使部分 Fiber 释放失败，仍会尝试释放同组其余实例并清理分组，再返回聚合错误。
    pub async fn unmount(&self, key: PluginKey) -> Result<usize, CoreError> {
        let fibers = self.inner.registry.begin_plugin_unmount(key)?;
        let count = fibers.len();
        let mut errors = Vec::new();
        for fiber in fibers {
            let mut handle = Fiber { inner: fiber };
            if let Err(error) = handle.dispose_wait().await {
                match error {
                    CoreError::DisposeFailed { errors: mut nested } => errors.append(&mut nested),
                    other => errors.push(other.to_string()),
                }
            }
        }
        self.inner.registry.finish_plugin_unmount(key);
        if errors.is_empty() {
            Ok(count)
        } else {
            Err(CoreError::DisposeFailed { errors })
        }
    }
}

impl Drop for RuntimeInner {
    fn drop(&mut self) {
        if self.scheduler_awaited.load(Ordering::Acquire) {
            // shutdown 已 await 调度器；Handle 已被 take。
            return;
        }
        // 尽力同步关闭：不启动 async disposer，abort 调度器以释放 Registry。
        self.root.inner.scope.abandon();
        self.registry.abort_scheduler();
    }
}
