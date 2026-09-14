use crate::{
    Context, CoreError, context::ContextInner, diagnostics::RuntimeSnapshot, effect::EffectScope,
    inject::Registry,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

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
    shutdown_completed: AtomicBool,
}

impl Runtime {
    pub fn new() -> Result<Self, CoreError> {
        let registry = Registry::new();
        registry.start_scheduler()?;
        let root_id = registry.allocate_id();
        let root_scope = EffectScope::root();
        registry.add_node(root_id, None);
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
                shutdown_completed: AtomicBool::new(false),
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
    /// 这是宿主应使用的正常关闭路径。
    pub async fn shutdown(&self) {
        if self.inner.shutdown_completed.swap(true, Ordering::AcqRel) {
            // 已关闭过：仍确保调度器已停并 awaited。
            self.inner.registry.stop_scheduler().await;
            return;
        }
        self.inner.root.inner.scope.dispose_wait().await;
        self.inner.registry.settle().await;
        self.inner.registry.stop_scheduler().await;
    }

    /// 调度器是否已停止（测试/诊断）。
    pub fn scheduler_stopped(&self) -> bool {
        self.inner.registry.scheduler_stopped()
    }

    /// 只读诊断快照：不含 Service 实例或业务数据。
    pub fn diagnostics(&self) -> RuntimeSnapshot {
        self.inner.registry.diagnostics()
    }
}

impl Drop for RuntimeInner {
    fn drop(&mut self) {
        if self.shutdown_completed.load(Ordering::Acquire) {
            // shutdown 已 await 调度器；Handle 已被 take。
            return;
        }
        // 尽力同步关闭：取消 Root 资源，abort 调度器以释放 Registry。
        self.root.inner.scope.dispose();
        self.registry.abort_scheduler();
    }
}
