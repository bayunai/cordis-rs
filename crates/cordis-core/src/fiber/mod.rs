//! 插件 Fiber 公开类型与句柄。
//!
//! 公开 [`Fiber`]、[`FiberState`]、[`FiberStateChange`]；
//! 子模块：`state` 合法转换与通知，`lifecycle` 重启/替换/释放，`activate` 依赖就绪后 apply，
//! `coordinator` 持有可等待的生命周期协调器。

mod activate;
mod coordinator;
mod lifecycle;
mod ownership;
mod state;

pub(crate) use coordinator::{HandleHandoff, InitialMountWaitGuard, LifecycleOp};
pub(crate) use ownership::EffectOwnership;
pub(crate) use state::ActivateClaim;
pub use state::{FiberState, FiberStateChange};

use crate::{
    Context, ServiceId,
    effect::EffectScope,
    plugin::{Plugin, PluginKey},
    registry::NodeId,
};
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicBool, Ordering},
};

use coordinator::LifecycleCompletion;

/// 已挂载插件的可重启 / 可替换生命周期句柄。
pub struct Fiber {
    pub(crate) inner: Arc<FiberInner>,
}

pub(crate) struct FiberInner {
    pub(crate) id: u64,
    pub(crate) plugin_key: PluginKey,
    pub(crate) node: NodeId,
    pub(crate) registry: Weak<crate::registry::Registry>,
    pub(crate) parent_scope: EffectScope,
    pub(crate) plugin: Mutex<Arc<dyn Plugin>>,
    pub(crate) dependencies: Mutex<Vec<ServiceId>>,
    pub(crate) ownership: Mutex<EffectOwnership>,
    /// 本轮 Fiber 释放的最终结果；供延迟 `dispose_wait` 读取同一错误。
    pub(crate) dispose_result: Mutex<Option<Result<(), crate::CoreError>>>,
    pub(crate) state: Mutex<FiberState>,
    pub(crate) last_error: Mutex<Option<String>>,
    pub(crate) resolved_providers: Mutex<Vec<u64>>,
    pub(crate) disposed: AtomicBool,
    pub(crate) busy: Mutex<bool>,
    pub(crate) lifecycle: Mutex<Option<Arc<LifecycleCompletion>>>,
    pub(crate) mount_ctx: Mutex<Option<Context>>,
    pub(crate) handoff: Mutex<HandleHandoff>,
}

impl Fiber {
    pub fn id(&self) -> u64 {
        self.inner.id
    }

    pub fn state(&self) -> FiberState {
        *self.inner.state.lock().expect("fiber state")
    }

    pub fn last_error(&self) -> Option<String> {
        self.inner.last_error.lock().expect("fiber error").clone()
    }

    pub fn is_disposed(&self) -> bool {
        self.inner.disposed.load(Ordering::Acquire) || matches!(self.state(), FiberState::Disposed)
    }

    pub fn missing_dependencies(&self) -> Vec<ServiceId> {
        self.inner.missing_dependencies()
    }

    pub fn snapshot(&self) -> crate::diagnostics::PluginFiberSnapshot {
        self.inner.snapshot()
    }
}

impl FiberInner {
    pub(crate) fn missing_dependencies(&self) -> Vec<ServiceId> {
        let Some(registry) = self.registry.upgrade() else {
            return Vec::new();
        };
        let deps = self.dependencies.lock().expect("deps").clone();
        deps.into_iter()
            .filter(|key| registry.resolve(self.node, *key).is_none())
            .collect()
    }

    pub(crate) fn snapshot(&self) -> crate::diagnostics::PluginFiberSnapshot {
        let state = match *self.state.lock().expect("fiber state") {
            FiberState::Pending => crate::diagnostics::FiberStateSnapshot::Pending,
            FiberState::Loading => crate::diagnostics::FiberStateSnapshot::Loading,
            FiberState::Active => crate::diagnostics::FiberStateSnapshot::Active,
            FiberState::Failed => crate::diagnostics::FiberStateSnapshot::Failed,
            FiberState::Unloading => crate::diagnostics::FiberStateSnapshot::Unloading,
            FiberState::Disposed => crate::diagnostics::FiberStateSnapshot::Disposed,
        };
        let deps = self.dependencies.lock().expect("deps").clone();
        let missing = self.missing_dependencies();
        crate::diagnostics::PluginFiberSnapshot {
            id: self.id,
            plugin_key: self.plugin_key.as_str(),
            node: self.node,
            state,
            dependencies: deps.iter().map(|d| d.as_str()).collect(),
            missing_dependencies: missing.iter().map(|d| d.as_str()).collect(),
            last_error: self.last_error.lock().expect("fiber error").clone(),
            root_effect: self
                .ownership
                .lock()
                .expect("ownership")
                .live_scope()
                .map(|scope| scope.id()),
        }
    }

    pub(crate) fn needs_unmount_wait(&self) -> bool {
        self.ownership
            .lock()
            .map(|guard| guard.needs_unmount_wait())
            .unwrap_or(true)
            || !self.disposed.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod claim_tests {
    use super::*;
    use crate::CoreError;
    use async_trait::async_trait;
    use std::sync::Barrier;

    struct NoopPlugin;

    #[async_trait]
    impl Plugin for NoopPlugin {
        fn key(&self) -> crate::plugin::PluginKey {
            crate::plugin::PluginKey::new("test.noop")
        }
        async fn apply(&self, _ctx: &Context) -> Result<(), CoreError> {
            Ok(())
        }
    }

    fn stub_fiber() -> Arc<FiberInner> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        let handle = runtime.handle().clone();
        // Keep the runtime alive for the scope handle duration of this test.
        std::mem::forget(runtime);
        Arc::new(FiberInner {
            id: 1,
            plugin_key: crate::plugin::PluginKey::new("test.noop"),
            node: 0,
            registry: Weak::new(),
            parent_scope: EffectScope::root(handle),
            plugin: Mutex::new(Arc::new(NoopPlugin)),
            dependencies: Mutex::new(Vec::new()),
            ownership: Mutex::new(EffectOwnership::Empty),
            dispose_result: Mutex::new(None),
            state: Mutex::new(FiberState::Pending),
            last_error: Mutex::new(None),
            resolved_providers: Mutex::new(Vec::new()),
            disposed: AtomicBool::new(false),
            busy: Mutex::new(false),
            lifecycle: Mutex::new(None),
            mount_ctx: Mutex::new(None),
            handoff: Mutex::new(HandleHandoff::Preparing),
        })
    }

    #[test]
    fn claim_activation_is_exclusive() {
        let fiber = stub_fiber();
        let barrier = Arc::new(Barrier::new(8));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let fiber = fiber.clone();
            let barrier = barrier.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                fiber.claim_activation()
            }));
        }
        let claimed = handles
            .into_iter()
            .map(|handle| handle.join().expect("join"))
            .filter(|claim| *claim == ActivateClaim::Claimed)
            .count();
        assert_eq!(claimed, 1);
        assert_eq!(*fiber.state.lock().expect("state"), FiberState::Loading);
    }
}
