use crate::{Context, CoreError, ServiceId, effect::EffectScope, inject::NodeId, plugin::Plugin};
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicBool, Ordering},
};

/// Plugin Fiber 公开生命周期状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FiberState {
    Pending,
    Loading,
    Active,
    Failed,
    Disposed,
}

/// 已挂载插件的可重启 / 可替换生命周期句柄。
pub struct Fiber {
    pub(crate) inner: Arc<FiberInner>,
}

pub(crate) struct FiberInner {
    pub(crate) id: u64,
    pub(crate) node: NodeId,
    pub(crate) registry: Weak<crate::inject::Registry>,
    pub(crate) parent_scope: EffectScope,
    pub(crate) plugin: Mutex<Arc<dyn Plugin>>,
    pub(crate) dependencies: Mutex<Vec<ServiceId>>,
    pub(crate) effect: Mutex<Option<EffectScope>>,
    pub(crate) state: Mutex<FiberState>,
    pub(crate) last_error: Mutex<Option<String>>,
    pub(crate) resolved_providers: Mutex<Vec<u64>>,
    pub(crate) disposed: AtomicBool,
    pub(crate) busy: Mutex<bool>,
    pub(crate) mount_ctx: Mutex<Option<Context>>,
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

    pub fn dispose(&mut self) {
        if self.inner.disposed.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Ok(mut busy) = self.inner.busy.lock() {
            *busy = false;
        }
        {
            let mut state = self.inner.state.lock().expect("state");
            *state = FiberState::Disposed;
        }
        if let Some(effect) = self.inner.effect.lock().expect("effect").take() {
            effect.dispose();
        }
        if let Some(registry) = self.inner.registry.upgrade() {
            registry.unregister_plugin_fiber(self.inner.id);
        }
    }

    pub async fn dispose_wait(&mut self) {
        if self.inner.disposed.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Ok(mut busy) = self.inner.busy.lock() {
            *busy = false;
        }
        {
            let mut state = self.inner.state.lock().expect("state");
            *state = FiberState::Disposed;
        }
        let effect = self.inner.effect.lock().expect("effect").take();
        if let Some(effect) = effect {
            effect.dispose_wait().await;
        }
        if let Some(registry) = self.inner.registry.upgrade() {
            registry.unregister_plugin_fiber(self.inner.id);
        }
    }

    /// 保留同一 Plugin，强制重新解析依赖并 `apply`。
    pub async fn restart(&mut self) -> Result<(), CoreError> {
        if self.is_disposed() {
            return Err(CoreError::FiberDisposed);
        }
        {
            let mut busy = self.inner.busy.lock().expect("busy");
            if *busy {
                return Err(CoreError::FiberBusy);
            }
            *busy = true;
        }
        let result = self.restart_inner().await;
        *self.inner.busy.lock().expect("busy") = false;
        if let Some(registry) = self.inner.registry.upgrade() {
            registry.mark_dirty_public();
        }
        result
    }

    async fn restart_inner(&mut self) -> Result<(), CoreError> {
        let effect = self.inner.effect.lock().expect("effect").take();
        if let Some(effect) = effect {
            effect.dispose_wait().await;
        }
        if self.is_disposed() {
            return Err(CoreError::FiberDisposed);
        }
        self.inner
            .resolved_providers
            .lock()
            .expect("providers")
            .clear();
        *self.inner.last_error.lock().expect("error") = None;
        if !self.inner.set_state_if_alive(FiberState::Pending) {
            return Err(CoreError::FiberDisposed);
        }
        self.inner.try_activate().await
    }

    /// 先等待旧任务结束，再替换 Plugin 并重新激活。
    pub async fn replace(&mut self, plugin: Arc<dyn Plugin>) -> Result<(), CoreError> {
        if self.is_disposed() {
            return Err(CoreError::FiberDisposed);
        }
        {
            let mut busy = self.inner.busy.lock().expect("busy");
            if *busy {
                return Err(CoreError::FiberBusy);
            }
            *busy = true;
        }
        let result = async {
            let effect = self.inner.effect.lock().expect("effect").take();
            if let Some(effect) = effect {
                effect.dispose_wait().await;
            }
            if self.is_disposed() {
                return Err(CoreError::FiberDisposed);
            }
            let deps = plugin.inject();
            *self.inner.plugin.lock().expect("plugin") = plugin;
            *self.inner.dependencies.lock().expect("deps") = deps;
            self.inner
                .resolved_providers
                .lock()
                .expect("providers")
                .clear();
            *self.inner.last_error.lock().expect("error") = None;
            if !self.inner.set_state_if_alive(FiberState::Pending) {
                return Err(CoreError::FiberDisposed);
            }
            self.inner.try_activate().await
        }
        .await;
        *self.inner.busy.lock().expect("busy") = false;
        if let Some(registry) = self.inner.registry.upgrade() {
            registry.mark_dirty_public();
        }
        result
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
            FiberState::Disposed => crate::diagnostics::FiberStateSnapshot::Disposed,
        };
        let deps = self.dependencies.lock().expect("deps").clone();
        let missing = self.missing_dependencies();
        crate::diagnostics::PluginFiberSnapshot {
            id: self.id,
            node: self.node,
            state,
            dependencies: deps.iter().map(|d| d.as_str()).collect(),
            missing_dependencies: missing.iter().map(|d| d.as_str()).collect(),
            last_error: self.last_error.lock().expect("fiber error").clone(),
            root_effect: self
                .effect
                .lock()
                .expect("effect")
                .as_ref()
                .map(|scope| scope.id()),
        }
    }

    /// 在同一把 state 锁内写入；已 dispose / 已是 Disposed 时保持 Disposed，返回 false。
    pub(crate) fn set_state_if_alive(&self, next: FiberState) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        if self.disposed.load(Ordering::Acquire) || *state == FiberState::Disposed {
            *state = FiberState::Disposed;
            return false;
        }
        *state = next;
        true
    }

    pub(crate) fn claim_activation(&self) -> ActivateClaim {
        if self.disposed.load(Ordering::Acquire) {
            return ActivateClaim::Skipped;
        }
        let Ok(mut state) = self.state.lock() else {
            return ActivateClaim::Skipped;
        };
        if self.disposed.load(Ordering::Acquire) || *state == FiberState::Disposed {
            *state = FiberState::Disposed;
            return ActivateClaim::Skipped;
        }
        match *state {
            FiberState::Pending => {
                *state = FiberState::Loading;
                ActivateClaim::Claimed
            }
            FiberState::Loading
            | FiberState::Active
            | FiberState::Failed
            | FiberState::Disposed => ActivateClaim::Skipped,
        }
    }

    fn resolve_provider_ids(
        &self,
        registry: &crate::inject::Registry,
        deps: &[ServiceId],
    ) -> Option<Vec<u64>> {
        let mut providers = Vec::with_capacity(deps.len());
        for key in deps {
            let service = registry.resolve_with_id(self.node, *key)?;
            providers.push(service.0);
        }
        Some(providers)
    }

    fn abandon_loading_to_pending(&self, effect_scope: Option<&EffectScope>, mark_dirty: bool) {
        if let Some(scope) = effect_scope {
            scope.dispose();
        }
        self.resolved_providers.lock().expect("providers").clear();
        *self.last_error.lock().expect("error") = None;
        let _ = self.set_state_if_alive(FiberState::Pending);
        if mark_dirty && let Some(registry) = self.registry.upgrade() {
            registry.mark_dirty_public();
        }
    }

    pub(crate) async fn try_activate(self: &Arc<Self>) -> Result<(), CoreError> {
        if self.disposed.load(Ordering::Acquire) {
            return Err(CoreError::FiberDisposed);
        }
        if self.claim_activation() != ActivateClaim::Claimed {
            return Ok(());
        }

        let Some(registry) = self.registry.upgrade() else {
            let _ = self.set_state_if_alive(FiberState::Failed);
            return Err(CoreError::ContextDisposed);
        };
        let deps = self.dependencies.lock().expect("deps").clone();
        let Some(providers) = self.resolve_provider_ids(&registry, &deps) else {
            self.abandon_loading_to_pending(None, false);
            return Ok(());
        };

        let plugin = self.plugin.lock().expect("plugin").clone();
        let parent_scope = self.parent_scope.clone();
        if parent_scope.is_disposed() {
            *self.last_error.lock().expect("error") =
                Some("parent scope disposed before apply".into());
            let _ = self.set_state_if_alive(FiberState::Failed);
            return Err(CoreError::ContextDisposed);
        }
        let effect_scope = parent_scope.child_named("plugin");
        let mount = match self.mount_ctx.lock().expect("mount").clone() {
            Some(ctx) => ctx,
            None => {
                effect_scope.dispose();
                *self.last_error.lock().expect("error") = Some("mount context missing".into());
                let _ = self.set_state_if_alive(FiberState::Failed);
                return Err(CoreError::ContextDisposed);
            }
        };
        let isolations = mount
            .inner
            .isolations
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_default();
        let apply_ctx = Context {
            inner: Arc::new(crate::context::ContextInner {
                id: mount.inner.id,
                registry: mount.inner.registry.clone(),
                scope: effect_scope.clone(),
                isolations: Mutex::new(isolations),
            }),
        };
        registry.register_effect(
            effect_scope.id(),
            effect_scope.name().to_string(),
            effect_scope.parent_id(),
            Some(mount.inner.id),
            Some(self.id),
            &effect_scope,
        );
        match plugin.apply(&apply_ctx).await {
            Ok(()) => {
                if self.disposed.load(Ordering::Acquire) {
                    effect_scope.dispose();
                    let _ = self.set_state_if_alive(FiberState::Disposed);
                    return Err(CoreError::FiberDisposed);
                }
                if effect_scope.is_disposed() || parent_scope.is_disposed() {
                    effect_scope.dispose();
                    *self.last_error.lock().expect("error") =
                        Some("plugin scope was disposed during apply".into());
                    let _ = self.set_state_if_alive(FiberState::Failed);
                    return Err(CoreError::PluginApply(
                        "plugin scope was disposed during apply".into(),
                    ));
                }
                // Loading 期间 Provider 可能被撤销/替换；成功前必须重解析。
                let Some(fresh) = self.resolve_provider_ids(&registry, &deps) else {
                    self.abandon_loading_to_pending(Some(&effect_scope), true);
                    return Ok(());
                };
                if fresh != providers {
                    self.abandon_loading_to_pending(Some(&effect_scope), true);
                    return Ok(());
                }
                *self.effect.lock().expect("effect") = Some(effect_scope);
                *self.resolved_providers.lock().expect("providers") = providers;
                *self.last_error.lock().expect("error") = None;
                if !self.set_state_if_alive(FiberState::Active) {
                    if let Some(scope) = self.effect.lock().expect("effect").take() {
                        scope.dispose();
                    }
                    self.resolved_providers.lock().expect("providers").clear();
                    return Err(CoreError::FiberDisposed);
                }
                // Active 提交后的补偿重算覆盖最终校验与提交之间的 Provider 漂移。
                registry.mark_dirty_public();
                Ok(())
            }
            Err(error) => {
                effect_scope.dispose();
                if self.disposed.load(Ordering::Acquire) {
                    let _ = self.set_state_if_alive(FiberState::Disposed);
                    return Err(CoreError::FiberDisposed);
                }
                *self.last_error.lock().expect("error") = Some(error.to_string());
                let _ = self.set_state_if_alive(FiberState::Failed);
                Err(CoreError::PluginApply(error.to_string()))
            }
        }
    }

    pub(crate) fn unload_to_pending(&self) {
        if let Some(effect) = self.effect.lock().expect("effect").take() {
            effect.dispose();
        }
        self.resolved_providers.lock().expect("providers").clear();
        let _ = self.set_state_if_alive(FiberState::Pending);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ActivateClaim {
    Claimed,
    Skipped,
}

impl Drop for Fiber {
    fn drop(&mut self) {
        self.dispose();
    }
}

#[cfg(test)]
mod claim_tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Barrier;

    struct NoopPlugin;

    #[async_trait]
    impl Plugin for NoopPlugin {
        async fn apply(&self, _ctx: &Context) -> Result<(), CoreError> {
            Ok(())
        }
    }

    fn stub_fiber() -> Arc<FiberInner> {
        Arc::new(FiberInner {
            id: 1,
            node: 0,
            registry: Weak::new(),
            parent_scope: EffectScope::root(),
            plugin: Mutex::new(Arc::new(NoopPlugin)),
            dependencies: Mutex::new(Vec::new()),
            effect: Mutex::new(None),
            state: Mutex::new(FiberState::Pending),
            last_error: Mutex::new(None),
            resolved_providers: Mutex::new(Vec::new()),
            disposed: AtomicBool::new(false),
            busy: Mutex::new(false),
            mount_ctx: Mutex::new(None),
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
