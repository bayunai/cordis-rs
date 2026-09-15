//! Fiber 的 restart / replace / dispose / unload。
//!
//! 编排经内部协调器执行；状态机规则在 `state`，依赖就绪后的 apply 在 `activate`。

use super::{
    Fiber, FiberInner, FiberState,
    coordinator::{LifecycleCompletion, LifecycleOp},
};
use crate::{
    CoreError,
    effect::EffectScope,
    plugin::{Plugin, read_metadata},
};
use std::sync::{Arc, atomic::Ordering};

impl Fiber {
    pub fn dispose(&mut self) {
        self.inner.dispose_now();
    }

    pub async fn dispose_wait(&mut self) -> Result<(), CoreError> {
        self.inner.dispose_wait_inner().await
    }

    /// 保留同一 Plugin，强制重新解析依赖并 `apply`。
    ///
    /// 调用方 Future 取消不中断内部协调器：旧 Effect 释放后会继续收敛并重激活。
    pub async fn restart(&mut self) -> Result<(), CoreError> {
        if self.is_disposed() {
            return Err(CoreError::FiberDisposed);
        }
        let completion = self.inner.start_lifecycle(LifecycleOp::Restart)?;
        completion.wait().await
    }

    /// 先等待旧任务结束，再替换为同 Key 的预校验不可变 Plugin 实例并重新激活。
    ///
    /// 配置 Schema、反序列化与校验属于宿主：配置无效时宿主不得调用本方法，旧
    /// Active 实例继续运行。候选元数据预检成功后即视为已提交；调用方取消不回滚
    /// 旧实例，协调器继续切换。新实例 `apply()` 失败时 Fiber 进入
    /// [`FiberState::Failed`]，不会自动恢复旧实例。
    pub async fn replace(&mut self, plugin: Arc<dyn Plugin>) -> Result<(), CoreError> {
        if self.is_disposed() {
            return Err(CoreError::FiberDisposed);
        }
        // 候选元数据必须在旧实例开始卸载前全部读取；panic 或不匹配不影响旧实例。
        let metadata = read_metadata(plugin.as_ref())?;
        if metadata.key != self.inner.plugin_key {
            return Err(CoreError::PluginKeyMismatch {
                expected: self.inner.plugin_key,
                actual: metadata.key,
            });
        }
        let completion = self
            .inner
            .start_lifecycle(LifecycleOp::Replace { plugin, metadata })?;
        completion.wait().await
    }
}

impl FiberInner {
    fn begin_dispose(&self) -> Option<Option<EffectScope>> {
        if self.disposed.swap(true, Ordering::AcqRel) {
            return None;
        }
        if let Ok(mut busy) = self.busy.lock() {
            *busy = false;
        }
        if let Ok(mut slot) = self.lifecycle.lock() {
            *slot = None;
        }
        if let Some(pending) = self.pending_effect.lock().expect("pending_effect").take() {
            pending.dispose();
        }
        let effect = self.effect.lock().expect("effect").take();
        if effect.is_some() {
            self.transition_disposing();
        } else {
            self.transition_disposed();
        }
        Some(effect)
    }

    pub(crate) fn dispose_now(&self) {
        let Some(effect) = self.begin_dispose() else {
            return;
        };
        if let Some(effect) = effect {
            *self.pending_wait.lock().expect("pending_wait") = Some(effect.clone());
            effect.dispose();
            self.transition_disposed();
        }
        if let Some(registry) = self.registry.upgrade() {
            registry.unregister_plugin_fiber(self.id);
        }
    }

    pub(crate) async fn dispose_wait_inner(&self) -> Result<(), CoreError> {
        if let Some(effect) = self.begin_dispose() {
            let result = if let Some(effect) = effect {
                *self.pending_wait.lock().expect("pending_wait") = Some(effect.clone());
                let result = effect.dispose_wait().await;
                self.transition_disposed();
                result
            } else {
                Ok(())
            };
            if let Some(registry) = self.registry.upgrade() {
                registry.unregister_plugin_fiber(self.id);
            }
            let _ = self.pending_wait.lock().expect("pending_wait").take();
            return result;
        }
        let pending = self.pending_wait.lock().expect("pending_wait").clone();
        if let Some(effect) = pending {
            let result = effect.dispose_wait().await;
            let _ = self.pending_wait.lock().expect("pending_wait").take();
            result
        } else {
            Ok(())
        }
    }

    pub(crate) async fn unload_effect_to_pending(&self) -> Result<(), CoreError> {
        let effect = self.effect.lock().expect("effect").take();
        if let Some(effect) = effect {
            if !self.transition_if_alive(FiberState::Unloading) {
                return Err(CoreError::FiberDisposed);
            }
            if let Err(error) = effect.dispose_wait().await {
                *self.last_error.lock().expect("error") = Some(error.to_string());
                let _ = self.transition_if_alive(FiberState::Failed);
                return Err(error);
            }
        }
        if self.disposed.load(Ordering::Acquire) {
            return Err(CoreError::FiberDisposed);
        }
        self.resolved_providers.lock().expect("providers").clear();
        *self.last_error.lock().expect("error") = None;
        if self.transition_if_alive(FiberState::Pending) {
            Ok(())
        } else {
            Err(CoreError::FiberDisposed)
        }
    }

    pub(crate) async fn unload_to_pending_wait(self: &Arc<Self>) -> Result<(), CoreError> {
        match self.start_lifecycle(LifecycleOp::Unload) {
            Ok(completion) => completion.wait().await,
            Err(CoreError::FiberBusy) => Err(CoreError::FiberBusy),
            Err(error) => Err(error),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn current_lifecycle(&self) -> Option<Arc<LifecycleCompletion>> {
        self.lifecycle.lock().ok().and_then(|slot| slot.clone())
    }
}

impl Drop for Fiber {
    fn drop(&mut self) {
        self.dispose();
    }
}
