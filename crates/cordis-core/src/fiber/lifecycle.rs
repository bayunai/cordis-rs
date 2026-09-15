//! Fiber 的 restart / replace / dispose / unload。
//!
//! 编排卸载与等待；状态机规则在 `state`，依赖就绪后的 apply 在 `activate`。

use super::{Fiber, FiberInner, FiberState};
use crate::{CoreError, effect::EffectScope, plugin::Plugin};
use std::sync::{Arc, atomic::Ordering};

impl Fiber {
    pub fn dispose(&mut self) {
        self.inner.dispose_now();
    }

    pub async fn dispose_wait(&mut self) -> Result<(), CoreError> {
        self.inner.dispose_wait_inner().await
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
        self.inner.unload_effect_to_pending().await?;
        self.inner.try_activate().await
    }

    /// 先等待旧任务结束，再替换为同 Key 的预校验不可变 Plugin 实例并重新激活。
    ///
    /// 配置 Schema、反序列化与校验属于宿主：配置无效时宿主不得调用本方法，旧
    /// Active 实例继续运行。本方法开始后旧实例已释放；新实例 `apply()` 失败时
    /// Fiber 进入 [`FiberState::Failed`]，不会自动恢复旧实例。
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
            let actual = plugin.key();
            if actual != self.inner.plugin_key {
                return Err(CoreError::PluginKeyMismatch {
                    expected: self.inner.plugin_key,
                    actual,
                });
            }
            self.inner.unload_effect_to_pending().await?;
            let deps = plugin.inject();
            *self.inner.plugin.lock().expect("plugin") = plugin;
            *self.inner.dependencies.lock().expect("deps") = deps;
            self.inner
                .resolved_providers
                .lock()
                .expect("providers")
                .clear();
            *self.inner.last_error.lock().expect("error") = None;
            if !self.inner.transition_if_alive(FiberState::Pending) {
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
    fn begin_dispose(&self) -> Option<Option<EffectScope>> {
        if self.disposed.swap(true, Ordering::AcqRel) {
            return None;
        }
        if let Ok(mut busy) = self.busy.lock() {
            *busy = false;
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
        // 已由 dispose_now 释放：等待同一轮 DisposeCompletion。
        let pending = self.pending_wait.lock().expect("pending_wait").clone();
        if let Some(effect) = pending {
            let result = effect.dispose_wait().await;
            let _ = self.pending_wait.lock().expect("pending_wait").take();
            result
        } else {
            Ok(())
        }
    }

    async fn unload_effect_to_pending(&self) -> Result<(), CoreError> {
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

    pub(crate) async fn unload_to_pending_wait(&self) -> Result<(), CoreError> {
        if self.disposed.load(Ordering::Acquire) {
            return Err(CoreError::FiberDisposed);
        }
        {
            let Ok(mut busy) = self.busy.lock() else {
                return Err(CoreError::FiberDisposed);
            };
            if *busy {
                return Err(CoreError::FiberBusy);
            }
            *busy = true;
        }
        let result = self.unload_effect_to_pending().await;
        *self.busy.lock().expect("busy") = false;
        result
    }
}

impl Drop for Fiber {
    fn drop(&mut self) {
        self.dispose();
    }
}
