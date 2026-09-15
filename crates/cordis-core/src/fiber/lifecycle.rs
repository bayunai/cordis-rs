//! Fiber 的 restart / replace / dispose / unload。
//!
//! 编排经内部协调器执行；状态机规则在 `state`，依赖就绪后的 apply 在 `activate`。

use super::{
    Fiber, FiberInner, FiberState,
    coordinator::{LifecycleCompletion, LifecycleOp},
    ownership::{EffectOwnership, FiberRelease},
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

enum TerminalRelease {
    Run {
        scope: EffectScope,
        start_dispose: bool,
    },
    Join(EffectScope),
    Vacant,
    Observe,
}

impl FiberInner {
    fn store_dispose_result(&self, result: Result<(), CoreError>) {
        let mut slot = self.dispose_result.lock().expect("dispose_result");
        if slot.is_none() {
            *slot = Some(result);
        }
    }

    fn read_dispose_result(&self) -> Option<Result<(), CoreError>> {
        self.dispose_result.lock().expect("dispose_result").clone()
    }

    fn mark_release_started(&self, scope: &EffectScope) {
        let mut ownership = self.ownership.lock().expect("ownership");
        if let EffectOwnership::Releasing(release) = &mut *ownership
            && release.scope.ptr_eq(scope)
        {
            release.dispose_started = true;
        }
    }

    fn begin_terminal_release(&self) -> TerminalRelease {
        let first = !self.disposed.swap(true, Ordering::AcqRel);
        if let Ok(mut busy) = self.busy.lock() {
            *busy = false;
        }
        if first && let Ok(mut slot) = self.lifecycle.lock() {
            *slot = None;
        }
        let mut ownership = self.ownership.lock().expect("ownership");
        if !first {
            return match &*ownership {
                EffectOwnership::Releasing(release) => TerminalRelease::Join(release.scope.clone()),
                EffectOwnership::Activating(scope) | EffectOwnership::Active(scope) => {
                    let scope = scope.clone();
                    *ownership = EffectOwnership::Releasing(FiberRelease {
                        scope: scope.clone(),
                        dispose_started: false,
                    });
                    TerminalRelease::Run {
                        scope,
                        start_dispose: true,
                    }
                }
                EffectOwnership::Empty => TerminalRelease::Observe,
            };
        }
        match std::mem::replace(&mut *ownership, EffectOwnership::Empty) {
            EffectOwnership::Activating(scope) | EffectOwnership::Active(scope) => {
                *ownership = EffectOwnership::Releasing(FiberRelease {
                    scope: scope.clone(),
                    dispose_started: false,
                });
                TerminalRelease::Run {
                    scope,
                    start_dispose: true,
                }
            }
            EffectOwnership::Releasing(release) => {
                *ownership = EffectOwnership::Releasing(release.clone());
                TerminalRelease::Join(release.scope)
            }
            EffectOwnership::Empty => TerminalRelease::Vacant,
        }
    }

    fn clear_release_if_matching(&self, scope: &EffectScope) {
        let mut ownership = self.ownership.lock().expect("ownership");
        if let EffectOwnership::Releasing(release) = &*ownership
            && release.scope.ptr_eq(scope)
        {
            *ownership = EffectOwnership::Empty;
        }
    }

    async fn finalize_terminal_release(
        self: &Arc<Self>,
        scope: EffectScope,
        start_dispose: bool,
    ) -> Result<(), CoreError> {
        if start_dispose {
            scope.dispose_owned();
            self.mark_release_started(&scope);
        }
        let result = scope.dispose_wait().await;
        self.store_dispose_result(result.clone());
        self.transition_disposed();
        self.clear_release_if_matching(&scope);
        if let Some(registry) = self.registry.upgrade() {
            registry.unregister_plugin_fiber(self.id);
        }
        result
    }

    async fn observe_dispose_result(self: &Arc<Self>) -> Result<(), CoreError> {
        if let Some(result) = self.read_dispose_result() {
            return result;
        }
        let scope = self
            .ownership
            .lock()
            .expect("ownership")
            .live_scope()
            .cloned();
        if let Some(scope) = scope {
            let result = scope.dispose_wait().await;
            if let Some(stored) = self.read_dispose_result() {
                return stored;
            }
            self.store_dispose_result(result.clone());
            return result;
        }
        self.read_dispose_result().unwrap_or(Ok(()))
    }

    fn complete_vacant_dispose(&self) {
        self.store_dispose_result(Ok(()));
        self.transition_disposed();
        if let Some(registry) = self.registry.upgrade() {
            registry.unregister_plugin_fiber(self.id);
        }
    }

    /// 启动释放；Effect 的 DisposeCompletion 完成前保留 Releasing 与 plugin 索引。
    pub(crate) fn dispose_now(self: &Arc<Self>) {
        match self.begin_terminal_release() {
            TerminalRelease::Run {
                scope,
                start_dispose,
            } => {
                self.transition_disposing();
                if start_dispose {
                    scope.dispose_owned();
                    self.mark_release_started(&scope);
                }
                let fiber = self.clone();
                scope.runtime_handle().spawn(async move {
                    let _ = fiber.finalize_terminal_release(scope, false).await;
                });
            }
            TerminalRelease::Join(scope) => {
                let fiber = self.clone();
                scope.runtime_handle().spawn(async move {
                    let _ = fiber.finalize_terminal_release(scope, false).await;
                });
            }
            TerminalRelease::Vacant => self.complete_vacant_dispose(),
            TerminalRelease::Observe => {}
        }
    }

    pub(crate) async fn dispose_wait_inner(self: &Arc<Self>) -> Result<(), CoreError> {
        match self.begin_terminal_release() {
            TerminalRelease::Run {
                scope,
                start_dispose,
            } => self.finalize_terminal_release(scope, start_dispose).await,
            TerminalRelease::Join(scope) => self.finalize_terminal_release(scope, false).await,
            TerminalRelease::Vacant => {
                self.complete_vacant_dispose();
                Ok(())
            }
            TerminalRelease::Observe => self.observe_dispose_result().await,
        }
    }

    pub(crate) fn discard_activating_to_pending(&self) {
        let mut ownership = self.ownership.lock().expect("ownership");
        match std::mem::replace(&mut *ownership, EffectOwnership::Empty) {
            EffectOwnership::Activating(scope) => scope.dispose(),
            other => *ownership = other,
        }
    }

    pub(crate) async fn unload_effect_to_pending(&self) -> Result<(), CoreError> {
        let scope = {
            let mut ownership = self.ownership.lock().expect("ownership");
            match std::mem::replace(&mut *ownership, EffectOwnership::Empty) {
                EffectOwnership::Active(scope) => {
                    *ownership = EffectOwnership::Releasing(FiberRelease {
                        scope: scope.clone(),
                        dispose_started: false,
                    });
                    Some(scope)
                }
                EffectOwnership::Releasing(release) => {
                    *ownership = EffectOwnership::Releasing(release.clone());
                    Some(release.scope)
                }
                EffectOwnership::Activating(scope) => {
                    *ownership = EffectOwnership::Releasing(FiberRelease {
                        scope: scope.clone(),
                        dispose_started: false,
                    });
                    Some(scope)
                }
                EffectOwnership::Empty => None,
            }
        };
        if let Some(scope) = scope {
            if !self.transition_if_alive(FiberState::Unloading) {
                if self.disposed.load(Ordering::Acquire) {
                    let _ = scope.dispose_wait().await;
                    return Err(CoreError::FiberDisposed);
                }
                self.clear_release_if_matching(&scope);
                return Err(CoreError::FiberDisposed);
            }
            let dispose_result = scope.dispose_wait().await;
            if self.disposed.load(Ordering::Acquire) {
                if let Err(ref error) = dispose_result {
                    self.store_dispose_result(Err(error.clone()));
                } else {
                    self.store_dispose_result(Ok(()));
                }
                return match dispose_result {
                    Ok(()) => Err(CoreError::FiberDisposed),
                    Err(error) => Err(error),
                };
            }
            self.clear_release_if_matching(&scope);
            if let Err(error) = dispose_result {
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
