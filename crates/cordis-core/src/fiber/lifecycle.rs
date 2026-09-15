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

    fn store_dispose_result(&self, result: Result<(), CoreError>) {
        let mut slot = self.dispose_result.lock().expect("dispose_result");
        if slot.is_none() {
            *slot = Some(result);
        }
    }

    fn read_dispose_result(&self) -> Option<Result<(), CoreError>> {
        self.dispose_result.lock().expect("dispose_result").clone()
    }

    /// 等待释放完成，写入 `dispose_result`，再 unregister 并清 `pending_wait`。
    ///
    /// `started_by_us`：本次从 Active 槽取出 Scope，需先 `dispose()`；否则仅 join
    /// 已在途的 unload/`pending_wait` 同一 `DisposeCompletion`。
    async fn finalize_dispose(
        self: &Arc<Self>,
        scope: EffectScope,
        started_by_us: bool,
    ) -> Result<(), CoreError> {
        if started_by_us {
            scope.dispose();
        }
        let result = scope.dispose_wait().await;
        self.store_dispose_result(result.clone());
        self.transition_disposed();
        if let Some(registry) = self.registry.upgrade() {
            registry.unregister_plugin_fiber(self.id);
        }
        let _ = self.pending_wait.lock().expect("pending_wait").take();
        result
    }

    /// 已 dispose 后观察同一轮结果：优先 join `pending_wait`，否则读落盘结果。
    async fn observe_dispose_result(self: &Arc<Self>) -> Result<(), CoreError> {
        loop {
            if let Some(result) = self.read_dispose_result() {
                return result;
            }
            let pending = self.pending_wait.lock().expect("pending_wait").clone();
            if let Some(scope) = pending {
                let result = scope.dispose_wait().await;
                self.store_dispose_result(result.clone());
                // 不在此 unregister / take：由首次 dispose 路径的 finalize 收口，
                // 避免与后台任务或 unload 抢归属。若 finalize 已跑完，结果已落盘。
                if let Some(stored) = self.read_dispose_result() {
                    return stored;
                }
                return result;
            }
            // 释放可能正在 finalize 落盘与清 pending_wait 之间；让出后再读。
            tokio::task::yield_now().await;
            if let Some(result) = self.read_dispose_result() {
                return result;
            }
            // 无 Effect 的空释放：dispose_now 会同步写 Ok(())。
            if !self.disposed.load(Ordering::Acquire) {
                return Ok(());
            }
            // 仍可能是极短窗口：再读一次后默认 Ok（无 scope 的 dispose）。
            if self.pending_wait.lock().expect("pending_wait").is_none() {
                return self.read_dispose_result().unwrap_or(Ok(()));
            }
        }
    }

    /// 启动释放；Effect 的 DisposeCompletion 完成前保留 `pending_wait` 与 plugin 索引。
    pub(crate) fn dispose_now(self: &Arc<Self>) {
        let Some(taken) = self.begin_dispose() else {
            return;
        };
        let started_by_us = taken.is_some();
        let scope = taken.or_else(|| self.pending_wait.lock().expect("pending_wait").clone());
        if let Some(scope) = scope {
            *self.pending_wait.lock().expect("pending_wait") = Some(scope.clone());
            self.transition_disposed();
            // fire-and-forget：同步 cleanup 必须在调用方线程立刻执行。
            if started_by_us {
                scope.dispose();
            }
            let fiber = self.clone();
            scope.runtime_handle().spawn(async move {
                let _ = fiber.finalize_dispose(scope, false).await;
            });
            return;
        }
        self.store_dispose_result(Ok(()));
        self.transition_disposed();
        if let Some(registry) = self.registry.upgrade() {
            registry.unregister_plugin_fiber(self.id);
        }
    }

    pub(crate) async fn dispose_wait_inner(self: &Arc<Self>) -> Result<(), CoreError> {
        if let Some(taken) = self.begin_dispose() {
            let started_by_us = taken.is_some();
            let scope = taken.or_else(|| self.pending_wait.lock().expect("pending_wait").clone());
            if let Some(scope) = scope {
                *self.pending_wait.lock().expect("pending_wait") = Some(scope.clone());
                return self.finalize_dispose(scope, started_by_us).await;
            }
            self.store_dispose_result(Ok(()));
            self.transition_disposed();
            if let Some(registry) = self.registry.upgrade() {
                registry.unregister_plugin_fiber(self.id);
            }
            return Ok(());
        }
        self.observe_dispose_result().await
    }

    pub(crate) async fn unload_effect_to_pending(&self) -> Result<(), CoreError> {
        let effect = self.effect.lock().expect("effect").take();
        if let Some(effect) = effect {
            // 释放完成前登记归属，供 unmount 重入与并发 dispose 可见。
            *self.pending_wait.lock().expect("pending_wait") = Some(effect.clone());
            if !self.transition_if_alive(FiberState::Unloading) {
                // 已 disposed：保留 pending_wait，由 dispose finalize 收口。
                if self.disposed.load(Ordering::Acquire) {
                    let _ = effect.dispose_wait().await;
                    return Err(CoreError::FiberDisposed);
                }
                let _ = self.pending_wait.lock().expect("pending_wait").take();
                return Err(CoreError::FiberDisposed);
            }
            let dispose_result = effect.dispose_wait().await;
            if self.disposed.load(Ordering::Acquire) {
                // dispose 路径负责 unregister / 清 pending_wait / 落盘结果。
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
            let _ = self.pending_wait.lock().expect("pending_wait").take();
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
