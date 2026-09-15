//! [`EffectContext`]：可处置资源的注册入口。
//!
//! 在 `inject` / `effect` 回调中提供 provide、事件监听与受控任务等 API；
//! 资源所有权归 `effect` 模块的 EffectScope，本文件只暴露 Context 侧门面。

use super::{Context, InjectionHandle};
use crate::{
    ConfigKey, CoreError, ServiceId, ServiceKey, Services,
    callback_context::{LifecycleFrame, USER_LIFECYCLE_CALLBACK},
    effect::EffectHandle,
    event::{EventKey, ListenOptions, Next, ParallelKey, SerialKey, Unsubscribe, WaterfallKey},
};
use std::{future::Future, sync::Arc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// `inject()` 或 `effect()` 回调中拥有资源的 Context。
#[derive(Clone)]
pub struct EffectContext {
    pub(super) context: Context,
}

impl EffectContext {
    pub fn provide<T: Send + Sync + 'static>(
        &self,
        key: ServiceKey<T>,
        service: T,
    ) -> Result<(), CoreError> {
        self.context.provide(key, service)
    }

    pub fn get<T: Send + Sync + 'static>(&self, key: ServiceKey<T>) -> Result<Arc<T>, CoreError> {
        self.context.get(key)
    }

    pub fn extend(&self) -> Result<Context, CoreError> {
        self.context.extend()
    }

    pub fn intercept<T: Send + Sync + 'static>(
        &self,
        key: ConfigKey<T>,
        value: T,
    ) -> Result<Context, CoreError> {
        self.context.intercept(key, value)
    }

    pub fn config<T: Send + Sync + 'static>(&self, key: ConfigKey<T>) -> Result<Arc<T>, CoreError> {
        self.context.config(key)
    }

    pub fn inject<I, F, Fut>(
        &self,
        dependencies: I,
        callback: F,
    ) -> Result<InjectionHandle, CoreError>
    where
        I: IntoIterator<Item = ServiceId>,
        F: Fn(Services, EffectContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), CoreError>> + Send + 'static,
    {
        self.context.inject(dependencies, callback)
    }

    pub fn on<T, F>(&self, key: EventKey<T>, handler: F) -> Result<Unsubscribe, CoreError>
    where
        T: Send + Sync + 'static,
        F: Fn(&T) -> Result<(), CoreError> + Send + Sync + 'static,
    {
        self.context.on(key, handler)
    }

    pub fn on_with_options<T, F>(
        &self,
        key: EventKey<T>,
        options: ListenOptions<T>,
        handler: F,
    ) -> Result<Unsubscribe, CoreError>
    where
        T: Send + Sync + 'static,
        F: Fn(&T) -> Result<(), CoreError> + Send + Sync + 'static,
    {
        self.context.on_with_options(key, options, handler)
    }

    pub fn emit<T: Send + Sync + 'static>(
        &self,
        key: EventKey<T>,
        payload: &T,
    ) -> Result<(), CoreError> {
        self.context.emit(key, payload)
    }

    pub fn on_waterfall<T, F, Fut>(
        &self,
        key: WaterfallKey<T>,
        handler: F,
    ) -> Result<Unsubscribe, CoreError>
    where
        T: Send + Sync + 'static,
        F: Fn(T, Next<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T, CoreError>> + Send + 'static,
    {
        self.context.on_waterfall(key, handler)
    }

    pub fn on_waterfall_with_options<T, F, Fut>(
        &self,
        key: WaterfallKey<T>,
        options: ListenOptions<T>,
        handler: F,
    ) -> Result<Unsubscribe, CoreError>
    where
        T: Send + Sync + 'static,
        F: Fn(T, Next<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T, CoreError>> + Send + 'static,
    {
        self.context
            .on_waterfall_with_options(key, options, handler)
    }

    pub async fn waterfall<T: Send + Sync + 'static>(
        &self,
        key: WaterfallKey<T>,
        value: T,
    ) -> Result<T, CoreError> {
        self.context.waterfall(key, value).await
    }

    pub fn on_serial<T, R, F, Fut>(
        &self,
        key: SerialKey<T, R>,
        handler: F,
    ) -> Result<Unsubscribe, CoreError>
    where
        T: Send + Sync + 'static,
        R: Send + Sync + 'static,
        F: Fn(&T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Option<R>, CoreError>> + Send + 'static,
    {
        self.context.on_serial(key, handler)
    }

    pub fn on_serial_with_options<T, R, F, Fut>(
        &self,
        key: SerialKey<T, R>,
        options: ListenOptions<T>,
        handler: F,
    ) -> Result<Unsubscribe, CoreError>
    where
        T: Send + Sync + 'static,
        R: Send + Sync + 'static,
        F: Fn(&T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Option<R>, CoreError>> + Send + 'static,
    {
        self.context.on_serial_with_options(key, options, handler)
    }

    pub async fn serial<T: Send + Sync + 'static, R: Send + Sync + 'static>(
        &self,
        key: SerialKey<T, R>,
        payload: &T,
    ) -> Result<Option<R>, CoreError> {
        self.context.serial(key, payload).await
    }

    pub fn on_parallel<T, F, Fut>(
        &self,
        key: ParallelKey<T>,
        handler: F,
    ) -> Result<Unsubscribe, CoreError>
    where
        T: Send + Sync + 'static,
        F: Fn(&T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), CoreError>> + Send + 'static,
    {
        self.context.on_parallel(key, handler)
    }

    pub fn on_parallel_with_options<T, F, Fut>(
        &self,
        key: ParallelKey<T>,
        options: ListenOptions<T>,
        handler: F,
    ) -> Result<Unsubscribe, CoreError>
    where
        T: Send + Sync + 'static,
        F: Fn(&T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), CoreError>> + Send + 'static,
    {
        self.context.on_parallel_with_options(key, options, handler)
    }

    pub async fn parallel<T: Send + Sync + 'static>(
        &self,
        key: ParallelKey<T>,
        payload: &T,
    ) -> Result<(), CoreError> {
        self.context.parallel(key, payload).await
    }

    pub fn on_dispose(&self, callback: impl FnOnce() + Send + 'static) {
        self.context.inner.scope.on_dispose(callback);
    }

    /// 注册仅在 Scope 首次释放时执行一次的异步收尾；不接收取消令牌。
    pub fn on_dispose_async<F, Fut>(&self, disposer: F) -> Result<(), CoreError>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), CoreError>> + Send + 'static,
    {
        self.context.inner.scope.on_dispose_async(disposer)
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.context.inner.scope.cancellation()
    }

    /// 释放当前 Effect 及其全部子资源（fire-and-forget；异步 disposer 上收至父 Scope）。
    pub fn dispose(&self) {
        self.context.inner.scope.dispose();
    }

    /// 释放并等待本 Effect 树上的受控任务与异步 disposer（不上收；无超时）。
    pub async fn dispose_wait(&self) -> Result<(), CoreError> {
        self.context.inner.scope.dispose_wait().await
    }

    pub fn spawn<F, Fut>(&self, task: F) -> Result<(), CoreError>
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        if self.context.is_disposed() {
            return Err(CoreError::ContextDisposed);
        }
        let cancellation = self.cancellation_token();
        let future = task(cancellation);
        let handle: JoinHandle<()> = tokio::spawn(USER_LIFECYCLE_CALLBACK.scope(
            LifecycleFrame {
                scope: self.context.inner.scope.clone(),
            },
            future,
        ));
        self.context.inner.scope.push_task(handle);
        Ok(())
    }

    pub fn as_context(&self) -> &Context {
        &self.context
    }

    /// 当前 Effect 的具名诊断句柄。
    pub fn handle(&self) -> EffectHandle {
        EffectHandle::from_scope(self.context.inner.scope.clone())
    }

    /// 当前 EffectScope 下仍登记的直接子 Scope 数量（诊断/测试用）。
    pub fn child_scope_count(&self) -> usize {
        self.context.inner.scope.child_scope_count()
    }
}
