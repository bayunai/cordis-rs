use crate::CoreError;
use std::{any::TypeId, fmt, future::Future, marker::PhantomData, pin::Pin, sync::Arc};

/// 同步事件过滤器：`false` 跳过该监听器，`Err` 中止本次派发。
pub type ListenFilter<T> = Arc<dyn Fn(&T) -> Result<bool, CoreError> + Send + Sync>;

/// 事件监听选项：`once` / `prepend` / `global` / `filter`。
#[derive(Clone)]
pub struct ListenOptions<T: Send + Sync + 'static> {
    pub(crate) once: bool,
    pub(crate) prepend: bool,
    pub(crate) global: bool,
    pub(crate) filter: Option<ListenFilter<T>>,
}

impl<T: Send + Sync + 'static> Default for ListenOptions<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Send + Sync + 'static> ListenOptions<T> {
    pub fn new() -> Self {
        Self {
            once: false,
            prepend: false,
            global: false,
            filter: None,
        }
    }

    pub fn once(mut self) -> Self {
        self.once = true;
        self
    }

    pub fn prepend(mut self) -> Self {
        self.prepend = true;
        self
    }

    pub fn global(mut self) -> Self {
        self.global = true;
        self
    }

    pub fn filter(
        mut self,
        filter: impl Fn(&T) -> Result<bool, CoreError> + Send + Sync + 'static,
    ) -> Self {
        self.filter = Some(Arc::new(filter));
        self
    }
}

/// Observe 模式事件 Key（同步 `on` / `emit`）。
pub struct EventKey<T: Send + Sync + 'static> {
    id: &'static str,
    marker: PhantomData<fn() -> T>,
}

impl<T: Send + Sync + 'static> EventKey<T> {
    pub const fn new(id: &'static str) -> Self {
        Self {
            id,
            marker: PhantomData,
        }
    }

    pub const fn id(&self) -> &'static str {
        self.id
    }

    pub(crate) fn type_id(&self) -> TypeId {
        TypeId::of::<T>()
    }
}

impl<T: Send + Sync + 'static> Copy for EventKey<T> {}

impl<T: Send + Sync + 'static> Clone for EventKey<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: Send + Sync + 'static> fmt::Debug for EventKey<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("EventKey").field(&self.id).finish()
    }
}

/// Waterfall 模式事件 Key（async `on_waterfall` / `waterfall`）。
pub struct WaterfallKey<T: Send + Sync + 'static> {
    id: &'static str,
    marker: PhantomData<fn() -> T>,
}

impl<T: Send + Sync + 'static> WaterfallKey<T> {
    pub const fn new(id: &'static str) -> Self {
        Self {
            id,
            marker: PhantomData,
        }
    }

    pub const fn id(&self) -> &'static str {
        self.id
    }

    pub(crate) fn type_id(&self) -> TypeId {
        TypeId::of::<T>()
    }
}

impl<T: Send + Sync + 'static> Copy for WaterfallKey<T> {}

impl<T: Send + Sync + 'static> Clone for WaterfallKey<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: Send + Sync + 'static> fmt::Debug for WaterfallKey<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("WaterfallKey")
            .field(&self.id)
            .finish()
    }
}

/// Serial 模式事件 Key（async `on_serial` / `serial`）；`R` 为答案类型。
pub struct SerialKey<T: Send + Sync + 'static, R: Send + Sync + 'static> {
    id: &'static str,
    marker: PhantomData<fn() -> (T, R)>,
}

impl<T: Send + Sync + 'static, R: Send + Sync + 'static> SerialKey<T, R> {
    pub const fn new(id: &'static str) -> Self {
        Self {
            id,
            marker: PhantomData,
        }
    }

    pub const fn id(&self) -> &'static str {
        self.id
    }

    pub(crate) fn payload_type_id(&self) -> TypeId {
        TypeId::of::<T>()
    }

    pub(crate) fn answer_type_id(&self) -> TypeId {
        TypeId::of::<R>()
    }
}

impl<T: Send + Sync + 'static, R: Send + Sync + 'static> Copy for SerialKey<T, R> {}

impl<T: Send + Sync + 'static, R: Send + Sync + 'static> Clone for SerialKey<T, R> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: Send + Sync + 'static, R: Send + Sync + 'static> fmt::Debug for SerialKey<T, R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("SerialKey").field(&self.id).finish()
    }
}

/// Parallel 模式事件 Key（async `on_parallel` / `parallel`）。
pub struct ParallelKey<T: Send + Sync + 'static> {
    id: &'static str,
    marker: PhantomData<fn() -> T>,
}

impl<T: Send + Sync + 'static> ParallelKey<T> {
    pub const fn new(id: &'static str) -> Self {
        Self {
            id,
            marker: PhantomData,
        }
    }

    pub const fn id(&self) -> &'static str {
        self.id
    }

    pub(crate) fn type_id(&self) -> TypeId {
        TypeId::of::<T>()
    }
}

impl<T: Send + Sync + 'static> Copy for ParallelKey<T> {}

impl<T: Send + Sync + 'static> Clone for ParallelKey<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: Send + Sync + 'static> fmt::Debug for ParallelKey<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ParallelKey")
            .field(&self.id)
            .finish()
    }
}

type NextFuture<T> = Pin<Box<dyn Future<Output = Result<T, CoreError>> + Send>>;

/// Waterfall 下游续体；必须 `call` 才会委托下一层（consume-once）。
pub struct Next<T: Send + 'static> {
    inner: Box<dyn FnOnce(T) -> NextFuture<T> + Send>,
}

impl<T: Send + 'static> Next<T> {
    pub(crate) fn new(inner: impl FnOnce(T) -> NextFuture<T> + Send + 'static) -> Self {
        Self {
            inner: Box::new(inner),
        }
    }

    /// 将值交给下游监听器（或链尾默认行为）。
    pub async fn call(self, value: T) -> Result<T, CoreError> {
        (self.inner)(value).await
    }
}

/// 事件订阅句柄。
///
/// 订阅默认归属当前 [`crate::effect::EffectScope`]；Scope 释放时自动退订。
/// 调用 [`Unsubscribe::dispose`] 可提前退订。丢弃本句柄**不会**退订。
pub struct Unsubscribe {
    dispose: Option<Box<dyn FnOnce() + Send>>,
}

impl Unsubscribe {
    pub(crate) fn new(dispose: impl FnOnce() + Send + 'static) -> Self {
        Self {
            dispose: Some(Box::new(dispose)),
        }
    }

    /// 立即退订；之后 Scope 释放时的清理为幂等空操作。
    pub fn dispose(mut self) {
        if let Some(dispose) = self.dispose.take() {
            dispose();
        }
    }
}
