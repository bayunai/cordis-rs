//! 公开句柄类型：不暴露内部 token / channel / Mutex。

use crate::TimerError;
use futures_util::Stream;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

/// 一次延迟或循环计时器的取消句柄。
pub struct TimerHandle {
    cancel: CancellationToken,
}

impl TimerHandle {
    pub(crate) fn new(cancel: CancellationToken) -> Self {
        Self { cancel }
    }

    /// 幂等取消该计时器。
    pub fn cancel(&self) {
        self.cancel.cancel();
    }
}

impl Drop for TimerHandle {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// 可等待的一次延迟；输出 `Result<(), TimerError>`。
pub struct TimerSleep {
    rx: oneshot::Receiver<Result<(), TimerError>>,
    cancel: CancellationToken,
}

impl TimerSleep {
    pub(crate) fn new(
        rx: oneshot::Receiver<Result<(), TimerError>>,
        cancel: CancellationToken,
    ) -> Self {
        Self { rx, cancel }
    }
}

impl Future for TimerSleep {
    type Output = Result<(), TimerError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            Poll::Ready(Err(_)) => Poll::Ready(Err(TimerError::Disposed)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for TimerSleep {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// 容量为 1 的 tick 流；落后时合并。
pub struct TickStream {
    rx: mpsc::Receiver<Result<(), TimerError>>,
    cancel: CancellationToken,
    done: bool,
}

impl TickStream {
    pub(crate) fn new(
        rx: mpsc::Receiver<Result<(), TimerError>>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            rx,
            cancel,
            done: false,
        }
    }
}

impl Stream for TickStream {
    type Item = Result<(), TimerError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        if self.cancel.is_cancelled() {
            self.done = true;
            return Poll::Ready(Some(Err(TimerError::Disposed)));
        }
        match Pin::new(&mut self.rx).poll_recv(cx) {
            Poll::Ready(Some(item)) => {
                if item.is_err() {
                    self.done = true;
                }
                Poll::Ready(Some(item))
            }
            Poll::Ready(None) => {
                self.done = true;
                Poll::Ready(Some(Err(TimerError::Disposed)))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for TickStream {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

pub(crate) type SharedCallback<Args> = Arc<dyn Fn(Args) + Send + Sync + 'static>;

/// 节流包装：显式 `call(args)` 触发。
pub struct Throttled<Args> {
    pub(crate) inner: Arc<ThrottleInner<Args>>,
}

pub(crate) struct ThrottleInner<Args> {
    pub(crate) disposed: AtomicBool,
    pub(crate) cancel: CancellationToken,
    pub(crate) state: Mutex<ThrottleState<Args>>,
    pub(crate) callback: SharedCallback<Args>,
    pub(crate) delay: std::time::Duration,
    pub(crate) no_trailing: bool,
    pub(crate) effect: cordis_core::EffectContext,
}

pub(crate) struct ThrottleState<Args> {
    pub(crate) window_open: bool,
    pub(crate) trailing: Option<Args>,
    pub(crate) pending: Option<CancellationToken>,
}

impl<Args> Throttled<Args>
where
    Args: Send + 'static,
{
    pub fn call(&self, args: Args) -> Result<(), TimerError> {
        if self.inner.disposed.load(Ordering::SeqCst) || self.inner.cancel.is_cancelled() {
            return Err(TimerError::Disposed);
        }
        crate::schedule::throttle_call(&self.inner, args)
    }

    pub fn dispose(&self) {
        self.inner.dispose();
    }

    pub fn is_disposed(&self) -> bool {
        self.inner.disposed.load(Ordering::SeqCst) || self.inner.cancel.is_cancelled()
    }
}

impl<Args> Drop for Throttled<Args> {
    fn drop(&mut self) {
        self.inner.dispose();
    }
}

impl<Args> ThrottleInner<Args> {
    pub(crate) fn dispose(&self) {
        self.disposed.store(true, Ordering::SeqCst);
        self.cancel.cancel();
        if let Ok(mut state) = self.state.lock() {
            if let Some(pending) = state.pending.take() {
                pending.cancel();
            }
            state.trailing = None;
            state.window_open = false;
        }
    }
}

/// 防抖包装：显式 `call(args)` 触发。
pub struct Debounced<Args> {
    pub(crate) inner: Arc<DebounceInner<Args>>,
}

pub(crate) struct DebounceInner<Args> {
    pub(crate) disposed: AtomicBool,
    pub(crate) cancel: CancellationToken,
    pub(crate) state: Mutex<DebounceState<Args>>,
    pub(crate) callback: SharedCallback<Args>,
    pub(crate) delay: std::time::Duration,
    pub(crate) effect: cordis_core::EffectContext,
}

pub(crate) struct DebounceState<Args> {
    pub(crate) generation: u64,
    pub(crate) pending: Option<CancellationToken>,
    pub(crate) args: Option<Args>,
}

impl<Args> Debounced<Args>
where
    Args: Send + 'static,
{
    pub fn call(&self, args: Args) -> Result<(), TimerError> {
        if self.inner.disposed.load(Ordering::SeqCst) || self.inner.cancel.is_cancelled() {
            return Err(TimerError::Disposed);
        }
        crate::schedule::debounce_call(&self.inner, args)
    }

    pub fn dispose(&self) {
        self.inner.dispose();
    }

    pub fn is_disposed(&self) -> bool {
        self.inner.disposed.load(Ordering::SeqCst) || self.inner.cancel.is_cancelled()
    }
}

impl<Args> Drop for Debounced<Args> {
    fn drop(&mut self) {
        self.inner.dispose();
    }
}

impl<Args> DebounceInner<Args> {
    pub(crate) fn dispose(&self) {
        self.disposed.store(true, Ordering::SeqCst);
        self.cancel.cancel();
        if let Ok(mut state) = self.state.lock() {
            state.generation = state.generation.wrapping_add(1);
            if let Some(pending) = state.pending.take() {
                pending.cancel();
            }
            state.args = None;
        }
    }
}
