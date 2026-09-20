//! timeout / sleep / interval / ticks / throttle / debounce 内部排程。

use crate::{
    Debounced, Throttled, TickStream, TimerError, TimerHandle, TimerSleep,
    handle::{DebounceInner, DebounceState, SharedCallback, ThrottleInner, ThrottleState},
};
use cordis_core::EffectContext;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};

pub(crate) fn spawn_timeout<F>(
    effect: &EffectContext,
    callback: F,
    delay: Duration,
) -> Result<TimerHandle, TimerError>
where
    F: FnOnce() + Send + 'static,
{
    let cancel = effect.cancellation_token().child_token();
    let task_cancel = cancel.clone();
    effect.spawn(move |_parent| async move {
        tokio::select! {
            biased;
            _ = task_cancel.cancelled() => {}
            _ = tokio::time::sleep(delay) => {
                callback();
            }
        }
    })?;
    Ok(TimerHandle::new(cancel))
}

pub(crate) fn spawn_sleep(
    effect: &EffectContext,
    delay: Duration,
) -> Result<TimerSleep, TimerError> {
    let cancel = effect.cancellation_token().child_token();
    let task_cancel = cancel.clone();
    let (tx, rx) = oneshot::channel();
    effect.spawn(move |_parent| async move {
        let result = tokio::select! {
            biased;
            _ = task_cancel.cancelled() => Err(TimerError::Disposed),
            _ = tokio::time::sleep(delay) => Ok(()),
        };
        let _ = tx.send(result);
    })?;
    Ok(TimerSleep::new(rx, cancel))
}

pub(crate) fn spawn_interval<F>(
    effect: &EffectContext,
    callback: F,
    delay: Duration,
) -> Result<TimerHandle, TimerError>
where
    F: Fn() + Send + 'static,
{
    let cancel = effect.cancellation_token().child_token();
    let task_cancel = cancel.clone();
    effect.spawn(move |_parent| async move {
        loop {
            tokio::select! {
                biased;
                _ = task_cancel.cancelled() => break,
                _ = tokio::time::sleep(delay) => {
                    callback();
                }
            }
        }
    })?;
    Ok(TimerHandle::new(cancel))
}

pub(crate) fn spawn_ticks(
    effect: &EffectContext,
    delay: Duration,
) -> Result<TickStream, TimerError> {
    let cancel = effect.cancellation_token().child_token();
    let task_cancel = cancel.clone();
    let (tx, rx) = mpsc::channel(1);
    effect.spawn(move |_parent| async move {
        loop {
            tokio::select! {
                biased;
                _ = task_cancel.cancelled() => {
                    // 容量满时不能 await send，否则 Effect dispose_wait 会永久挂起。
                    let _ = tx.try_send(Err(TimerError::Disposed));
                    break;
                }
                _ = tokio::time::sleep(delay) => {
                    // 容量 1：Full 表示已有未消费 tick，合并丢弃本次。
                    match tx.try_send(Ok(())) {
                        Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => {}
                        Err(mpsc::error::TrySendError::Closed(_)) => break,
                    }
                }
            }
        }
    })?;
    Ok(TickStream::new(rx, cancel))
}

pub(crate) fn spawn_throttle<Args, F>(
    effect: &EffectContext,
    callback: F,
    delay: Duration,
    no_trailing: bool,
) -> Result<Throttled<Args>, TimerError>
where
    Args: Send + 'static,
    F: Fn(Args) + Send + Sync + 'static,
{
    let cancel = effect.cancellation_token().child_token();
    let effect_clone = effect.clone();

    Ok(Throttled {
        inner: Arc::new(ThrottleInner {
            disposed: AtomicBool::new(false),
            cancel,
            state: Mutex::new(ThrottleState {
                window_open: false,
                trailing: None,
                pending: None,
            }),
            callback: Arc::new(callback) as SharedCallback<Args>,
            delay,
            no_trailing,
            effect: effect_clone,
        }),
    })
}

pub(crate) fn throttle_call<Args>(
    inner: &Arc<ThrottleInner<Args>>,
    args: Args,
) -> Result<(), TimerError>
where
    Args: Send + 'static,
{
    if inner.disposed.load(Ordering::SeqCst) || inner.cancel.is_cancelled() {
        return Err(TimerError::Disposed);
    }

    let mut invoke_now = None;
    let mut schedule_window = false;

    {
        let mut state = inner.state.lock().expect("throttle state");
        if !state.window_open {
            state.window_open = true;
            invoke_now = Some(args);
            schedule_window = true;
        } else if !inner.no_trailing {
            state.trailing = Some(args);
        }
    }

    if schedule_window {
        // 先排程再调 leading，避免 callback panic 后 window_open 卡住且无窗口任务。
        if let Err(err) = schedule_throttle_window(inner) {
            clear_throttle_window(inner);
            return Err(err);
        }
    }

    if let Some(args) = invoke_now {
        (inner.callback)(args);
    }

    Ok(())
}

/// 放弃尚未成功登记任务的节流窗口，恢复为下一次 `call()` 可重新开启的状态。
fn clear_throttle_window<Args>(inner: &ThrottleInner<Args>) {
    let mut state = inner.state.lock().expect("throttle state");
    state.window_open = false;
    state.trailing = None;
    if let Some(pending) = state.pending.take() {
        pending.cancel();
    }
}

fn schedule_throttle_window<Args>(inner: &Arc<ThrottleInner<Args>>) -> Result<(), TimerError>
where
    Args: Send + 'static,
{
    let pending = inner.cancel.child_token();
    {
        let mut state = inner.state.lock().expect("throttle state");
        if let Some(prev) = state.pending.take() {
            prev.cancel();
        }
        state.pending = Some(pending.clone());
    }

    let delay = inner.delay;
    let effect = inner.effect.clone();
    let inner = Arc::clone(inner);
    let task_cancel = pending;
    let owner_cancel = inner.cancel.clone();
    effect.spawn(move |_parent| async move {
        tokio::select! {
            biased;
            _ = owner_cancel.cancelled() => {}
            _ = task_cancel.cancelled() => {}
            _ = tokio::time::sleep(delay) => {
                let trailing = {
                    let mut state = inner.state.lock().expect("throttle state");
                    state.pending = None;
                    let trailing = state.trailing.take();
                    // 尾随执行会开启下一冷却窗口；无尾随时才关闭窗口。
                    state.window_open = trailing.is_some();
                    trailing
                };
                if inner.disposed.load(Ordering::SeqCst) || owner_cancel.is_cancelled() {
                    return;
                }
                if let Some(args) = trailing {
                    // 先排程下一窗口再调尾随回调，避免 panic 后窗口悬空。
                    if schedule_throttle_window(&inner).is_err() {
                        clear_throttle_window(&inner);
                        return;
                    }
                    (inner.callback)(args);
                }
            }
        }
    })?;
    Ok(())
}

pub(crate) fn spawn_debounce<Args, F>(
    effect: &EffectContext,
    callback: F,
    delay: Duration,
) -> Result<Debounced<Args>, TimerError>
where
    Args: Send + 'static,
    F: Fn(Args) + Send + Sync + 'static,
{
    let cancel = effect.cancellation_token().child_token();
    Ok(Debounced {
        inner: Arc::new(DebounceInner {
            disposed: AtomicBool::new(false),
            cancel,
            state: Mutex::new(DebounceState {
                generation: 0,
                pending: None,
                args: None,
            }),
            callback: Arc::new(callback) as SharedCallback<Args>,
            delay,
            effect: effect.clone(),
        }),
    })
}

pub(crate) fn debounce_call<Args>(
    inner: &Arc<DebounceInner<Args>>,
    args: Args,
) -> Result<(), TimerError>
where
    Args: Send + 'static,
{
    if inner.disposed.load(Ordering::SeqCst) || inner.cancel.is_cancelled() {
        return Err(TimerError::Disposed);
    }

    let pending = inner.cancel.child_token();
    let my_gen = {
        let mut state = inner.state.lock().expect("debounce state");
        if let Some(prev) = state.pending.take() {
            prev.cancel();
        }
        state.generation = state.generation.wrapping_add(1);
        let my_gen = state.generation;
        state.args = Some(args);
        state.pending = Some(pending.clone());
        my_gen
    };

    let delay = inner.delay;
    let effect = inner.effect.clone();
    let task_inner = Arc::clone(inner);
    let task_cancel = pending;
    let owner_cancel = task_inner.cancel.clone();
    if let Err(err) = effect.spawn(move |_parent| async move {
        tokio::select! {
            biased;
            _ = owner_cancel.cancelled() => {}
            _ = task_cancel.cancelled() => {}
            _ = tokio::time::sleep(delay) => {
                let args = {
                    let mut state = task_inner.state.lock().expect("debounce state");
                    if state.generation != my_gen {
                        return;
                    }
                    state.pending = None;
                    state.args.take()
                };
                if task_inner.disposed.load(Ordering::SeqCst) || owner_cancel.is_cancelled() {
                    return;
                }
                if let Some(args) = args {
                    (task_inner.callback)(args);
                }
            }
        }
    }) {
        // `spawn()` 可能在写入状态后因 Effect 释放而失败。仅清理本次
        // generation，避免与并发的下一次 `call()` 相互覆盖。
        let mut state = inner.state.lock().expect("debounce state");
        if state.generation == my_gen {
            if let Some(pending) = state.pending.take() {
                pending.cancel();
            }
            state.args = None;
        }
        return Err(err.into());
    }
    Ok(())
}
