//! Timer 能力标记与 [`TimerExt`]。

use crate::{
    Debounced, Throttled, TickStream, TimerError, TimerHandle, TimerSleep,
    schedule::{
        spawn_debounce, spawn_interval, spawn_sleep, spawn_throttle, spawn_ticks, spawn_timeout,
    },
};
use async_trait::async_trait;
use cordis_core::{Context, CoreError, EffectContext, Plugin, PluginKey, ServiceKey};
use std::time::Duration;

/// Timer 能力服务 Key。
pub static TIMER: ServiceKey<TimerService> = ServiceKey::new("cordis.timer");

/// 零状态能力标记；不持有计时器任务。
#[derive(Debug, Default)]
pub struct TimerService;

/// Host 显式挂载的 Timer 能力插件。
pub struct TimerPlugin;

#[async_trait]
impl Plugin for TimerPlugin {
    fn key(&self) -> PluginKey {
        PluginKey::new("cordis.timer")
    }

    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        ctx.provide(TIMER, TimerService)?;
        Ok(())
    }
}

/// 仅实现于 [`EffectContext`] 的计时器扩展。
pub trait TimerExt {
    fn timeout<F>(&self, callback: F, delay: Duration) -> Result<TimerHandle, TimerError>
    where
        F: FnOnce() + Send + 'static;

    fn sleep(&self, delay: Duration) -> Result<TimerSleep, TimerError>;

    fn interval<F>(&self, callback: F, delay: Duration) -> Result<TimerHandle, TimerError>
    where
        F: Fn() + Send + 'static;

    fn ticks(&self, delay: Duration) -> Result<TickStream, TimerError>;

    fn throttle<Args, F>(
        &self,
        callback: F,
        delay: Duration,
        no_trailing: bool,
    ) -> Result<Throttled<Args>, TimerError>
    where
        Args: Send + 'static,
        F: Fn(Args) + Send + Sync + 'static;

    fn debounce<Args, F>(
        &self,
        callback: F,
        delay: Duration,
    ) -> Result<Debounced<Args>, TimerError>
    where
        Args: Send + 'static,
        F: Fn(Args) + Send + Sync + 'static;
}

impl TimerExt for EffectContext {
    fn timeout<F>(&self, callback: F, delay: Duration) -> Result<TimerHandle, TimerError>
    where
        F: FnOnce() + Send + 'static,
    {
        ensure_timer(self)?;
        spawn_timeout(self, callback, delay)
    }

    fn sleep(&self, delay: Duration) -> Result<TimerSleep, TimerError> {
        ensure_timer(self)?;
        spawn_sleep(self, delay)
    }

    fn interval<F>(&self, callback: F, delay: Duration) -> Result<TimerHandle, TimerError>
    where
        F: Fn() + Send + 'static,
    {
        ensure_timer(self)?;
        spawn_interval(self, callback, delay)
    }

    fn ticks(&self, delay: Duration) -> Result<TickStream, TimerError> {
        ensure_timer(self)?;
        spawn_ticks(self, delay)
    }

    fn throttle<Args, F>(
        &self,
        callback: F,
        delay: Duration,
        no_trailing: bool,
    ) -> Result<Throttled<Args>, TimerError>
    where
        Args: Send + 'static,
        F: Fn(Args) + Send + Sync + 'static,
    {
        ensure_timer(self)?;
        spawn_throttle(self, callback, delay, no_trailing)
    }

    fn debounce<Args, F>(&self, callback: F, delay: Duration) -> Result<Debounced<Args>, TimerError>
    where
        Args: Send + 'static,
        F: Fn(Args) + Send + Sync + 'static,
    {
        ensure_timer(self)?;
        spawn_debounce(self, callback, delay)
    }
}

fn ensure_timer(effect: &EffectContext) -> Result<(), TimerError> {
    effect.get(TIMER)?;
    Ok(())
}
