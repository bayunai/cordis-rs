//! Effect 作用域计时器能力插件。
//!
//! Host 须在 Root Context 显式挂载 [`TimerPlugin`]；业务插件通过静态 `inject([TIMER.id()])`
//! 依赖它，并在 [`EffectContext`](cordis_core::EffectContext) 上使用 [`TimerExt`]。

mod handle;
mod schedule;
mod service;

pub use handle::{Debounced, Throttled, TickStream, TimerHandle, TimerSleep};
pub use service::{TIMER, TimerExt, TimerPlugin, TimerService};

use cordis_core::CoreError;

/// Timer 公开错误。
#[derive(Debug, thiserror::Error)]
pub enum TimerError {
    #[error(transparent)]
    Core(#[from] CoreError),
    #[error("timer 已取消或所属 Effect 已释放")]
    Disposed,
}

/// 此插件的共享定义；展示名称不参与身份比较。
pub static PLUGIN_DEFINITION: std::sync::LazyLock<cordis_core::PluginDefinition> =
    std::sync::LazyLock::new(|| cordis_core::PluginDefinition::named("cordis.timer"));
