//! Fiber 合法状态转换、终态保护与状态通知。
//!
//! 状态：Pending / Loading / Active / Failed / Unloading / Disposed；
//! 生命周期动作与激活逻辑分别在 `lifecycle`、`activate`。

use super::FiberInner;
use crate::{diagnostics::UnavailableDependencySnapshot, plugin::PluginKey};
use std::sync::atomic::Ordering;

/// Plugin Fiber 公开生命周期状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FiberState {
    Pending,
    Loading,
    Active,
    Failed,
    Unloading,
    Disposed,
}

/// Plugin Fiber 的只读生命周期转换事件。
#[derive(Debug, Clone)]
pub struct FiberStateChange {
    pub fiber_id: u64,
    pub plugin_key: PluginKey,
    pub previous: Option<FiberState>,
    pub current: FiberState,
    /// 当前处于严格解析不可用状态、但仍已注册的依赖。
    pub unavailable_dependencies: Vec<UnavailableDependencySnapshot>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ActivateClaim {
    Claimed,
    Skipped,
}

impl FiberInner {
    pub(crate) fn publish_initial_state(&self) {
        if let Some(registry) = self.registry.upgrade() {
            registry.publish_fiber_state(FiberStateChange {
                fiber_id: self.id,
                plugin_key: self.plugin_key,
                previous: None,
                current: FiberState::Pending,
                unavailable_dependencies: self.unavailable_dependencies(),
            });
        }
    }

    pub(super) fn publish_transition(&self, previous: FiberState, current: FiberState) {
        if previous == current {
            return;
        }
        if let Some(registry) = self.registry.upgrade() {
            registry.publish_fiber_state(FiberStateChange {
                fiber_id: self.id,
                plugin_key: self.plugin_key,
                previous: Some(previous),
                current,
                unavailable_dependencies: self.unavailable_dependencies(),
            });
        }
    }

    /// 活跃生命周期转换；已释放时将状态收敛到 Disposed，拒绝后续非终态转换。
    pub(crate) fn transition_if_alive(&self, next: FiberState) -> bool {
        let (alive, changed) = {
            let Ok(mut state) = self.state.lock() else {
                return false;
            };
            if self.disposed.load(Ordering::Acquire) || *state == FiberState::Disposed {
                let previous = *state;
                if previous != FiberState::Disposed {
                    *state = FiberState::Disposed;
                    (false, Some((previous, FiberState::Disposed)))
                } else {
                    (false, None)
                }
            } else {
                let previous = *state;
                if previous != next {
                    *state = next;
                    (true, Some((previous, next)))
                } else {
                    (true, None)
                }
            }
        };
        if let Some((previous, current)) = changed {
            self.publish_transition(previous, current);
        }
        alive
    }

    pub(super) fn transition_disposed(&self) {
        let changed = {
            let Ok(mut state) = self.state.lock() else {
                return;
            };
            let previous = *state;
            if previous == FiberState::Disposed {
                None
            } else {
                *state = FiberState::Disposed;
                Some(previous)
            }
        };
        if let Some(previous) = changed {
            self.publish_transition(previous, FiberState::Disposed);
        }
    }

    #[allow(dead_code)]
    pub(super) fn transition_disposing(&self) {
        let changed = {
            let Ok(mut state) = self.state.lock() else {
                return;
            };
            let previous = *state;
            if previous == FiberState::Disposed || previous == FiberState::Unloading {
                None
            } else {
                *state = FiberState::Unloading;
                Some(previous)
            }
        };
        if let Some(previous) = changed {
            self.publish_transition(previous, FiberState::Unloading);
        }
    }
}
