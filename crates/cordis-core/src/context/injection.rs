//! [`InjectionHandle`] / [`InjectionState`] 公开 DTO 与 inject 门面。
//!
//! 仅暴露只读注入状态；实际响应式收敛与重算由 `registry/injection` 负责。

use crate::registry::{InjectionPhase, Registry};
use std::sync::Weak;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InjectionState {
    Pending,
    Active,
    Failed,
    Disposed,
}

/// 一个注入注册的只读状态句柄；它不拥有或延长 Effect 生命周期。
pub struct InjectionHandle {
    pub(super) id: u64,
    pub(super) registry: Weak<Registry>,
}

impl InjectionHandle {
    pub fn state(&self) -> InjectionState {
        self.registry
            .upgrade()
            .and_then(|registry| registry.injection_phase(self.id))
            .map(InjectionState::from)
            .unwrap_or(InjectionState::Disposed)
    }
}

impl From<InjectionPhase> for InjectionState {
    fn from(value: InjectionPhase) -> Self {
        match value {
            InjectionPhase::Pending => Self::Pending,
            InjectionPhase::Active => Self::Active,
            InjectionPhase::Failed => Self::Failed,
            InjectionPhase::Disposed => Self::Disposed,
        }
    }
}
