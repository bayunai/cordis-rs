//! Fiber 上 Effect Scope 的唯一所有权状态。

use crate::effect::EffectScope;

/// Loading 临时 Scope / Active Scope / 在途释放，互斥且同锁更新。
pub(crate) enum EffectOwnership {
    Empty,
    Activating(EffectScope),
    Active(EffectScope),
    Releasing(FiberRelease),
}

#[derive(Clone)]
pub(crate) struct FiberRelease {
    pub(crate) scope: EffectScope,
    pub(crate) dispose_started: bool,
}

impl EffectOwnership {
    pub(crate) fn live_scope(&self) -> Option<&EffectScope> {
        match self {
            Self::Empty => None,
            Self::Activating(scope) | Self::Active(scope) => Some(scope),
            Self::Releasing(release) => Some(&release.scope),
        }
    }

    pub(crate) fn needs_unmount_wait(&self) -> bool {
        !matches!(self, Self::Empty)
    }
}
