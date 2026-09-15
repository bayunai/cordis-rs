//! 生命周期受管用户回调的任务上下文。
//!
//! Core 用此模块区分宿主调用与会被 Runtime 释放/调度器等待的用户 Future，
//! 并携带所属 [`EffectScope`] 身份，供精确 unmount 重入判定。

use crate::effect::EffectScope;

/// 当前任务上的用户生命周期帧。
#[derive(Clone)]
pub(crate) struct LifecycleFrame {
    pub(crate) scope: EffectScope,
}

tokio::task_local! {
    pub(crate) static USER_LIFECYCLE_CALLBACK: LifecycleFrame;
}

tokio::task_local! {
    pub(crate) static SHUTDOWN_COORDINATOR: ();
}

pub(crate) fn in_user_lifecycle_callback() -> bool {
    USER_LIFECYCLE_CALLBACK.try_with(|_| ()).is_ok()
}

pub(crate) fn current_lifecycle_scope() -> Option<EffectScope> {
    USER_LIFECYCLE_CALLBACK
        .try_with(|frame| frame.scope.clone())
        .ok()
}

pub(crate) fn in_shutdown_coordinator() -> bool {
    SHUTDOWN_COORDINATOR.try_with(|_| ()).is_ok()
}
