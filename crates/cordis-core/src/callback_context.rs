//! 生命周期受管用户回调的任务上下文。
//!
//! Core 用此模块区分宿主调用与会被 Runtime 释放/调度器等待的用户 Future，
//! 防止后者递归等待全局 shutdown 造成自锁。

tokio::task_local! {
    pub(crate) static USER_LIFECYCLE_CALLBACK: ();
}

tokio::task_local! {
    pub(crate) static SHUTDOWN_COORDINATOR: ();
}

pub(crate) fn in_user_lifecycle_callback() -> bool {
    USER_LIFECYCLE_CALLBACK.try_with(|_| ()).is_ok()
}

pub(crate) fn in_shutdown_coordinator() -> bool {
    SHUTDOWN_COORDINATOR.try_with(|_| ()).is_ok()
}
