//! [`CoreError`]：公开失败契约的错误变体。
//!
//! 覆盖 Context、Service、事件、Fiber、注入与调度等失败路径。

use crate::service::ServiceId;
use std::any::Any;

/// 将扩展边界捕获到的 panic 转换为可诊断、但不泄漏非字符串载荷的错误文本。
pub(crate) fn format_panic_message(boundary: &str, payload: Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        format!("{boundary} panicked: {message}")
    } else if let Some(message) = payload.downcast_ref::<String>() {
        format!("{boundary} panicked: {message}")
    } else {
        format!("{boundary} panicked")
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum CoreError {
    #[error("Context 已释放")]
    ContextDisposed,
    #[error("Service {service} 当前不可用")]
    ServiceUnavailable { service: ServiceId },
    #[error("Service {service} 已在当前 Context 注册")]
    ServiceConflict { service: ServiceId },
    #[error("Service {service} 的 Rust 类型不匹配")]
    ServiceTypeMismatch { service: ServiceId },
    #[error("Service {service} 已绑定其他 Rust 类型")]
    ServiceKeyTypeConflict { service: ServiceId },
    #[error("事件 {event} 的 Rust 类型不匹配")]
    EventTypeMismatch { event: &'static str },
    #[error("事件 {event} 已绑定其他 Rust 类型")]
    EventKeyTypeConflict { event: &'static str },
    #[error("事件 {event} 已绑定其他分发模式")]
    EventModeMismatch { event: &'static str },
    #[error("事件 {event} 的答案类型已绑定其他 Rust 类型")]
    EventAnswerTypeConflict { event: &'static str },
    #[error("注入依赖不能为空")]
    EmptyInjection,
    #[error("运行时调度器不可用；请在 Tokio Runtime 中创建 Runtime")]
    SchedulerUnavailable,
    #[error("shutdown 只能由宿主生命周期外部调用")]
    ShutdownReentrant,
    #[error("不可卸载当前回调所在 Effect 树内的插件目标")]
    UnmountReentrant,
    #[error("事件监听器失败: {0}")]
    EventListener(String),
    #[error("并行事件 {event} 分发失败: {errors:?}")]
    ParallelDispatchFailed {
        event: &'static str,
        errors: Vec<String>,
    },
    #[error("插件挂载失败: {0}")]
    PluginApply(String),
    #[error("IsolationLabel 不属于当前 Runtime")]
    IsolationRuntimeMismatch,
    #[error("Fiber 已释放")]
    FiberDisposed,
    #[error("Fiber 正在执行生命周期操作")]
    FiberBusy,
    #[error("Config {config} 当前不可用")]
    ConfigUnavailable { config: crate::config::ConfigId },
    #[error("Config {config} 的 Rust 类型不匹配")]
    ConfigTypeMismatch { config: crate::config::ConfigId },
    #[error("Config {config} 已绑定其他 Rust 类型")]
    ConfigKeyTypeConflict { config: crate::config::ConfigId },
    #[error("插件 {plugin} 正在卸载")]
    PluginUnmounting { plugin: crate::plugin::PluginKey },
    #[error("插件 Key 不匹配：期望 {expected}，实际 {actual}")]
    PluginKeyMismatch {
        expected: crate::plugin::PluginKey,
        actual: crate::plugin::PluginKey,
    },
    #[error("释放失败: {errors:?}")]
    DisposeFailed { errors: Vec<String> },
    #[error("生命周期协调器异常终止: {reason}")]
    CoordinatorAborted { reason: String },
}
