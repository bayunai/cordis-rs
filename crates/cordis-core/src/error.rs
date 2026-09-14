use crate::service::ServiceId;

#[derive(Debug, thiserror::Error)]
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
}
