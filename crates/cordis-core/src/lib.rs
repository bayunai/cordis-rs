//! Cordis 风格运行时原语的公开 crate 表面。
//!
//! Core 不含网关、HTTP、数据库、Redis、持久化配置或插件发现；
//! 宿主围绕 [`Runtime`] 组装这些能力。

mod callback_context;
mod config;
mod context;
mod diagnostics;
mod effect;
mod error;
mod event;
mod fiber;
mod isolation;
mod logger;
mod plugin;
mod registry;
mod runtime;
mod service;

pub use config::{ConfigId, ConfigKey};
pub use context::{Context, EffectContext, InjectionHandle, InjectionState};
pub use diagnostics::{
    EffectSnapshot, FiberStateSnapshot, InjectFiberSnapshot, IsolationSnapshot,
    PluginFiberSnapshot, PluginRegistryFiberSnapshot, PluginRegistrySnapshot, ProviderSnapshot,
    RuntimeSnapshot, UnavailableDependencySnapshot,
};
pub use effect::EffectHandle;
pub use error::CoreError;
pub use event::{
    EventKey, ListenFilter, ListenOptions, Next, ParallelKey, SerialKey, Unsubscribe, WaterfallKey,
};
pub use fiber::{Fiber, FiberState, FiberStateChange};
pub use isolation::IsolationLabel;
pub use logger::{
    LOG_BUFFER_CAPACITY, LOGGER_CONFIG, LogExporter, LogLevel, LogRecord, Logger, LoggerConfig,
    LoggerService,
};
pub use plugin::{Plugin, PluginKey};
pub use registry::provider::ProviderHandle;
pub use runtime::Runtime;
pub use service::{ProviderAvailability, ServiceId, ServiceKey, Services};
