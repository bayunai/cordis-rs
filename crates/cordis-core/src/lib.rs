//! Minimal Cordis-style runtime primitives for Rust.
//!
//! The crate deliberately has no gateway, HTTP, persistence, configuration, or
//! package-loading concepts. A host assembles those concerns around [`Runtime`].

mod config;
mod context;
mod diagnostics;
mod effect;
mod error;
mod event;
mod fiber;
mod inject;
mod isolation;
mod plugin;
mod runtime;
mod service;

pub use config::{ConfigId, ConfigKey};
pub use context::{Context, EffectContext, InjectionHandle, InjectionState};
pub use diagnostics::{
    ContextIsolationSnapshot, ContextSnapshot, EffectSnapshot, FiberStateSnapshot,
    InjectFiberSnapshot, IsolationSnapshot, PluginFiberSnapshot, PluginRegistryFiberSnapshot,
    PluginRegistrySnapshot, ProviderSnapshot, RuntimeSnapshot,
};
pub use effect::EffectHandle;
pub use error::CoreError;
pub use event::{
    EventKey, ListenFilter, ListenOptions, Next, ParallelKey, SerialKey, Unsubscribe, WaterfallKey,
};
pub use fiber::{Fiber, FiberState};
pub use isolation::IsolationLabel;
pub use plugin::{Plugin, PluginKey};
pub use runtime::Runtime;
pub use service::{ServiceId, ServiceKey, Services};
