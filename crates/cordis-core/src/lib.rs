//! Minimal Cordis-style runtime primitives for Rust.
//!
//! The crate deliberately has no gateway, HTTP, persistence, configuration, or
//! package-loading concepts. A host assembles those concerns around [`Runtime`].

mod context;
mod diagnostics;
mod effect;
mod error;
mod event;
mod inject;
mod plugin;
mod runtime;
mod service;

pub use context::{Context, EffectContext, InjectionHandle, InjectionState};
pub use diagnostics::{
    ContextSnapshot, FiberSnapshot, InjectionPhaseSnapshot, PluginSnapshot, ProviderSnapshot,
    RuntimeSnapshot,
};
pub use error::CoreError;
pub use event::{EventKey, Next, ParallelKey, SerialKey, Unsubscribe, WaterfallKey};
pub use plugin::{Plugin, PluginHandle};
pub use runtime::Runtime;
pub use service::{ServiceId, ServiceKey, Services};
