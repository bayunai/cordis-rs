//! 进程内静态插件 Loader：显式工厂目录、严格文件配置与 reconcile。
//!
//! Core 仍只负责 Plugin、Fiber、Service、Effect 与生命周期。Loader 负责读取配置、
//! 构造插件并按差异挂载 / 替换 / 卸载。首版不加载动态库、不监听文件、不提供
//! HTTP 管理接口。

mod bootstrap;
mod catalog;
mod config;
mod error;
mod loader;
mod plugin;
mod snapshot;

pub use bootstrap::{load_bootstrap, load_extensions};
pub use catalog::{
    ExtensionCatalog, ExtensionFactory, FactoryDescriptor, InjectionDescriptor,
    InjectionDescriptorInfo,
};
pub use config::{
    BootstrapConfig, BootstrapConfigSource, CONFIG_VERSION, ConfigDriver, EntryOptions,
    ExtensionsConfig, GROUP_NAME, InjectConfig,
};
pub use error::LoaderError;
pub use loader::{EntryUpdate, LOADER, Loader, LoaderControlError};
pub use plugin::LoaderPlugin;
pub use snapshot::{EntryId, EntrySnapshot, LoaderSnapshot};

pub use schemars;
pub use toml;
