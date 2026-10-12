//! 进程内静态插件 Loader：显式工厂目录、严格文件配置与按 Entry/子树差分 reconcile。
//!
//! Core 仍只负责 Plugin、Fiber、Service、Effect 与生命周期。Loader 负责读取配置、
//! 构造插件，并仅对创建、删除、移动、启停或配置变化的节点做生命周期操作；无关 Entry
//! 保留原 Fiber、Context 与服务。首版不加载动态库、不监听文件、不提供 HTTP 管理接口。
//! 纯排序只更新快照/文件顺序，不重启 Fiber。支持可附着的文件子树（Include）。
//! [`PreparedExtension::volatile`] 可将配置划分为普通部分与热部分，后者通过只读
//! [`ConfigRef`] 原地提交完整快照，不重建 Active 插件的服务和 Effect。

mod bootstrap;
mod catalog;
mod config;
mod error;
mod loader;
mod meta;
mod plugin;
mod prepared;
mod reconcile;
mod snapshot;
mod tree;
mod volatile;

pub use bootstrap::{load_bootstrap, load_extensions};
pub use catalog::{
    ExtensionCatalog, ExtensionFactory, FactoryDescriptor, InjectionDescriptor,
    InjectionDescriptorInfo, IsolationDescriptor, IsolationDescriptorInfo,
};
pub use config::{
    BootstrapConfig, BootstrapConfigSource, CONFIG_VERSION, ConfigDriver, EntryOptions,
    ExtensionsConfig, GROUP_NAME, InjectConfig, IsolateConfig, IsolateValue,
};
pub use error::LoaderError;
pub use loader::{EntryUpdate, LOADER, Loader, LoaderControlError, LoaderEvent, LoaderSubtree};
pub use meta::{ENTRY_LOCATION, EntryLocation};
pub use plugin::LoaderPlugin;
pub use prepared::PreparedExtension;
pub use snapshot::{EntryId, EntrySnapshot, LoaderSnapshot};
pub use volatile::{ConfigClosed, ConfigRef, ConfigSnapshot, ConfigSubscription};

pub use schemars;
pub use toml;
