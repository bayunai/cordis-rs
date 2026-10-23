//! 进程内静态插件 Loader：显式工厂目录、来源无关条目树与差分 reconcile。
//!
//! Core 仍只负责 Plugin、Fiber、Service、Effect 与生命周期。Loader 负责校验条目、
//! 构造插件，并仅对创建、删除、移动、启停或配置变化的节点做生命周期操作；无关 Entry
//! 保留原 Fiber、Context 与服务。文件编解码、来源目录、初始文件及 patches 属于 Include。
//! 纯排序只更新快照/声明顺序，不重启 Fiber。不加载动态库、不提供 HTTP 管理接口。
//! [`PreparedExtension::volatile`] 可将配置划分为普通部分与热部分，后者通过只读
//! [`ConfigRef`] 原地提交完整快照，不重建 Active 插件的服务和 Effect。

mod catalog;
mod config;
mod config_control;
mod error;
mod fiber_events;
mod lifecycle_logs;
mod loader;
mod meta;
mod plugin;
mod prepared;
mod reconcile;
mod release;
mod snapshot;
mod source;
mod tree;
mod volatile;

pub use catalog::{
    ExtensionCatalog, ExtensionFactory, FactoryDescriptor, InjectionDescriptor,
    InjectionDescriptorInfo, IsolationDescriptor, IsolationDescriptorInfo,
};
pub use config::{
    CONFIG_VERSION, EntryOptions, ExtensionsConfig, GROUP_NAME, InjectConfig, IsolateConfig,
    IsolateValue,
};
pub use config_control::{ConfigRevision, EntryConfigSnapshot};
pub use error::LoaderError;
pub use loader::{EntryUpdate, LOADER, Loader, LoaderControlError, LoaderEvent, LoaderSubtree};
pub use meta::{ENTRY_LOCATION, EntryLocation};
pub use plugin::LoaderPlugin;
pub use prepared::PreparedExtension;
pub use snapshot::{EntryId, EntrySnapshot, LoaderSnapshot};
pub use source::{EntryTreeSource, SourceDocument, SourceError, SourceErrorKind, SourceRevision};
pub use volatile::{ConfigClosed, ConfigRef, ConfigSnapshot, ConfigSubscription};

pub use schemars;
pub use toml;
