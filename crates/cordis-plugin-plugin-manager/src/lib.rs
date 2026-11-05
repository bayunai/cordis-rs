//! Host 显式挂载的插件库存与启停门面。
//!
//! 对照 DeepSeek `pluginManager`：`list_plugins` / `set_plugin_enabled`；
//! 写回委托 [`cordis_plugin_config_editor`]；不做 HTTP、安装或 bundle 选层。

mod service;

pub use service::{
    PLUGIN_MANAGER, PluginInfo, PluginManagerPlugin, PluginManagerService, ReadOnlyReason,
};

pub use cordis_loader::LoaderControlError;

/// 此插件的共享定义；展示名称不参与身份比较。
pub static PLUGIN_DEFINITION: std::sync::LazyLock<cordis_core::PluginDefinition> =
    std::sync::LazyLock::new(|| cordis_core::PluginDefinition::named("cordis.plugin-manager"));
