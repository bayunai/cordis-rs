//! Host 显式挂载的配置设置服务：Schema 表单投影、秘密脱敏与字段操作。
//!
//! 写回委托 [`cordis_plugin_config_editor`]；Loader 仅提供受信任 snapshot/commit。

mod form;
mod secrets;
mod service;

pub use form::{ConfigOperation, EntryConfigView, SecretFieldState};
pub use service::{SETTINGS, SettingsPlugin, SettingsService};

pub use cordis_loader::{ConfigRevision, LoaderControlError};
pub use cordis_plugin_config_editor::{
    CONFIG_EDITOR, ConfigEditorConfig, ConfigEditorPlugin, ConfigEditorService,
};
