//! Host 显式挂载的配置编辑器：deploy ∘ user ∘ home + Loader reconcile。
//!
//! 表单默认只写 user patch 文档，不 bake 空根或各插件 deploy patch。
//! 投影见 [`cordis_plugin_settings`]。

mod layers;
mod patch_file;
mod service;
mod user_patch;

pub use layers::{ConfigEditorConfig, LayeredEntryConfig};
pub use patch_file::{
    PATCH_FILE_VERSION, PatchFileDocument, compose_entries, load_ordered_patches,
};
pub use service::{CONFIG_EDITOR, ConfigEditorPlugin, ConfigEditorService};
pub use user_patch::{USER_PATCH_VERSION, UserPatch, UserPatchDocument, UserPatchStore};

pub use cordis_loader::{ConfigRevision, EntryConfigSnapshot, LoaderControlError};
