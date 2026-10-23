//! 条目挂载时注入的私有位置元数据（供 Include 等树载体读取）。

use crate::loader::Loader;
use cordis_core::ConfigKey;

/// 当前 Entry 在 Loader 中的位置；来源专有元数据由来源提供方注入。
#[derive(Clone)]
pub struct EntryLocation {
    /// 完整条目路径（含 Include 前缀），例如 `reports:importer`。
    pub path: String,
    /// 所属树前缀；根树为空串。
    pub tree_prefix: String,
    /// 当前 Loader 句柄（ConfigKey 注入，不依赖 Fiber Active）。
    pub loader: Loader,
}

/// 私有 ConfigKey：挂载时 intercept，不进入公开 inject Catalog。
pub static ENTRY_LOCATION: ConfigKey<EntryLocation> =
    ConfigKey::new("cordis.loader.entry_location@1");
