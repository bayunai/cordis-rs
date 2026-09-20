use cordis_loader::{ExtensionCatalog, LoaderError};

use crate::plugins::greeting::GreetingFactory;

/// 此程序编译期可用插件的唯一目录。
///
/// 注册到这里不代表插件会启动；`extensions.toml` 的条目配置才是唯一启停来源。
pub fn build_catalog() -> Result<ExtensionCatalog, LoaderError> {
    let mut catalog = ExtensionCatalog::new();
    catalog.register(GreetingFactory)?;
    // 可选：嵌套 TOML EntryTree（默认不启用）。
    // catalog.register(cordis_plugin_include::IncludeFactory)?;
    Ok(catalog)
}
