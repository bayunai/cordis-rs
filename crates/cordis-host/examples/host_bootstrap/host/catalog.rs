use cordis_loader::{ExtensionCatalog, LoaderError};

use crate::plugins::greeting::GreetingFactory;

/// 此程序编译期可用插件的唯一目录。
///
/// 注册到这里不代表插件会启动；`cordis.toml` 的条目配置才是唯一启停来源。
pub fn build_catalog() -> Result<ExtensionCatalog, LoaderError> {
    let mut catalog = ExtensionCatalog::new();
    catalog.register(GreetingFactory)?;
    // 根文件与嵌套文件都由显式 Include 条目承载；Loader 根树本身只保存在内存。
    catalog.register(cordis_plugin_include::IncludeFactory)?;
    Ok(catalog)
}
