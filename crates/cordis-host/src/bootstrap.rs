//! 读取 `bootstrap.toml` 并解析文件配置源路径。

use crate::{
    config::{BootstrapConfig, ExtensionsConfig, parse_toml},
    error::HostError,
};
use std::{
    fs,
    path::{Path, PathBuf},
};

/// 读取启动文件，返回配置以及解析后的 `extensions.toml` 绝对路径。
///
/// 相对 `path` 相对于 bootstrap 文件所在目录解析。
pub fn load_bootstrap(path: impl AsRef<Path>) -> Result<(BootstrapConfig, PathBuf), HostError> {
    let path = path.as_ref();
    let text = read_to_string(path)?;
    let config: BootstrapConfig = parse_toml(&text, path)?;
    crate::config::ensure_version(config.version)?;
    let extensions_path = resolve_extensions_path(path, &config.config.path);
    Ok((config, extensions_path))
}

/// 读取运行期扩展清单。
pub fn load_extensions(path: impl AsRef<Path>) -> Result<ExtensionsConfig, HostError> {
    let path = path.as_ref();
    let text = read_to_string(path)?;
    parse_toml(&text, path)
}

pub(crate) fn resolve_extensions_path(bootstrap_path: &Path, configured: &Path) -> PathBuf {
    if configured.is_absolute() {
        configured.to_path_buf()
    } else {
        let base = bootstrap_path.parent().unwrap_or_else(|| Path::new("."));
        base.join(configured)
    }
}

fn read_to_string(path: &Path) -> Result<String, HostError> {
    fs::read_to_string(path).map_err(|source| HostError::io(path.to_path_buf(), source))
}
