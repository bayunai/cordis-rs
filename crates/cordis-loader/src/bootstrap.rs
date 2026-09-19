//! 读取 `bootstrap.toml` 并解析文件配置源路径。

use crate::{
    config::{BootstrapConfig, ExtensionsConfig, parse_toml},
    error::LoaderError,
};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
};

static TEMP_FILE_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// 读取启动文件，返回配置以及解析后的 `extensions.toml` 绝对路径。
///
/// 相对 `path` 相对于 bootstrap 文件所在目录解析。
pub fn load_bootstrap(path: impl AsRef<Path>) -> Result<(BootstrapConfig, PathBuf), LoaderError> {
    let path = path.as_ref();
    let text = read_to_string(path)?;
    let config: BootstrapConfig = parse_toml(&text, path)?;
    crate::config::ensure_version(config.version)?;
    let extensions_path = resolve_extensions_path(path, &config.config.path);
    Ok((config, extensions_path))
}

/// 读取运行期扩展清单。
pub fn load_extensions(path: impl AsRef<Path>) -> Result<ExtensionsConfig, LoaderError> {
    Ok(load_extensions_source(path)?.0)
}

/// 读取运行期扩展清单与其原始内容，用于 Loader 冲突检测。
pub(crate) fn load_extensions_source(
    path: impl AsRef<Path>,
) -> Result<(ExtensionsConfig, String), LoaderError> {
    let path = path.as_ref();
    let text = read_to_string(path)?;
    let config = parse_toml(&text, path)?;
    Ok((config, text))
}

/// 将完整扩展清单原子替换到目标文件，并返回写入后的原始文本。
pub(crate) fn write_extensions(
    path: &Path,
    config: &ExtensionsConfig,
) -> Result<String, LoaderError> {
    let text = toml::to_string_pretty(config).map_err(|error| LoaderError::Toml {
        path: path.to_path_buf(),
        message: error.to_string(),
    })?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| LoaderError::Io {
            path: path.to_path_buf(),
            message: "extensions path has no valid file name".into(),
        })?;
    let temporary = parent.join(format!(
        ".{file_name}.{}.{}.tmp",
        std::process::id(),
        TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let write_result = (|| -> Result<(), LoaderError> {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|source| LoaderError::io(temporary.clone(), source))?;
        use std::io::Write;
        file.write_all(text.as_bytes())
            .map_err(|source| LoaderError::io(temporary.clone(), source))?;
        file.sync_all()
            .map_err(|source| LoaderError::io(temporary.clone(), source))?;
        fs::rename(&temporary, path)
            .map_err(|source| LoaderError::io(path.to_path_buf(), source))?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    write_result?;
    Ok(text)
}

pub(crate) fn resolve_extensions_path(bootstrap_path: &Path, configured: &Path) -> PathBuf {
    if configured.is_absolute() {
        configured.to_path_buf()
    } else {
        let base = bootstrap_path.parent().unwrap_or_else(|| Path::new("."));
        base.join(configured)
    }
}

fn read_to_string(path: &Path) -> Result<String, LoaderError> {
    fs::read_to_string(path).map_err(|source| LoaderError::io(path.to_path_buf(), source))
}
