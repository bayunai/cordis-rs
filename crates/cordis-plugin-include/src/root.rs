//! 根 Include 声明构造与只读扩展文档读取。
//!
//! Host 直接挂载 `cordis.toml`（或任意 v3 扩展清单路径）；不再经独立 bootstrap 配方文件。

use crate::{IncludeConfig, codec::Format, source::file_task};
use cordis_loader::{EntryOptions, ExtensionsConfig, SourceError, SourceErrorKind};
use std::{
    fs,
    path::{Path, PathBuf},
};

/// 由应用显式指定根 Include ID 与配置文件路径（通常为绝对路径的 `cordis.toml`）。
///
/// 文件内条目的全局路径以该 ID 为前缀，不提供无前缀别名。
/// 相对路径在文件已存在时 canonicalize；否则按当前工作目录拼成绝对路径。
pub fn include_root_entry(
    id: impl Into<String>,
    path: impl AsRef<Path>,
) -> Result<EntryOptions, SourceError> {
    let path = path.as_ref();
    Format::for_path(path)?;
    let absolute = resolve_include_path(path)?;
    let config = toml::Value::try_from(IncludeConfig::new(absolute)).map_err(|_| {
        SourceError::new(SourceErrorKind::InvalidData, "无法表示 Include 启动配置")
    })?;
    Ok(EntryOptions::new(id.into(), "cordis:include").with_config(config))
}

fn resolve_include_path(path: &Path) -> Result<PathBuf, SourceError> {
    if let Ok(canonical) = path.canonicalize() {
        return Ok(canonical);
    }
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    let cwd = std::env::current_dir().map_err(|_| {
        SourceError::new(SourceErrorKind::Io, "无法解析 Include 路径：无当前工作目录")
    })?;
    Ok(cwd.join(path))
}

/// 只读工具入口，按扩展名读取完整 v3 文档，不应用另外的运行时 patches。
pub async fn load_extensions(path: impl AsRef<Path>) -> Result<ExtensionsConfig, SourceError> {
    let path = path.as_ref().to_owned();
    file_task(move || {
        let bytes = fs::read(&path)
            .map_err(|_| SourceError::new(SourceErrorKind::Io, "无法读取 Include 配置文档"))?;
        Format::for_path(&path)?.parse(&bytes)
    })
    .await
}
