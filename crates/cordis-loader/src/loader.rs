//! EntryTree 管理控制面。

use crate::{
    bootstrap::{load_extensions_source, write_extensions},
    catalog::{FactoryDescriptor, InjectionDescriptorInfo},
    config::{EntryOptions, ExtensionsConfig, InjectConfig},
    error::LoaderError,
    plugin::LoaderInner,
    snapshot::LoaderSnapshot,
};
use cordis_core::ServiceKey;
use std::{path::PathBuf, sync::Arc};

pub static LOADER: ServiceKey<Loader> = ServiceKey::new("cordis.loader@1");

#[derive(Debug, Clone, Default)]
pub struct EntryUpdate {
    pub config: Option<toml::Value>,
    pub disabled: Option<bool>,
    pub inject: Option<Option<InjectConfig>>,
}

#[derive(Debug, thiserror::Error)]
pub enum LoaderControlError {
    #[error(transparent)]
    Loader(#[from] LoaderError),
    #[error("Loader 已关闭，Loader 不再可用")]
    LoaderUnavailable,
    #[error("条目 {path} 已存在")]
    EntryExists { path: String },
    #[error("条目 {path} 不存在")]
    EntryNotFound { path: String },
    #[error("目标父 Group {path} 不存在或不是 Group")]
    ParentNotGroup { path: String },
    #[error("不能将条目移动到自己或自己的子树: {path}")]
    InvalidMove { path: String },
    #[error("配置文件已在 Loader 外修改；请先 reload: {path}", path = path.display())]
    ConfigConflict { path: PathBuf },
    #[error("运行时已生效，但未能写回配置 {path}: {message}", path = path.display())]
    Persist {
        snapshot: Box<LoaderSnapshot>,
        path: PathBuf,
        message: String,
    },
}

#[derive(Clone)]
pub struct Loader {
    inner: Arc<LoaderInner>,
}
impl Loader {
    pub(crate) fn new(inner: Arc<LoaderInner>) -> Self {
        Self { inner }
    }
    pub fn entries(&self) -> Result<ExtensionsConfig, LoaderControlError> {
        Ok(self.inner()?.desired.lock().expect("desired").clone())
    }
    pub fn entry_context(
        &self,
        path: impl AsRef<str>,
    ) -> Result<cordis_core::Context, LoaderControlError> {
        Ok(self.inner()?.entry_context(path.as_ref())?)
    }
    pub fn factories(&self) -> Result<Vec<FactoryDescriptor>, LoaderControlError> {
        Ok(self.inner()?.catalog.factories()?)
    }
    pub fn injections(&self) -> Result<Vec<InjectionDescriptorInfo>, LoaderControlError> {
        Ok(self.inner()?.catalog.injections())
    }
    pub async fn await_idle(&self) -> Result<LoaderSnapshot, LoaderControlError> {
        Ok(self.inner()?.await_idle().await?)
    }
    pub async fn create(
        &self,
        entry: EntryOptions,
        parent: Option<&str>,
        position: Option<usize>,
    ) -> Result<LoaderSnapshot, LoaderControlError> {
        self.mutate(move |config| insert_entry(&mut config.extensions, parent, position, entry))
            .await
    }
    pub async fn update(
        &self,
        path: impl AsRef<str>,
        patch: EntryUpdate,
        parent: Option<Option<&str>>,
        position: Option<usize>,
    ) -> Result<LoaderSnapshot, LoaderControlError> {
        let path = path.as_ref().to_owned();
        self.mutate(move |config| {
            let destination = if let Some(parent) = parent {
                if parent.is_some_and(|parent| {
                    parent == path || parent.starts_with(&(path.clone() + ":"))
                }) {
                    return Err(LoaderControlError::InvalidMove { path: path.clone() });
                }
                let entry = remove_entry(&mut config.extensions, &path)?;
                let next_path = join_path(parent, &entry.id);
                insert_entry(&mut config.extensions, parent, position, entry)?;
                next_path
            } else {
                path.clone()
            };
            patch_entry(&mut config.extensions, &destination, patch)
        })
        .await
    }
    pub async fn remove(
        &self,
        path: impl AsRef<str>,
    ) -> Result<LoaderSnapshot, LoaderControlError> {
        let path = path.as_ref().to_owned();
        self.mutate(move |config| remove_entry(&mut config.extensions, &path).map(|_| ()))
            .await
    }
    pub async fn reload(&self) -> Result<LoaderSnapshot, LoaderControlError> {
        let inner = self.inner()?;
        let _operation = inner.loader_operations.lock().await;
        let path = source_path(&inner)?;
        let (config, revision) = load_extensions_source(&path)?;
        Ok(inner.apply(config, Some(revision)).await?)
    }
    async fn mutate<F>(&self, mutate: F) -> Result<LoaderSnapshot, LoaderControlError>
    where
        F: FnOnce(&mut ExtensionsConfig) -> Result<(), LoaderControlError>,
    {
        let inner = self.inner()?;
        let _operation = inner.loader_operations.lock().await;
        let path = source_path(&inner)?;
        ensure_revision(&inner, &path)?;
        let mut target = inner.desired.lock().expect("desired").clone();
        mutate(&mut target)?;
        let snapshot = inner.apply(target.clone(), None).await?;
        match write_extensions(&path, &target) {
            Ok(revision) => {
                *inner.revision.lock().expect("revision") = Some(revision);
                Ok(snapshot)
            }
            Err(error) => Err(LoaderControlError::Persist {
                snapshot: Box::new(snapshot),
                path,
                message: error.to_string(),
            }),
        }
    }
    fn inner(&self) -> Result<Arc<LoaderInner>, LoaderControlError> {
        if self.inner.alive.load(std::sync::atomic::Ordering::Acquire) {
            Ok(self.inner.clone())
        } else {
            Err(LoaderControlError::LoaderUnavailable)
        }
    }
}

fn source_path(inner: &LoaderInner) -> Result<PathBuf, LoaderControlError> {
    inner
        .extensions_path
        .lock()
        .expect("extensions path")
        .clone()
        .ok_or(LoaderError::NoConfigSource.into())
}
fn ensure_revision(inner: &LoaderInner, path: &PathBuf) -> Result<(), LoaderControlError> {
    let (_, current) = load_extensions_source(path)?;
    if inner.revision.lock().expect("revision").as_deref() == Some(current.as_str()) {
        Ok(())
    } else {
        Err(LoaderControlError::ConfigConflict { path: path.clone() })
    }
}
fn path_parts(path: &str) -> Result<Vec<&str>, LoaderControlError> {
    let parts = path
        .split(':')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if parts.is_empty() || parts.join(":") != path {
        Err(LoaderControlError::EntryNotFound { path: path.into() })
    } else {
        Ok(parts)
    }
}
fn join_path(parent: Option<&str>, id: &str) -> String {
    parent.map_or_else(|| id.into(), |parent| format!("{parent}:{id}"))
}

fn insert_entry(
    entries: &mut Vec<EntryOptions>,
    parent: Option<&str>,
    position: Option<usize>,
    entry: EntryOptions,
) -> Result<(), LoaderControlError> {
    match parent {
        None => insert_local(entries, position, entry, None),
        Some(parent) => insert_nested(entries, &path_parts(parent)?, parent, position, entry),
    }
}
fn insert_local(
    entries: &mut Vec<EntryOptions>,
    position: Option<usize>,
    entry: EntryOptions,
    parent: Option<&str>,
) -> Result<(), LoaderControlError> {
    if entries.iter().any(|item| item.id == entry.id) {
        return Err(LoaderControlError::EntryExists {
            path: join_path(parent, &entry.id),
        });
    }
    let index = position.unwrap_or(entries.len()).min(entries.len());
    entries.insert(index, entry);
    Ok(())
}
fn insert_nested(
    entries: &mut [EntryOptions],
    parts: &[&str],
    full_parent: &str,
    position: Option<usize>,
    entry: EntryOptions,
) -> Result<(), LoaderControlError> {
    let group = entries
        .iter_mut()
        .find(|item| item.id == parts[0])
        .ok_or_else(|| LoaderControlError::ParentNotGroup {
            path: full_parent.into(),
        })?;
    if !group.group {
        return Err(LoaderControlError::ParentNotGroup {
            path: full_parent.into(),
        });
    }
    let mut children = group.children()?;
    if parts.len() == 1 {
        insert_local(&mut children, position, entry, Some(full_parent))?;
    } else {
        insert_nested(&mut children, &parts[1..], full_parent, position, entry)?;
    }
    group.set_children(children)?;
    Ok(())
}
fn remove_entry(
    entries: &mut Vec<EntryOptions>,
    path: &str,
) -> Result<EntryOptions, LoaderControlError> {
    remove_nested(entries, &path_parts(path)?, path)
}
#[allow(clippy::ptr_arg)] // 递归终点必须从当前 Group 的 Vec 中移除条目。
fn remove_nested(
    entries: &mut Vec<EntryOptions>,
    parts: &[&str],
    full_path: &str,
) -> Result<EntryOptions, LoaderControlError> {
    let index = entries
        .iter()
        .position(|item| item.id == parts[0])
        .ok_or_else(|| LoaderControlError::EntryNotFound {
            path: full_path.into(),
        })?;
    if parts.len() == 1 {
        return Ok(entries.remove(index));
    }
    let group = &mut entries[index];
    if !group.group {
        return Err(LoaderControlError::EntryNotFound {
            path: full_path.into(),
        });
    }
    let mut children = group.children()?;
    let result = remove_nested(&mut children, &parts[1..], full_path)?;
    group.set_children(children)?;
    Ok(result)
}
fn patch_entry(
    entries: &mut [EntryOptions],
    path: &str,
    patch: EntryUpdate,
) -> Result<(), LoaderControlError> {
    patch_nested(entries, &path_parts(path)?, path, patch)
}
fn patch_nested(
    entries: &mut [EntryOptions],
    parts: &[&str],
    full_path: &str,
    patch: EntryUpdate,
) -> Result<(), LoaderControlError> {
    let entry = entries
        .iter_mut()
        .find(|item| item.id == parts[0])
        .ok_or_else(|| LoaderControlError::EntryNotFound {
            path: full_path.into(),
        })?;
    if parts.len() == 1 {
        if let Some(config) = patch.config {
            entry.config = config;
        }
        if let Some(disabled) = patch.disabled {
            entry.disabled = disabled;
        }
        if let Some(inject) = patch.inject {
            entry.inject = inject;
        }
        return Ok(());
    }
    if !entry.group {
        return Err(LoaderControlError::EntryNotFound {
            path: full_path.into(),
        });
    }
    let mut children = entry.children()?;
    patch_nested(&mut children, &parts[1..], full_path, patch)?;
    entry.set_children(children)?;
    Ok(())
}
