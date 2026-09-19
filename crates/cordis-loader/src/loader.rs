//! 面向插件管理界面的静态 Catalog 控制面。

use crate::{
    bootstrap::{load_extensions_source, write_extensions},
    catalog::FactoryDescriptor,
    config::{ExtensionEntry, ExtensionsConfig},
    error::LoaderError,
    runtime::{CordisLoader, LoaderInner},
    snapshot::LoaderSnapshot,
};
use cordis_core::ServiceKey;
use std::{path::PathBuf, sync::Weak};

/// 根 Context 中的 Loader 服务键。
pub static LOADER: ServiceKey<Loader> = ServiceKey::new("cordis.loader@1");

/// 单实例可编辑字段。Factory 身份不可通过更新修改。
#[derive(Debug, Clone)]
pub struct ExtensionUpdate {
    pub enabled: bool,
    pub config: toml::Value,
}

/// Loader 管理操作的失败。
#[derive(Debug, thiserror::Error)]
pub enum LoaderControlError {
    #[error(transparent)]
    Loader(#[from] LoaderError),
    #[error("Loader 已关闭，Loader 不再可用")]
    LoaderUnavailable,
    #[error("实例 {instance} 已存在")]
    InstanceExists { instance: String },
    #[error("实例 {instance} 不存在")]
    InstanceNotFound { instance: String },
    #[error("配置文件已在 Loader 外修改；请先 reload: {path}", path = path.display())]
    ConfigConflict { path: PathBuf },
    #[error("运行时已生效，但未能写回配置 {path}: {message}", path = path.display())]
    Persist {
        snapshot: LoaderSnapshot,
        path: PathBuf,
        message: String,
    },
}

/// 管理已编译进 Catalog 的插件实例，并将成功结果持久化到 `extensions.toml`。
#[derive(Clone)]
pub struct Loader {
    inner: Weak<LoaderInner>,
}

impl Loader {
    pub(crate) fn new(inner: Weak<LoaderInner>) -> Self {
        Self { inner }
    }

    /// 返回最近一次成功收敛的完整目标配置，包含已禁用实例。
    pub fn entries(&self) -> Result<ExtensionsConfig, LoaderControlError> {
        let inner = self.inner()?;
        Ok(inner.desired.lock().expect("desired").clone())
    }

    /// 返回静态 Catalog 中全部可创建 Factory 及其 JSON Schema。
    pub fn factories(&self) -> Result<Vec<FactoryDescriptor>, LoaderControlError> {
        let inner = self.inner()?;
        Ok(inner.catalog.factories()?)
    }

    /// 新建一个实例，并在成功挂载后写回完整配置。
    pub async fn create(
        &self,
        entry: ExtensionEntry,
    ) -> Result<LoaderSnapshot, LoaderControlError> {
        let instance = entry.instance.clone();
        self.mutate(move |config| {
            if config
                .extensions
                .iter()
                .any(|existing| existing.instance == instance)
            {
                return Err(LoaderControlError::InstanceExists { instance });
            }
            config.extensions.push(entry);
            Ok(())
        })
        .await
    }

    /// 更新实例启停状态与配置，不允许更换 Factory。
    pub async fn update(
        &self,
        instance: impl AsRef<str>,
        update: ExtensionUpdate,
    ) -> Result<LoaderSnapshot, LoaderControlError> {
        let instance = instance.as_ref().to_string();
        self.mutate(move |config| {
            let entry = config
                .extensions
                .iter_mut()
                .find(|entry| entry.instance == instance)
                .ok_or_else(|| LoaderControlError::InstanceNotFound {
                    instance: instance.clone(),
                })?;
            entry.enabled = update.enabled;
            entry.config = update.config;
            Ok(())
        })
        .await
    }

    /// 仅更新一个实例的启停状态。
    pub async fn set_enabled(
        &self,
        instance: impl AsRef<str>,
        enabled: bool,
    ) -> Result<LoaderSnapshot, LoaderControlError> {
        let instance = instance.as_ref().to_string();
        self.mutate(move |config| {
            let entry = config
                .extensions
                .iter_mut()
                .find(|entry| entry.instance == instance)
                .ok_or_else(|| LoaderControlError::InstanceNotFound {
                    instance: instance.clone(),
                })?;
            entry.enabled = enabled;
            Ok(())
        })
        .await
    }

    /// 删除一个实例。
    pub async fn remove(
        &self,
        instance: impl AsRef<str>,
    ) -> Result<LoaderSnapshot, LoaderControlError> {
        let instance = instance.as_ref().to_string();
        self.mutate(move |config| {
            let index = config
                .extensions
                .iter()
                .position(|entry| entry.instance == instance)
                .ok_or_else(|| LoaderControlError::InstanceNotFound {
                    instance: instance.clone(),
                })?;
            config.extensions.remove(index);
            Ok(())
        })
        .await
    }

    /// 从配置文件重新读取并收敛；此操作明确接受外部文件变更。
    pub async fn reload(&self) -> Result<LoaderSnapshot, LoaderControlError> {
        let inner = self.inner()?;
        let _operation = inner.loader_operations.lock().await;
        let path = source_path(&inner)?;
        let (config, revision) = load_extensions_source(&path)?;
        let host = CordisLoader {
            inner: inner.clone(),
        };
        Ok(host.apply_with_revision(config, Some(revision)).await?)
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
        let host = CordisLoader {
            inner: inner.clone(),
        };
        let snapshot = host.apply(target.clone()).await?;
        match write_extensions(&path, &target) {
            Ok(revision) => {
                *inner.revision.lock().expect("revision") = Some(revision);
                Ok(snapshot)
            }
            Err(error) => Err(LoaderControlError::Persist {
                snapshot,
                path,
                message: error.to_string(),
            }),
        }
    }

    fn inner(&self) -> Result<std::sync::Arc<LoaderInner>, LoaderControlError> {
        self.inner
            .upgrade()
            .ok_or(LoaderControlError::LoaderUnavailable)
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
    let expected = inner.revision.lock().expect("revision").clone();
    if expected.as_deref() == Some(current.as_str()) {
        Ok(())
    } else {
        Err(LoaderControlError::ConfigConflict { path: path.clone() })
    }
}
