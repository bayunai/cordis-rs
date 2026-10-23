//! 空根 ∘ deploy ∘ user ∘ home；运行时经根 Include.patches 全量同步。

use crate::{
    patch_file::{compose_entries, load_ordered_patches},
    user_patch::{UserPatchDocument, UserPatchStore},
};
use cordis_loader::{
    ConfigRevision, EntryId, EntryOptions, EntryUpdate, Loader, LoaderControlError, LoaderError,
    SourceError, SourceErrorKind,
};
use cordis_plugin_include::{EntryPatch, IncludeConfig};
use std::path::PathBuf;
use std::sync::Arc;

/// Host 注入的编辑器配置。
#[derive(Clone, Debug)]
pub struct ConfigEditorConfig {
    /// 有序 deploy patch 文件（各插件 `cordis.patch.toml`）。
    pub deploy_patch_paths: Vec<PathBuf>,
    /// 本机 user patch 文档绝对路径。
    pub user_patch_path: PathBuf,
    /// home patch 文档绝对路径（只读叠加）。
    pub home_patch_path: PathBuf,
    /// 根 Include 条目完整路径，默认 `include`。
    pub include_root: String,
    /// 解析相对 Include `path` 的目录（通常为 `cordis.toml` 所在目录）。
    pub config_dir: PathBuf,
}

impl ConfigEditorConfig {
    pub fn new(
        deploy_patch_paths: Vec<PathBuf>,
        user_patch_path: impl Into<PathBuf>,
        home_patch_path: impl Into<PathBuf>,
        config_dir: impl Into<PathBuf>,
    ) -> Self {
        Self {
            deploy_patch_paths,
            user_patch_path: user_patch_path.into(),
            home_patch_path: home_patch_path.into(),
            include_root: "include".into(),
            config_dir: config_dir.into(),
        }
    }
}

/// 分层配置快照；各层可含明文秘密。
#[derive(Clone, Debug)]
pub struct LayeredEntryConfig {
    pub path: EntryId,
    pub factory: String,
    pub schema: serde_json::Value,
    /// 仅 deploy 组成后的声明。
    pub base: toml::Value,
    /// user patch 中的整表覆盖；无覆盖时为 `None`。
    pub user: Option<toml::Value>,
    /// deploy ∘ user ∘ home。
    pub effective: toml::Value,
    pub revision: ConfigRevision,
}

#[derive(Clone)]
pub(crate) struct LayerEngine {
    pub loader: Loader,
    pub store: Arc<UserPatchStore>,
    /// 串行化表单编辑；revision 检查与写 user/reconcile 在同一把锁内。
    pub edit_lock: Arc<tokio::sync::Mutex<()>>,
    pub include_root: String,
    pub deploy_patch_paths: Vec<PathBuf>,
    pub home_patch_path: PathBuf,
}

impl LayerEngine {
    pub async fn sync_patches(&self) -> Result<(), LoaderControlError> {
        let document = self.store.load().await.map_err(user_layer_error)?;
        self.apply_include_patches(&document).await
    }

    pub async fn layers(
        &self,
        full_path: &str,
    ) -> Result<LayeredEntryConfig, LoaderControlError> {
        let snapshot = self.loader.snapshot_entry_config(full_path).await?;
        // 根 Include 载体本身：无产品 user 层，仅投影当前声明。
        if full_path == self.include_root {
            return Ok(LayeredEntryConfig {
                path: snapshot.path,
                factory: snapshot.factory,
                schema: snapshot.schema,
                base: snapshot.config.clone(),
                user: None,
                effective: snapshot.config,
                revision: snapshot.revision,
            });
        }
        let local = self.local_id(full_path)?;
        let document = self.store.load().await.map_err(user_layer_error)?;
        let user = document.config_for(&local).cloned();
        let base_entry = match self.read_deploy_entry(&local) {
            Ok(entry) => entry,
            Err(LoaderControlError::EntryNotFound { .. }) => {
                // 嵌套 Include 等：首版不提供独立 deploy base。
                return Ok(LayeredEntryConfig {
                    path: snapshot.path,
                    factory: snapshot.factory,
                    schema: snapshot.schema,
                    base: snapshot.config.clone(),
                    user,
                    effective: snapshot.config,
                    revision: snapshot.revision,
                });
            }
            Err(error) => return Err(error),
        };
        let effective_entry = self
            .compose_full(&document)
            .and_then(|tree| {
                find_entry(&tree.extensions, &local).ok_or_else(|| {
                    LoaderControlError::EntryNotFound {
                        path: format!("{}:{local}", self.include_root),
                    }
                })
            })?;
        Ok(LayeredEntryConfig {
            path: snapshot.path,
            factory: snapshot.factory,
            schema: snapshot.schema,
            base: base_entry.config,
            user,
            effective: effective_entry.config,
            revision: snapshot.revision,
        })
    }

    /// 更新 user 层启停；与 deploy base 相同时去掉覆盖。不写空根或 deploy 文件。
    pub async fn set_enabled(
        &self,
        full_path: &str,
        enabled: bool,
    ) -> Result<(), LoaderControlError> {
        if full_path == self.include_root {
            return Err(LoaderControlError::ConfigNotEditable);
        }
        let local = self.local_id(full_path)?;
        let base_entry = self.read_deploy_entry(&local).map_err(|error| match error {
            LoaderControlError::EntryNotFound { .. } => LoaderControlError::ConfigNotEditable,
            other => other,
        })?;
        let engine = self.clone();
        let full_path = full_path.to_owned();
        tokio::spawn(async move {
            let _edit = engine.edit_lock.lock().await;
            let current = engine.loader.snapshot_entry_config(&full_path).await?;
            let factory = current.factory.clone();
            let next_disabled = !enabled;
            let override_disabled = if next_disabled == base_entry.disabled {
                None
            } else {
                Some(next_disabled)
            };
            let previous = engine.store.load().await.map_err(user_layer_error)?;
            let mut document = previous.clone();
            document.upsert_disabled(&local, &factory, override_disabled);
            engine
                .store
                .store(&document)
                .await
                .map_err(user_layer_error)?;
            if let Err(error) = engine.apply_include_patches(&document).await {
                let _ = engine.store.store(&previous).await;
                return Err(map_form_edit_error(error));
            }
            Ok(())
        })
        .await
        .map_err(|_| LoaderControlError::ConfigOperationAborted)?
    }

    pub async fn edit_user(
        &self,
        full_path: &str,
        expected: ConfigRevision,
        next_effective: toml::Value,
    ) -> Result<LayeredEntryConfig, LoaderControlError> {
        if full_path == self.include_root {
            return Err(LoaderControlError::ConfigNotEditable);
        }
        let local = self.local_id(full_path)?;
        let base_entry = self.read_deploy_entry(&local).map_err(|error| match error {
            LoaderControlError::EntryNotFound { .. } => LoaderControlError::ConfigNotEditable,
            other => other,
        })?;
        let engine = self.clone();
        let full_path = full_path.to_owned();
        tokio::spawn(async move {
            let _edit = engine.edit_lock.lock().await;
            let current = engine.loader.snapshot_entry_config(&full_path).await?;
            if expected != current.revision {
                return Err(LoaderControlError::ConfigRevisionConflict);
            }
            let factory = current.factory.clone();
            let needs_explicit_prepare = engine
                .loader
                .await_idle()
                .await?
                .entry(&full_path)
                .map(|entry| entry.state != Some(cordis_core::FiberState::Active))
                .unwrap_or(true);
            if needs_explicit_prepare {
                engine
                    .loader
                    .prepare_entry_config(&factory, &next_effective)?;
            }
            let previous = engine.store.load().await.map_err(user_layer_error)?;
            let mut document = previous.clone();
            if next_effective == base_entry.config {
                document.upsert_config(&local, &factory, None);
            } else {
                document.upsert_config(&local, &factory, Some(next_effective.clone()));
            }
            // 更高层 home 盖住则拒绝写入（对齐 DSH）。
            let composed = engine.compose_full(&document)?;
            let after = find_entry(&composed.extensions, &local).ok_or_else(|| {
                LoaderControlError::EntryNotFound {
                    path: format!("{}:{local}", engine.include_root),
                }
            })?;
            if after.config != next_effective {
                return Err(LoaderControlError::ConfigNotEditable);
            }
            engine
                .store
                .store(&document)
                .await
                .map_err(user_layer_error)?;
            if let Err(error) = engine.apply_include_patches(&document).await {
                let _ = engine.store.store(&previous).await;
                return Err(map_form_edit_error(error));
            }
            engine.layers(&full_path).await
        })
        .await
        .map_err(|_| LoaderControlError::ConfigOperationAborted)?
    }

    fn runtime_patches(
        &self,
        user: &UserPatchDocument,
    ) -> Result<Vec<EntryPatch>, LoaderControlError> {
        let mut patches =
            load_ordered_patches(&self.deploy_patch_paths).map_err(user_layer_error)?;
        patches.extend(user.to_entry_patches());
        patches.extend(
            crate::patch_file::PatchFileDocument::load(&self.home_patch_path)
                .map_err(user_layer_error)?
                .patches,
        );
        Ok(patches)
    }

    fn compose_full(
        &self,
        user: &UserPatchDocument,
    ) -> Result<cordis_loader::ExtensionsConfig, LoaderControlError> {
        compose_entries(&self.runtime_patches(user)?).map_err(user_layer_error)
    }

    fn compose_deploy(&self) -> Result<cordis_loader::ExtensionsConfig, LoaderControlError> {
        let patches =
            load_ordered_patches(&self.deploy_patch_paths).map_err(user_layer_error)?;
        compose_entries(&patches).map_err(user_layer_error)
    }

    async fn apply_include_patches(
        &self,
        document: &UserPatchDocument,
    ) -> Result<(), LoaderControlError> {
        let mut include = self.include_config()?;
        let patches = self.runtime_patches(document)?;
        if include.patches == patches {
            return Ok(());
        }
        include.patches = patches;
        let config = toml::Value::try_from(&include).map_err(|_| {
            user_layer_error("无法序列化 Include 配置以同步 patches".into())
        })?;
        match self
            .loader
            .update(
                &self.include_root,
                EntryUpdate {
                    config: Some(config),
                    ..Default::default()
                },
                None,
                None,
            )
            .await
        {
            Ok(_) => {}
            // 个别 Entry Failed 不回滚 Include.patches；编辑器仍须保持可用。
            Err(error) if error.is_entry_lifecycle_failure() => {}
            Err(error) => return Err(map_form_edit_error(error)),
        }
        match self.loader.await_idle().await {
            Ok(_) => {}
            Err(error) if error.is_entry_lifecycle_failure() => {}
            Err(error) => return Err(map_form_edit_error(error)),
        }
        Ok(())
    }

    fn include_config(&self) -> Result<IncludeConfig, LoaderControlError> {
        let roots = self.loader.entries()?;
        let entry = find_entry(&roots.extensions, &self.include_root).ok_or_else(|| {
            LoaderControlError::EntryNotFound {
                path: self.include_root.clone(),
            }
        })?;
        entry.config.clone().try_into().map_err(|_| {
            user_layer_error(format!(
                "根条目 {} 不是合法的 Include 配置",
                self.include_root
            ))
        })
    }

    fn read_deploy_entry(&self, local: &str) -> Result<EntryOptions, LoaderControlError> {
        let tree = self.compose_deploy()?;
        find_entry(&tree.extensions, local).ok_or_else(|| LoaderControlError::EntryNotFound {
            path: format!("{}:{local}", self.include_root),
        })
    }

    fn local_id(&self, full_path: &str) -> Result<String, LoaderControlError> {
        let prefix = format!("{}:", self.include_root);
        full_path
            .strip_prefix(&prefix)
            .map(str::to_owned)
            .ok_or_else(|| LoaderControlError::EntryNotFound {
                path: full_path.into(),
            })
    }
}

fn find_entry(entries: &[EntryOptions], path: &str) -> Option<EntryOptions> {
    let (head, tail) = path
        .split_once(':')
        .map_or((path, None), |(a, b)| (a, Some(b)));
    if head.is_empty() {
        return None;
    }
    let entry = entries.iter().find(|entry| entry.id == head)?;
    if let Some(tail) = tail {
        if !entry.group {
            return None;
        }
        let children = entry.children().ok()?;
        find_entry(&children, tail)
    } else {
        Some(entry.clone())
    }
}

fn user_layer_error(message: String) -> LoaderControlError {
    LoaderControlError::Source {
        source: SourceError::new(SourceErrorKind::InvalidData, message),
    }
}

fn map_form_edit_error(error: LoaderControlError) -> LoaderControlError {
    match error {
        LoaderControlError::Loader(
            LoaderError::InvalidConfig { .. }
            | LoaderError::FactoryPanic { .. }
            | LoaderError::PluginBuild { .. }
            | LoaderError::PluginPanic { .. },
        ) => LoaderControlError::ConfigValidation,
        LoaderControlError::Loader(LoaderError::Source { source })
            if source.kind == SourceErrorKind::Conflict =>
        {
            LoaderControlError::ConfigConflict {
                source_id: source.to_string(),
            }
        }
        LoaderControlError::Source { source } if source.kind == SourceErrorKind::Conflict => {
            LoaderControlError::ConfigConflict {
                source_id: source.to_string(),
            }
        }
        other => other,
    }
}
