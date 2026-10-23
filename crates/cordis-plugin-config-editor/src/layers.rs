//! 空根 ∘ deploy ∘ user ∘ home；运行时经根 Include.patches 全量同步。

use crate::{
    patch_file::{compose_entries, load_ordered_patches},
    user_patch::{UserPatchDocument, UserPatchStore},
};
use cordis_loader::{
    ConfigRevision, ConfigTransaction, EntryId, EntryOptions, Loader, LoaderControlError,
    LoaderError, SourceError, SourceErrorKind,
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
}

impl ConfigEditorConfig {
    pub fn new(
        deploy_patch_paths: Vec<PathBuf>,
        user_patch_path: impl Into<PathBuf>,
        home_patch_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            deploy_patch_paths,
            user_patch_path: user_patch_path.into(),
            home_patch_path: home_patch_path.into(),
            include_root: "include".into(),
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
    pub include_root: String,
    pub deploy_patch_paths: Vec<PathBuf>,
    pub home_patch_path: PathBuf,
}

impl LayerEngine {
    pub async fn can_edit(&self, full_path: &str) -> Result<bool, LoaderControlError> {
        if full_path == self.include_root
            || !full_path.starts_with(&format!("{}:", self.include_root))
        {
            return Ok(false);
        }
        let engine = self.clone();
        let path = full_path.to_owned();
        self.loader
            .config_transaction(move |tx| {
                Box::pin(async move {
                    let current = match tx.snapshot_entry_config(&path).await {
                        Ok(current) => current,
                        Err(LoaderControlError::ConfigNotEditable) => return Ok(false),
                        Err(error) => return Err(error),
                    };
                    let local = engine.local_id(&path)?;
                    match engine.read_deploy_entry(&local).await {
                        Ok(entry) => Ok(!entry.group && entry.name == current.factory),
                        Err(LoaderControlError::EntryNotFound { .. }) => Ok(false),
                        Err(error) => Err(error),
                    }
                })
            })
            .await
    }

    pub async fn sync_patches(&self) -> Result<(), LoaderControlError> {
        let engine = self.clone();
        self.loader
            .config_transaction(move |tx| {
                Box::pin(async move {
                    let document = engine.store.load().await.map_err(user_layer_error)?;
                    // 启动时不主动重试 Include 已加载的失败声明。
                    if engine.include_config(tx).await?.0.patches
                        == engine.runtime_patches(&document).await?
                    {
                        return Ok(());
                    }
                    engine.apply_include_patches(tx, &document).await
                })
            })
            .await
    }

    pub async fn layers(&self, full_path: &str) -> Result<LayeredEntryConfig, LoaderControlError> {
        let engine = self.clone();
        let path = full_path.to_owned();
        self.loader
            .config_transaction(move |tx| {
                Box::pin(async move { engine.layers_in_transaction(tx, &path).await })
            })
            .await
    }

    async fn layers_in_transaction(
        &self,
        tx: &ConfigTransaction,
        full_path: &str,
    ) -> Result<LayeredEntryConfig, LoaderControlError> {
        let snapshot = tx.snapshot_entry_config(full_path).await?;
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
        let base_entry = match self.read_deploy_entry(&local).await {
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
        let effective_entry = self.compose_full(&document).await.and_then(|tree| {
            find_entry(&tree.extensions, &local).ok_or_else(|| LoaderControlError::EntryNotFound {
                path: format!("{}:{local}", self.include_root),
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
        let engine = self.clone();
        let full_path = full_path.to_owned();
        self.loader
            .config_transaction(move |tx| {
                Box::pin(async move {
                    let base_entry = engine
                        .read_deploy_entry(&local)
                        .await
                        .map_err(editability_error)?;
                    let current = tx.snapshot_entry_config(&full_path).await?;
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
                    let composed = engine.compose_full(&document).await?;
                    if find_entry(&composed.extensions, &local)
                        .is_none_or(|entry| entry.disabled != next_disabled)
                    {
                        return Err(LoaderControlError::ConfigNotEditable);
                    }
                    engine
                        .store
                        .store(&document)
                        .await
                        .map_err(user_layer_error)?;
                    if let Err(error) = engine.apply_include_patches(tx, &document).await {
                        return Err(engine.restore_user_after_failure(&previous, error).await);
                    }
                    Ok(())
                })
            })
            .await
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
        let engine = self.clone();
        let full_path = full_path.to_owned();
        self.loader
            .config_transaction(move |tx| {
                Box::pin(async move {
                    let base_entry = engine
                        .read_deploy_entry(&local)
                        .await
                        .map_err(editability_error)?;
                    let current = tx.snapshot_entry_config(&full_path).await?;
                    if expected != current.revision {
                        return Err(LoaderControlError::ConfigRevisionConflict);
                    }
                    let factory = current.factory.clone();
                    let needs_explicit_prepare = tx
                        .await_idle()
                        .await?
                        .entry(&full_path)
                        .map(|entry| entry.state != Some(cordis_core::FiberState::Active))
                        .unwrap_or(true);
                    if needs_explicit_prepare {
                        tx.prepare_entry_config(&factory, &next_effective)?;
                    }
                    let previous = engine.store.load().await.map_err(user_layer_error)?;
                    let mut document = previous.clone();
                    if next_effective == base_entry.config {
                        document.upsert_config(&local, &factory, None);
                    } else {
                        document.upsert_config(&local, &factory, Some(next_effective.clone()));
                    }
                    // 更高层 home 盖住则拒绝写入（对齐 DSH）。
                    let composed = engine.compose_full(&document).await?;
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
                    if let Err(error) = engine.apply_include_patches(tx, &document).await {
                        return Err(engine.restore_user_after_failure(&previous, error).await);
                    }
                    engine.layers_in_transaction(tx, &full_path).await
                })
            })
            .await
    }

    async fn runtime_patches(
        &self,
        user: &UserPatchDocument,
    ) -> Result<Vec<EntryPatch>, LoaderControlError> {
        let mut patches = load_ordered_patches(&self.deploy_patch_paths)
            .await
            .map_err(user_layer_error)?;
        patches.extend(user.to_entry_patches());
        patches.extend(
            crate::patch_file::PatchFileDocument::load(&self.home_patch_path)
                .await
                .map_err(user_layer_error)?
                .patches,
        );
        Ok(patches)
    }

    async fn compose_full(
        &self,
        user: &UserPatchDocument,
    ) -> Result<cordis_loader::ExtensionsConfig, LoaderControlError> {
        compose_entries(&self.runtime_patches(user).await?).map_err(user_layer_error)
    }

    async fn compose_deploy(&self) -> Result<cordis_loader::ExtensionsConfig, LoaderControlError> {
        let patches = load_ordered_patches(&self.deploy_patch_paths)
            .await
            .map_err(user_layer_error)?;
        compose_entries(&patches).map_err(user_layer_error)
    }

    async fn apply_include_patches(
        &self,
        tx: &ConfigTransaction,
        document: &UserPatchDocument,
    ) -> Result<(), LoaderControlError> {
        let (mut include, revision) = self.include_config(tx).await?;
        let patches = self.runtime_patches(document).await?;
        // 即便声明未变，之前失败的来源绑定仍可能需要协调；交给 Loader 判断。
        include.patches = patches;
        let config = toml::Value::try_from(&include)
            .map_err(|_| user_layer_error("无法序列化 Include 配置以同步 patches".into()))?;
        tx.commit_entry_config(&self.include_root, revision, config)
            .await
            .map_err(map_form_edit_error)?;
        tx.await_idle().await.map_err(map_form_edit_error)?;
        Ok(())
    }

    async fn restore_user_after_failure(
        &self,
        previous: &UserPatchDocument,
        error: LoaderControlError,
    ) -> LoaderControlError {
        match self.store.store(previous).await {
            Ok(()) => map_form_edit_error(error),
            Err(restore_error) => LoaderControlError::Source {
                source: SourceError::new(
                    SourceErrorKind::Io,
                    format!("配置应用失败：{error}；恢复 user 配置也失败：{restore_error}"),
                ),
            },
        }
    }

    async fn include_config(
        &self,
        tx: &ConfigTransaction,
    ) -> Result<(IncludeConfig, ConfigRevision), LoaderControlError> {
        let snapshot = tx.snapshot_entry_config(&self.include_root).await?;
        let config = snapshot.config.try_into().map_err(|_| {
            user_layer_error(format!(
                "根条目 {} 不是合法的 Include 配置",
                self.include_root
            ))
        })?;
        Ok((config, snapshot.revision))
    }

    async fn read_deploy_entry(&self, local: &str) -> Result<EntryOptions, LoaderControlError> {
        let tree = self.compose_deploy().await?;
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

fn editability_error(error: LoaderControlError) -> LoaderControlError {
    match error {
        LoaderControlError::EntryNotFound { .. } => LoaderControlError::ConfigNotEditable,
        other => other,
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
