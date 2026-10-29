//! 空根 ∘ deploy ∘ user ∘ home；运行时经根 Include.patches 全量同步。

use crate::{
    composition::{LayerPatches, layer_error as user_layer_error, read_layer_patches},
    patch_file::{compose_entries, load_ordered_patches},
    user_patch::{UserPatchDocument, UserPatchEdit, UserPatchStore},
};
use cordis_core::FiberState;
use cordis_loader::{
    ConfigProjectionError, ConfigRevision, ConfigTransaction, EntryId, EntryOptions, Loader,
    LoaderControlError, LoaderError, SourceError, SourceErrorKind,
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

/// 条目不可经 ConfigEditor user 层编辑的原因（启停与表单共用）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigReadOnlyReason {
    /// Group 仅组织树，不接受配置或启停写入。
    Group,
    /// 嵌套 Include 载体；由树 CRUD / 重载路径处理。
    NestedInclude,
    /// 不属于本编辑器拥有的 deploy 条目（含嵌套来源内业务条目、根 Include）。
    UnmanagedEntry,
}

/// 路径是否允许经 ConfigEditor `edit` / `set_enabled` 写入。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigEditability {
    Editable,
    ReadOnly(ConfigReadOnlyReason),
}

/// 根 Include 下一条由 ConfigEditor 管理的 Entry（含编辑能力与运行快照字段）。
#[derive(Clone, Debug)]
pub struct ManagedEntry {
    pub path: String,
    pub factory: String,
    pub group: bool,
    pub enabled: bool,
    pub state: Option<FiberState>,
    pub last_error: Option<String>,
    pub editability: ConfigEditability,
}

/// 分层配置快照；各层可含明文秘密。
#[derive(Clone, Debug)]
pub struct LayeredEntryConfig {
    pub path: EntryId,
    pub factory: String,
    pub schema: serde_json::Value,
    pub resolved_schema: serde_json::Value,
    /// 去除 user 后的继承声明（deploy ∘ home）**raw TOML**；Unset 恢复源。
    pub base: toml::Value,
    /// 对 `base` 的类型投影（含 Serde 默认）；输入无法独立解析时为 `None`。
    /// 投影执行故障仍明确返回错误，不与不完整的继承输入混淆。
    pub inherited: Option<serde_json::Value>,
    /// user patch 中的整表覆盖；无覆盖时为 `None`。
    pub user: Option<toml::Value>,
    /// deploy ∘ user ∘ home。
    pub effective: toml::Value,
    /// Active 实例实际接收的类型解析快照（含 Serde 默认值），并非重新解析声明。
    pub resolved: Option<serde_json::Value>,
    /// 与分层快照同一事务算出的可编辑性。
    pub editability: ConfigEditability,
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
        Ok(matches!(
            self.editability(full_path).await?,
            ConfigEditability::Editable
        ))
    }

    pub async fn editability(
        &self,
        full_path: &str,
    ) -> Result<ConfigEditability, LoaderControlError> {
        if full_path == self.include_root
            || !full_path.starts_with(&format!("{}:", self.include_root))
        {
            return Ok(ConfigEditability::ReadOnly(
                ConfigReadOnlyReason::UnmanagedEntry,
            ));
        }
        // 能力属于条目元数据，不能依赖配置快照：Group 没有可读取的 Factory Schema。
        self.managed_entries()
            .await?
            .into_iter()
            .find(|entry| entry.path == full_path)
            .map(|entry| entry.editability)
            .ok_or_else(|| LoaderControlError::EntryNotFound {
                path: full_path.into(),
            })
    }

    /// 根 Include 下全部托管条目；同事务一次 compose deploy，批量算 editability。
    pub async fn managed_entries(&self) -> Result<Vec<ManagedEntry>, LoaderControlError> {
        let engine = self.clone();
        self.loader
            .config_transaction(move |tx| {
                Box::pin(async move {
                    let snapshot = tx.await_idle().await?;
                    let deploy = engine.compose_deploy().await?;
                    let ownership = ownership_index(&deploy.extensions)?;
                    let prefix = format!("{}:", engine.include_root);
                    let mut out = Vec::new();
                    for entry in &snapshot.entries {
                        let path = entry.path.as_str();
                        if path == engine.include_root || !path.starts_with(&prefix) {
                            continue;
                        }
                        let local = path
                            .strip_prefix(&prefix)
                            .expect("prefix checked")
                            .to_owned();
                        let editability =
                            classify_editability(entry.group, &entry.name, ownership.get(&local));
                        out.push(ManagedEntry {
                            path: path.to_owned(),
                            factory: entry.name.clone(),
                            group: entry.group,
                            enabled: entry.enabled,
                            state: entry.state,
                            last_error: entry.last_error.clone(),
                            editability,
                        });
                    }
                    Ok(out)
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
        let edit = self.store.begin_edit().await.map_err(user_layer_error)?;
        self.layers_with_edit(tx, &edit, full_path).await
    }

    async fn layers_with_edit(
        &self,
        tx: &ConfigTransaction,
        edit: &UserPatchEdit,
        full_path: &str,
    ) -> Result<LayeredEntryConfig, LoaderControlError> {
        let uneditable = ConfigEditability::ReadOnly(ConfigReadOnlyReason::UnmanagedEntry);

        if full_path == self.include_root
            || !full_path.starts_with(&format!("{}:", self.include_root))
        {
            let snapshot = tx.snapshot_entry_config(full_path).await?;
            return Ok(LayeredEntryConfig {
                path: snapshot.path,
                factory: snapshot.factory,
                schema: snapshot.schema,
                resolved_schema: snapshot.resolved_schema,
                base: snapshot.config.clone(),
                inherited: None,
                user: None,
                effective: snapshot.config,
                resolved: snapshot.resolved,
                editability: uneditable,
                revision: snapshot.revision,
            });
        }

        let local = self.local_id(full_path)?;
        let snapshot = tx.snapshot_entry_config(full_path).await?;
        let document = edit.load().await.map_err(user_layer_error)?;
        let captured = LayerPatches::read(&self.deploy_patch_paths, &self.home_patch_path).await?;
        let patches = captured.with_user(&document);
        // 在使用磁盘配置之前检查所属 Include 已加载声明；后续只使用 captured，
        // 禁止在校验后再次读取 deploy/home 并套用旧 Factory 的 Schema。
        self.ensure_captured_layers_current(tx, &patches).await?;
        let deploy = compose_entries(&captured.deploy).map_err(user_layer_error)?;
        let ownership = ownership_index(&deploy.extensions)?;
        let editability = classify_editability(
            ownership.get(&local).is_some_and(|o| o.group),
            &snapshot.factory,
            ownership.get(&local),
        );

        let user = document.config_for(&local).cloned();
        let inherited_tree = compose_entries(&captured.with_user(&UserPatchDocument::empty()))
            .map_err(user_layer_error)?;
        let inherited_entry = match find_entry(&inherited_tree.extensions, &local) {
            Some(entry) => entry,
            None => {
                return Ok(LayeredEntryConfig {
                    path: snapshot.path,
                    factory: snapshot.factory,
                    schema: snapshot.schema,
                    resolved_schema: snapshot.resolved_schema,
                    base: snapshot.config.clone(),
                    inherited: None,
                    user,
                    effective: snapshot.config,
                    resolved: snapshot.resolved,
                    editability,
                    revision: snapshot.revision,
                });
            }
        };
        let effective_entry = compose_entries(&patches)
            .map_err(user_layer_error)
            .and_then(|tree| {
                find_entry(&tree.extensions, &local).ok_or_else(|| {
                    LoaderControlError::EntryNotFound {
                        path: format!("{}:{local}", self.include_root),
                    }
                })
            })?;
        if inherited_entry.name != snapshot.factory || effective_entry.name != snapshot.factory {
            return Err(self.layer_conflict());
        }
        let base = inherited_entry.config.clone();
        let inherited = match self.loader.project_entry_config(&snapshot.factory, &base) {
            Ok(value) => Some(value),
            Err(ConfigProjectionError::InputInvalid) => None,
            Err(ConfigProjectionError::Loader(error)) => return Err(error.into()),
            Err(ConfigProjectionError::LoaderUnavailable) => {
                return Err(LoaderControlError::LoaderUnavailable);
            }
        };
        Ok(LayeredEntryConfig {
            path: snapshot.path,
            factory: snapshot.factory,
            schema: snapshot.schema,
            resolved_schema: snapshot.resolved_schema,
            base,
            inherited,
            user,
            effective: effective_entry.config,
            resolved: snapshot.resolved,
            editability,
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
                    let edit = engine.store.begin_edit().await.map_err(user_layer_error)?;
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
                    let previous = edit.load().await.map_err(user_layer_error)?;
                    engine.ensure_layers_current(tx, &previous).await?;
                    let mut document = previous.clone();
                    document.upsert_disabled(&local, &factory, override_disabled);
                    let composed = engine.compose_full(&document).await?;
                    if find_entry(&composed.extensions, &local)
                        .is_none_or(|entry| entry.disabled != next_disabled)
                    {
                        return Err(LoaderControlError::ConfigNotEditable);
                    }
                    edit.store(&document).await.map_err(user_layer_error)?;
                    if let Err(error) = engine
                        .apply_include_patches(tx, &document, enabled.then_some(full_path.as_str()))
                        .await
                    {
                        return Err(engine
                            .restore_user_after_failure(&edit, &previous, error)
                            .await);
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
                    let edit = engine.store.begin_edit().await.map_err(user_layer_error)?;
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
                    let previous = edit.load().await.map_err(user_layer_error)?;
                    engine.ensure_layers_current(tx, &previous).await?;
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
                    edit.store(&document).await.map_err(user_layer_error)?;
                    if let Err(error) = engine.apply_include_patches(tx, &document, None).await {
                        return Err(engine
                            .restore_user_after_failure(&edit, &previous, error)
                            .await);
                    }
                    engine.layers_with_edit(tx, &edit, &full_path).await
                })
            })
            .await
    }

    async fn runtime_patches(
        &self,
        user: &UserPatchDocument,
    ) -> Result<Vec<EntryPatch>, LoaderControlError> {
        read_layer_patches(&self.deploy_patch_paths, user, &self.home_patch_path).await
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
        required: Option<&str>,
    ) -> Result<(), LoaderControlError> {
        let (mut include, revision) = self.include_config(tx).await?;
        let patches = self.runtime_patches(document).await?;
        // 即便声明未变，之前失败的来源绑定仍可能需要协调；交给 Loader 判断。
        include.patches = patches;
        let config = toml::Value::try_from(&include)
            .map_err(|_| user_layer_error("无法序列化 Include 配置以同步 patches".into()))?;
        match required {
            Some(required) => {
                tx.commit_entry_config_requiring(&self.include_root, revision, config, required)
                    .await
            }
            None => {
                tx.commit_entry_config(&self.include_root, revision, config)
                    .await
            }
        }
        .map_err(map_form_edit_error)?;
        tx.await_idle().await.map_err(map_form_edit_error)?;
        Ok(())
    }

    async fn restore_user_after_failure(
        &self,
        edit: &UserPatchEdit,
        previous: &UserPatchDocument,
        error: LoaderControlError,
    ) -> LoaderControlError {
        match edit.store(previous).await {
            Ok(()) => map_form_edit_error(error),
            Err(restore_error) => LoaderControlError::Source {
                source: SourceError::new(
                    SourceErrorKind::Io,
                    format!("配置应用失败：{error}；恢复 user 配置也失败：{restore_error}"),
                ),
            },
        }
    }

    async fn ensure_layers_current(
        &self,
        tx: &ConfigTransaction,
        user: &UserPatchDocument,
    ) -> Result<(), LoaderControlError> {
        self.ensure_captured_layers_current(tx, &self.runtime_patches(user).await?)
            .await
    }

    async fn ensure_captured_layers_current(
        &self,
        tx: &ConfigTransaction,
        patches: &[EntryPatch],
    ) -> Result<(), LoaderControlError> {
        if self.include_config(tx).await?.0.patches != patches {
            return Err(self.layer_conflict());
        }
        Ok(())
    }

    fn layer_conflict(&self) -> LoaderControlError {
        LoaderControlError::ConfigConflict {
            source_id: self.store.path().display().to_string(),
        }
    }

    pub async fn reload(&self) -> Result<(), LoaderControlError> {
        let engine = self.clone();
        self.loader
            .config_transaction(move |tx| {
                Box::pin(async move {
                    let edit = engine.store.begin_edit().await.map_err(user_layer_error)?;
                    let document = edit.load().await.map_err(user_layer_error)?;
                    engine.apply_include_patches(tx, &document, None).await
                })
            })
            .await
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

struct DeployOwner {
    name: String,
    group: bool,
}

fn ownership_index(
    entries: &[EntryOptions],
) -> Result<std::collections::HashMap<String, DeployOwner>, LoaderControlError> {
    let mut map = std::collections::HashMap::new();
    fn walk(
        prefix: &str,
        entries: &[EntryOptions],
        map: &mut std::collections::HashMap<String, DeployOwner>,
    ) -> Result<(), LoaderControlError> {
        for entry in entries {
            let path = if prefix.is_empty() {
                entry.id.clone()
            } else {
                format!("{prefix}:{}", entry.id)
            };
            map.insert(
                path.clone(),
                DeployOwner {
                    name: entry.name.clone(),
                    group: entry.group,
                },
            );
            if entry.group {
                walk(&path, &entry.children()?, map)?;
            }
        }
        Ok(())
    }
    walk("", entries, &mut map)?;
    Ok(map)
}

fn classify_editability(
    group: bool,
    factory: &str,
    owned: Option<&DeployOwner>,
) -> ConfigEditability {
    if group || owned.is_some_and(|owner| owner.group) {
        return ConfigEditability::ReadOnly(ConfigReadOnlyReason::Group);
    }
    if factory == "cordis:include" {
        return ConfigEditability::ReadOnly(ConfigReadOnlyReason::NestedInclude);
    }
    match owned {
        Some(owner) if owner.name == factory => ConfigEditability::Editable,
        _ => ConfigEditability::ReadOnly(ConfigReadOnlyReason::UnmanagedEntry),
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
