//! 启动时的配置组合。只生成声明，不访问 Loader、不挂载插件或写配置内容；读取可创建锁锚点。

use crate::{
    ConfigEditorConfig,
    patch_file::{PatchFileDocument, file_task, load_ordered_patches},
    user_patch::{UserPatchDocument, UserPatchStore},
};
use cordis_loader::{EntryOptions, LoaderControlError, SourceError, SourceErrorKind};
use cordis_plugin_include::{EntryPatch, IncludeConfig, include_root_entry};
use std::path::{Path, PathBuf};

/// 读取 deploy → user → home，生成包含完整 patches 的根 Include 声明。
///
/// Host 在 `loader.create()` 前调用；业务生命周期仅由 Include 初次加载执行。
/// 根 Entry ID 来自 `config.include_root`，必须是单段 ID，不是嵌套路径。
/// 不执行 Factory 校验或生命周期；文件内容、Factory 与来源树预检仍由 Loader/Include 负责。
pub async fn prepare_root_include(
    path: impl AsRef<Path>,
    config: &ConfigEditorConfig,
) -> Result<EntryOptions, LoaderControlError> {
    if config.include_root.is_empty() || config.include_root.contains(':') {
        return Err(layer_error("根 Include ID 必须是非空单段 ID".into()));
    }
    let path = path.as_ref().to_owned();
    let id = config.include_root.clone();
    // include_root_entry 的路径规范化也属于文件工作，不能在异步调度线程执行。
    let mut entry = file_task(move || Ok(include_root_entry(id, path)))
        .await
        .map_err(layer_error)?
        .map_err(|source| LoaderControlError::Source { source })?;
    let user = UserPatchStore::new(&config.user_patch_path)
        .load()
        .await
        .map_err(layer_error)?;
    let patches =
        read_layer_patches(&config.deploy_patch_paths, &user, &config.home_patch_path).await?;
    let mut include: IncludeConfig = entry
        .config
        .try_into()
        .map_err(|_| layer_error("无法解析生成的根 Include 配置".into()))?;
    include.patches = patches;
    entry.config = toml::Value::try_from(include)
        .map_err(|_| layer_error("无法序列化根 Include 配置".into()))?;
    Ok(entry)
}

/// 启动组合与运行期编辑共用同一层次顺序，避免维护两套 patch 算法。
pub(crate) async fn read_layer_patches(
    deploy: &[PathBuf],
    user: &UserPatchDocument,
    home: &Path,
) -> Result<Vec<EntryPatch>, LoaderControlError> {
    let mut patches = load_ordered_patches(deploy).await.map_err(layer_error)?;
    patches.extend(user.to_entry_patches());
    patches.extend(
        PatchFileDocument::load(home)
            .await
            .map_err(layer_error)?
            .patches,
    );
    Ok(patches)
}

pub(crate) fn layer_error(message: String) -> LoaderControlError {
    LoaderControlError::Source {
        source: SourceError::new(SourceErrorKind::InvalidData, message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PATCH_FILE_VERSION, PatchFileDocument};
    use cordis_loader::ExtensionsConfig;
    use cordis_plugin_include::apply_patches;

    #[tokio::test]
    async fn preparation_composes_layers_in_order_without_creating_the_root_file() {
        let dir = tempfile::tempdir().unwrap();
        let deploy_path = dir.path().join("deploy.toml");
        let user_path = dir.path().join("user.toml");
        let home_path = dir.path().join("home.toml");
        let root_path = dir.path().join("cordis.toml");
        let deploy = PatchFileDocument {
            version: PATCH_FILE_VERSION,
            patches: vec![EntryPatch {
                insert: Some(vec![
                    EntryOptions::new("demo", "demo").with_config(toml::toml! { value = 1 }.into()),
                ]),
                ..Default::default()
            }],
        };
        std::fs::write(&deploy_path, toml::to_string(&deploy).unwrap()).unwrap();
        let mut user = UserPatchDocument::empty();
        user.upsert_config("demo", "demo", Some(toml::toml! { value = 2 }.into()));
        UserPatchStore::new(&user_path).store(&user).await.unwrap();
        let home = PatchFileDocument {
            version: PATCH_FILE_VERSION,
            patches: vec![EntryPatch {
                id: Some("demo".into()),
                config: Some(toml::toml! { value = 3 }.into()),
                ..Default::default()
            }],
        };
        std::fs::write(&home_path, toml::to_string(&home).unwrap()).unwrap();
        let user_before = std::fs::read(&user_path).unwrap();
        let mut config = ConfigEditorConfig::new(vec![deploy_path.clone()], &user_path, &home_path);
        config.include_root = "project".into();
        let entry = prepare_root_include(&root_path, &config).await.unwrap();
        assert_eq!(entry.id, "project");
        let include: IncludeConfig = entry.config.try_into().unwrap();
        assert!(include.path.is_absolute());
        assert_eq!(include.patches.len(), 3);
        let tree = apply_patches(
            &ExtensionsConfig {
                version: 3,
                extensions: vec![],
            },
            &include.patches,
        )
        .unwrap();
        assert_eq!(tree.extensions[0].config["value"].as_integer(), Some(3));
        assert!(!root_path.exists());
        assert_eq!(std::fs::read(&user_path).unwrap(), user_before);
        assert_eq!(
            std::fs::read_to_string(&deploy_path).unwrap(),
            toml::to_string(&deploy).unwrap()
        );
    }

    #[tokio::test]
    async fn preparation_rejects_nested_root_ids_and_invalid_layer_documents() {
        let dir = tempfile::tempdir().unwrap();
        let deploy_path = dir.path().join("deploy.toml");
        std::fs::write(&deploy_path, "invalid = [").unwrap();
        let mut config = ConfigEditorConfig::new(
            vec![deploy_path],
            dir.path().join("user.toml"),
            dir.path().join("home.toml"),
        );
        let root_path = dir.path().join("cordis.toml");
        assert!(prepare_root_include(&root_path, &config).await.is_err());
        config.include_root = "parent:include".into();
        let error = prepare_root_include(&root_path, &config)
            .await
            .err()
            .unwrap();
        assert!(error.to_string().contains("单段 ID"));
        assert!(!root_path.exists());
        assert!(!config.user_patch_path.exists());
    }
}
