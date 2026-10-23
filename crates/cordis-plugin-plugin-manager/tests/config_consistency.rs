//! 配置控制面读取必须把层数据与已加载 Schema 作为同一版本处理。
use async_trait::async_trait;
use cordis_core::{Context, CoreError, Fiber, Plugin, PluginKey, Runtime};
use cordis_loader::{
    EntryOptions, ExtensionCatalog, ExtensionFactory, LOADER, LoaderControlError, LoaderError,
    LoaderPlugin, PreparedExtension,
};
use cordis_plugin_config_editor::{
    CONFIG_EDITOR, ConfigEditability, ConfigEditorConfig, ConfigEditorPlugin, ConfigReadOnlyReason,
    PATCH_FILE_VERSION, PatchFileDocument, UserPatchDocument, UserPatchStore, prepare_root_include,
};
use cordis_plugin_include::{EntryPatch, IncludeFactory};
use cordis_plugin_settings::{ConfigOperation, SETTINGS, SettingsPlugin};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Deserialize, Serialize, JsonSchema)]
struct PublicConfig {
    message: String,
}
#[derive(Deserialize, Serialize, JsonSchema)]
struct SecretConfig {
    #[schemars(extend("writeOnly" = true))]
    token: String,
}
#[derive(Deserialize, JsonSchema)]
struct FaultConfig {
    mode: u32,
}
impl Serialize for FaultConfig {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.mode {
            1 => Err(serde::ser::Error::custom("private-projection-error")),
            2 => panic!("private-projection-panic"),
            _ => self.mode.serialize(serializer),
        }
    }
}

struct PublicFactory;
struct SecretFactory;
struct FaultFactory;
struct Instance(&'static str);
macro_rules! factory {
    ($factory:ty, $config:ty, $id:literal) => {
        impl ExtensionFactory for $factory {
            type Config = $config;
            fn id(&self) -> &'static str {
                $id
            }
            fn prepare(&self, _: Self::Config) -> Result<PreparedExtension, LoaderError> {
                Ok(PreparedExtension::ordinary(|| Ok(Arc::new(Instance($id)))))
            }
        }
    };
}
factory!(PublicFactory, PublicConfig, "public");
factory!(SecretFactory, SecretConfig, "secret");
factory!(FaultFactory, FaultConfig, "fault");
#[async_trait]
impl Plugin for Instance {
    fn key(&self) -> PluginKey {
        PluginKey::new(self.0)
    }
    async fn apply(&self, _: &Context) -> Result<(), CoreError> {
        Ok(())
    }
}

fn deploy(entry: EntryOptions) -> PatchFileDocument {
    PatchFileDocument {
        version: PATCH_FILE_VERSION,
        patches: vec![EntryPatch {
            insert: Some(vec![entry]),
            ..Default::default()
        }],
    }
}
fn write_deploy(path: &std::path::Path, entry: EntryOptions) {
    std::fs::write(path, toml::to_string(&deploy(entry)).unwrap()).unwrap();
}
async fn mount(config: &ConfigEditorConfig, root: &std::path::Path) -> (Runtime, Vec<Fiber>) {
    let mut catalog = ExtensionCatalog::new();
    catalog.register(PublicFactory).unwrap();
    catalog.register(SecretFactory).unwrap();
    catalog.register(FaultFactory).unwrap();
    catalog.register(IncludeFactory).unwrap();
    let runtime = Runtime::new().unwrap();
    let mut fibers = vec![
        runtime
            .root()
            .plugin(Arc::new(LoaderPlugin::new(catalog)))
            .await
            .unwrap(),
    ];
    runtime
        .root()
        .get(LOADER)
        .unwrap()
        .create(
            prepare_root_include(root, config).await.unwrap(),
            None,
            None,
        )
        .await
        .unwrap();
    fibers.push(
        runtime
            .root()
            .plugin(Arc::new(ConfigEditorPlugin::new(config.clone())))
            .await
            .unwrap(),
    );
    fibers.push(
        runtime
            .root()
            .plugin(Arc::new(SettingsPlugin))
            .await
            .unwrap(),
    );
    (runtime, fibers)
}
fn config(dir: &std::path::Path) -> ConfigEditorConfig {
    std::fs::write(dir.join("cordis.toml"), "version=3\nextensions=[]\n").unwrap();
    ConfigEditorConfig::new(
        vec![dir.join("deploy.toml")],
        dir.join("user.toml"),
        dir.join("home.toml"),
    )
}

#[tokio::test]
async fn split_user_rows_preserve_enablement_on_edit_and_config_on_enable() {
    for config_last in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(dir.path());
        write_deploy(
            &cfg.deploy_patch_paths[0],
            EntryOptions::new("one", "public")
                .with_config(toml::toml! { message = "deploy" }.into()),
        );
        let config_row = "[[patches]]\nid='one'\nconfig={message='user'}\n";
        let disabled_row = "[[patches]]\nid='one'\ndisabled=true\n";
        let rows = if config_last {
            format!("version=1\n{disabled_row}{config_row}")
        } else {
            format!("version=1\n{config_row}{disabled_row}")
        };
        std::fs::write(&cfg.user_patch_path, rows).unwrap();
        let (runtime, _fibers) = mount(&cfg, &dir.path().join("cordis.toml")).await;
        let settings = runtime.root().get(SETTINGS).unwrap();
        let before = settings.describe("include:one").await.unwrap();
        assert_eq!(before.user.unwrap()["message"].as_str(), Some("user"));
        settings
            .mutate(
                "include:one",
                before.revision,
                vec![ConfigOperation::Set {
                    path: vec!["message".into()],
                    value: "updated".into(),
                }],
            )
            .await
            .unwrap();
        assert!(
            !runtime
                .root()
                .get(LOADER)
                .unwrap()
                .await_idle()
                .await
                .unwrap()
                .entry("include:one")
                .unwrap()
                .enabled
        );
        runtime
            .root()
            .get(CONFIG_EDITOR)
            .unwrap()
            .set_enabled("include:one", true)
            .await
            .unwrap();
        let after = settings.describe("include:one").await.unwrap();
        assert_eq!(after.config["message"].as_str(), Some("updated"));
        assert_eq!(after.value.unwrap()["message"], "updated");
        let persisted = UserPatchStore::new(&cfg.user_patch_path)
            .load()
            .await
            .unwrap();
        assert_eq!(
            persisted.config_for("one").unwrap()["message"].as_str(),
            Some("updated")
        );
        assert!(persisted.disabled_for("one").is_none());
        runtime.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn settings_errors_do_not_expose_invalid_layer_secret_text() {
    for layer in ["deploy", "user"] {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(dir.path());
        write_deploy(
            &cfg.deploy_patch_paths[0],
            EntryOptions::new("one", "public").with_config(toml::toml! { message = "ok" }.into()),
        );
        let (runtime, _fibers) = mount(&cfg, &dir.path().join("cordis.toml")).await;
        let path = if layer == "deploy" {
            &cfg.deploy_patch_paths[0]
        } else {
            &cfg.user_patch_path
        };
        let text =
            "version=1\n[[patches]]\nid='one'\n[patches.config]\ntoken=\"AUDIT-PRIVATE-SECRET\n";
        std::fs::write(path, text).unwrap();
        let error = runtime
            .root()
            .get(SETTINGS)
            .unwrap()
            .describe("include:one")
            .await
            .unwrap_err();
        assert!(!format!("{error:?}").contains("AUDIT-PRIVATE-SECRET"));
        assert!(error.to_string().contains(path.to_str().unwrap()));
        assert!(error.to_string().contains("行，第"));
        assert_eq!(std::fs::read_to_string(path).unwrap(), text);
        runtime.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn external_factory_change_is_rejected_before_secret_projection() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    write_deploy(
        &cfg.deploy_patch_paths[0],
        EntryOptions::new("one", "public").with_config(toml::toml! { message = "hello" }.into()),
    );
    let (runtime, _fibers) = mount(&cfg, &dir.path().join("cordis.toml")).await;
    let settings = runtime.root().get(SETTINGS).unwrap();
    let before = settings.describe("include:one").await.unwrap();
    let before_tree = runtime
        .root()
        .get(LOADER)
        .unwrap()
        .await_idle()
        .await
        .unwrap();
    write_deploy(
        &cfg.deploy_patch_paths[0],
        EntryOptions::new("one", "secret")
            .with_config(toml::toml! { token = "private-external-token" }.into()),
    );
    let error = settings.describe("include:one").await.unwrap_err();
    assert!(matches!(error, LoaderControlError::ConfigConflict { .. }));
    assert!(!format!("{error:?}").contains("private-external-token"));
    let loader = runtime.root().get(LOADER).unwrap();
    let actual = loader.snapshot_entry_config("include:one").await.unwrap();
    assert_eq!(actual.factory, "public");
    assert_eq!(actual.revision, before.revision);
    assert_eq!(
        loader
            .await_idle()
            .await
            .unwrap()
            .entry("include:one")
            .unwrap()
            .fiber_id,
        before_tree.entry("include:one").unwrap().fiber_id
    );
    assert!(!cfg.user_patch_path.exists());
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn external_user_and_home_changes_require_explicit_reload() {
    for layer in ["user", "home"] {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(dir.path());
        write_deploy(
            &cfg.deploy_patch_paths[0],
            EntryOptions::new("one", "public").with_config(toml::toml! { message = "old" }.into()),
        );
        let (runtime, _fibers) = mount(&cfg, &dir.path().join("cordis.toml")).await;
        let next: toml::Value = toml::toml! { message = "new" }.into();
        if layer == "user" {
            let mut document = UserPatchDocument::empty();
            document.upsert_config("one", "public", Some(next));
            UserPatchStore::new(&cfg.user_patch_path)
                .store(&document)
                .await
                .unwrap();
        } else {
            let document = PatchFileDocument {
                version: PATCH_FILE_VERSION,
                patches: vec![EntryPatch {
                    id: Some("one".into()),
                    config: Some(next),
                    ..Default::default()
                }],
            };
            std::fs::write(&cfg.home_patch_path, toml::to_string(&document).unwrap()).unwrap();
        }
        let settings = runtime.root().get(SETTINGS).unwrap();
        assert!(matches!(
            settings.describe("include:one").await,
            Err(LoaderControlError::ConfigConflict { .. })
        ));
        runtime
            .root()
            .get(CONFIG_EDITOR)
            .unwrap()
            .reload()
            .await
            .unwrap();
        let view = settings.describe("include:one").await.unwrap();
        assert_eq!(view.config["message"].as_str(), Some("new"));
        assert_eq!(view.value.unwrap()["message"], "new");
        runtime.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn group_editability_matches_directory_without_reading_a_form() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    write_deploy(
        &cfg.deploy_patch_paths[0],
        EntryOptions::group(
            "bucket",
            vec![
                EntryOptions::new("one", "public")
                    .with_config(toml::toml! { message = "ok" }.into()),
            ],
        )
        .unwrap(),
    );
    let (runtime, _fibers) = mount(&cfg, &dir.path().join("cordis.toml")).await;
    let editor = runtime.root().get(CONFIG_EDITOR).unwrap();
    let reason = ConfigEditability::ReadOnly(ConfigReadOnlyReason::Group);
    assert_eq!(editor.editability("include:bucket").await.unwrap(), reason);
    assert!(!editor.can_edit("include:bucket").await.unwrap());
    assert_eq!(
        editor
            .managed_entries()
            .await
            .unwrap()
            .iter()
            .find(|entry| entry.path == "include:bucket")
            .unwrap()
            .editability,
        reason
    );
    assert!(matches!(
        editor.editability("include:missing").await,
        Err(LoaderControlError::EntryNotFound { .. })
    ));
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn inherited_projection_execution_faults_are_not_absent_values() {
    for mode in [None, Some(1), Some(2)] {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(dir.path());
        let base = mode.map_or_else(
            || toml::Value::Table(toml::Table::new()),
            |mode| toml::toml! { mode = mode }.into(),
        );
        write_deploy(
            &cfg.deploy_patch_paths[0],
            EntryOptions::new("one", "fault").with_config(base),
        );
        let mut user = UserPatchDocument::empty();
        user.upsert_config("one", "fault", Some(toml::toml! { mode = 0 }.into()));
        UserPatchStore::new(&cfg.user_patch_path)
            .store(&user)
            .await
            .unwrap();
        let (runtime, _fibers) = mount(&cfg, &dir.path().join("cordis.toml")).await;
        let result = runtime
            .root()
            .get(SETTINGS)
            .unwrap()
            .describe("include:one")
            .await;
        if mode.is_none() {
            assert!(result.unwrap().inherited.is_none());
        } else {
            let error = result.unwrap_err();
            assert!(!matches!(error, LoaderControlError::ConfigValidation));
            assert!(!format!("{error:?}").contains("private-projection"));
        }
        runtime.shutdown().await.unwrap();
    }
}
