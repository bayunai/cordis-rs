//! Settings 页面接入：list / editable / subscribe。

use async_trait::async_trait;
use cordis_core::{Context, CoreError, Plugin, PluginKey, Runtime};
use cordis_loader::{
    EntryOptions, EntryUpdate, ExtensionCatalog, ExtensionFactory, ExtensionsConfig, LOADER,
    LoaderControlError, LoaderError, LoaderPlugin, PreparedExtension,
};
use cordis_plugin_config_editor::{
    ConfigEditorConfig, ConfigEditorPlugin, ConfigReadOnlyReason, PATCH_FILE_VERSION,
    PatchFileDocument, prepare_root_include,
};
use cordis_plugin_include::{EntryPatch, IncludeConfig, IncludeFactory};
use cordis_plugin_settings::{
    ConfigOperation, SETTINGS, SettingsEvent, SettingsPlugin,
};
use schemars::JsonSchema;
use serde::Deserialize;
use std::sync::Arc;

#[derive(serde::Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Input {
    #[allow(dead_code)]
    value: u32,
}

struct Factory;
struct Instance;

impl ExtensionFactory for Factory {
    type Config = Input;
    fn id(&self) -> &'static str {
        "demo.row"
    }
    fn prepare(&self, _: Input) -> Result<PreparedExtension, LoaderError> {
        Ok(PreparedExtension::ordinary(|| Ok(Arc::new(Instance))))
    }
}

#[async_trait]
impl Plugin for Instance {
    fn key(&self) -> PluginKey {
        PluginKey::new("demo.row")
    }
    async fn apply(&self, _: &Context) -> Result<(), CoreError> {
        Ok(())
    }
}

async fn harness() -> (Runtime, tempfile::TempDir, Vec<cordis_core::Fiber>) {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("cordis.toml"),
        "version = 3\n\nextensions = []\n",
    )
    .unwrap();
    let one = EntryOptions::new("one", "demo.row").with_config(toml::toml! { value = 1 }.into());
    let group = EntryOptions::group(
        "bucket",
        vec![EntryOptions::new("nested", "demo.row").with_config(toml::toml! { value = 2 }.into())],
    )
    .unwrap();
    let deploy = PatchFileDocument {
        version: PATCH_FILE_VERSION,
        patches: vec![EntryPatch {
            insert: Some(vec![one, group]),
            ..Default::default()
        }],
    };
    std::fs::write(
        dir.path().join("deploy.patches.toml"),
        toml::to_string_pretty(&deploy).unwrap(),
    )
    .unwrap();
    std::fs::write(dir.path().join("home.patches.toml"), "version = 1\n").unwrap();

    let mut catalog = ExtensionCatalog::new();
    catalog.register(Factory).unwrap();
    catalog.register(IncludeFactory).unwrap();
    let runtime = Runtime::new().unwrap();
    let loader_fiber = runtime
        .root()
        .plugin(Arc::new(LoaderPlugin::new(catalog)))
        .await
        .unwrap();
    let editor_config = ConfigEditorConfig::new(
        vec![dir.path().join("deploy.patches.toml")],
        dir.path().join("user.patches.toml"),
        dir.path().join("home.patches.toml"),
    );
    runtime
        .root()
        .get(LOADER)
        .unwrap()
        .create(
            prepare_root_include(dir.path().join("cordis.toml"), &editor_config)
                .await
                .unwrap(),
            None,
            None,
        )
        .await
        .unwrap();
    let editor_fiber = runtime
        .root()
        .plugin(Arc::new(ConfigEditorPlugin::new(editor_config)))
        .await
        .unwrap();
    let settings_fiber = runtime
        .root()
        .plugin(Arc::new(SettingsPlugin::default()))
        .await
        .unwrap();
    (
        runtime,
        dir,
        vec![loader_fiber, editor_fiber, settings_fiber],
    )
}

#[tokio::test]
async fn list_excludes_groups_and_marks_editable_deploy_entries() {
    let (runtime, _dir, _fibers) = harness().await;
    let settings = runtime.root().get(SETTINGS).unwrap();
    let list = settings.list().await.unwrap();
    let paths: Vec<_> = list.iter().map(|e| e.path.as_str()).collect();
    assert!(paths.contains(&"include:one"));
    assert!(paths.contains(&"include:bucket:nested"));
    assert!(!paths.contains(&"include:bucket"));
    let one = list.iter().find(|e| e.path == "include:one").unwrap();
    assert!(one.editable);
    assert!(one.read_only_reason.is_none());
    let view = settings.describe("include:one").await.unwrap();
    assert!(view.editable);
    assert!(view.read_only_reason.is_none());
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn describe_readonly_entries_and_mutate_rejects_early() {
    let (runtime, dir, _fibers) = harness().await;
    let nested_file = dir.path().join("nested.toml");
    let child =
        EntryOptions::new("child", "demo.row").with_config(toml::toml! { value = 3 }.into());
    std::fs::write(
        &nested_file,
        toml::to_string(&ExtensionsConfig {
            version: cordis_loader::CONFIG_VERSION,
            extensions: vec![child],
        })
        .unwrap(),
    )
    .unwrap();
    let nested = EntryOptions::new("nested-source", "cordis:include")
        .with_config(toml::Value::try_from(IncludeConfig::new(nested_file)).unwrap());
    let unmanaged =
        EntryOptions::new("unmanaged", "demo.row").with_config(toml::toml! { value = 4 }.into());
    let loader = runtime.root().get(LOADER).unwrap();
    let root_entry = loader.entries().unwrap().extensions.remove(0);
    let mut include: IncludeConfig = root_entry.config.try_into().unwrap();
    include.patches.push(EntryPatch {
        insert: Some(vec![nested, unmanaged]),
        ..Default::default()
    });
    loader
        .update(
            "include",
            EntryUpdate {
                config: Some(toml::Value::try_from(include).unwrap()),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap();

    let settings = runtime.root().get(SETTINGS).unwrap();
    let list = settings.list().await.unwrap();
    for (path, reason) in [
        ("include:nested-source", ConfigReadOnlyReason::NestedInclude),
        (
            "include:nested-source:child",
            ConfigReadOnlyReason::UnmanagedEntry,
        ),
        ("include:unmanaged", ConfigReadOnlyReason::UnmanagedEntry),
    ] {
        let row = list.iter().find(|e| e.path == path).unwrap();
        assert!(!row.editable);
        assert_eq!(row.read_only_reason, Some(reason));
        let view = settings.describe(path).await.unwrap();
        assert!(!view.editable);
        assert_eq!(view.read_only_reason, Some(reason));
        assert!(matches!(
            settings
                .mutate(
                    path,
                    view.revision,
                    vec![ConfigOperation::Set {
                        path: vec!["value".into()],
                        value: 9.into(),
                    }],
                )
                .await,
            Err(LoaderControlError::ConfigNotEditable)
        ));
    }
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn subscribe_forwards_loader_changes_and_closes_on_unload() {
    let (runtime, _dir, mut fibers) = harness().await;
    let old = (*runtime.root().get(SETTINGS).unwrap()).clone();
    let mut rx = old.subscribe().unwrap();
    let before = old.describe("include:one").await.unwrap();
    old.mutate(
        "include:one",
        before.revision,
        vec![ConfigOperation::Set {
            path: vec!["value".into()],
            value: 7.into(),
        }],
    )
    .await
    .unwrap();
    let event = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
        .await
        .expect("settings must forward SnapshotChanged")
        .unwrap();
    assert!(matches!(event, SettingsEvent::Changed { .. }));

    fibers.last_mut().unwrap().dispose_wait().await.unwrap();
    assert!(matches!(
        old.subscribe(),
        Err(LoaderControlError::LoaderUnavailable)
    ));
    assert!(matches!(
        old.list().await,
        Err(LoaderControlError::LoaderUnavailable)
    ));
    runtime.shutdown().await.unwrap();
}
