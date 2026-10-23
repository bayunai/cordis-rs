//! PluginManager：库存与启停只经 user patch。

use async_trait::async_trait;
use cordis_core::{Context, CoreError, FiberState, Plugin, PluginKey, Runtime};
use cordis_loader::{
    EntryOptions, EntryUpdate, ExtensionCatalog, ExtensionFactory, ExtensionsConfig, LOADER,
    LoaderControlError, LoaderError, LoaderPlugin, PreparedExtension,
};
use cordis_plugin_config_editor::{
    ConfigEditorConfig, ConfigEditorPlugin, PATCH_FILE_VERSION, PatchFileDocument,
    prepare_root_include,
};
use cordis_plugin_include::{EntryPatch, IncludeConfig, IncludeFactory};
use cordis_plugin_plugin_manager::{PLUGIN_MANAGER, PluginManagerPlugin, ReadOnlyReason};
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
    let manager_fiber = runtime
        .root()
        .plugin(Arc::new(PluginManagerPlugin::default()))
        .await
        .unwrap();
    (
        runtime,
        dir,
        vec![loader_fiber, editor_fiber, manager_fiber],
    )
}

#[tokio::test]
async fn list_plugins_includes_rows_and_marks_groups_read_only() {
    let (runtime, _dir, _fibers) = harness().await;
    let manager = runtime.root().get(PLUGIN_MANAGER).unwrap();
    let list = manager.list_plugins().await.unwrap();
    let paths: Vec<_> = list.iter().map(|p| p.path.as_str()).collect();
    assert!(paths.contains(&"include:one"));
    assert!(paths.contains(&"include:bucket"));
    assert!(paths.contains(&"include:bucket:nested"));
    let group = list.iter().find(|p| p.path == "include:bucket").unwrap();
    assert_eq!(group.read_only_reason, Some(ReadOnlyReason::Group));
    let one = list.iter().find(|p| p.path == "include:one").unwrap();
    assert!(one.read_only_reason.is_none());
    assert!(one.enabled);
    assert_eq!(one.state, Some(FiberState::Active));
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn set_plugin_enabled_writes_user_patch_not_cordis_root() {
    let (runtime, dir, _fibers) = harness().await;
    let manager = runtime.root().get(PLUGIN_MANAGER).unwrap();
    let cordis_before = std::fs::read(dir.path().join("cordis.toml")).unwrap();
    let deploy_before = std::fs::read(dir.path().join("deploy.patches.toml")).unwrap();
    manager
        .set_plugin_enabled("include:one", false)
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(dir.path().join("cordis.toml")).unwrap(),
        cordis_before
    );
    assert_eq!(
        std::fs::read(dir.path().join("deploy.patches.toml")).unwrap(),
        deploy_before
    );
    let user = std::fs::read_to_string(dir.path().join("user.patches.toml")).unwrap();
    assert!(user.contains("one") || user.contains("disabled"));
    let list = manager.list_plugins().await.unwrap();
    let one = list.iter().find(|p| p.path == "include:one").unwrap();
    assert!(!one.enabled);
    assert!(matches!(
        manager.set_plugin_enabled("include:bucket", false).await,
        Err(cordis_loader::LoaderControlError::ConfigNotEditable)
    ));
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn nested_source_children_and_non_deploy_entries_are_read_only() {
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
    let manager = runtime.root().get(PLUGIN_MANAGER).unwrap();
    let list = manager.list_plugins().await.unwrap();
    for (path, reason) in [
        ("include:nested-source", ReadOnlyReason::NestedInclude),
        (
            "include:nested-source:child",
            ReadOnlyReason::UnmanagedEntry,
        ),
        ("include:unmanaged", ReadOnlyReason::UnmanagedEntry),
    ] {
        assert_eq!(
            list.iter()
                .find(|entry| entry.path == path)
                .unwrap()
                .read_only_reason,
            Some(reason)
        );
        assert!(matches!(
            manager.set_plugin_enabled(path, false).await,
            Err(LoaderControlError::ConfigNotEditable)
        ));
    }
    assert!(!dir.path().join("user.patches.toml").exists());
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn subscriptions_are_shared_and_close_on_unload_without_affecting_new_instance() {
    let (runtime, _dir, mut fibers) = harness().await;
    let old_manager = (*runtime.root().get(PLUGIN_MANAGER).unwrap()).clone();
    // 反复进入/退出页面仅增删广播接收端，不创建独立转发任务。
    for _ in 0..1000 {
        drop(old_manager.subscribe().unwrap());
    }
    let mut old_rx = old_manager.subscribe().unwrap();
    old_manager
        .set_plugin_enabled("include:one", false)
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), old_rx.recv())
        .await
        .expect("old instance must forward events")
        .unwrap();
    fibers.last_mut().unwrap().dispose_wait().await.unwrap();
    assert!(matches!(
        old_manager.subscribe(),
        Err(LoaderControlError::LoaderUnavailable)
    ));
    // 卸载前积压可以消费，但此后通道必须关闭，不等待 Loader 本身关闭。
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while old_rx.recv().await.is_ok() {}
    })
    .await
    .expect("unloaded instance must close its channel");
    assert!(runtime.root().get(LOADER).is_ok());
    let mut new_fiber = runtime
        .root()
        .plugin(Arc::new(PluginManagerPlugin::default()))
        .await
        .unwrap();
    let new_manager = runtime.root().get(PLUGIN_MANAGER).unwrap();
    let mut new_rx = new_manager.subscribe().unwrap();
    new_manager
        .set_plugin_enabled("include:one", true)
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), new_rx.recv())
        .await
        .expect("new instance must forward independently")
        .unwrap();
    assert!(matches!(
        old_rx.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Closed)
    ));
    assert!(matches!(
        old_manager.list_plugins().await,
        Err(LoaderControlError::LoaderUnavailable)
    ));
    assert!(matches!(
        old_manager.set_plugin_enabled("include:one", false).await,
        Err(LoaderControlError::LoaderUnavailable)
    ));
    new_fiber.dispose_wait().await.unwrap();
    runtime.shutdown().await.unwrap();
}
