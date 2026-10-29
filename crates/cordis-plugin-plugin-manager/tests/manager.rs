//! PluginManager：库存与启停只经 user patch。

use async_trait::async_trait;
use cordis_core::{
    Context, CoreError, FiberState, Plugin, PluginKey, Runtime, ServiceId, ServiceKey,
};
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

static PENDING_DEP: ServiceKey<()> = ServiceKey::new("test.pending-dependency");
struct PendingFactory;
struct PendingInstance;
impl ExtensionFactory for PendingFactory {
    type Config = Input;
    fn id(&self) -> &'static str {
        "demo.pending"
    }
    fn prepare(&self, _: Input) -> Result<PreparedExtension, LoaderError> {
        Ok(PreparedExtension::ordinary(|| {
            Ok(Arc::new(PendingInstance))
        }))
    }
}
#[async_trait]
impl Plugin for PendingInstance {
    fn key(&self) -> PluginKey {
        PluginKey::new("demo.pending")
    }
    fn inject(&self) -> Vec<ServiceId> {
        vec![PENDING_DEP.id()]
    }
    async fn apply(&self, _: &Context) -> Result<(), CoreError> {
        Ok(())
    }
}

static FAILURE_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
static FAIL_ATTEMPTS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
struct FailingFactory;
struct FailingInstance;

impl ExtensionFactory for FailingFactory {
    type Config = Input;
    fn id(&self) -> &'static str {
        "demo.failing"
    }
    fn prepare(&self, _: Input) -> Result<PreparedExtension, LoaderError> {
        Ok(PreparedExtension::ordinary(|| {
            Ok(Arc::new(FailingInstance))
        }))
    }
}

#[async_trait]
impl Plugin for FailingInstance {
    fn key(&self) -> PluginKey {
        PluginKey::new("demo.failing")
    }
    async fn apply(&self, _: &Context) -> Result<(), CoreError> {
        FAIL_ATTEMPTS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(CoreError::PluginApply("unsupported platform".into()))
    }
}

async fn harness() -> (Runtime, tempfile::TempDir, Vec<cordis_core::Fiber>) {
    harness_with_failure(false).await
}

async fn harness_with_failure(
    failure: bool,
) -> (Runtime, tempfile::TempDir, Vec<cordis_core::Fiber>) {
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
    let mut pending =
        EntryOptions::new("waiting", "demo.pending").with_config(toml::toml! { value = 1 }.into());
    pending.disabled = true;
    let mut entries = vec![one, group, pending];
    if failure {
        entries.push(
            EntryOptions::new("broken", "demo.failing")
                .with_config(toml::toml! { value = 1 }.into()),
        );
    }
    let deploy = PatchFileDocument {
        version: PATCH_FILE_VERSION,
        patches: vec![EntryPatch {
            insert: Some(entries),
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
    catalog.register(FailingFactory).unwrap();
    catalog.register(PendingFactory).unwrap();
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
        .plugin(Arc::new(PluginManagerPlugin))
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
async fn changes_are_retained_without_subscribers() {
    let (runtime, _dir, _fibers) = harness().await;
    let manager = runtime.root().get(PLUGIN_MANAGER).unwrap();
    // 只读取初值后立即丢弃接收端；实际变更期间没有页面订阅。
    let before = *manager.subscribe().unwrap().borrow();
    manager
        .set_plugin_enabled("include:one", false)
        .await
        .unwrap();
    let after = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let generation = *manager.subscribe().unwrap().borrow();
            if generation != before {
                break generation;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("a later subscriber must observe changes recorded without receivers");
    assert!(after > before);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn subscriptions_are_shared_and_close_on_unload_without_affecting_new_instance() {
    let (runtime, _dir, mut fibers) = harness().await;
    let old_manager = (*runtime.root().get(PLUGIN_MANAGER).unwrap()).clone();
    // 反复进入/退出页面仅增删 watch 接收端，不创建独立转发任务。
    for _ in 0..1000 {
        drop(old_manager.subscribe().unwrap());
    }
    let mut old_rx = old_manager.subscribe().unwrap();
    old_rx.borrow_and_update();
    old_manager
        .set_plugin_enabled("include:one", false)
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), old_rx.changed())
        .await
        .expect("old instance must forward events")
        .unwrap();
    fibers.last_mut().unwrap().dispose_wait().await.unwrap();
    assert!(matches!(
        old_manager.subscribe(),
        Err(LoaderControlError::LoaderUnavailable)
    ));
    // 卸载后 sender 关闭；changed 返回错误。
    tokio::time::timeout(std::time::Duration::from_secs(2), old_rx.changed())
        .await
        .expect("unloaded instance must close its channel")
        .expect_err("watch closed");
    assert!(runtime.root().get(LOADER).is_ok());
    let mut new_fiber = runtime
        .root()
        .plugin(Arc::new(PluginManagerPlugin))
        .await
        .unwrap();
    let new_manager = runtime.root().get(PLUGIN_MANAGER).unwrap();
    let mut new_rx = new_manager.subscribe().unwrap();
    new_rx.borrow_and_update();
    new_manager
        .set_plugin_enabled("include:one", true)
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), new_rx.changed())
        .await
        .expect("new instance must forward independently")
        .unwrap();
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

#[tokio::test]
async fn unrelated_plugin_toggle_succeeds_with_existing_failed_entry() {
    let _guard = FAILURE_TEST_LOCK.lock().await;
    let (runtime, dir, _fibers) = harness_with_failure(true).await;
    let manager = runtime.root().get(PLUGIN_MANAGER).unwrap();
    let before = manager.list_plugins().await.unwrap();
    let failed = before
        .iter()
        .find(|entry| entry.path == "include:broken")
        .unwrap();
    assert_eq!(failed.state, Some(FiberState::Failed));
    let attempts = FAIL_ATTEMPTS.load(std::sync::atomic::Ordering::SeqCst);
    manager
        .set_plugin_enabled("include:one", false)
        .await
        .unwrap();
    let patch = std::fs::read_to_string(dir.path().join("user.patches.toml")).unwrap();
    assert!(patch.contains("disabled = true"));
    manager
        .set_plugin_enabled("include:one", true)
        .await
        .unwrap();
    assert_eq!(
        FAIL_ATTEMPTS.load(std::sync::atomic::Ordering::SeqCst),
        attempts
    );
    let after = manager.list_plugins().await.unwrap();
    let failed = after
        .iter()
        .find(|entry| entry.path == "include:broken")
        .unwrap();
    assert_eq!(failed.state, Some(FiberState::Failed));
    let one = after
        .iter()
        .find(|entry| entry.path == "include:one")
        .unwrap();
    assert!(one.enabled);
    assert_eq!(one.state, Some(FiberState::Active));
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn explicitly_enabling_existing_failed_target_reports_failure() {
    let _guard = FAILURE_TEST_LOCK.lock().await;
    let (runtime, _dir, _fibers) = harness_with_failure(true).await;
    let manager = runtime.root().get(PLUGIN_MANAGER).unwrap();
    let error = manager
        .set_plugin_enabled("include:broken", true)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("unsupported platform"));
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn enable_pending_plugin_persists_and_activates_when_dependency_appears() {
    let (runtime, dir, _fibers) = harness().await;
    let manager = runtime.root().get(PLUGIN_MANAGER).unwrap();
    manager
        .set_plugin_enabled("include:waiting", true)
        .await
        .unwrap();
    let entry = manager
        .list_plugins()
        .await
        .unwrap()
        .into_iter()
        .find(|p| p.path == "include:waiting")
        .unwrap();
    assert!(entry.enabled);
    assert_eq!(entry.state, Some(FiberState::Pending));
    let patch = std::fs::read_to_string(dir.path().join("user.patches.toml")).unwrap();
    assert!(patch.contains("disabled = false"));
    runtime.root().provide(PENDING_DEP, ()).unwrap();
    runtime.settle().await;
    let entry = manager
        .list_plugins()
        .await
        .unwrap()
        .into_iter()
        .find(|p| p.path == "include:waiting")
        .unwrap();
    assert!(entry.enabled);
    assert_eq!(entry.state, Some(FiberState::Active));
    runtime.shutdown().await.unwrap();
}
