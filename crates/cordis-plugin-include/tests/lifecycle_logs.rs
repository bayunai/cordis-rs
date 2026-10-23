use async_trait::async_trait;
use cordis_core::{
    Context, CoreError, Fiber, LogLevel, Plugin, PluginKey, Runtime, ServiceId, ServiceKey,
};
use cordis_loader::{
    EntryOptions, EntryUpdate, ExtensionCatalog, ExtensionFactory, ExtensionsConfig, LOADER,
    Loader, LoaderError, LoaderPlugin, PreparedExtension,
};
use cordis_plugin_include::{IncludeConfig, IncludeFactory};
use schemars::JsonSchema;
use serde::Deserialize;
use std::{fs, path::Path, sync::Arc};

static SERVICE: ServiceKey<()> = ServiceKey::new("log-test.service@1");
#[derive(Deserialize, JsonSchema)]
struct Input {
    live: u32,
    #[serde(default)]
    fail: bool,
    #[serde(default)]
    dependency: bool,
}
struct Factory;
struct Instance {
    fail: bool,
    dependency: bool,
}
impl ExtensionFactory for Factory {
    type Config = Input;
    fn id(&self) -> &'static str {
        "log-test"
    }
    fn prepare(&self, cfg: Input) -> Result<PreparedExtension, LoaderError> {
        Ok(PreparedExtension::volatile(
            (cfg.fail, cfg.dependency),
            cfg.live,
            |(fail, dependency), _| Ok(Arc::new(Instance { fail, dependency })),
        ))
    }
}
#[async_trait]
impl Plugin for Instance {
    fn key(&self) -> PluginKey {
        PluginKey::new("log-test")
    }
    fn inject(&self) -> Vec<ServiceId> {
        if self.dependency {
            vec![SERVICE.id()]
        } else {
            vec![]
        }
    }
    async fn apply(&self, _: &Context) -> Result<(), CoreError> {
        if self.fail {
            Err(CoreError::PluginApply("测试失败".into()))
        } else {
            Ok(())
        }
    }
}
struct Provider;
#[async_trait]
impl Plugin for Provider {
    fn key(&self) -> PluginKey {
        PluginKey::new("log-test.provider")
    }
    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        ctx.provide(SERVICE, ())?;
        Ok(())
    }
}
fn leaf(id: &str, live: u32, fail: bool, dependency: bool) -> EntryOptions {
    EntryOptions::new(id, "log-test").with_config(
        toml::toml! {
            live = live
            fail = fail
            dependency = dependency
            secret = "must-not-appear-in-logs"
        }
        .into(),
    )
}
fn save(path: &Path, entries: Vec<EntryOptions>) {
    fs::write(
        path,
        toml::to_string(&ExtensionsConfig {
            version: 3,
            extensions: entries,
        })
        .unwrap(),
    )
    .unwrap();
}
fn include(id: &str, path: &Path, logs: Option<bool>) -> EntryOptions {
    let mut config = IncludeConfig::new(path);
    config.enable_logs = logs;
    EntryOptions::new(id, "cordis:include").with_config(toml::Value::try_from(config).unwrap())
}
async fn mount(entries: Vec<EntryOptions>) -> (Runtime, Fiber, Loader) {
    let mut catalog = ExtensionCatalog::new();
    catalog.register(IncludeFactory).unwrap();
    catalog.register(Factory).unwrap();
    let runtime = Runtime::new().unwrap();
    let fiber = runtime
        .root()
        .plugin(Arc::new(LoaderPlugin::new(catalog)))
        .await
        .unwrap();
    let loader = (*runtime.root().get(LOADER).unwrap()).clone();
    for entry in entries {
        loader.create(entry, None, None).await.unwrap();
    }
    loader.await_idle().await.unwrap();
    (runtime, fiber, loader)
}
fn messages(runtime: &Runtime) -> Vec<String> {
    runtime
        .logger_service()
        .buffer_snapshot()
        .into_iter()
        .filter(|log| log.target == "loader" && log.level == LogLevel::Info)
        .map(|log| log.message)
        .collect()
}
fn count(runtime: &Runtime, operation: &str, path: &str) -> usize {
    messages(runtime)
        .iter()
        .filter(|message| message.as_str() == format!("{operation} {path} (log-test)"))
        .count()
}

#[tokio::test]
async fn lifecycle_logs_cover_mount_hot_reload_disable_and_reenable_without_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("entries.toml");
    save(
        &path,
        vec![EntryOptions::group("group", vec![leaf("worker", 1, false, false)]).unwrap()],
    );
    let (runtime, _fiber, loader) = mount(vec![include("include", &path, Some(true))]).await;
    assert_eq!(count(&runtime, "挂载", "include:group:worker"), 1);
    let tree = loader.subtree("include").unwrap();
    tree.update(
        "group:worker",
        EntryUpdate {
            config: Some(leaf("worker", 2, false, false).config),
            ..Default::default()
        },
        None,
        None,
    )
    .await
    .unwrap();
    assert_eq!(count(&runtime, "重载", "include:group:worker"), 1);
    assert_eq!(count(&runtime, "挂载", "include:group:worker"), 1);
    tree.update(
        "group:worker",
        EntryUpdate {
            disabled: Some(true),
            ..Default::default()
        },
        None,
        None,
    )
    .await
    .unwrap();
    assert_eq!(count(&runtime, "卸载", "include:group:worker"), 1);
    tree.update(
        "group:worker",
        EntryUpdate {
            disabled: Some(false),
            ..Default::default()
        },
        None,
        None,
    )
    .await
    .unwrap();
    assert_eq!(count(&runtime, "挂载", "include:group:worker"), 2);
    let before = messages(&runtime);
    tree.reload().await.unwrap();
    assert_eq!(messages(&runtime), before);
    assert!(
        before
            .iter()
            .all(|m| !m.contains("must-not-appear") && !m.contains("cordis.loader.group"))
    );
    runtime.shutdown().await.unwrap();
    assert_eq!(count(&runtime, "卸载", "include:group:worker"), 2);
}

#[tokio::test]
async fn default_logging_is_off_but_lifecycle_errors_remain_visible() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("entries.toml");
    save(&path, vec![leaf("bad", 1, true, false)]);
    let (runtime, _fiber, _) = mount(vec![include("include", &path, None)]).await;
    assert!(messages(&runtime).is_empty());
    assert!(
        runtime
            .logger_service()
            .buffer_snapshot()
            .iter()
            .any(|log| log.level == LogLevel::Error && log.message.contains("测试失败"))
    );
    runtime.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn readonly_preflight_rejects_edit_without_changing_runtime_revision_or_file() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("entries.toml");
    save(&path, vec![leaf("worker", 1, false, false)]);
    let (runtime, _fiber, loader) = mount(vec![include("include", &path, Some(true))]).await;
    let before = loader.describe_config("include:worker").await.unwrap();
    let id = loader
        .await_idle()
        .await
        .unwrap()
        .entry("include:worker")
        .unwrap()
        .fiber_id;
    let bytes = fs::read(&path).unwrap();
    let logs = messages(&runtime);
    fs::set_permissions(&path, fs::Permissions::from_mode(0o444)).unwrap();
    let result = loader
        .subtree("include")
        .unwrap()
        .update(
            "worker",
            EntryUpdate {
                config: Some(leaf("worker", 2, false, false).config),
                ..Default::default()
            },
            None,
            None,
        )
        .await;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(
        matches!(result, Err(cordis_loader::LoaderControlError::Source { source })
        if source.kind == cordis_loader::SourceErrorKind::ReadOnly)
    );
    let after = loader.describe_config("include:worker").await.unwrap();
    assert_eq!(before.revision, after.revision);
    assert_eq!(before.value, after.value);
    assert_eq!(
        id,
        loader
            .await_idle()
            .await
            .unwrap()
            .entry("include:worker")
            .unwrap()
            .fiber_id
    );
    assert_eq!(fs::read(&path).unwrap(), bytes);
    assert_eq!(messages(&runtime), logs);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn dependency_loss_and_reactivation_use_the_same_lifecycle_log_stream() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("entries.toml");
    save(&path, vec![leaf("worker", 1, false, true)]);
    let (runtime, _fiber, loader) = mount(vec![include("include", &path, Some(true))]).await;
    assert_eq!(count(&runtime, "挂载", "include:worker"), 0);
    let mut provider = runtime.root().plugin(Arc::new(Provider)).await.unwrap();
    loader.await_idle().await.unwrap();
    cordis_testkit::wait_until(|| count(&runtime, "挂载", "include:worker") == 1).await;
    provider.dispose_wait().await.unwrap();
    loader.await_idle().await.unwrap();
    cordis_testkit::wait_until(|| count(&runtime, "卸载", "include:worker") == 1).await;
    let _provider = runtime.root().plugin(Arc::new(Provider)).await.unwrap();
    loader.await_idle().await.unwrap();
    cordis_testkit::wait_until(|| count(&runtime, "挂载", "include:worker") == 2).await;
    assert_eq!(count(&runtime, "卸载", "include:worker"), 1);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn nested_logging_inherits_and_updates_only_after_parent_definition_commits() {
    let dir = tempfile::tempdir().unwrap();
    let nested = dir.path().join("nested.toml");
    let muted = dir.path().join("muted.toml");
    let root = dir.path().join("root.toml");
    save(&nested, vec![leaf("worker", 1, false, false)]);
    save(&muted, vec![leaf("quiet", 1, false, false)]);
    save(
        &root,
        vec![
            include("nested", Path::new("nested.toml"), None),
            include("muted", Path::new("muted.toml"), Some(false)),
        ],
    );
    let (runtime, _fiber, loader) = mount(vec![include("include", &root, Some(true))]).await;
    assert_eq!(count(&runtime, "挂载", "include:nested:worker"), 1);
    assert_eq!(count(&runtime, "挂载", "include:muted:quiet"), 0);
    let id = loader
        .await_idle()
        .await
        .unwrap()
        .entry("include:nested:worker")
        .unwrap()
        .fiber_id;
    loader
        .update(
            "include",
            EntryUpdate {
                config: Some(include("include", &root, Some(false)).config),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        loader
            .await_idle()
            .await
            .unwrap()
            .entry("include:nested:worker")
            .unwrap()
            .fiber_id,
        id
    );
    let before = messages(&runtime);
    loader
        .subtree("include:nested")
        .unwrap()
        .update(
            "worker",
            EntryUpdate {
                config: Some(leaf("worker", 2, false, false).config),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(messages(&runtime), before);
    loader
        .update(
            "include",
            EntryUpdate {
                config: Some(include("include", &root, Some(true)).config),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap();
    loader
        .subtree("include:nested")
        .unwrap()
        .update(
            "worker",
            EntryUpdate {
                config: Some(leaf("worker", 3, false, false).config),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(count(&runtime, "重载", "include:nested:worker"), 1);
    runtime.shutdown().await.unwrap();
}
