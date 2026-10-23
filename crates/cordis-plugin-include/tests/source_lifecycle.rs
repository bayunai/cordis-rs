use async_trait::async_trait;
use cordis_core::{Context, CoreError, Fiber, FiberState, Plugin, PluginKey, Runtime};
use cordis_loader::{
    ConfigRef, EntryOptions, EntryUpdate, ExtensionCatalog, ExtensionFactory, ExtensionsConfig,
    LOADER, Loader, LoaderError, LoaderPlugin, PreparedExtension,
};
use cordis_plugin_include::{EntryPatch, IncludeConfig, IncludeFactory};
use schemars::JsonSchema;
use serde::Deserialize;
use std::{
    fs,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Clone, Deserialize, JsonSchema)]
struct Input {
    value: u32,
    #[serde(default)]
    fail: bool,
}
#[derive(Default)]
struct Counts {
    builds: AtomicUsize,
    applies: AtomicUsize,
    values: Mutex<Vec<ConfigRef<u32>>>,
    started: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
struct Factory(Arc<Counts>);
struct Instance {
    fail: bool,
    value: ConfigRef<u32>,
    counts: Arc<Counts>,
}
impl ExtensionFactory for Factory {
    type Config = Input;
    fn id(&self) -> &'static str {
        "test"
    }
    fn prepare(&self, input: Input) -> Result<PreparedExtension, LoaderError> {
        if input.value == 0 {
            return Err(LoaderError::invalid_config("value 必须为正数"));
        }
        let counts = self.0.clone();
        Ok(PreparedExtension::volatile(
            input.fail,
            input.value,
            move |fail, value| {
                counts.builds.fetch_add(1, Ordering::SeqCst);
                counts.values.lock().unwrap().push(value.clone());
                Ok(Arc::new(Instance {
                    fail,
                    value,
                    counts,
                }))
            },
        ))
    }
}
#[async_trait]
impl Plugin for Instance {
    fn key(&self) -> PluginKey {
        PluginKey::new("test")
    }
    async fn apply(&self, _: &Context) -> Result<(), CoreError> {
        self.counts.applies.fetch_add(1, Ordering::SeqCst);
        if *self.value.get().value == 99 {
            self.counts.started.notify_one();
            self.counts.release.notified().await;
        }
        if self.fail {
            return Err(CoreError::PluginApply("生命周期屏障测试".into()));
        }
        Ok(())
    }
}
struct Harness {
    runtime: Runtime,
    loader: Loader,
    _fiber: Fiber,
    dir: tempfile::TempDir,
    counts: Arc<Counts>,
    config: IncludeConfig,
}
fn entry(id: &str, value: u32) -> EntryOptions {
    EntryOptions::new(id, "test").with_config(toml::toml! {value=value}.into())
}
impl Harness {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("main.toml");
        fs::write(
            &path,
            toml::to_string(&ExtensionsConfig {
                version: 3,
                extensions: vec![entry("one", 1), entry("two", 2)],
            })
            .unwrap(),
        )
        .unwrap();
        let config = IncludeConfig::new(&path);
        let counts = Arc::new(Counts::default());
        let mut catalog = ExtensionCatalog::new();
        catalog.register(IncludeFactory).unwrap();
        catalog.register(Factory(counts.clone())).unwrap();
        let runtime = Runtime::new().unwrap();
        let fiber = runtime
            .root()
            .plugin(Arc::new(
                LoaderPlugin::new(
                    catalog,
                    ExtensionsConfig {
                        version: 3,
                        extensions: vec![
                            EntryOptions::new("include", "cordis:include")
                                .with_config(toml::Value::try_from(&config).unwrap()),
                        ],
                    },
                )
                .unwrap(),
            ))
            .await
            .unwrap();
        let loader = (*runtime.root().get(LOADER).unwrap()).clone();
        Self {
            runtime,
            loader,
            _fiber: fiber,
            dir,
            counts,
            config,
        }
    }
    async fn update(
        &self,
        config: &IncludeConfig,
    ) -> Result<(), cordis_loader::LoaderControlError> {
        self.loader
            .update(
                "include",
                EntryUpdate {
                    config: Some(toml::Value::try_from(config).unwrap()),
                    ..Default::default()
                },
                None,
                None,
            )
            .await
            .map(|_| ())
    }
    fn file(&self) -> Vec<u8> {
        fs::read(self.dir.path().join("main.toml")).unwrap()
    }
}
fn patch(value: u32, fail: bool) -> EntryPatch {
    EntryPatch {
        id: Some("one".into()),
        config: Some(toml::toml! {value=value fail=fail}.into()),
        ..Default::default()
    }
}

#[tokio::test]
async fn patches_update_live_values_without_rebuilding_carrier_or_unrelated_children() {
    let h = Harness::new().await;
    let before = h.loader.await_idle().await.unwrap();
    let file = h.file();
    let refs = h.counts.values.lock().unwrap().clone();
    let mut config = h.config.clone();
    config.patches = vec![patch(10, false)];
    h.update(&config).await.unwrap();
    let after = h.loader.await_idle().await.unwrap();
    for path in ["include", "include:one", "include:two"] {
        assert_eq!(
            before.entry(path).unwrap().fiber_id,
            after.entry(path).unwrap().fiber_id
        );
    }
    assert_eq!(*refs[0].get().value, 10);
    assert_eq!(*refs[1].get().value, 2);
    assert_eq!(h.counts.builds.load(Ordering::SeqCst), 2);
    assert_eq!(h.file(), file);
    // 相同定义不重复提交热值；移除覆盖后重新读取同一基础文件。
    let revision = refs[0].get().revision;
    h.update(&config).await.unwrap();
    assert_eq!(refs[0].get().revision, revision);
    h.update(&h.config).await.unwrap();
    assert_eq!(*refs[0].get().value, 1);
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_patch_preflight_preserves_active_tree_and_parent_declaration() {
    let h = Harness::new().await;
    let before = h.loader.await_idle().await.unwrap();
    let parent = h.loader.entries().unwrap();
    let mut config = h.config.clone();
    config.patches = vec![patch(0, false)];
    assert!(h.update(&config).await.is_err());
    assert_eq!(h.loader.entries().unwrap(), parent);
    let after = h.loader.await_idle().await.unwrap();
    assert_eq!(
        after.entry("include:one").unwrap().fiber_id,
        before.entry("include:one").unwrap().fiber_id
    );
    assert_eq!(
        after.entry("include:one").unwrap().state,
        Some(FiberState::Active)
    );
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn patch_lifecycle_failure_is_waited_and_same_target_can_retry() {
    let h = Harness::new().await;
    let mut config = h.config.clone();
    config.patches = vec![patch(99, true)];
    let file = h.file();
    let loader = h.loader.clone();
    let input = toml::Value::try_from(&config).unwrap();
    let call = tokio::spawn(async move {
        loader
            .update(
                "include",
                EntryUpdate {
                    config: Some(input),
                    ..Default::default()
                },
                None,
                None,
            )
            .await
    });
    h.counts.started.notified().await;
    assert!(!call.is_finished());
    h.counts.release.notify_one();
    assert!(call.await.unwrap().is_err());
    assert_eq!(h.file(), file);
    assert_eq!(
        h.loader
            .await_idle()
            .await
            .unwrap()
            .entry("include:one")
            .unwrap()
            .state,
        Some(FiberState::Failed)
    );
    // 失败后重试“上次成功定义”也必须修复实际子树，不能因父绑定相等而跳过。
    h.update(&h.config).await.unwrap();
    assert_eq!(
        h.loader
            .await_idle()
            .await
            .unwrap()
            .entry("include:one")
            .unwrap()
            .state,
        Some(FiberState::Active)
    );
    assert_eq!(
        h.loader
            .await_idle()
            .await
            .unwrap()
            .entry("include")
            .unwrap()
            .state,
        Some(FiberState::Active)
    );
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn changing_path_rebuilds_source_and_invalidates_old_handles() {
    let h = Harness::new().await;
    let old = h.loader.subtree("include").unwrap();
    let form = old.describe_config("one").await.unwrap();
    let before = h
        .loader
        .await_idle()
        .await
        .unwrap()
        .entry("include:one")
        .unwrap()
        .fiber_id;
    let path = h.dir.path().join("other.json");
    fs::write(
        &path,
        serde_json::to_vec(&ExtensionsConfig {
            version: 3,
            extensions: vec![entry("one", 7)],
        })
        .unwrap(),
    )
    .unwrap();
    h.update(&IncludeConfig::new(path)).await.unwrap();
    // Core 的同 Key replace 可以保留载体 Fiber ID，但来源树及其子实例必须替换。
    assert_ne!(
        h.loader
            .await_idle()
            .await
            .unwrap()
            .entry("include:one")
            .unwrap()
            .fiber_id,
        before
    );
    assert!(old.entries().is_err());
    assert_ne!(
        h.loader
            .describe_config("include:one")
            .await
            .unwrap()
            .revision,
        form.revision
    );
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn log_policy_updates_existing_inheriting_contexts_without_restarting_children() {
    let h = Harness::new().await;
    let path = h.dir.path().join("nested.toml");
    fs::write(
        &path,
        toml::to_string(&ExtensionsConfig {
            version: 3,
            extensions: vec![entry("child", 3)],
        })
        .unwrap(),
    )
    .unwrap();
    let nested = IncludeConfig::new("nested.toml");
    h.loader
        .subtree("include")
        .unwrap()
        .create(
            EntryOptions::new("nested", "cordis:include")
                .with_config(toml::Value::try_from(nested).unwrap()),
            None,
            None,
        )
        .await
        .unwrap();
    let child = h.loader.entry_context("include:nested:child").unwrap();
    let location = child
        .config(cordis_plugin_include::INCLUDE_LOCATION)
        .unwrap();
    assert!(!location.enable_logs());
    let before = h
        .loader
        .await_idle()
        .await
        .unwrap()
        .entry("include:nested:child")
        .unwrap()
        .fiber_id;
    let mut config = h.config.clone();
    config.enable_logs = Some(true);
    h.update(&config).await.unwrap();
    assert!(location.enable_logs());
    assert_eq!(
        h.loader
            .await_idle()
            .await
            .unwrap()
            .entry("include:nested:child")
            .unwrap()
            .fiber_id,
        before
    );
    config.enable_logs = Some(false);
    h.update(&config).await.unwrap();
    assert!(!location.enable_logs());
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn carrier_description_does_not_expose_secrets_embedded_in_initial_or_patches() {
    let h = Harness::new().await;
    let mut config = h.config.clone();
    let secret = EntryOptions::new("unused", "test")
        .with_config(toml::toml! {value=4 token="embedded-private-token"}.into());
    config.initial = Some(vec![secret]);
    config.patches = vec![EntryPatch {
        id: Some("one".into()),
        config: Some(toml::toml! {value=5 token="embedded-private-token"}.into()),
        ..Default::default()
    }];
    h.update(&config).await.unwrap();
    let form = h.loader.describe_config("include").await.unwrap();
    assert!(form.value.get("initial").is_none());
    assert!(form.value.get("patches").is_none());
    assert!(!format!("{form:?}").contains("embedded-private-token"));
    assert!(
        form.secrets
            .iter()
            .any(|field| field.path == ["patches"] && field.is_set)
    );
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn one_source_definition_failure_does_not_skip_an_independent_source_update() {
    let h = Harness::new().await;
    for (id, value) in [("left", 3), ("right", 4)] {
        let path = h.dir.path().join(format!("{id}.toml"));
        fs::write(
            &path,
            toml::to_string(&ExtensionsConfig {
                version: 3,
                extensions: vec![entry("child", value)],
            })
            .unwrap(),
        )
        .unwrap();
        h.loader
            .subtree("include")
            .unwrap()
            .create(
                EntryOptions::new(id, "cordis:include")
                    .with_config(toml::Value::try_from(IncludeConfig::new(path)).unwrap()),
                None,
                None,
            )
            .await
            .unwrap();
    }
    let right = h.counts.values.lock().unwrap().last().unwrap().clone();
    let mut declaration: ExtensionsConfig =
        toml::from_str(std::str::from_utf8(&h.file()).unwrap()).unwrap();
    for item in &mut declaration.extensions {
        if item.id == "left" || item.id == "right" {
            let mut cfg: IncludeConfig = item.config.clone().try_into().unwrap();
            cfg.patches = vec![EntryPatch {
                id: Some("child".into()),
                config: Some(toml::toml! {value=12 fail=(item.id=="left")}.into()),
                ..Default::default()
            }];
            item.config = toml::Value::try_from(cfg).unwrap();
        }
    }
    fs::write(
        h.dir.path().join("main.toml"),
        toml::to_string(&declaration).unwrap(),
    )
    .unwrap();
    assert!(h.loader.subtree("include").unwrap().reload().await.is_err());
    let actual = h.loader.await_idle().await.unwrap();
    assert_eq!(
        actual.entry("include:left:child").unwrap().state,
        Some(FiberState::Failed)
    );
    assert_eq!(
        actual.entry("include:right:child").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(*right.get().value, 12);
    h.runtime.shutdown().await.unwrap();
}
