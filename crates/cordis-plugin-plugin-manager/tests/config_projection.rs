use async_trait::async_trait;
use cordis_core::{Context, CoreError, Fiber, Plugin, PluginKey, Runtime, ServiceKey};
use cordis_loader::{
    ConfigRef, EntryOptions, ExtensionCatalog, ExtensionFactory, LOADER, LoaderError, LoaderPlugin,
    PreparedExtension,
};
use cordis_plugin_config_editor::{
    CONFIG_EDITOR, ConfigEditorConfig, ConfigEditorPlugin, LoaderControlError, PATCH_FILE_VERSION,
    PatchFileDocument, UserPatchStore, prepare_root_include,
};
use cordis_plugin_include::{EntryPatch, IncludeFactory};
use cordis_plugin_settings::{ConfigOperation, SETTINGS, SettingsPlugin};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

static DEFAULT_CALLS: AtomicU32 = AtomicU32::new(0);
static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
static RUNNING: ServiceKey<ConfigRef<Live>> = ServiceKey::new("projection.running");
fn next_default() -> u32 {
    DEFAULT_CALLS.fetch_add(1, Ordering::SeqCst) + 1
}
fn schema_default() -> u32 {
    999_999
}
fn secret_default() -> String {
    "server-default-secret".into()
}

#[derive(Deserialize, Serialize, JsonSchema, PartialEq)]
struct Nested {
    timeout: u32,
}
impl Default for Nested {
    fn default() -> Self {
        Self { timeout: 17 }
    }
}
#[derive(Deserialize, Serialize, JsonSchema, PartialEq)]
struct Live {
    #[serde(default = "next_default")]
    #[schemars(default = "schema_default")]
    limit: u32,
    #[serde(default)]
    nested: Nested,
    #[serde(default)]
    note: Option<String>,
    #[serde(default)]
    tag: Option<String>,
    #[serde(default = "secret_default")]
    #[serde(rename(serialize = "serialized_token", deserialize = "token"))]
    #[schemars(extend("writeOnly" = true))]
    token: String,
}
#[derive(Deserialize, Serialize, JsonSchema)]
struct Input {
    address: String,
    #[serde(flatten)]
    live: Live,
}
struct Factory;
struct Instance {
    address: String,
    live: ConfigRef<Live>,
}
impl ExtensionFactory for Factory {
    type Config = Input;
    fn id(&self) -> &'static str {
        "projection"
    }
    fn prepare(&self, input: Input) -> Result<PreparedExtension, LoaderError> {
        Ok(PreparedExtension::volatile(
            input.address,
            input.live,
            |address, live| Ok(Arc::new(Instance { address, live })),
        ))
    }
}
#[async_trait]
impl Plugin for Instance {
    fn key(&self) -> PluginKey {
        PluginKey::new("projection")
    }
    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        if self.address == "fail" {
            return Err(CoreError::PluginApply("controlled failure".into()));
        }
        ctx.provide(RUNNING, self.live.clone())?;
        Ok(())
    }
}
struct Harness {
    runtime: Runtime,
    _fibers: Vec<Fiber>,
}
async fn mount(config: ConfigEditorConfig, root: &std::path::Path) -> Harness {
    let mut catalog = ExtensionCatalog::new();
    catalog.register(Factory).unwrap();
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
            prepare_root_include(root, &config).await.unwrap(),
            None,
            None,
        )
        .await
        .unwrap();
    fibers.push(
        runtime
            .root()
            .plugin(Arc::new(ConfigEditorPlugin::new(config)))
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
    Harness {
        runtime,
        _fibers: fibers,
    }
}
fn files(dir: &std::path::Path, disabled: bool) -> ConfigEditorConfig {
    std::fs::write(dir.join("cordis.toml"), "version = 3\nextensions = []\n").unwrap();
    let mut entry =
        EntryOptions::new("one", "projection").with_config(toml::toml! { address = "ok" }.into());
    entry.disabled = disabled;
    let deploy = PatchFileDocument {
        version: PATCH_FILE_VERSION,
        patches: vec![EntryPatch {
            insert: Some(vec![entry]),
            ..Default::default()
        }],
    };
    std::fs::write(dir.join("deploy.toml"), toml::to_string(&deploy).unwrap()).unwrap();
    ConfigEditorConfig::new(
        vec![dir.join("deploy.toml")],
        dir.join("user.toml"),
        dir.join("home.toml"),
    )
}
fn set(key: &str, value: &str) -> ConfigOperation {
    ConfigOperation::Set {
        path: vec![key.into()],
        value: value.into(),
    }
}

#[tokio::test]
async fn applied_defaults_are_cached_redacted_and_never_written_into_user_config() {
    let _serial = TEST_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let config = files(dir.path(), false);
    let h = mount(config.clone(), &dir.path().join("cordis.toml")).await;
    let settings = h.runtime.root().get(SETTINGS).unwrap();
    let actual = h
        .runtime
        .root()
        .get(LOADER)
        .unwrap()
        .entry_context("include:one")
        .unwrap()
        .get(RUNNING)
        .unwrap();
    let expected_limit = actual.get().value.limit;
    let before_calls = DEFAULT_CALLS.load(Ordering::SeqCst);
    let before = settings.describe("include:one").await.unwrap();
    for _ in 0..3 {
        let view = settings.describe("include:one").await.unwrap();
        let value = view.value.as_ref().unwrap();
        assert_eq!(value["limit"], expected_limit);
        assert_ne!(value["limit"], schema_default());
        assert_eq!(value["nested"]["timeout"], 17);
        assert!(value["note"].is_null());
        assert!(value.get("token").is_none());
        assert!(value.get("serialized_token").is_none());
        assert!(view.config.get("limit").is_none());
        assert!(!format!("{view:?}").contains("server-default-secret"));
    }
    assert_eq!(DEFAULT_CALLS.load(Ordering::SeqCst), before_calls);
    let after = settings
        .mutate("include:one", before.revision, vec![set("note", "hello")])
        .await
        .unwrap();
    assert_eq!(after.value.as_ref().unwrap()["note"], "hello");
    let user = UserPatchStore::new(config.user_patch_path)
        .load()
        .await
        .unwrap();
    let raw = user.config_for("one").unwrap();
    assert_eq!(raw["note"].as_str(), Some("hello"));
    for key in ["limit", "nested", "token", "tag"] {
        assert!(raw.get(key).is_none());
    }
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn inactive_and_failed_instances_do_not_claim_an_applied_value() {
    let _serial = TEST_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let config = files(dir.path(), true);
    let h = mount(config, &dir.path().join("cordis.toml")).await;
    let settings = h.runtime.root().get(SETTINGS).unwrap();
    assert!(
        settings
            .describe("include:one")
            .await
            .unwrap()
            .value
            .is_none()
    );
    h.runtime
        .root()
        .get(CONFIG_EDITOR)
        .unwrap()
        .set_enabled("include:one", true)
        .await
        .unwrap();
    let before = settings.describe("include:one").await.unwrap();
    assert!(before.value.is_some());
    assert!(
        settings
            .mutate("include:one", before.revision, vec![set("address", "fail")])
            .await
            .is_err()
    );
    let failed = settings.describe("include:one").await.unwrap();
    assert!(failed.value.is_none());
    assert_eq!(failed.config["address"].as_str(), Some("ok"));
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn independent_loaders_reject_external_edits_then_reload_without_losing_other_fields() {
    let _serial = TEST_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let config = files(dir.path(), false);
    let root = dir.path().join("cordis.toml");
    let a = mount(config.clone(), &root).await;
    let b = mount(config.clone(), &root).await;
    let sa = a.runtime.root().get(SETTINGS).unwrap();
    let sb = b.runtime.root().get(SETTINGS).unwrap();
    let va = sa.describe("include:one").await.unwrap();
    let vb = sb.describe("include:one").await.unwrap();
    sa.mutate("include:one", va.revision, vec![set("note", "from-a")])
        .await
        .unwrap();
    let before = std::fs::read(&config.user_patch_path).unwrap();
    assert!(matches!(
        sb.mutate("include:one", vb.revision, vec![set("tag", "from-b")])
            .await,
        Err(LoaderControlError::ConfigConflict { .. })
    ));
    assert_eq!(std::fs::read(&config.user_patch_path).unwrap(), before);
    b.runtime
        .root()
        .get(CONFIG_EDITOR)
        .unwrap()
        .reload()
        .await
        .unwrap();
    let current = sb.describe("include:one").await.unwrap();
    sb.mutate("include:one", current.revision, vec![set("tag", "from-b")])
        .await
        .unwrap();
    let user = UserPatchStore::new(&config.user_patch_path)
        .load()
        .await
        .unwrap();
    let raw = user.config_for("one").unwrap();
    assert_eq!(raw["note"].as_str(), Some("from-a"));
    assert_eq!(raw["tag"].as_str(), Some("from-b"));
    a.runtime.shutdown().await.unwrap();
    b.runtime.shutdown().await.unwrap();
}
