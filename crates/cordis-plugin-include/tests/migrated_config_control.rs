use async_trait::async_trait;
use cordis_core::{
    Context, CoreError, Fiber, FiberState, Plugin, PluginKey, Runtime, ServiceId, ServiceKey,
};
use cordis_loader::{
    ConfigRef, EntryOptions, ExtensionCatalog, ExtensionFactory, ExtensionsConfig, LOADER, Loader,
    LoaderControlError, LoaderError, LoaderPlugin, PreparedExtension,
};
use cordis_plugin_config_editor::{
    CONFIG_EDITOR, ConfigEditorConfig, ConfigEditorPlugin, PATCH_FILE_VERSION, PatchFileDocument,
};
use cordis_plugin_include::EntryPatch;
use cordis_plugin_settings::{ConfigOperation, SETTINGS, SettingsPlugin, SettingsService};
use schemars::JsonSchema;
use serde::Deserialize;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

static MISSING: ServiceKey<()> = ServiceKey::new("config.control.missing");
#[derive(serde::Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Input {
    address: String,
    limit: u32,
    #[serde(default)]
    #[schemars(extend("writeOnly" = true))]
    token: Option<String>,
}
#[derive(Default)]
struct Counts {
    prepares: AtomicUsize,
    builds: AtomicUsize,
    applies: AtomicUsize,
    persist_fault: std::sync::Mutex<Option<std::path::PathBuf>>,
    started: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
struct Factory(Arc<Counts>);
struct Instance {
    address: String,
    live: ConfigRef<u32>,
    counts: Arc<Counts>,
}
impl ExtensionFactory for Factory {
    type Config = Input;
    fn id(&self) -> &'static str {
        "editable"
    }
    fn prepare(&self, input: Input) -> Result<PreparedExtension, LoaderError> {
        self.0.prepares.fetch_add(1, Ordering::SeqCst);
        assert!(input.address != "panic", "private-panic");
        if input.limit == 0 {
            return Err(LoaderError::invalid_config("private-validation"));
        }
        let counts = self.0.clone();
        Ok(PreparedExtension::volatile(
            (input.address, input.token),
            input.limit,
            move |(address, _), live| {
                counts.builds.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(Instance {
                    address,
                    live,
                    counts,
                }))
            },
        ))
    }
}
#[async_trait]
impl Plugin for Instance {
    fn key(&self) -> PluginKey {
        PluginKey::new("editable")
    }
    fn inject(&self) -> Vec<ServiceId> {
        if self.address == "pending" {
            vec![MISSING.id()]
        } else {
            vec![]
        }
    }
    async fn apply(&self, _: &Context) -> Result<(), CoreError> {
        self.counts.applies.fetch_add(1, Ordering::SeqCst);
        // 权限/写回故障发生在预判之后，才能验证已生效状态上的 Persist 语义。
        if let Some(path) = self.counts.persist_fault.lock().unwrap().take() {
            std::fs::remove_file(&path).unwrap();
            std::fs::create_dir(path).unwrap();
        }
        if self.address == "blocked" {
            self.counts.started.notify_one();
            self.counts.release.notified().await;
        }
        let _ = self.live.get();
        if self.address == "fail" {
            Err(CoreError::PluginApply("deliberate failure".into()))
        } else {
            Ok(())
        }
    }
}
struct Harness {
    runtime: Runtime,
    loader: Loader,
    settings: SettingsService,
    _fibers: Vec<Fiber>,
    dir: tempfile::TempDir,
    counts: Arc<Counts>,
}
impl Harness {
    async fn new(address: &str, disabled: bool) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut entry = EntryOptions::new("one", "editable")
            .with_config(toml::toml! { address = address limit = 1 token = "secret-value" }.into());
        entry.disabled = disabled;
        std::fs::write(
            dir.path().join("cordis.toml"),
            "version = 3\n\nextensions = []\n",
        )
        .unwrap();
        let deploy = PatchFileDocument {
            version: PATCH_FILE_VERSION,
            patches: vec![EntryPatch {
                insert: Some(vec![entry]),
                ..Default::default()
            }],
        };
        std::fs::write(
            dir.path().join("deploy.patches.toml"),
            toml::to_string_pretty(&deploy).unwrap(),
        )
        .unwrap();
        std::fs::write(dir.path().join("home.patches.toml"), "version = 1\n").unwrap();
        let counts = Arc::new(Counts::default());
        let mut catalog = ExtensionCatalog::new();
        catalog.register(Factory(counts.clone())).unwrap();
        catalog
            .register(cordis_plugin_include::IncludeFactory)
            .unwrap();
        let runtime = Runtime::new().unwrap();
        let (fibers, settings) = mount_file(
            &runtime,
            catalog,
            dir.path().join("cordis.toml"),
            dir.path().join("deploy.patches.toml"),
            dir.path().join("user.patches.toml"),
            dir.path().join("home.patches.toml"),
        )
        .await;
        let loader = (*runtime.root().get(LOADER).unwrap()).clone();
        Self {
            runtime,
            loader,
            settings,
            _fibers: fibers,
            dir,
            counts,
        }
    }
    fn file(&self) -> String {
        std::fs::read_to_string(self.dir.path().join("deploy.patches.toml")).unwrap()
    }
    fn user_file(&self) -> String {
        let path = self.dir.path().join("user.patches.toml");
        if path.is_file() {
            std::fs::read_to_string(path).unwrap()
        } else {
            String::new()
        }
    }
}
fn set(key: &str, value: impl Into<toml::Value>) -> ConfigOperation {
    ConfigOperation::Set {
        path: vec![key.into()],
        value: value.into(),
    }
}

#[tokio::test]
async fn disabled_edit_validates_once_without_constructing_and_preserves_secrets() {
    let h = Harness::new("ok", true).await;
    assert_eq!(h.counts.prepares.load(Ordering::SeqCst), 0);
    let before = h.settings.describe("include:one").await.unwrap();
    assert!(before.user.is_none());
    assert!(before.config.get("token").is_none());
    assert_eq!(before.secrets[0].path, ["token"]);
    assert!(before.secrets[0].is_set);
    assert!(!format!("{before:?}").contains("secret-value"));
    let base_before = h.file();
    let after = h
        .settings
        .mutate("include:one", before.revision, vec![set("limit", 2)])
        .await
        .unwrap();
    assert_ne!(before.revision, after.revision);
    assert!(after.user.is_some());
    assert_eq!(h.counts.prepares.load(Ordering::SeqCst), 1);
    assert_eq!(h.counts.builds.load(Ordering::SeqCst), 0);
    assert_eq!(h.file(), base_before, "表单不得 bake 进 Include path");
    assert!(h.user_file().contains("secret-value"));
    assert!(matches!(
        h.settings
            .mutate("include:one", before.revision, vec![set("limit", 3)])
            .await,
        Err(LoaderControlError::ConfigRevisionConflict)
    ));
    let after = h
        .settings
        .mutate(
            "include:one",
            after.revision,
            vec![ConfigOperation::Unset {
                path: vec!["token".into()],
            }],
        )
        .await
        .unwrap();
    assert!(!after.secrets[0].is_set);
    assert!(h.file().contains("secret-value"), "base 源文件仍保留秘密");
    assert!(!h.user_file().contains("secret-value"));
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn active_hot_and_static_changes_use_existing_reconcile_and_one_prepare() {
    let h = Harness::new("ok", false).await;
    let before = h
        .loader
        .await_idle()
        .await
        .unwrap()
        .entry("include:one")
        .unwrap()
        .fiber_id;
    let form = h.settings.describe("include:one").await.unwrap();
    let prepares = h.counts.prepares.load(Ordering::SeqCst);
    let form = h
        .settings
        .mutate("include:one", form.revision, vec![set("limit", 2)])
        .await
        .unwrap();
    assert_eq!(h.counts.prepares.load(Ordering::SeqCst), prepares + 1);
    assert_eq!(h.counts.builds.load(Ordering::SeqCst), 1);
    assert_eq!(h.counts.applies.load(Ordering::SeqCst), 1);
    assert_eq!(
        h.loader
            .await_idle()
            .await
            .unwrap()
            .entry("include:one")
            .unwrap()
            .fiber_id,
        before
    );
    h.settings
        .mutate(
            "include:one",
            form.revision,
            vec![set("address", "changed")],
        )
        .await
        .unwrap();
    assert_eq!(h.counts.builds.load(Ordering::SeqCst), 2);
    assert_eq!(h.counts.applies.load(Ordering::SeqCst), 2);
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_input_does_not_change_file_or_revision_and_does_not_echo_values() {
    let h = Harness::new("ok", false).await;
    let before = h.settings.describe("include:one").await.unwrap();
    let file = h.file();
    let user = h.user_file();
    for operation in [
        set("limit", "sensitive-invalid"),
        set("limit", 0),
        set("address", "panic"),
    ] {
        let error = h
            .settings
            .mutate("include:one", before.revision, vec![operation])
            .await
            .unwrap_err();
        assert!(matches!(error, LoaderControlError::ConfigValidation));
        assert!(!error.to_string().contains("private"));
        assert!(!error.to_string().contains("sensitive"));
        assert_eq!(h.file(), file);
        assert_eq!(h.user_file(), user);
        assert_eq!(
            h.settings.describe("include:one").await.unwrap().revision,
            before.revision
        );
    }
    let error = h
        .settings
        .mutate(
            "include:one",
            before.revision,
            vec![ConfigOperation::Set {
                path: vec!["limit".into(), "0".into()],
                value: 1.into(),
            }],
        )
        .await
        .unwrap_err();
    assert!(matches!(error, LoaderControlError::InvalidConfigPath));
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_and_pending_instances_can_be_fixed_and_failure_invalidates_revision() {
    for address in ["fail", "pending"] {
        let h = Harness::new(address, false).await;
        let before = h.settings.describe("include:one").await.unwrap();
        let after = h
            .settings
            .mutate("include:one", before.revision, vec![set("address", "ok")])
            .await
            .unwrap();
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
        let file = h.file();
        let user_before = h.user_file();
        // 生命周期失败返回错误，保留已提交声明和 user 文件；编辑器仍可用于修复。
        let error = h
            .settings
            .mutate("include:one", after.revision, vec![set("address", "fail")])
            .await
            .unwrap_err();
        assert!(error.is_entry_lifecycle_failure());
        assert_eq!(h.file(), file);
        assert_eq!(h.user_file(), user_before);
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
        let failed = h.settings.describe("include:one").await.unwrap();
        assert_eq!(failed.config["address"].as_str(), Some("ok"));
        assert_ne!(failed.revision, after.revision);
        h.settings
            .mutate("include:one", failed.revision, vec![set("address", "ok")])
            .await
            .unwrap();
        h.runtime.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn enabling_a_failing_plugin_reports_error_and_preserves_user_and_declaration() {
    let h = Harness::new("fail", true).await;
    let before = h.settings.describe("include:one").await.unwrap();
    let user_before = h.user_file();
    let editor = h.runtime.root().get(CONFIG_EDITOR).unwrap();
    let error = editor.set_enabled("include:one", true).await.unwrap_err();
    assert!(error.is_entry_lifecycle_failure());
    assert_eq!(h.user_file(), user_before);
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
    let tree = h.loader.subtree("include").unwrap();
    assert!(tree.entries().unwrap().extensions[0].disabled);
    let after = h.settings.describe("include:one").await.unwrap();
    assert_ne!(after.revision, before.revision);
    h.settings
        .mutate("include:one", after.revision, vec![set("address", "ok")])
        .await
        .unwrap();
    editor.set_enabled("include:one", true).await.unwrap();
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
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn concurrent_edits_conflict_and_external_base_edit_is_explicit() {
    let h = Harness::new("ok", false).await;
    let form = h.settings.describe("include:one").await.unwrap();
    let (a, b) = tokio::join!(
        h.settings
            .mutate("include:one", form.revision, vec![set("limit", 2)]),
        h.settings
            .mutate("include:one", form.revision, vec![set("limit", 3)])
    );
    assert_ne!(a.is_ok(), b.is_ok());
    assert!(matches!(
        a.err().or(b.err()).unwrap(),
        LoaderControlError::ConfigRevisionConflict
    ));
    let form = h.settings.describe("include:one").await.unwrap();
    std::fs::write(
        h.dir.path().join("cordis.toml"),
        format!(
            "{}\n# outside edit\n",
            std::fs::read_to_string(h.dir.path().join("cordis.toml")).unwrap()
        ),
    )
    .unwrap();
    // 同步 user→Include.patches 时 FileSource 仍检查来源版本。
    assert!(matches!(
        h.settings
            .mutate("include:one", form.revision, vec![set("limit", 4)])
            .await,
        Err(LoaderControlError::ConfigConflict { .. })
    ));
    assert!(!h.file().contains("limit = 4"));
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn ordinary_update_invalidates_config_form_and_shutdown_rejects_reads() {
    let h = Harness::new("ok", true).await;
    let form = h.settings.describe("include:one").await.unwrap();
    h.runtime
        .root()
        .get(CONFIG_EDITOR)
        .unwrap()
        .set_enabled("include:one", true)
        .await
        .unwrap();
    assert!(matches!(
        h.settings
            .mutate("include:one", form.revision, vec![set("limit", 4)])
            .await,
        Err(LoaderControlError::ConfigRevisionConflict)
    ));
    h.runtime.shutdown().await.unwrap();
    assert!(matches!(
        h.settings.describe("include:one").await,
        Err(LoaderControlError::LoaderUnavailable)
    ));
}

#[tokio::test]
async fn nested_include_is_readable_but_not_editable_via_root_user_layer() {
    let dir = tempfile::tempdir().unwrap();
    let child_path = dir.path().join("child.toml");
    let child = EntryOptions::new("child", "editable")
        .with_config(toml::toml! { address = "child" limit = 1 }.into());
    std::fs::write(
        &child_path,
        toml::to_string(&ExtensionsConfig {
            version: 3,
            extensions: vec![EntryOptions::group("group", vec![child]).unwrap()],
        })
        .unwrap(),
    )
    .unwrap();
    let child_name = child_path.to_str().unwrap();
    let nested = EntryOptions::new("nested", "cordis:include")
        .with_config(toml::toml! { path = child_name }.into());
    let mut one = EntryOptions::new("one", "editable")
        .with_config(toml::toml! { address = "ok" limit = 1 token = "secret-value" }.into());
    one.disabled = false;
    std::fs::write(
        dir.path().join("cordis.toml"),
        "version = 3\n\nextensions = []\n",
    )
    .unwrap();
    let deploy = PatchFileDocument {
        version: PATCH_FILE_VERSION,
        patches: vec![EntryPatch {
            insert: Some(vec![one, nested]),
            ..Default::default()
        }],
    };
    std::fs::write(
        dir.path().join("deploy.patches.toml"),
        toml::to_string_pretty(&deploy).unwrap(),
    )
    .unwrap();
    std::fs::write(dir.path().join("home.patches.toml"), "version = 1\n").unwrap();
    let counts = Arc::new(Counts::default());
    let mut catalog = ExtensionCatalog::new();
    catalog.register(Factory(counts.clone())).unwrap();
    catalog
        .register(cordis_plugin_include::IncludeFactory)
        .unwrap();
    let runtime = Runtime::new().unwrap();
    let (_fibers, settings) = mount_file(
        &runtime,
        catalog,
        dir.path().join("cordis.toml"),
        dir.path().join("deploy.patches.toml"),
        dir.path().join("user.patches.toml"),
        dir.path().join("home.patches.toml"),
    )
    .await;
    let loader = (*runtime.root().get(LOADER).unwrap()).clone();
    let child_before = std::fs::read_to_string(&child_path).unwrap();
    let root_fiber = loader
        .await_idle()
        .await
        .unwrap()
        .entry("include:one")
        .unwrap()
        .fiber_id;
    let tree = loader.subtree("include:nested").unwrap();
    let form = settings
        .describe("include:nested:group:child")
        .await
        .unwrap();
    assert_eq!(form.path.as_str(), "include:nested:group:child");
    assert!(matches!(
        settings
            .mutate(
                "include:nested:group:child",
                form.revision,
                vec![set("limit", 8)],
            )
            .await,
        Err(LoaderControlError::ConfigNotEditable)
    ));
    assert_eq!(std::fs::read_to_string(&child_path).unwrap(), child_before);
    assert_eq!(
        loader
            .await_idle()
            .await
            .unwrap()
            .entry("include:one")
            .unwrap()
            .fiber_id,
        root_fiber
    );
    assert!(matches!(
        settings.describe("include:nested:group").await,
        Err(LoaderControlError::ConfigNotEditable)
    ));
    runtime
        .root()
        .get(CONFIG_EDITOR)
        .unwrap()
        .set_enabled("include:nested", false)
        .await
        .unwrap();
    assert!(tree.entries().is_err());
    assert!(matches!(
        settings.describe("include:nested:group:child").await,
        Err(LoaderControlError::EntryNotFound { .. })
    ));
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn form_mutate_writes_user_and_leaves_base_file_unchanged() {
    let h = Harness::new("ok", false).await;
    let form = h.settings.describe("include:one").await.unwrap();
    let base_before = h.file();
    let current = h
        .settings
        .mutate(
            "include:one",
            form.revision,
            vec![set("address", "updated"), set("limit", 9)],
        )
        .await
        .unwrap();
    assert_eq!(current.config["limit"].as_integer(), Some(9));
    assert_eq!(current.config["address"].as_str(), Some("updated"));
    assert!(current.user.is_some());
    assert_ne!(current.revision, form.revision);
    assert_eq!(h.file(), base_before);
    assert!(h.user_file().contains("updated"));
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn ordered_operations_and_secret_replacement_do_not_restore_deleted_fields() {
    let h = Harness::new("ok", true).await;
    let form = h.settings.describe("include:one").await.unwrap();
    let form = h
        .settings
        .mutate(
            "include:one",
            form.revision,
            vec![set("limit", 2), set("limit", 3), set("token", "new-secret")],
        )
        .await
        .unwrap();
    assert_eq!(form.config["limit"].as_integer(), Some(3));
    assert!(form.config.get("token").is_none());
    assert!(!format!("{form:?}").contains("new-secret"));
    assert!(!h.file().contains("new-secret"));
    assert!(h.user_file().contains("new-secret"));
    let before = h.file();
    let user_before = h.user_file();
    assert!(matches!(
        h.settings
            .mutate(
                "include:one",
                form.revision,
                vec![ConfigOperation::Unset {
                    path: vec!["address".into()]
                }]
            )
            .await,
        Err(LoaderControlError::ConfigValidation)
    ));
    assert_eq!(h.file(), before);
    assert_eq!(h.user_file(), user_before);
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn describe_waits_for_in_flight_config_commit_and_returns_matching_revision() {
    let h = Harness::new("ok", false).await;
    let before = h.settings.describe("include:one").await.unwrap();
    let settings = h.settings.clone();
    let update = tokio::spawn(async move {
        settings
            .mutate(
                "include:one",
                before.revision,
                vec![set("address", "blocked")],
            )
            .await
    });
    h.counts.started.notified().await;
    let settings = h.settings.clone();
    let read = tokio::spawn(async move { settings.describe("include:one").await });
    tokio::task::yield_now().await;
    assert!(!read.is_finished());
    h.counts.release.notify_one();
    let committed = update.await.unwrap().unwrap();
    let observed = read.await.unwrap().unwrap();
    assert_eq!(observed.revision, committed.revision);
    assert_eq!(observed.config["address"].as_str(), Some("blocked"));
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn instance_edits_preserve_other_instance_config_and_fiber() {
    let dir = tempfile::tempdir().unwrap();
    let one = EntryOptions::new("one", "editable")
        .with_config(toml::toml! { address = "ok" limit = 1 token = "secret-value" }.into());
    let two = EntryOptions::new("two", "editable")
        .with_config(toml::toml! { address = "second" limit = 7 }.into());
    std::fs::write(
        dir.path().join("cordis.toml"),
        "version = 3\n\nextensions = []\n",
    )
    .unwrap();
    let deploy = PatchFileDocument {
        version: PATCH_FILE_VERSION,
        patches: vec![EntryPatch {
            insert: Some(vec![one, two]),
            ..Default::default()
        }],
    };
    std::fs::write(
        dir.path().join("deploy.patches.toml"),
        toml::to_string_pretty(&deploy).unwrap(),
    )
    .unwrap();
    std::fs::write(dir.path().join("home.patches.toml"), "version = 1\n").unwrap();
    let counts = Arc::new(Counts::default());
    let mut catalog = ExtensionCatalog::new();
    catalog.register(Factory(counts)).unwrap();
    catalog
        .register(cordis_plugin_include::IncludeFactory)
        .unwrap();
    let runtime = Runtime::new().unwrap();
    let (_fibers, settings) = mount_file(
        &runtime,
        catalog,
        dir.path().join("cordis.toml"),
        dir.path().join("deploy.patches.toml"),
        dir.path().join("user.patches.toml"),
        dir.path().join("home.patches.toml"),
    )
    .await;
    let loader = (*runtime.root().get(LOADER).unwrap()).clone();
    let other = loader
        .await_idle()
        .await
        .unwrap()
        .entry("include:two")
        .unwrap()
        .fiber_id;
    let form = settings.describe("include:one").await.unwrap();
    settings
        .mutate("include:one", form.revision, vec![set("limit", 4)])
        .await
        .unwrap();
    assert_eq!(
        settings.describe("include:two").await.unwrap().config["limit"].as_integer(),
        Some(7)
    );
    assert_eq!(
        loader
            .await_idle()
            .await
            .unwrap()
            .entry("include:two")
            .unwrap()
            .fiber_id,
        other
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn revision_roundtrips_as_an_opaque_transport_token() {
    let h = Harness::new("ok", true).await;
    let revision = h.settings.describe("include:one").await.unwrap().revision;
    assert_eq!(
        revision
            .to_string()
            .parse::<cordis_loader::ConfigRevision>()
            .unwrap(),
        revision
    );
    for invalid in [
        "",
        "1:1",
        "00000000000000000000000000000000:000000000000000g",
    ] {
        assert!(matches!(
            invalid.parse::<cordis_loader::ConfigRevision>(),
            Err(LoaderControlError::InvalidConfigRevision)
        ));
    }
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelling_the_waiter_does_not_skip_persistence_after_reconcile() {
    let h = Harness::new("ok", false).await;
    let before = h.settings.describe("include:one").await.unwrap();
    let settings = h.settings.clone();
    let waiter = tokio::spawn(async move {
        settings
            .mutate(
                "include:one",
                before.revision,
                vec![set("address", "blocked")],
            )
            .await
    });
    h.counts.started.notified().await;
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    h.counts.release.notify_one();
    let after = h.settings.describe("include:one").await.unwrap();
    assert_eq!(after.config["address"].as_str(), Some("blocked"));
    assert!(!h.file().contains("blocked"));
    assert!(h.user_file().contains("blocked"));
    h.runtime.shutdown().await.unwrap();
}

async fn mount_file(
    runtime: &Runtime,
    catalog: ExtensionCatalog,
    cordis: impl AsRef<std::path::Path>,
    deploy_patch: impl AsRef<std::path::Path>,
    user_patch: impl AsRef<std::path::Path>,
    home_patch: impl AsRef<std::path::Path>,
) -> (Vec<Fiber>, SettingsService) {
    let config = ConfigEditorConfig::new(
        vec![deploy_patch.as_ref().to_path_buf()],
        user_patch.as_ref(),
        home_patch.as_ref(),
    );
    let entry = cordis_plugin_config_editor::prepare_root_include(cordis, &config)
        .await
        .unwrap();
    let loader_fiber = runtime
        .root()
        .plugin(Arc::new(LoaderPlugin::new(catalog)))
        .await
        .unwrap();
    runtime
        .root()
        .get(LOADER)
        .unwrap()
        .create(entry, None, None)
        .await
        .unwrap();
    let editor_fiber = runtime
        .root()
        .plugin(Arc::new(ConfigEditorPlugin::new(config)))
        .await
        .unwrap();
    let settings_fiber = runtime
        .root()
        .plugin(Arc::new(SettingsPlugin))
        .await
        .unwrap();
    let settings = (*runtime.root().get(SETTINGS).unwrap()).clone();
    (vec![loader_fiber, editor_fiber, settings_fiber], settings)
}
