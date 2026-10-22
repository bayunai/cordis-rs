use async_trait::async_trait;
use cordis_core::{
    Context, CoreError, Fiber, FiberState, Plugin, PluginKey, Runtime, ServiceId, ServiceKey,
};
use cordis_loader::{
    ConfigOperation, ConfigRef, EntryOptions, EntryUpdate, ExtensionCatalog, ExtensionFactory,
    ExtensionsConfig, LOADER, Loader, LoaderControlError, LoaderError, LoaderPlugin,
    PreparedExtension,
};
use schemars::JsonSchema;
use serde::Deserialize;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

static MISSING: ServiceKey<()> = ServiceKey::new("config.control.missing");
#[derive(Deserialize, JsonSchema)]
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
        if let Some(path) = self.0.persist_fault.lock().unwrap().take() {
            std::fs::remove_file(&path).unwrap();
            std::fs::create_dir(path).unwrap();
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
    _fiber: Fiber,
    dir: tempfile::TempDir,
    counts: Arc<Counts>,
}
impl Harness {
    async fn new(address: &str, disabled: bool) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut entry = EntryOptions::new("one", "editable")
            .with_config(toml::toml! { address = address limit = 1 token = "secret-value" }.into());
        entry.disabled = disabled;
        let config = ExtensionsConfig {
            version: 3,
            extensions: vec![entry],
        };
        std::fs::write(
            dir.path().join("extensions.toml"),
            toml::to_string(&config).unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join("bootstrap.toml"),
            "version = 3\n[config]\ndriver = 'file'\npath = 'extensions.toml'\n",
        )
        .unwrap();
        let counts = Arc::new(Counts::default());
        let mut catalog = ExtensionCatalog::new();
        catalog.register(Factory(counts.clone())).unwrap();
        catalog
            .register(cordis_plugin_include::IncludeFactory)
            .unwrap();
        let runtime = Runtime::new().unwrap();
        let fiber = runtime
            .root()
            .plugin(Arc::new(
                LoaderPlugin::bootstrap(catalog, dir.path().join("bootstrap.toml")).unwrap(),
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
        }
    }
    fn file(&self) -> String {
        std::fs::read_to_string(self.dir.path().join("extensions.toml")).unwrap()
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
    let before = h.loader.describe_config("one").await.unwrap();
    assert!(before.value.get("token").is_none());
    assert_eq!(before.secrets[0].path, ["token"]);
    assert!(before.secrets[0].is_set);
    assert!(!format!("{before:?}").contains("secret-value"));
    let after = h
        .loader
        .mutate_config("one", before.revision, vec![set("limit", 2)])
        .await
        .unwrap();
    assert_ne!(before.revision, after.revision);
    assert_eq!(h.counts.prepares.load(Ordering::SeqCst), 1);
    assert_eq!(h.counts.builds.load(Ordering::SeqCst), 0);
    assert!(h.file().contains("secret-value"));
    assert!(matches!(
        h.loader
            .mutate_config("one", before.revision, vec![set("limit", 3)])
            .await,
        Err(LoaderControlError::ConfigRevisionConflict)
    ));
    let after = h
        .loader
        .mutate_config(
            "one",
            after.revision,
            vec![ConfigOperation::Unset {
                path: vec!["token".into()],
            }],
        )
        .await
        .unwrap();
    assert!(!after.secrets[0].is_set);
    assert!(!h.file().contains("secret-value"));
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
        .entry("one")
        .unwrap()
        .fiber_id;
    let form = h.loader.describe_config("one").await.unwrap();
    let prepares = h.counts.prepares.load(Ordering::SeqCst);
    let form = h
        .loader
        .mutate_config("one", form.revision, vec![set("limit", 2)])
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
            .entry("one")
            .unwrap()
            .fiber_id,
        before
    );
    h.loader
        .mutate_config("one", form.revision, vec![set("address", "changed")])
        .await
        .unwrap();
    assert_eq!(h.counts.builds.load(Ordering::SeqCst), 2);
    assert_eq!(h.counts.applies.load(Ordering::SeqCst), 2);
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_input_does_not_change_file_or_revision_and_does_not_echo_values() {
    let h = Harness::new("ok", false).await;
    let before = h.loader.describe_config("one").await.unwrap();
    let file = h.file();
    for operation in [
        set("limit", "sensitive-invalid"),
        set("limit", 0),
        set("address", "panic"),
    ] {
        let error = h
            .loader
            .mutate_config("one", before.revision, vec![operation])
            .await
            .unwrap_err();
        assert!(matches!(error, LoaderControlError::ConfigValidation));
        assert!(!error.to_string().contains("private"));
        assert!(!error.to_string().contains("sensitive"));
        assert_eq!(h.file(), file);
        assert_eq!(
            h.loader.describe_config("one").await.unwrap().revision,
            before.revision
        );
    }
    let error = h
        .loader
        .mutate_config(
            "one",
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
        let before = h.loader.describe_config("one").await.unwrap();
        let after = h
            .loader
            .mutate_config("one", before.revision, vec![set("address", "ok")])
            .await
            .unwrap();
        assert_eq!(
            h.loader
                .await_idle()
                .await
                .unwrap()
                .entry("one")
                .unwrap()
                .state,
            Some(FiberState::Active)
        );
        let file = h.file();
        assert!(
            h.loader
                .mutate_config("one", after.revision, vec![set("address", "fail")])
                .await
                .is_err()
        );
        assert_eq!(h.file(), file);
        let retry = h.loader.describe_config("one").await.unwrap();
        assert_ne!(retry.revision, after.revision);
        h.loader
            .mutate_config("one", retry.revision, vec![set("address", "ok")])
            .await
            .unwrap();
        h.runtime.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn external_file_conflict_and_concurrent_edits_are_explicit() {
    let h = Harness::new("ok", false).await;
    let form = h.loader.describe_config("one").await.unwrap();
    let (a, b) = tokio::join!(
        h.loader
            .mutate_config("one", form.revision, vec![set("limit", 2)]),
        h.loader
            .mutate_config("one", form.revision, vec![set("limit", 3)])
    );
    assert_ne!(a.is_ok(), b.is_ok());
    assert!(matches!(
        a.err().or(b.err()).unwrap(),
        LoaderControlError::ConfigRevisionConflict
    ));
    let form = h.loader.describe_config("one").await.unwrap();
    std::fs::write(
        h.dir.path().join("extensions.toml"),
        format!("{}\n# outside edit\n", h.file()),
    )
    .unwrap();
    assert!(matches!(
        h.loader
            .mutate_config("one", form.revision, vec![set("limit", 4)])
            .await,
        Err(LoaderControlError::ConfigConflict { .. })
    ));
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn ordinary_update_invalidates_config_form_and_shutdown_rejects_reads() {
    let h = Harness::new("ok", true).await;
    let form = h.loader.describe_config("one").await.unwrap();
    h.loader
        .update(
            "one",
            EntryUpdate {
                disabled: Some(false),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap();
    assert!(matches!(
        h.loader
            .mutate_config("one", form.revision, vec![set("limit", 4)])
            .await,
        Err(LoaderControlError::ConfigRevisionConflict)
    ));
    h.runtime.shutdown().await.unwrap();
    assert!(matches!(
        h.loader.describe_config("one").await,
        Err(LoaderControlError::LoaderUnavailable)
    ));
}

#[tokio::test]
async fn include_edits_write_only_the_owning_file_and_recreated_trees_reject_old_tokens() {
    let h = Harness::new("ok", false).await;
    let child_path = h.dir.path().join("child.toml");
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
    h.loader
        .create(
            EntryOptions::new("include", "cordis:include")
                .with_config(toml::toml! { path = child_name }.into()),
            None,
            None,
        )
        .await
        .unwrap();
    let root_before = h.file();
    let root_fiber = h
        .loader
        .await_idle()
        .await
        .unwrap()
        .entry("one")
        .unwrap()
        .fiber_id;
    let tree = h.loader.subtree("include").unwrap();
    let form = h
        .loader
        .describe_config("include:group:child")
        .await
        .unwrap();
    assert_eq!(form.path.as_str(), "include:group:child");
    assert_eq!(
        tree.describe_config("group:child").await.unwrap().revision,
        form.revision
    );
    let form = tree
        .mutate_config("group:child", form.revision, vec![set("limit", 8)])
        .await
        .unwrap();
    assert_eq!(h.file(), root_before);
    assert!(
        std::fs::read_to_string(&child_path)
            .unwrap()
            .contains("limit = 8")
    );
    assert_eq!(
        h.loader
            .await_idle()
            .await
            .unwrap()
            .entry("one")
            .unwrap()
            .fiber_id,
        root_fiber
    );
    assert!(matches!(
        tree.describe_config("group").await,
        Err(LoaderControlError::ConfigNotEditable)
    ));
    h.loader
        .update(
            "include",
            EntryUpdate {
                disabled: Some(true),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap();
    assert!(matches!(
        tree.describe_config("group:child").await,
        Err(LoaderControlError::LoaderUnavailable)
    ));
    h.loader
        .update(
            "include",
            EntryUpdate {
                disabled: Some(false),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap();
    assert!(matches!(
        h.loader
            .mutate_config("include:group:child", form.revision, vec![set("limit", 9)])
            .await,
        Err(LoaderControlError::ConfigRevisionConflict)
    ));
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn persistence_failure_reports_applied_state_and_invalidates_old_revision() {
    let h = Harness::new("ok", false).await;
    let form = h.loader.describe_config("one").await.unwrap();
    *h.counts.persist_fault.lock().unwrap() = Some(h.dir.path().join("extensions.toml"));
    assert!(matches!(
        h.loader
            .mutate_config("one", form.revision, vec![set("limit", 9)])
            .await,
        Err(LoaderControlError::Persist { .. })
    ));
    let current = h.loader.describe_config("one").await.unwrap();
    assert_eq!(current.value["limit"].as_integer(), Some(9));
    assert_ne!(current.revision, form.revision);
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn ordered_operations_and_secret_replacement_do_not_restore_deleted_fields() {
    let h = Harness::new("ok", true).await;
    let form = h.loader.describe_config("one").await.unwrap();
    let form = h
        .loader
        .mutate_config(
            "one",
            form.revision,
            vec![set("limit", 2), set("limit", 3), set("token", "new-secret")],
        )
        .await
        .unwrap();
    assert_eq!(form.value["limit"].as_integer(), Some(3));
    assert!(form.value.get("token").is_none());
    assert!(!format!("{form:?}").contains("new-secret"));
    assert!(h.file().contains("new-secret"));
    let before = h.file();
    assert!(matches!(
        h.loader
            .mutate_config(
                "one",
                form.revision,
                vec![ConfigOperation::Unset {
                    path: vec!["address".into()]
                }]
            )
            .await,
        Err(LoaderControlError::ConfigValidation)
    ));
    assert_eq!(h.file(), before);
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn describe_waits_for_in_flight_config_commit_and_returns_matching_revision() {
    let h = Harness::new("ok", false).await;
    let before = h.loader.describe_config("one").await.unwrap();
    let loader = h.loader.clone();
    let update = tokio::spawn(async move {
        loader
            .mutate_config("one", before.revision, vec![set("address", "blocked")])
            .await
    });
    h.counts.started.notified().await;
    let loader = h.loader.clone();
    let read = tokio::spawn(async move { loader.describe_config("one").await });
    tokio::task::yield_now().await;
    assert!(!read.is_finished());
    h.counts.release.notify_one();
    let committed = update.await.unwrap().unwrap();
    let observed = read.await.unwrap().unwrap();
    assert_eq!(observed.revision, committed.revision);
    assert_eq!(observed.value["address"].as_str(), Some("blocked"));
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn instance_edits_preserve_other_instance_config_and_fiber() {
    let h = Harness::new("ok", false).await;
    h.loader
        .create(
            EntryOptions::new("two", "editable")
                .with_config(toml::toml! { address="second" limit=7 }.into()),
            None,
            None,
        )
        .await
        .unwrap();
    let other = h
        .loader
        .await_idle()
        .await
        .unwrap()
        .entry("two")
        .unwrap()
        .fiber_id;
    let form = h.loader.describe_config("one").await.unwrap();
    h.loader
        .mutate_config("one", form.revision, vec![set("limit", 4)])
        .await
        .unwrap();
    assert_eq!(
        h.loader.describe_config("two").await.unwrap().value["limit"].as_integer(),
        Some(7)
    );
    assert_eq!(
        h.loader
            .await_idle()
            .await
            .unwrap()
            .entry("two")
            .unwrap()
            .fiber_id,
        other
    );
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn revision_roundtrips_as_an_opaque_transport_token() {
    let h = Harness::new("ok", true).await;
    let revision = h.loader.describe_config("one").await.unwrap().revision;
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
    let before = h.loader.describe_config("one").await.unwrap();
    let loader = h.loader.clone();
    let waiter = tokio::spawn(async move {
        loader
            .mutate_config("one", before.revision, vec![set("address", "blocked")])
            .await
    });
    h.counts.started.notified().await;
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    h.counts.release.notify_one();
    let after = h.loader.describe_config("one").await.unwrap();
    assert_eq!(after.value["address"].as_str(), Some("blocked"));
    assert!(h.file().contains("blocked"));
    h.runtime.shutdown().await.unwrap();
}
