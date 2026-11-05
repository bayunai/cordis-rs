use async_trait::async_trait;
use cordis_core::{Context, CoreError, Fiber, FiberState, Plugin, PluginDefinition, Runtime};
use cordis_loader::{
    EntryOptions, EntryTreeSource, EntryUpdate, ExtensionCatalog, ExtensionFactory,
    ExtensionsConfig, LOADER, Loader, LoaderControlError, LoaderError, LoaderPlugin,
    MovePersistence, PreparedExtension, SourceDocument, SourceError, SourceErrorKind,
    SourceRevision,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::Notify;

struct Memory {
    id: &'static str,
    document: Mutex<(ExtensionsConfig, u8)>,
    fail: AtomicBool,
    readonly: AtomicBool,
    writes: Arc<Mutex<Vec<&'static str>>>,
    write_started: Notify,
    write_gate: Mutex<Option<Arc<Notify>>>,
}
impl Memory {
    fn new(
        id: &'static str,
        extensions: Vec<EntryOptions>,
        writes: Arc<Mutex<Vec<&'static str>>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            id,
            document: Mutex::new((
                ExtensionsConfig {
                    version: 3,
                    extensions,
                },
                0,
            )),
            fail: AtomicBool::new(false),
            readonly: AtomicBool::new(false),
            writes,
            write_started: Notify::new(),
            write_gate: Mutex::new(None),
        })
    }
}
#[async_trait]
impl EntryTreeSource for Memory {
    fn identity(&self) -> String {
        self.id.into()
    }
    fn enable_logs(&self) -> bool {
        false
    }
    fn context(&self, owner: &Context) -> Result<Context, SourceError> {
        Ok(owner.clone())
    }
    async fn read(&self) -> Result<SourceDocument, SourceError> {
        let (config, revision) = self.document.lock().unwrap().clone();
        Ok(SourceDocument {
            config,
            revision: SourceRevision::new(vec![revision]),
        })
    }
    async fn check_revision(&self, expected: &SourceRevision) -> Result<(), SourceError> {
        if expected.as_bytes() != [self.document.lock().unwrap().1] {
            return Err(SourceError::new(
                SourceErrorKind::Conflict,
                "source changed",
            ));
        }
        Ok(())
    }
    async fn validate_write(&self, _: &ExtensionsConfig) -> Result<(), SourceError> {
        if self.readonly.load(Ordering::Acquire) {
            return Err(SourceError::new(SourceErrorKind::ReadOnly, "read only"));
        }
        Ok(())
    }
    async fn write(
        &self,
        config: &ExtensionsConfig,
        expected: &SourceRevision,
    ) -> Result<SourceRevision, SourceError> {
        self.writes.lock().unwrap().push(self.id);
        let gate = self.write_gate.lock().unwrap().clone();
        self.write_started.notify_one();
        if let Some(gate) = gate {
            gate.notified().await;
        }
        if self.fail.load(Ordering::Acquire) {
            return Err(SourceError::new(SourceErrorKind::Io, "write rejected"));
        }
        self.check_revision(expected).await?;
        let mut doc = self.document.lock().unwrap();
        doc.0 = config.clone();
        doc.1 += 1;
        Ok(SourceRevision::new(vec![doc.1]))
    }
}
#[derive(Clone, Deserialize, Serialize, JsonSchema)]
struct CarrierConfig {
    source: String,
}
struct CarrierFactory(HashMap<String, Arc<Memory>>);
impl ExtensionFactory for CarrierFactory {
    type Config = CarrierConfig;
    fn id(&self) -> &'static str {
        "include"
    }
    fn prepare(&self, config: CarrierConfig) -> Result<PreparedExtension, LoaderError> {
        let source = self.0.get(&config.source).unwrap().clone();
        Ok(PreparedExtension::tree(
            config.source,
            (),
            move |_, _, _| {
                let source = source.clone();
                async move { Ok(source as Arc<dyn EntryTreeSource>) }
            },
        ))
    }
}
#[derive(Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(default)]
struct LeafConfig {
    fail: bool,
    cleanup_panic: bool,
    pause_cleanup: bool,
}
struct CleanupControl {
    entered: Notify,
    release: Notify,
}
struct LeafFactory(Arc<Mutex<Vec<String>>>, Arc<CleanupControl>);
struct Leaf(LeafConfig, Arc<Mutex<Vec<String>>>, Arc<CleanupControl>);
impl ExtensionFactory for LeafFactory {
    type Config = LeafConfig;
    fn id(&self) -> &'static str {
        "leaf"
    }
    fn prepare(&self, config: LeafConfig) -> Result<PreparedExtension, LoaderError> {
        let events = self.0.clone();
        let cleanup = self.1.clone();
        Ok(PreparedExtension::ordinary(move || {
            Ok(Arc::new(Leaf(config, events, cleanup)))
        }))
    }
}
#[async_trait]
impl Plugin for Leaf {
    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        let location = ctx.config(cordis_loader::ENTRY_LOCATION)?.path.clone();
        self.1.lock().unwrap().push(format!("start:{location}"));
        let events = self.1.clone();
        let cleanup_panic = self.0.cleanup_panic;
        ctx.effect()?.on_dispose(move || {
            events.lock().unwrap().push(format!("stop:{location}"));
            assert!(!cleanup_panic, "cleanup failure");
        });
        if self.0.pause_cleanup {
            let cleanup = self.2.clone();
            ctx.effect()?.on_dispose_async(move || async move {
                cleanup.entered.notify_one();
                cleanup.release.notified().await;
                Ok(())
            })?;
        }
        if self.0.fail {
            return Err(CoreError::PluginApply("activation failed".into()));
        }
        Ok(())
    }
}
fn leaf(id: &str) -> EntryOptions {
    EntryOptions::new(id, "leaf")
}
fn group(id: &str, children: Vec<EntryOptions>) -> EntryOptions {
    EntryOptions::group(id, children).unwrap()
}
fn carrier(id: &str, source: &str) -> EntryOptions {
    EntryOptions::new(id, "include").with_config(
        toml::Value::try_from(CarrierConfig {
            source: source.into(),
        })
        .unwrap(),
    )
}
struct Harness {
    runtime: Runtime,
    _fiber: Fiber,
    loader: Loader,
    a: Arc<Memory>,
    b: Arc<Memory>,
    writes: Arc<Mutex<Vec<&'static str>>>,
    events: Arc<Mutex<Vec<String>>>,
    cleanup: Arc<CleanupControl>,
}
impl Harness {
    async fn new(a_entries: Vec<EntryOptions>, b_entries: Vec<EntryOptions>) -> Self {
        let writes = Arc::new(Mutex::new(Vec::new()));
        let a = Memory::new("a", a_entries, writes.clone());
        let b = Memory::new("b", b_entries, writes.clone());
        let c = Memory::new("c", vec![leaf("inner")], writes.clone());
        let events = Arc::new(Mutex::new(Vec::new()));
        let cleanup = Arc::new(CleanupControl {
            entered: Notify::new(),
            release: Notify::new(),
        });
        let mut catalog = ExtensionCatalog::new();
        catalog
            .register(
                &PluginDefinition::named("include"),
                CarrierFactory(HashMap::from([
                    ("a".into(), a.clone()),
                    ("b".into(), b.clone()),
                    ("c".into(), c),
                ])),
            )
            .unwrap();
        catalog
            .register(
                &PluginDefinition::named("leaf"),
                LeafFactory(events.clone(), cleanup.clone()),
            )
            .unwrap();
        let runtime = Runtime::new().unwrap();
        let fiber = runtime
            .root()
            .plugin(
                &cordis_loader::PLUGIN_DEFINITION,
                Arc::new(LoaderPlugin::new(catalog)),
            )
            .await
            .unwrap();
        let loader = runtime.root().get(LOADER).unwrap().as_ref().clone();
        loader.create(carrier("a", "a"), None, None).await.unwrap();
        loader.create(carrier("b", "b"), None, None).await.unwrap();
        Self {
            runtime,
            _fiber: fiber,
            loader,
            a,
            b,
            writes,
            events,
            cleanup,
        }
    }
    async fn move_x(&self) -> Result<cordis_loader::LoaderSnapshot, LoaderControlError> {
        self.loader
            .update("a:x", EntryUpdate::default(), Some(Some("b")), None)
            .await
    }
}

#[tokio::test]
async fn groups_share_tree_addresses_and_reparent_waits_for_old_cleanup() {
    let h = Harness::new(
        vec![
            group("one", vec![group("nested", vec![leaf("x")])]),
            group("two", vec![]),
        ],
        vec![],
    )
    .await;
    let before = h.loader.await_idle().await.unwrap();
    let old = before.entry("a:x").unwrap().fiber_id;
    assert_eq!(
        before
            .entry("a:x")
            .unwrap()
            .parent
            .as_ref()
            .unwrap()
            .as_str(),
        "a:nested"
    );
    assert!(before.entry("a:one:nested:x").is_none());
    let moved = h
        .loader
        .update("a:x", EntryUpdate::default(), Some(Some("a:two")), None)
        .await
        .unwrap();
    assert_eq!(
        moved
            .entry("a:x")
            .unwrap()
            .parent
            .as_ref()
            .unwrap()
            .as_str(),
        "a:two"
    );
    assert_ne!(moved.entry("a:x").unwrap().fiber_id, old);
    assert_eq!(
        *h.events.lock().unwrap(),
        ["start:a:x", "stop:a:x", "start:a:x"]
    );
    let before_sort = moved.entry("a:x").unwrap().fiber_id;
    let sorted = h
        .loader
        .update("a:x", EntryUpdate::default(), Some(Some("a:two")), Some(0))
        .await
        .unwrap();
    assert_eq!(sorted.entry("a:x").unwrap().fiber_id, before_sort);
    assert!(h.loader.snapshot_entry_config("a:x").await.is_ok());
    assert!(h.loader.snapshot_entry_config("a:two:x").await.is_err());
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn cross_tree_move_persists_destination_first_and_reload_does_not_duplicate() {
    let h = Harness::new(vec![leaf("x")], vec![]).await;
    let old = h
        .loader
        .await_idle()
        .await
        .unwrap()
        .entry("a:x")
        .unwrap()
        .fiber_id;
    let moved = h.move_x().await.unwrap();
    assert!(moved.entry("a:x").is_none());
    assert_ne!(moved.entry("b:x").unwrap().fiber_id, old);
    assert_eq!(
        *h.events.lock().unwrap(),
        ["start:a:x", "stop:a:x", "start:b:x"]
    );
    assert_eq!(*h.writes.lock().unwrap(), ["b", "a"]);
    h.loader.subtree("a").unwrap().reload().await.unwrap();
    h.loader.subtree("b").unwrap().reload().await.unwrap();
    let reloaded = h.loader.await_idle().await.unwrap();
    assert!(reloaded.entry("a:x").is_none());
    assert_eq!(
        reloaded
            .entries
            .iter()
            .filter(|e| e.path.as_str() == "b:x")
            .count(),
        1
    );
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn move_group_transfers_members_and_preserves_parent_relationships() {
    let h = Harness::new(
        vec![group("g", vec![leaf("x")])],
        vec![group("target", vec![])],
    )
    .await;
    let moved = h
        .loader
        .update("a:g", EntryUpdate::default(), Some(Some("b:target")), None)
        .await
        .unwrap();
    assert!(moved.entry("a:g").is_none());
    assert_eq!(
        moved
            .entry("b:g")
            .unwrap()
            .parent
            .as_ref()
            .unwrap()
            .as_str(),
        "b:target"
    );
    assert_eq!(
        moved
            .entry("b:x")
            .unwrap()
            .parent
            .as_ref()
            .unwrap()
            .as_str(),
        "b:g"
    );
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn conflict_readonly_and_invalid_parent_fail_before_disposal_or_write() {
    let h = Harness::new(vec![group("g", vec![leaf("x")])], vec![leaf("x")]).await;
    assert!(
        h.loader
            .update("a:g", EntryUpdate::default(), Some(Some("b")), None)
            .await
            .is_err()
    );
    assert!(
        h.loader
            .update("a:g", EntryUpdate::default(), Some(Some("a:x")), None)
            .await
            .is_err()
    );
    h.b.readonly.store(true, Ordering::Release);
    assert!(
        h.loader
            .update("a:g", EntryUpdate::default(), Some(Some("b")), None)
            .await
            .is_err()
    );
    assert!(h.writes.lock().unwrap().is_empty());
    assert_eq!(*h.events.lock().unwrap(), ["start:a:x", "start:b:x"]);
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn destination_write_failure_keeps_source_file_and_reports_both_sides() {
    let h = Harness::new(vec![leaf("x")], vec![]).await;
    h.b.fail.store(true, Ordering::Release);
    let LoaderControlError::Move {
        snapshot,
        origin,
        destination,
        ..
    } = h.move_x().await.unwrap_err()
    else {
        panic!("expected move result");
    };
    assert!(snapshot.entry("a:x").is_none());
    assert!(snapshot.entry("b:x").is_some());
    assert!(matches!(origin, MovePersistence::NotAttempted { .. }));
    assert!(matches!(destination, MovePersistence::Failed { .. }));
    assert_eq!(*h.writes.lock().unwrap(), ["b"]);
    assert_eq!(h.a.document.lock().unwrap().0.extensions[0].id, "x");
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn source_write_failure_reports_destination_success_without_rollback() {
    let h = Harness::new(vec![leaf("x")], vec![]).await;
    h.a.fail.store(true, Ordering::Release);
    let LoaderControlError::Move {
        snapshot,
        origin,
        destination,
        ..
    } = h.move_x().await.unwrap_err()
    else {
        panic!("expected move result");
    };
    assert!(matches!(origin, MovePersistence::Failed { .. }));
    assert!(matches!(destination, MovePersistence::Written { .. }));
    assert!(snapshot.entry("b:x").is_some());
    assert!(snapshot.entry("a:x").is_none());
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn activation_failure_retains_target_declaration_and_failed_diagnostic() {
    let h = Harness::new(vec![leaf("x")], vec![]).await;
    let result = h
        .loader
        .update(
            "a:x",
            EntryUpdate {
                config: Some(
                    toml::Value::try_from(LeafConfig {
                        fail: true,
                        ..Default::default()
                    })
                    .unwrap(),
                ),
                ..Default::default()
            },
            Some(Some("b")),
            None,
        )
        .await;
    let LoaderControlError::Move {
        snapshot,
        lifecycle_errors,
        ..
    } = result.unwrap_err()
    else {
        panic!("expected move result");
    };
    assert!(!lifecycle_errors.is_empty());
    assert!(snapshot.entry("a:x").is_none());
    assert_eq!(
        snapshot.entry("b:x").unwrap().state,
        Some(FiberState::Failed)
    );
    assert!(h.loader.snapshot_entry_config("b:x").await.is_ok());
    assert_eq!(h.b.document.lock().unwrap().0.extensions[0].id, "x");
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelling_waiter_does_not_cancel_two_source_commit() {
    let h = Harness::new(vec![leaf("x")], vec![]).await;
    let gate = Arc::new(Notify::new());
    *h.b.write_gate.lock().unwrap() = Some(gate.clone());
    let loader = h.loader.clone();
    let waiter = tokio::spawn(async move {
        loader
            .update("a:x", EntryUpdate::default(), Some(Some("b")), None)
            .await
    });
    h.b.write_started.notified().await;
    waiter.abort();
    let _ = waiter.await;
    gate.notify_one();
    h.loader.await_idle().await.unwrap();
    assert_eq!(*h.writes.lock().unwrap(), ["b", "a"]);
    assert!(h.a.document.lock().unwrap().0.extensions.is_empty());
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn moving_group_with_include_reattaches_nested_namespace_and_rejects_descendant_target() {
    let h = Harness::new(
        vec![group("g", vec![leaf("x"), carrier("nested", "c")])],
        vec![],
    )
    .await;
    let before = h.loader.await_idle().await.unwrap();
    assert!(before.entry("a:nested:inner").is_some());
    assert!(matches!(
        h.loader
            .update("a:g", EntryUpdate::default(), Some(Some("a:nested")), None)
            .await,
        Err(LoaderControlError::InvalidMove { .. })
    ));
    assert!(h.writes.lock().unwrap().is_empty());
    let moved = h
        .loader
        .update("a:g", EntryUpdate::default(), Some(Some("b")), None)
        .await
        .unwrap();
    assert!(moved.entry("a:nested:inner").is_none());
    assert!(moved.entry("b:nested:inner").is_some());
    assert_eq!(
        moved
            .entry("b:nested")
            .unwrap()
            .parent
            .as_ref()
            .unwrap()
            .as_str(),
        "b:g"
    );
    assert!(h.loader.subtree("a:nested").is_err());
    assert!(h.loader.subtree("b:nested").is_ok());
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn stale_source_revision_fails_before_resources_or_files_change() {
    let h = Harness::new(vec![leaf("x")], vec![]).await;
    h.b.document.lock().unwrap().1 += 1;
    assert!(matches!(
        h.move_x().await,
        Err(LoaderControlError::ConfigConflict { .. })
    ));
    assert_eq!(*h.events.lock().unwrap(), ["start:a:x"]);
    assert!(h.writes.lock().unwrap().is_empty());
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn cleanup_panic_does_not_prevent_target_activation_and_persistence() {
    let item = leaf("x").with_config(
        toml::Value::try_from(LeafConfig {
            cleanup_panic: true,
            ..Default::default()
        })
        .unwrap(),
    );
    let h = Harness::new(vec![item], vec![]).await;
    let LoaderControlError::Move {
        snapshot,
        lifecycle_errors,
        origin,
        destination,
    } = h.move_x().await.unwrap_err()
    else {
        panic!("expected move error");
    };
    assert!(!lifecycle_errors.is_empty());
    assert_eq!(
        snapshot.entry("b:x").unwrap().state,
        Some(FiberState::Active)
    );
    assert!(snapshot.entry("a:x").is_none());
    assert!(matches!(origin, MovePersistence::Written { .. }));
    assert!(matches!(destination, MovePersistence::Written { .. }));
    assert_eq!(*h.writes.lock().unwrap(), ["b", "a"]);
    assert!(h.runtime.shutdown().await.is_err());
}

#[tokio::test]
async fn global_null_parent_moves_to_root_while_subtree_null_parent_stays_local() {
    let h = Harness::new(vec![group("g", vec![leaf("x")])], vec![]).await;
    h.loader
        .subtree("a")
        .unwrap()
        .update("x", EntryUpdate::default(), Some(None), None)
        .await
        .unwrap();
    assert_eq!(
        h.loader
            .await_idle()
            .await
            .unwrap()
            .entry("a:x")
            .unwrap()
            .parent
            .as_ref()
            .unwrap()
            .as_str(),
        "a"
    );
    let moved = h
        .loader
        .update("a:x", EntryUpdate::default(), Some(None), None)
        .await
        .unwrap();
    assert!(moved.entry("a:x").is_none());
    assert!(moved.entry("x").unwrap().parent.is_none());
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_waits_for_inflight_move_to_finish_both_writes() {
    let h = Harness::new(vec![leaf("x")], vec![]).await;
    let gate = Arc::new(Notify::new());
    *h.b.write_gate.lock().unwrap() = Some(gate.clone());
    let loader = h.loader.clone();
    let mover = tokio::spawn(async move {
        loader
            .update("a:x", EntryUpdate::default(), Some(Some("b")), None)
            .await
    });
    h.b.write_started.notified().await;
    let runtime = h.runtime.clone();
    let shutdown = tokio::spawn(async move { runtime.shutdown().await });
    gate.notify_one();
    let _ = mover.await.unwrap();
    shutdown.await.unwrap().unwrap();
    assert_eq!(*h.writes.lock().unwrap(), ["b", "a"]);
}

#[tokio::test]
async fn target_does_not_activate_until_old_asynchronous_cleanup_finishes() {
    let item = leaf("x").with_config(
        toml::Value::try_from(LeafConfig {
            pause_cleanup: true,
            ..Default::default()
        })
        .unwrap(),
    );
    let h = Harness::new(vec![item], vec![]).await;
    let loader = h.loader.clone();
    let mover = tokio::spawn(async move {
        loader
            .update(
                "a:x",
                EntryUpdate {
                    config: Some(toml::Value::try_from(LeafConfig::default()).unwrap()),
                    ..Default::default()
                },
                Some(Some("b")),
                None,
            )
            .await
    });
    h.cleanup.entered.notified().await;
    assert_eq!(*h.events.lock().unwrap(), ["start:a:x", "stop:a:x"]);
    assert!(h.writes.lock().unwrap().is_empty());
    h.cleanup.release.notify_one();
    mover.await.unwrap().unwrap();
    assert_eq!(
        *h.events.lock().unwrap(),
        ["start:a:x", "stop:a:x", "start:b:x"]
    );
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn readonly_source_and_invalid_candidate_are_rejected_before_cleanup() {
    let h = Harness::new(vec![leaf("x")], vec![]).await;
    h.b.readonly.store(true, Ordering::Release);
    assert!(matches!(
        h.move_x().await,
        Err(LoaderControlError::Source { .. })
    ));
    h.b.readonly.store(false, Ordering::Release);
    let bad_config: toml::Value = toml::toml! { fail = "invalid boolean" }.into();
    assert!(
        h.loader
            .update(
                "a:x",
                EntryUpdate {
                    config: Some(bad_config),
                    ..Default::default()
                },
                Some(Some("b")),
                None
            )
            .await
            .is_err()
    );
    assert_eq!(*h.events.lock().unwrap(), ["start:a:x"]);
    assert!(h.writes.lock().unwrap().is_empty());
    assert!(h.loader.await_idle().await.unwrap().entry("a:x").is_some());
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn same_tree_failed_reparent_keeps_new_parent_declaration_and_writes_once() {
    let h = Harness::new(
        vec![group("one", vec![leaf("x")]), group("two", vec![])],
        vec![],
    )
    .await;
    let failure = h
        .loader
        .update(
            "a:x",
            EntryUpdate {
                config: Some(
                    toml::Value::try_from(LeafConfig {
                        fail: true,
                        ..Default::default()
                    })
                    .unwrap(),
                ),
                ..Default::default()
            },
            Some(Some("a:two")),
            None,
        )
        .await
        .unwrap_err();
    let LoaderControlError::Move { snapshot, .. } = failure else {
        panic!("expected move result");
    };
    assert_eq!(
        snapshot
            .entry("a:x")
            .unwrap()
            .parent
            .as_ref()
            .unwrap()
            .as_str(),
        "a:two"
    );
    assert_eq!(
        snapshot.entry("a:x").unwrap().state,
        Some(FiberState::Failed)
    );
    assert_eq!(*h.writes.lock().unwrap(), ["a"]);
    let config = h.loader.snapshot_entry_config("a:x").await.unwrap();
    assert_eq!(
        config.config.get("fail").and_then(toml::Value::as_bool),
        Some(true)
    );
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn moving_from_ancestor_into_unsettled_child_does_not_relock_or_retry_its_failed_entries() {
    let failed = leaf("bad").with_config(
        toml::Value::try_from(LeafConfig {
            fail: true,
            ..Default::default()
        })
        .unwrap(),
    );
    let h = Harness::new(vec![], vec![failed]).await;
    h.loader.create(leaf("x"), None, None).await.unwrap();
    let before = h
        .loader
        .await_idle()
        .await
        .unwrap()
        .entry("b:bad")
        .unwrap()
        .fiber_id;
    let moved = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        h.loader
            .update("x", EntryUpdate::default(), Some(Some("b")), None),
    )
    .await
    .expect("move must not reacquire the destination operations lock")
    .unwrap();
    assert!(moved.entry("x").is_none());
    assert_eq!(moved.entry("b:x").unwrap().state, Some(FiberState::Active));
    assert_eq!(moved.entry("b:bad").unwrap().fiber_id, before);
    assert_eq!(
        h.events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| *event == "start:b:bad")
            .count(),
        1
    );
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn subtree_reparent_uses_the_same_failed_move_completion_and_declaration_rules() {
    let h = Harness::new(
        vec![group("one", vec![leaf("x")]), group("two", vec![])],
        vec![],
    )
    .await;
    let failure = h
        .loader
        .subtree("a")
        .unwrap()
        .update(
            "x",
            EntryUpdate {
                config: Some(
                    toml::Value::try_from(LeafConfig {
                        fail: true,
                        ..Default::default()
                    })
                    .unwrap(),
                ),
                ..Default::default()
            },
            Some(Some("two")),
            None,
        )
        .await
        .unwrap_err();
    let LoaderControlError::Move { snapshot, .. } = failure else {
        panic!("expected move result");
    };
    assert_eq!(
        snapshot
            .entry("a:x")
            .unwrap()
            .parent
            .as_ref()
            .unwrap()
            .as_str(),
        "a:two"
    );
    assert_eq!(
        h.loader
            .snapshot_entry_config("a:x")
            .await
            .unwrap()
            .config
            .get("fail")
            .and_then(toml::Value::as_bool),
        Some(true)
    );
    assert_eq!(*h.writes.lock().unwrap(), ["a"]);
    h.runtime.shutdown().await.unwrap();
}
