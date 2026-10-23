use async_trait::async_trait;
use cordis_core::{Context, CoreError, Fiber, FiberState, Plugin, PluginKey, Runtime, ServiceKey};
use cordis_loader::{
    ConfigRef, EntryOptions, EntryUpdate, ExtensionCatalog, ExtensionFactory, ExtensionsConfig,
    LOADER, Loader, LoaderControlError, LoaderError, LoaderPlugin, PreparedExtension,
};
use schemars::JsonSchema;
use std::{
    fs,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

static LIVE: ServiceKey<ConfigRef<Live>> = ServiceKey::new("test.volatile.live@1");
static DEP: ServiceKey<()> = ServiceKey::new("test.volatile.dep@1");

#[derive(serde::Deserialize, JsonSchema)]
struct Input {
    address: String,
    limit: u32,
    timeout: u32,
    #[serde(default)]
    provide: bool,
    #[serde(default)]
    dependency: bool,
}

#[derive(Clone, PartialEq)]
struct Settings {
    address: String,
    provide: bool,
    dependency: bool,
}
#[derive(Debug, PartialEq)]
struct Live {
    limit: u32,
    timeout: u32,
}

#[derive(Default)]
struct State {
    builds: AtomicUsize,
    applies: AtomicUsize,
    disposes: Arc<AtomicUsize>,
    refs: Mutex<Vec<ConfigRef<Live>>>,
    drop_provider: Mutex<Option<cordis_core::EffectContext>>,
    waiting_fiber: std::sync::atomic::AtomicU64,
    subscription_updates: Arc<AtomicUsize>,
    active_subscriptions: Arc<AtomicUsize>,
    persist_fault: Mutex<Option<std::path::PathBuf>>,
}
struct Factory(Arc<State>);
struct TestPlugin {
    settings: Settings,
    live: ConfigRef<Live>,
    state: Arc<State>,
}

struct SubscriptionTask(Arc<AtomicUsize>);
impl Drop for SubscriptionTask {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl ExtensionFactory for Factory {
    type Config = Input;
    fn id(&self) -> &'static str {
        "live"
    }
    fn prepare(&self, input: Input) -> Result<PreparedExtension, LoaderError> {
        assert_ne!(input.address, "prepare-panic");
        if input.limit == 0 || input.timeout < input.limit {
            return Err(LoaderError::invalid_config(
                "limit must be positive and not exceed timeout",
            ));
        }
        if let Some(path) = self.0.persist_fault.lock().unwrap().take() {
            fs::remove_file(&path).unwrap();
            fs::create_dir(path).unwrap();
        }
        let settings = Settings {
            address: input.address,
            provide: input.provide,
            dependency: input.dependency,
        };
        let live = Live {
            limit: input.limit,
            timeout: input.timeout,
        };
        let state = self.0.clone();
        Ok(PreparedExtension::volatile(
            settings,
            live,
            move |settings, live| {
                state.builds.fetch_add(1, Ordering::SeqCst);
                assert_ne!(settings.address, "build-panic");
                if settings.address == "build-error" {
                    return Err(LoaderError::invalid_config("build error"));
                }
                state.refs.lock().unwrap().push(live.clone());
                Ok(Arc::new(TestPlugin {
                    settings,
                    live,
                    state,
                }))
            },
        ))
    }
}
#[async_trait]
impl Plugin for TestPlugin {
    fn key(&self) -> PluginKey {
        PluginKey::new("test.live")
    }
    fn inject(&self) -> Vec<cordis_core::ServiceId> {
        self.settings
            .dependency
            .then_some(DEP.id())
            .into_iter()
            .collect()
    }
    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        self.state.applies.fetch_add(1, Ordering::SeqCst);
        if self.settings.address == "apply-error" {
            return Err(CoreError::PluginApply("apply error".into()));
        }
        if self.settings.address == "drop-provider" {
            let mut changes = ctx.subscribe_fiber_states()?;
            let provider = self.state.drop_provider.lock().unwrap().take().unwrap();
            provider.dispose_wait().await?;
            let id = self.state.waiting_fiber.load(Ordering::SeqCst);
            loop {
                let change = changes.recv().await.unwrap();
                if change.fiber_id == id && change.current == FiberState::Pending {
                    break;
                }
            }
        }
        if self.settings.provide {
            ctx.provide(LIVE, self.live.clone())?;
        }
        let disposes = self.state.disposes.clone();
        ctx.effect()?.on_dispose(move || {
            disposes.fetch_add(1, Ordering::SeqCst);
        });
        if self.settings.address == "dispose-error" {
            ctx.effect()?.on_dispose_async(|| async {
                Err(CoreError::PluginApply("dispose error".into()))
            })?;
        }
        let mut subscription = self.live.subscribe();
        let updates = self.state.subscription_updates.clone();
        self.state
            .active_subscriptions
            .fetch_add(1, Ordering::SeqCst);
        let task = SubscriptionTask(self.state.active_subscriptions.clone());
        ctx.effect()?.spawn(move |cancel| async move {
            let _task = task;
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    value = subscription.changed() => {
                        if value.is_err() { break; }
                        updates.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        })?;
        Ok(())
    }
}

fn config(address: &str, limit: u32, timeout: u32, provide: bool, dependency: bool) -> toml::Value {
    toml::from_str(&format!("address = {address:?}\nlimit = {limit}\ntimeout = {timeout}\nprovide = {provide}\ndependency = {dependency}")).unwrap()
}
fn entry(id: &str, address: &str, limit: u32) -> EntryOptions {
    EntryOptions::new(id, "live").with_config(config(address, limit, limit * 10, false, false))
}
fn catalog(state: &Arc<State>) -> ExtensionCatalog {
    let mut catalog = ExtensionCatalog::new();
    catalog.register(Factory(state.clone())).unwrap();
    catalog
        .register(cordis_plugin_include::IncludeFactory)
        .unwrap();
    catalog
        .register_isolation(cordis_loader::IsolationDescriptor::new("dep", DEP))
        .unwrap();
    catalog
}
fn target(entries: Vec<EntryOptions>) -> ExtensionsConfig {
    ExtensionsConfig {
        version: cordis_loader::CONFIG_VERSION,
        extensions: entries,
    }
}
struct Harness {
    runtime: Runtime,
    loader: Loader,
    _fiber: Fiber,
    directory: tempfile::TempDir,
}
impl Harness {
    async fn new(state: &Arc<State>, entries: Vec<EntryOptions>) -> Self {
        Self::with_child(state, entries, None).await
    }
    async fn with_child(
        state: &Arc<State>,
        entries: Vec<EntryOptions>,
        child: Option<Vec<EntryOptions>>,
    ) -> Self {
        let directory = tempfile::tempdir().unwrap();
        if let Some(child) = child {
            fs::write(
                directory.path().join("child.toml"),
                toml::to_string(&target(child)).unwrap(),
            )
            .unwrap();
        }
        fs::write(
            directory.path().join("bootstrap.toml"),
            "version = 3\n[config]\ndriver = \"file\"\npath = \"extensions.toml\"\n",
        )
        .unwrap();
        fs::write(
            directory.path().join("extensions.toml"),
            toml::to_string(&target(entries)).unwrap(),
        )
        .unwrap();
        let runtime = Runtime::new().unwrap();
        let plugin = file_plugin(catalog(state), directory.path().join("bootstrap.toml"))
            .await
            .unwrap();
        let fiber = runtime.root().plugin(Arc::new(plugin)).await.unwrap();
        assert_eq!(fiber.state(), FiberState::Active);
        let loader = runtime.root().get(LOADER).unwrap();
        Self {
            runtime,
            loader: (*loader).clone(),
            _fiber: fiber,
            directory,
        }
    }
    fn file(&self) -> std::path::PathBuf {
        self.directory.path().join("extensions.toml")
    }
    fn write(&self, entries: Vec<EntryOptions>) {
        fs::write(self.file(), toml::to_string(&target(entries)).unwrap()).unwrap();
    }
    async fn update(
        &self,
        id: &str,
        value: toml::Value,
    ) -> Result<cordis_loader::LoaderSnapshot, LoaderControlError> {
        self.loader
            .subtree("include")
            .unwrap()
            .update(
                id,
                EntryUpdate {
                    config: Some(value),
                    ..Default::default()
                },
                None,
                None,
            )
            .await
    }
}

#[tokio::test]
async fn hot_changes_preserve_fiber_provider_effect_and_coherent_snapshots() {
    let state = Arc::new(State::default());
    let e = EntryOptions::new("a", "live").with_config(config("db", 1, 10, true, false));
    let h = Harness::new(&state, vec![e]).await;
    let before = h.loader.await_idle().await.unwrap();
    let service = h.runtime.root().get(LIVE).unwrap();
    let mut sub = service.subscribe();
    h.update("a", config("db", 2, 20, true, false))
        .await
        .unwrap();
    let current = sub.changed().await.unwrap();
    assert_eq!(current.revision, 1);
    assert_eq!((current.value.limit, current.value.timeout), (2, 20));
    assert_eq!(
        before.entry("include:a").unwrap().fiber_id,
        h.loader
            .await_idle()
            .await
            .unwrap()
            .entry("include:a")
            .unwrap()
            .fiber_id
    );
    assert!(Arc::ptr_eq(&service, &h.runtime.root().get(LIVE).unwrap()));
    assert_eq!(state.builds.load(Ordering::SeqCst), 1);
    assert_eq!(state.applies.load(Ordering::SeqCst), 1);
    assert_eq!(state.disposes.load(Ordering::SeqCst), 0);
    h.update("a", config("db", 2, 20, true, false))
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), sub.changed())
            .await
            .is_err()
    );
    h.update("a", config("db", 3, 30, true, false))
        .await
        .unwrap();
    h.update("a", config("db", 4, 40, true, false))
        .await
        .unwrap();
    assert_eq!(sub.changed().await.unwrap().revision, 3);
    h.runtime.shutdown().await.unwrap();
    assert!(sub.changed().await.is_err());
    assert_eq!(service.get().value.limit, 4);
    assert_eq!(state.active_subscriptions.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn static_and_mixed_changes_replace_and_close_old_reference() {
    let state = Arc::new(State::default());
    let h = Harness::new(&state, vec![entry("a", "db", 1)]).await;
    let old = state.refs.lock().unwrap()[0].clone();
    let mut sub = old.subscribe();
    h.update("a", config("other", 2, 20, false, false))
        .await
        .unwrap();
    assert!(sub.changed().await.is_err());
    assert_eq!(old.get().value.limit, 1);
    let next = state.refs.lock().unwrap()[1].clone();
    assert_eq!(next.get().revision, 0);
    assert_eq!(next.get().value.limit, 2);
    assert_eq!(state.disposes.load(Ordering::SeqCst), 1);
    h.update("a", config("other", 3, 30, false, false))
        .await
        .unwrap();
    assert_eq!(next.get().revision, 1);
    assert_eq!(old.get().value.limit, 1);
    h.update("a", config("third", 3, 30, false, false))
        .await
        .unwrap();
    let third = state.refs.lock().unwrap().last().unwrap().clone();
    assert_eq!(third.get().revision, 0);
    assert_eq!(third.get().value.limit, 3);
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_candidates_and_panics_do_not_mutate_runtime_or_file() {
    let state = Arc::new(State::default());
    let h = Harness::new(&state, vec![entry("a", "db", 1)]).await;
    let before = fs::read_to_string(h.file()).unwrap();
    for candidate in [
        config("db", 0, 20, false, false),
        config("db", 30, 2, false, false),
        config("prepare-panic", 2, 20, false, false),
        config("build-panic", 2, 20, false, false),
        config("build-error", 2, 20, false, false),
        toml::from_str("address = 123").unwrap(),
    ] {
        assert!(h.update("a", candidate).await.is_err());
        assert_eq!(fs::read_to_string(h.file()).unwrap(), before);
        assert_eq!(state.refs.lock().unwrap()[0].get().revision, 0);
        assert_eq!(
            h.loader
                .await_idle()
                .await
                .unwrap()
                .entry("include:a")
                .unwrap()
                .state,
            Some(FiberState::Active)
        );
    }
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn lifecycle_failure_defers_hot_commit_and_same_target_retries() {
    let state = Arc::new(State::default());
    let h = Harness::new(&state, vec![entry("a", "db", 1), entry("b", "db", 1)]).await;
    let first = state.refs.lock().unwrap()[0].clone();
    h.write(vec![entry("a", "db", 2), entry("b", "apply-error", 2)]);
    for _ in 0..2 {
        assert!(h.loader.subtree("include").unwrap().reload().await.is_err());
        assert_eq!(first.get().revision, 0);
        assert_eq!(
            h.loader
                .subtree("include")
                .unwrap()
                .entries()
                .unwrap()
                .extensions[0]
                .config,
            entry("a", "db", 1).config
        );
    }
    h.write(vec![entry("a", "db", 2), entry("b", "fixed", 2)]);
    h.loader.subtree("include").unwrap().reload().await.unwrap();
    assert_eq!(first.get().revision, 1);
    assert_eq!(first.get().value.limit, 2);
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn multiple_instances_disable_restore_and_unmount_rebuild() {
    let state = Arc::new(State::default());
    let h = Harness::new(&state, vec![entry("a", "db", 1), entry("b", "db", 1)]).await;
    let refs = state.refs.lock().unwrap().clone();
    h.update("a", config("db", 2, 20, false, false))
        .await
        .unwrap();
    assert_eq!(refs[0].get().value.limit, 2);
    assert_eq!(refs[1].get().value.limit, 1);
    let mut sub = refs[0].subscribe();
    h.loader
        .subtree("include")
        .unwrap()
        .update(
            "a",
            EntryUpdate {
                disabled: Some(true),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap();
    assert!(sub.changed().await.is_err());
    h.loader
        .subtree("include")
        .unwrap()
        .update(
            "a",
            EntryUpdate {
                disabled: Some(false),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap();
    let next = state.refs.lock().unwrap().last().unwrap().clone();
    assert_eq!(next.get().revision, 0);
    let mut next_updates = next.subscribe();
    h.runtime
        .unmount(PluginKey::new("test.live"))
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(1), next_updates.changed())
            .await
            .unwrap()
            .is_err()
    );
    h.loader.subtree("include").unwrap().reload().await.unwrap();
    assert!(
        h.loader
            .await_idle()
            .await
            .unwrap()
            .entries
            .iter()
            .all(|e| e.state == Some(FiberState::Active))
    );
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn pending_updates_build_new_config_but_dependency_reactivation_keeps_reference() {
    let state = Arc::new(State::default());
    let e = EntryOptions::new("a", "live").with_config(config("db", 1, 10, false, true));
    let h = Harness::new(&state, vec![e]).await;
    let old = state.refs.lock().unwrap()[0].clone();
    let mut sub = old.subscribe();
    assert_eq!(
        h.loader
            .await_idle()
            .await
            .unwrap()
            .entry("include:a")
            .unwrap()
            .state,
        Some(FiberState::Pending)
    );
    h.update("a", config("db", 2, 20, false, true))
        .await
        .unwrap();
    assert!(sub.changed().await.is_err());
    let next = state.refs.lock().unwrap()[1].clone();
    let provider = h.runtime.root().effect().unwrap();
    provider.provide(DEP, ()).unwrap();
    h.runtime.settle().await;
    h.loader.await_idle().await.unwrap();
    assert_eq!(next.get().value.limit, 2);
    provider.dispose_wait().await.unwrap();
    h.runtime.settle().await;
    h.loader.await_idle().await.unwrap();
    let provider = h.runtime.root().effect().unwrap();
    provider.provide(DEP, ()).unwrap();
    h.runtime.settle().await;
    h.loader.await_idle().await.unwrap();
    assert_eq!(state.builds.load(Ordering::SeqCst), 2);
    h.update("a", config("db", 3, 30, false, true))
        .await
        .unwrap();
    assert_eq!(next.get().revision, 1);
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn fiber_leaving_active_before_commit_rejects_and_retries_actual_binding() {
    let state = Arc::new(State::default());
    let e = EntryOptions::new("a", "live").with_config(config("db", 1, 10, false, true));
    let h = Harness::new(&state, vec![e.clone(), entry("b", "db", 1)]).await;
    let provider = h.runtime.root().effect().unwrap();
    provider.provide(DEP, ()).unwrap();
    h.runtime.settle().await;
    let id = h
        .loader
        .await_idle()
        .await
        .unwrap()
        .entry("include:a")
        .unwrap()
        .fiber_id
        .unwrap();
    state.waiting_fiber.store(id, Ordering::SeqCst);
    *state.drop_provider.lock().unwrap() = Some(provider);
    let first = state.refs.lock().unwrap()[0].clone();
    h.write(vec![
        EntryOptions::new("a", "live").with_config(config("db", 2, 20, false, true)),
        entry("b", "drop-provider", 1),
    ]);
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        h.loader.subtree("include").unwrap().reload(),
    )
    .await
    .unwrap();
    assert!(matches!(
        result,
        Err(LoaderControlError::Loader(
            LoaderError::HotConfigStale { .. }
        ))
    ));
    assert_eq!(first.get().revision, 0);
    assert_eq!(
        h.loader
            .subtree("include")
            .unwrap()
            .entries()
            .unwrap()
            .extensions[0]
            .config,
        e.config
    );
    let provider = h.runtime.root().effect().unwrap();
    provider.provide(DEP, ()).unwrap();
    h.runtime.settle().await;
    h.loader.subtree("include").unwrap().reload().await.unwrap();
    assert_eq!(first.get().revision, 1);
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn conflict_precedes_hot_commit_and_persist_error_retains_applied_snapshot() {
    let state = Arc::new(State::default());
    let h = Harness::new(&state, vec![entry("a", "db", 1)]).await;
    let reference = state.refs.lock().unwrap()[0].clone();
    h.write(vec![entry("a", "db", 2)]);
    assert!(matches!(
        h.update("a", config("db", 3, 30, false, false)).await,
        Err(LoaderControlError::ConfigConflict { .. })
    ));
    assert_eq!(reference.get().revision, 0);
    h.loader.subtree("include").unwrap().reload().await.unwrap();
    assert_eq!(reference.get().revision, 1);
    // 故障在 revision 检查之后注入：目标变为目录，原子 rename 必须失败。
    *state.persist_fault.lock().unwrap() = Some(h.file());
    let result = h.update("a", config("db", 3, 30, false, false)).await;
    match result {
        Err(LoaderControlError::Persist {
            snapshot,
            source_id,
            ..
        }) => {
            assert!(source_id.contains(h.file().to_str().unwrap()));
            assert_eq!(
                snapshot.entry("include:a").unwrap().state,
                Some(FiberState::Active)
            );
        }
        _ => panic!("expected Persist"),
    }
    assert_eq!(reference.get().revision, 2);
    assert_eq!(reference.get().value.limit, 3);
    assert_eq!(
        h.loader
            .subtree("include")
            .unwrap()
            .entries()
            .unwrap()
            .extensions[0]
            .config,
        entry("a", "db", 3).config
    );
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn move_and_group_rebuild_replace_only_affected_references() {
    let state = Arc::new(State::default());
    let h = Harness::new(
        &state,
        vec![
            EntryOptions::group("g", vec![entry("a", "db", 1)]).unwrap(),
            entry("b", "db", 1),
        ],
    )
    .await;
    let old = state.refs.lock().unwrap()[1].clone();
    // Build order is path-sorted: b precedes g:a.
    let untouched = state.refs.lock().unwrap()[0].clone();
    let mut sub = old.subscribe();
    h.loader
        .subtree("include")
        .unwrap()
        .update("g:a", EntryUpdate::default(), Some(None), None)
        .await
        .unwrap();
    assert!(sub.changed().await.is_err());
    assert_eq!(untouched.get().revision, 0);
    h.update("a", config("db", 2, 20, false, false))
        .await
        .unwrap();
    assert_eq!(old.get().value.limit, 1);
    h.loader
        .subtree("include")
        .unwrap()
        .create(entry("c", "db", 1), Some("g"), None)
        .await
        .unwrap();
    let child = state.refs.lock().unwrap().last().unwrap().clone();
    let mut child_updates = child.subscribe();
    h.loader
        .subtree("include")
        .unwrap()
        .update(
            "g",
            EntryUpdate {
                isolate: Some(Some(std::collections::BTreeMap::from([(
                    "dep".into(),
                    cordis_loader::IsolateValue::Flag(true),
                )]))),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap();
    assert!(child_updates.changed().await.is_err());
    assert_eq!(state.refs.lock().unwrap().last().unwrap().get().revision, 0);
    assert_eq!(untouched.get().revision, 0);
    let restored_child = state.refs.lock().unwrap().last().unwrap().clone();
    let mut restored_changes = restored_child.subscribe();
    h.loader
        .subtree("include")
        .unwrap()
        .update(
            "g",
            EntryUpdate {
                disabled: Some(true),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap();
    assert!(restored_changes.changed().await.is_err());
    h.loader
        .subtree("include")
        .unwrap()
        .update(
            "g",
            EntryUpdate {
                disabled: Some(false),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(state.refs.lock().unwrap().last().unwrap().get().revision, 0);
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_old_disposer_does_not_associate_candidate_binding_with_old_plugin() {
    let state = Arc::new(State::default());
    let h = Harness::new(&state, vec![entry("a", "dispose-error", 1)]).await;
    let old = state.refs.lock().unwrap()[0].clone();
    h.write(vec![entry("a", "fixed", 2)]);
    assert!(h.loader.subtree("include").unwrap().reload().await.is_err());
    let abandoned = state.refs.lock().unwrap()[1].clone();
    assert_eq!(old.get().revision, 0);
    assert_eq!(old.get().value.limit, 1);
    assert!(abandoned.subscribe().changed().await.is_err());
    h.loader.subtree("include").unwrap().reload().await.unwrap();
    assert!(old.subscribe().changed().await.is_err());
    let current = state.refs.lock().unwrap().last().unwrap().clone();
    h.update("a", config("fixed", 3, 30, false, false))
        .await
        .unwrap();
    assert_eq!(current.get().revision, 1);
    assert_eq!(current.get().value.limit, 3);
    assert_eq!(abandoned.get().value.limit, 2);
    // Core 保留并上报已发生的 disposer 错误；恢复成功不擦除历史释放诊断。
    assert!(
        matches!(h.runtime.shutdown().await, Err(CoreError::DisposeFailed { errors }) if errors.iter().any(|error| error.contains("dispose error")))
    );
}

#[tokio::test]
async fn include_subtree_uses_same_hot_update_and_persistence_path() {
    let state = Arc::new(State::default());
    let include = EntryOptions::new("reports", "cordis:include")
        .with_config(toml::from_str("path = 'child.toml'").unwrap());
    let h = Harness::with_child(&state, vec![include], Some(vec![entry("a", "db", 1)])).await;
    let reference = state.refs.lock().unwrap()[0].clone();
    let subtree = h.loader.subtree("include:reports").unwrap();
    let before = h
        .loader
        .await_idle()
        .await
        .unwrap()
        .entries
        .iter()
        .find(|e| e.path.as_str() == "include:reports:a")
        .unwrap()
        .fiber_id;
    let root_text = fs::read_to_string(h.file()).unwrap();
    subtree
        .update(
            "a",
            EntryUpdate {
                config: Some(config("db", 2, 20, false, false)),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(reference.get().revision, 1);
    assert_eq!(fs::read_to_string(h.file()).unwrap(), root_text);
    fs::write(
        h.directory.path().join("child.toml"),
        toml::to_string(&target(vec![entry("a", "db", 3)])).unwrap(),
    )
    .unwrap();
    subtree.reload().await.unwrap();
    assert_eq!(reference.get().revision, 2);
    assert_eq!(
        before,
        h.loader
            .await_idle()
            .await
            .unwrap()
            .entries
            .iter()
            .find(|e| e.path.as_str() == "include:reports:a")
            .unwrap()
            .fiber_id
    );
    let mut changes = reference.subscribe();
    h.loader
        .subtree("include")
        .unwrap()
        .remove("reports")
        .await
        .unwrap();
    assert!(changes.changed().await.is_err());
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn registration_shares_id_checks_and_schema_is_complete_raw_input() {
    let state = Arc::new(State::default());
    let mut catalog = catalog(&state);
    assert!(matches!(
        catalog.register(Factory(state)),
        Err(LoaderError::DuplicateFactory { .. })
    ));
    let descriptor = catalog
        .factories()
        .unwrap()
        .into_iter()
        .find(|f| f.id == "live")
        .unwrap();
    assert!(descriptor.schema["properties"].get("address").is_some());
    assert!(descriptor.schema["properties"].get("limit").is_some());
    assert!(!descriptor.schema.to_string().contains("revision"));
    struct Ordinary;
    impl cordis_loader::ExtensionFactory for Ordinary {
        type Config = Input;
        fn id(&self) -> &'static str {
            "live"
        }
        fn prepare(&self, _: Input) -> Result<PreparedExtension, LoaderError> {
            Ok(PreparedExtension::ordinary(|| {
                Err(LoaderError::invalid_config("test factory not mounted"))
            }))
        }
    }
    assert!(matches!(
        catalog.register(Ordinary),
        Err(LoaderError::DuplicateFactory { .. })
    ));
    let mut reverse = ExtensionCatalog::new();
    reverse.register(Ordinary).unwrap();
    assert!(matches!(
        reverse.register(Factory(Arc::new(State::default()))),
        Err(LoaderError::DuplicateFactory { .. })
    ));
}

#[tokio::test]
async fn partial_hot_commit_is_visible_and_retry_does_not_duplicate_revision() {
    let state = Arc::new(State::default());
    let b = EntryOptions::new("b", "live").with_config(config("db", 1, 10, false, true));
    let h = Harness::new(&state, vec![entry("a", "db", 1), b, entry("c", "db", 1)]).await;
    let provider = h.runtime.root().effect().unwrap();
    provider.provide(DEP, ()).unwrap();
    h.runtime.settle().await;
    let before = h.loader.await_idle().await.unwrap();
    state.waiting_fiber.store(
        before.entry("include:b").unwrap().fiber_id.unwrap(),
        Ordering::SeqCst,
    );
    *state.drop_provider.lock().unwrap() = Some(provider);
    let refs = state.refs.lock().unwrap().clone();
    h.write(vec![
        entry("a", "db", 2),
        EntryOptions::new("b", "live").with_config(config("db", 2, 20, false, true)),
        entry("c", "drop-provider", 1),
    ]);
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        h.loader.subtree("include").unwrap().reload(),
    )
    .await
    .unwrap();
    assert!(matches!(
        result,
        Err(LoaderControlError::Loader(
            LoaderError::HotConfigStale { .. }
        ))
    ));
    assert_eq!(refs[0].get().revision, 1);
    assert_eq!(refs[1].get().revision, 0);
    assert_eq!(
        h.loader
            .subtree("include")
            .unwrap()
            .entries()
            .unwrap()
            .extensions[0]
            .config,
        entry("a", "db", 1).config
    );
    let provider = h.runtime.root().effect().unwrap();
    provider.provide(DEP, ()).unwrap();
    h.runtime.settle().await;
    h.loader.subtree("include").unwrap().reload().await.unwrap();
    assert_eq!(refs[0].get().revision, 1);
    assert_eq!(refs[1].get().revision, 1);
    h.runtime.shutdown().await.unwrap();
}

async fn file_plugin(
    catalog: ExtensionCatalog,
    bootstrap: impl AsRef<std::path::Path>,
) -> Result<LoaderPlugin, LoaderError> {
    let entry = cordis_plugin_include::bootstrap_entry("include", bootstrap)
        .await
        .map_err(|error| LoaderError::invalid_config(error.to_string()))?;
    LoaderPlugin::new(
        catalog,
        ExtensionsConfig {
            version: 3,
            extensions: vec![entry],
        },
    )
}
