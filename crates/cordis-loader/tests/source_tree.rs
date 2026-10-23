//! Loader 的来源契约测试只使用内存后端，不依赖文件插件或编解码器。
use async_trait::async_trait;
use cordis_core::{
    ConfigKey, Context, CoreError, Fiber, FiberState, Plugin, PluginKey, Runtime, ServiceKey,
};
use cordis_loader::{
    EntryOptions, EntryTreeSource, EntryUpdate, ExtensionCatalog, ExtensionFactory,
    ExtensionsConfig, LOADER, Loader, LoaderControlError, LoaderError, LoaderPlugin,
    PreparedExtension, SourceDocument, SourceError, SourceErrorKind, SourceRevision,
};
use schemars::JsonSchema;
use serde::Deserialize;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};

static MARKER: ConfigKey<u32> = ConfigKey::new("test.source.marker");
static MISSING: ServiceKey<()> = ServiceKey::new("test.source.missing@1");
#[derive(Default)]
struct Gate {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
impl Gate {
    async fn wait(&self) {
        self.entered.notify_one();
        self.release.notified().await;
    }
}
struct MemorySource {
    data: Mutex<ExtensionsConfig>,
    version: AtomicU64,
    writes: AtomicUsize,
    reads: AtomicUsize,
    prepares: AtomicUsize,
    applies: AtomicUsize,
    reject_apply: AtomicBool,
    pending: AtomicBool,
    reject_dispose: AtomicBool,
    disposes: AtomicUsize,
    fail: AtomicBool,
    read_gate: Mutex<Option<Arc<Gate>>>,
    write_gate: Mutex<Option<Arc<Gate>>>,
}
impl MemorySource {
    fn new() -> Self {
        Self {
            data: Mutex::new(config(vec![entry("leaf", 1)])),
            version: AtomicU64::new(0),
            writes: AtomicUsize::new(0),
            reads: AtomicUsize::new(0),
            prepares: AtomicUsize::new(0),
            applies: AtomicUsize::new(0),
            reject_apply: AtomicBool::new(false),
            pending: AtomicBool::new(false),
            reject_dispose: AtomicBool::new(false),
            disposes: AtomicUsize::new(0),
            fail: AtomicBool::new(false),
            read_gate: Mutex::new(None),
            write_gate: Mutex::new(None),
        }
    }
    fn revision(&self) -> SourceRevision {
        SourceRevision::new(self.version.load(Ordering::SeqCst).to_le_bytes().to_vec())
    }
}
#[async_trait]
impl EntryTreeSource for MemorySource {
    fn enable_logs(&self) -> bool {
        false
    }
    fn identity(&self) -> String {
        "test-memory-source".into()
    }
    fn context(&self, ctx: &Context) -> Result<Context, SourceError> {
        ctx.intercept(MARKER, 42)
            .map_err(|_| SourceError::new(SourceErrorKind::Unavailable, "context closed"))
    }
    async fn read(&self) -> Result<SourceDocument, SourceError> {
        let gate = self.read_gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            gate.wait().await;
        }
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(SourceDocument {
            config: self.data.lock().unwrap().clone(),
            revision: self.revision(),
        })
    }
    async fn check_revision(&self, expected: &SourceRevision) -> Result<(), SourceError> {
        if *expected == self.revision() {
            Ok(())
        } else {
            Err(SourceError::new(
                SourceErrorKind::Conflict,
                "source changed",
            ))
        }
    }
    async fn validate_write(&self, _: &ExtensionsConfig) -> Result<(), SourceError> {
        Ok(())
    }
    async fn write(
        &self,
        target: &ExtensionsConfig,
        expected: &SourceRevision,
    ) -> Result<SourceRevision, SourceError> {
        let gate = self.write_gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            gate.wait().await;
        }
        self.check_revision(expected).await?;
        self.writes.fetch_add(1, Ordering::SeqCst);
        if self.fail.load(Ordering::SeqCst) {
            return Err(SourceError::new(SourceErrorKind::Io, "write rejected"));
        }
        *self.data.lock().unwrap() = target.clone();
        self.version.fetch_add(1, Ordering::SeqCst);
        Ok(self.revision())
    }
}
#[derive(Clone, PartialEq, Deserialize, JsonSchema)]
struct Empty {}
struct Carrier(Arc<MemorySource>);
impl ExtensionFactory for Carrier {
    type Config = Empty;
    fn id(&self) -> &'static str {
        "source"
    }
    fn prepare(&self, cfg: Empty) -> Result<PreparedExtension, LoaderError> {
        let source = self.0.clone();
        Ok(PreparedExtension::tree(
            PluginKey::new("test.source"),
            (),
            cfg,
            move |_, _, _| {
                let source = source.clone();
                async move { Ok(source as Arc<dyn EntryTreeSource>) }
            },
        ))
    }
}
#[derive(Deserialize, JsonSchema)]
struct Input {
    value: u32,
}
struct LeafFactory(Arc<MemorySource>);
struct Leaf {
    value: u32,
    source: Arc<MemorySource>,
    needs_dep: bool,
}
impl ExtensionFactory for LeafFactory {
    type Config = Input;
    fn id(&self) -> &'static str {
        "leaf"
    }
    fn prepare(&self, cfg: Input) -> Result<PreparedExtension, LoaderError> {
        self.0.prepares.fetch_add(1, Ordering::SeqCst);
        let source = self.0.clone();
        let needs_dep = source.pending.load(Ordering::SeqCst);
        Ok(PreparedExtension::ordinary(move || {
            Ok(Arc::new(Leaf {
                value: cfg.value,
                source,
                needs_dep,
            }))
        }))
    }
}
#[async_trait]
impl Plugin for Leaf {
    fn key(&self) -> PluginKey {
        PluginKey::new("test.leaf")
    }
    fn inject(&self) -> Vec<cordis_core::ServiceId> {
        self.needs_dep.then_some(MISSING.id()).into_iter().collect()
    }
    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        self.source.applies.fetch_add(1, Ordering::SeqCst);
        if self.source.reject_apply.load(Ordering::SeqCst) {
            return Err(CoreError::PluginApply(
                "controlled lifecycle rejection".into(),
            ));
        }
        assert_eq!(*ctx.config(MARKER)?, 42);
        assert!(self.value > 0);
        let source = self.source.clone();
        ctx.effect_named("source-test-cleanup")?
            .on_dispose_async(move || async move {
                source.disposes.fetch_add(1, Ordering::SeqCst);
                if source.reject_dispose.load(Ordering::SeqCst) {
                    Err(CoreError::PluginApply("controlled dispose failure".into()))
                } else {
                    Ok(())
                }
            })?;
        Ok(())
    }
}
fn config(extensions: Vec<EntryOptions>) -> ExtensionsConfig {
    ExtensionsConfig {
        version: 3,
        extensions,
    }
}
fn entry(id: &str, value: u32) -> EntryOptions {
    EntryOptions::new(id, "leaf").with_config(toml::toml! {value=value}.into())
}
async fn mount() -> (Runtime, Fiber, Loader, Arc<MemorySource>) {
    let source = Arc::new(MemorySource::new());
    mount_source(source).await
}

async fn mount_source(source: Arc<MemorySource>) -> (Runtime, Fiber, Loader, Arc<MemorySource>) {
    let mut catalog = ExtensionCatalog::new();
    catalog.register(Carrier(source.clone())).unwrap();
    catalog.register(LeafFactory(source.clone())).unwrap();
    let runtime = Runtime::new().unwrap();
    let fiber = runtime
        .root()
        .plugin(Arc::new(
            LoaderPlugin::new(catalog, config(vec![EntryOptions::new("tree", "source")])).unwrap(),
        ))
        .await
        .unwrap();
    let loader = (*runtime.root().get(LOADER).unwrap()).clone();
    (runtime, fiber, loader, source)
}

#[tokio::test]
async fn memory_root_mutation_does_not_persist_and_source_tree_uses_its_backend() {
    let (runtime, _fiber, loader, source) = mount().await;
    loader
        .update("tree", EntryUpdate::default(), None, None)
        .await
        .unwrap();
    assert_eq!(source.writes.load(Ordering::SeqCst), 0);
    assert!(matches!(
        loader.reload().await,
        Err(LoaderControlError::Loader(LoaderError::NoConfigSource))
    ));
    let tree = loader.subtree("tree").unwrap();
    tree.create(entry("other", 2), None, None).await.unwrap();
    assert_eq!(source.writes.load(Ordering::SeqCst), 1);
    assert_eq!(source.data.lock().unwrap().extensions.len(), 2);
    let form = loader.describe_config("tree:leaf").await.unwrap();
    loader
        .mutate_config(
            "tree:leaf",
            form.revision,
            vec![cordis_loader::ConfigOperation::Set {
                path: vec!["value".into()],
                value: 3.into(),
            }],
        )
        .await
        .unwrap();
    assert_eq!(source.writes.load(Ordering::SeqCst), 2);
    assert_eq!(
        source.data.lock().unwrap().extensions[0].config["value"].as_integer(),
        Some(3)
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn unchanged_reload_reads_source_but_does_not_prepare_revise_or_publish() {
    let (runtime, _fiber, loader, source) = mount().await;
    loader.await_idle().await.unwrap();
    tokio::task::yield_now().await;
    let tree = loader.subtree("tree").unwrap();
    let before = loader.describe_config("tree:leaf").await.unwrap();
    let fiber = tree
        .snapshot()
        .unwrap()
        .entry("tree:leaf")
        .unwrap()
        .fiber_id;
    let prepares = source.prepares.load(Ordering::SeqCst);
    let applies = source.applies.load(Ordering::SeqCst);
    let reads = source.reads.load(Ordering::SeqCst);
    let mut events = loader.subscribe().unwrap();
    for _ in 0..3 {
        let snapshot = tree.reload().await.unwrap();
        assert_eq!(snapshot.entry("tree:leaf").unwrap().fiber_id, fiber);
        assert_eq!(
            loader.describe_config("tree:leaf").await.unwrap().revision,
            before.revision
        );
    }
    tokio::task::yield_now().await;
    assert_eq!(source.prepares.load(Ordering::SeqCst), prepares);
    assert_eq!(source.applies.load(Ordering::SeqCst), applies);
    assert_eq!(source.reads.load(Ordering::SeqCst), reads + 3);
    assert!(matches!(
        events.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn unchanged_source_still_retries_failed_and_manually_unmounted_entries() {
    let source = Arc::new(MemorySource::new());
    source.reject_apply.store(true, Ordering::SeqCst);
    let (runtime, _fiber, loader, source) = mount_source(source).await;
    let tree = loader.subtree("tree").unwrap();
    assert_eq!(
        tree.snapshot().unwrap().entry("tree:leaf").unwrap().state,
        Some(FiberState::Failed)
    );
    let prepares = source.prepares.load(Ordering::SeqCst);
    source.reject_apply.store(false, Ordering::SeqCst);
    tree.reload().await.unwrap();
    assert!(source.prepares.load(Ordering::SeqCst) > prepares);
    let active = tree.snapshot().unwrap().entry("tree:leaf").unwrap().clone();
    assert_eq!(active.state, Some(FiberState::Active));
    runtime.unmount(PluginKey::new("test.leaf")).await.unwrap();
    tree.reload().await.unwrap();
    let restored = tree.snapshot().unwrap().entry("tree:leaf").unwrap().clone();
    assert_eq!(restored.state, Some(FiberState::Active));
    assert_ne!(restored.fiber_id, active.fiber_id);
    runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn stable_pending_entry_does_not_force_unchanged_reload() {
    let source = Arc::new(MemorySource::new());
    source.pending.store(true, Ordering::SeqCst);
    let (runtime, _fiber, loader, source) = mount_source(source).await;
    let tree = loader.subtree("tree").unwrap();
    let before = loader.describe_config("tree:leaf").await.unwrap();
    let snapshot = tree.snapshot().unwrap();
    assert_eq!(
        snapshot.entry("tree:leaf").unwrap().state,
        Some(FiberState::Pending)
    );
    let prepares = source.prepares.load(Ordering::SeqCst);
    tree.reload().await.unwrap();
    assert_eq!(source.prepares.load(Ordering::SeqCst), prepares);
    assert_eq!(
        loader.describe_config("tree:leaf").await.unwrap().revision,
        before.revision
    );
    assert_eq!(
        tree.snapshot()
            .unwrap()
            .entry("tree:leaf")
            .unwrap()
            .fiber_id,
        snapshot.entry("tree:leaf").unwrap().fiber_id
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn failed_reload_is_retried_without_accepting_the_candidate_revision() {
    let (runtime, _fiber, loader, source) = mount().await;
    let tree = loader.subtree("tree").unwrap();
    *source.data.lock().unwrap() = config(vec![entry("leaf", 2)]);
    source.version.fetch_add(1, Ordering::SeqCst);
    source.reject_apply.store(true, Ordering::SeqCst);
    assert!(tree.reload().await.is_err());
    source.reject_apply.store(false, Ordering::SeqCst);
    tree.reload().await.unwrap();
    assert_eq!(
        tree.snapshot().unwrap().entry("tree:leaf").unwrap().state,
        Some(FiberState::Active)
    );
    // 成功后才接受新的来源版本；后续提交不应再报告外部来源冲突。
    tree.update("leaf", EntryUpdate::default(), None, None)
        .await
        .unwrap();
    assert_eq!(source.writes.load(Ordering::SeqCst), 1);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn generic_source_conflict_persist_and_close_are_not_file_specific() {
    let (runtime, _fiber, loader, source) = mount().await;
    let tree = loader.subtree("tree").unwrap();
    source.version.fetch_add(1, Ordering::SeqCst);
    assert!(matches!(
        tree.create(entry("other", 2), None, None).await,
        Err(LoaderControlError::ConfigConflict { .. })
    ));
    tree.reload().await.unwrap();
    source.fail.store(true, Ordering::SeqCst);
    assert!(matches!(
        tree.create(entry("other", 2), None, None).await,
        Err(LoaderControlError::Persist { .. })
    ));
    assert!(tree.snapshot().unwrap().entry("tree:other").is_some());
    assert_eq!(source.data.lock().unwrap().extensions.len(), 1);
    // 写回失败不能发布候选来源版本；恢复后同一目标仍能基于最后成功版本提交。
    assert_eq!(source.version.load(Ordering::SeqCst), 1);
    source.fail.store(false, Ordering::SeqCst);
    tree.update("other", EntryUpdate::default(), None, None)
        .await
        .unwrap();
    assert_eq!(source.version.load(Ordering::SeqCst), 2);
    assert_eq!(source.data.lock().unwrap().extensions.len(), 2);
    runtime.shutdown().await.unwrap();
    assert!(tree.entries().is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn reload_waits_for_async_read_without_blocking_the_runtime() {
    let (runtime, _fiber, loader, source) = mount().await;
    let gate = Arc::new(Gate::default());
    *source.read_gate.lock().unwrap() = Some(gate.clone());
    let tree = loader.subtree("tree").unwrap();
    let reload = tokio::spawn(async move { tree.reload().await });
    gate.entered.notified().await;
    assert!(!reload.is_finished());
    // 此任务与 reload 共用唯一异步线程，来源等待时必须仍可调度。
    tokio::spawn(async {}).await.unwrap();
    let subtree = loader.subtree("tree").unwrap();
    let subtree_idle = tokio::spawn(async move { subtree.await_idle().await });
    let loader_idle = tokio::spawn(async move { loader.await_idle().await });
    tokio::task::yield_now().await;
    // 异步来源读取尚未进入 reconcile，也不能提前宣称 Loader 已稳定。
    assert!(!subtree_idle.is_finished());
    assert!(!loader_idle.is_finished());
    gate.release.notify_one();
    reload.await.unwrap().unwrap();
    subtree_idle.await.unwrap().unwrap();
    loader_idle.await.unwrap().unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_caller_does_not_abandon_write_and_shutdown_waits_for_settlement() {
    let (runtime, _fiber, loader, source) = mount().await;
    let gate = Arc::new(Gate::default());
    *source.write_gate.lock().unwrap() = Some(gate.clone());
    let tree = loader.subtree("tree").unwrap();
    let edit = tokio::spawn(async move { tree.create(entry("other", 2), None, None).await });
    gate.entered.notified().await;
    assert!(!edit.is_finished());
    edit.abort();
    assert!(edit.await.unwrap_err().is_cancelled());
    let shutdown = tokio::spawn(async move { runtime.shutdown().await });
    tokio::task::yield_now().await;
    assert!(!shutdown.is_finished());
    assert_eq!(source.writes.load(Ordering::SeqCst), 0);
    gate.release.notify_one();
    shutdown.await.unwrap().unwrap();
    assert_eq!(source.writes.load(Ordering::SeqCst), 1);
    assert_eq!(source.data.lock().unwrap().extensions.len(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_reload_still_settles_before_detach() {
    let (runtime, _fiber, loader, source) = mount().await;
    let gate = Arc::new(Gate::default());
    *source.read_gate.lock().unwrap() = Some(gate.clone());
    let tree = loader.subtree("tree").unwrap();
    let reload_tree = tree.clone();
    let reload = tokio::spawn(async move { reload_tree.reload().await });
    gate.entered.notified().await;
    let reads_before = source.reads.load(Ordering::SeqCst);
    reload.abort();
    assert!(reload.await.unwrap_err().is_cancelled());
    let detach = tokio::spawn(async move { tree.detach().await });
    tokio::task::yield_now().await;
    assert!(!detach.is_finished());
    assert_eq!(source.reads.load(Ordering::SeqCst), reads_before);
    gate.release.notify_one();
    detach.await.unwrap().unwrap();
    assert_eq!(source.reads.load(Ordering::SeqCst), reads_before + 1);
    assert!(loader.subtree("tree").is_err());
    runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn concurrent_detach_survives_waiter_cancellation_and_closes_admission() {
    let (runtime, _fiber, loader, source) = mount().await;
    let gate = Arc::new(Gate::default());
    *source.write_gate.lock().unwrap() = Some(gate.clone());
    let tree = loader.subtree("tree").unwrap();
    let edit_tree = tree.clone();
    let edit = tokio::spawn(async move { edit_tree.create(entry("other", 2), None, None).await });
    gate.entered.notified().await;
    let first_tree = tree.clone();
    let first = tokio::spawn(async move { first_tree.detach().await });
    tokio::task::yield_now().await;
    assert!(!first.is_finished());
    assert!(tree.entries().is_err());
    assert!(tree.create(entry("late", 3), None, None).await.is_err());
    let reads = source.reads.load(Ordering::SeqCst);
    assert!(
        loader
            .attach_subtree(&loader.entry_context("tree:leaf").unwrap(), source.clone())
            .await
            .is_err()
    );
    assert_eq!(source.reads.load(Ordering::SeqCst), reads);
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let second_tree = tree.clone();
    let second = tokio::spawn(async move { second_tree.detach().await });
    tokio::task::yield_now().await;
    assert!(!second.is_finished());
    gate.release.notify_one();
    edit.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    tree.detach().await.unwrap();
    assert_eq!(source.writes.load(Ordering::SeqCst), 1);
    assert_eq!(source.disposes.load(Ordering::SeqCst), 2);
    assert!(loader.subtree("tree").is_err());
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn detach_attempts_all_entries_and_reuses_the_failure_result() {
    let (runtime, _fiber, loader, source) = mount().await;
    let tree = loader.subtree("tree").unwrap();
    tree.create(entry("other", 2), None, None).await.unwrap();
    source.reject_dispose.store(true, Ordering::SeqCst);
    let first = tree.clone().detach().await.unwrap_err().to_string();
    assert!(first.contains("controlled dispose failure"));
    assert_eq!(source.disposes.load(Ordering::SeqCst), 2);
    assert!(loader.subtree("tree").is_err());
    source.reject_dispose.store(false, Ordering::SeqCst);
    assert_eq!(tree.detach().await.unwrap_err().to_string(), first);
    assert_eq!(source.disposes.load(Ordering::SeqCst), 2);
    // 原树的失败结果不会被关闭开关或后续清理伪装成成功。
    assert!(runtime.shutdown().await.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn async_reload_preflight_failure_keeps_runtime_and_source_revision_uncommitted() {
    let (runtime, _fiber, loader, source) = mount().await;
    let tree = loader.subtree("tree").unwrap();
    let before = tree.snapshot().unwrap();
    let leaf_fiber = before.entry("tree:leaf").unwrap().fiber_id;
    let mut invalid = entry("leaf", 2);
    invalid.config["value"] = "not-a-number".into();
    *source.data.lock().unwrap() = config(vec![entry("other", 2), invalid]);
    source.version.fetch_add(1, Ordering::SeqCst);
    let gate = Arc::new(Gate::default());
    *source.read_gate.lock().unwrap() = Some(gate.clone());
    let reload_tree = tree.clone();
    let reload = tokio::spawn(async move { reload_tree.reload().await });
    gate.entered.notified().await;
    assert!(tree.snapshot().unwrap().entry("tree:other").is_none());
    gate.release.notify_one();
    assert!(reload.await.unwrap().is_err());
    let after = tree.snapshot().unwrap();
    assert_eq!(after.entry("tree:leaf").unwrap().fiber_id, leaf_fiber);
    assert!(after.entry("tree:other").is_none());
    assert_eq!(source.writes.load(Ordering::SeqCst), 0);
    // 失败读取取得的新来源版本不能成为已提交版本，否则此操作会错误通过。
    assert!(matches!(
        tree.create(entry("added", 3), None, None).await,
        Err(LoaderControlError::ConfigConflict { .. })
    ));
    *source.read_gate.lock().unwrap() = None;
    *source.data.lock().unwrap() = config(vec![entry("other", 2), entry("leaf", 2)]);
    tree.reload().await.unwrap();
    assert_eq!(
        tree.snapshot().unwrap().entry("tree:other").unwrap().state,
        Some(FiberState::Active)
    );
    tree.update("leaf", EntryUpdate::default(), None, None)
        .await
        .unwrap();
    runtime.shutdown().await.unwrap();
}
