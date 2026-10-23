//! Loader 的来源契约测试只使用内存后端，不依赖文件插件或编解码器。
use async_trait::async_trait;
use cordis_core::{ConfigKey, Context, CoreError, Fiber, Plugin, PluginKey, Runtime};
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
struct LeafFactory;
struct Leaf(u32);
impl ExtensionFactory for LeafFactory {
    type Config = Input;
    fn id(&self) -> &'static str {
        "leaf"
    }
    fn prepare(&self, cfg: Input) -> Result<PreparedExtension, LoaderError> {
        Ok(PreparedExtension::ordinary(move || {
            Ok(Arc::new(Leaf(cfg.value)))
        }))
    }
}
#[async_trait]
impl Plugin for Leaf {
    fn key(&self) -> PluginKey {
        PluginKey::new("test.leaf")
    }
    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        assert_eq!(*ctx.config(MARKER)?, 42);
        assert!(self.0 > 0);
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
    let mut catalog = ExtensionCatalog::new();
    catalog.register(Carrier(source.clone())).unwrap();
    catalog.register(LeafFactory).unwrap();
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
    runtime.shutdown().await.unwrap();
}
