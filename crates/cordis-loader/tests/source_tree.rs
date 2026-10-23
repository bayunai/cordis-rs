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
struct MemorySource {
    data: Mutex<ExtensionsConfig>,
    version: AtomicU64,
    writes: AtomicUsize,
    fail: AtomicBool,
}
impl MemorySource {
    fn new() -> Self {
        Self {
            data: Mutex::new(config(vec![entry("leaf", 1)])),
            version: AtomicU64::new(0),
            writes: AtomicUsize::new(0),
            fail: AtomicBool::new(false),
        }
    }
    fn revision(&self) -> SourceRevision {
        SourceRevision::new(self.version.load(Ordering::SeqCst).to_le_bytes().to_vec())
    }
}
impl EntryTreeSource for MemorySource {
    fn identity(&self) -> String {
        "test-memory-source".into()
    }
    fn context(&self, ctx: &Context) -> Result<Context, SourceError> {
        ctx.intercept(MARKER, 42)
            .map_err(|_| SourceError::new(SourceErrorKind::Unavailable, "context closed"))
    }
    fn read(&self) -> Result<SourceDocument, SourceError> {
        Ok(SourceDocument {
            config: self.data.lock().unwrap().clone(),
            revision: self.revision(),
        })
    }
    fn check_revision(&self, expected: &SourceRevision) -> Result<(), SourceError> {
        if *expected == self.revision() {
            Ok(())
        } else {
            Err(SourceError::new(
                SourceErrorKind::Conflict,
                "source changed",
            ))
        }
    }
    fn validate_write(&self, _: &ExtensionsConfig) -> Result<(), SourceError> {
        Ok(())
    }
    fn write(
        &self,
        target: &ExtensionsConfig,
        expected: &SourceRevision,
    ) -> Result<SourceRevision, SourceError> {
        self.check_revision(expected)?;
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
            move |_, _, _| Ok(source.clone() as Arc<dyn EntryTreeSource>),
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
    runtime.shutdown().await.unwrap();
    assert!(tree.entries().is_err());
}
