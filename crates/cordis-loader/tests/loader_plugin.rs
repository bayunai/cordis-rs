use async_trait::async_trait;
use cordis_core::{Context, CoreError, Fiber, FiberState, Plugin, PluginKey, Runtime, ServiceKey};
use cordis_loader::{
    ExtensionCatalog, ExtensionEntry, ExtensionFactory, ExtensionsConfig, LOADER, Loader,
    LoaderControlError, LoaderError, LoaderPlugin,
};
use schemars::JsonSchema;
use std::{
    fs,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::Notify;

static PRIVATE: ServiceKey<&'static str> = ServiceKey::new("loader.test.private@1");
static BLOCK: ServiceKey<()> = ServiceKey::new("loader.test.block@1");

#[derive(Clone)]
struct TestFactory {
    id: &'static str,
    dependency: bool,
    provide_private: bool,
    disposals: Arc<AtomicUsize>,
}

impl TestFactory {
    fn new(id: &'static str) -> Self {
        Self {
            id,
            dependency: false,
            provide_private: false,
            disposals: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn dependency(mut self) -> Self {
        self.dependency = true;
        self
    }

    fn provide_private(mut self) -> Self {
        self.provide_private = true;
        self
    }
}

#[async_trait]
impl Plugin for TestPlugin {
    fn key(&self) -> PluginKey {
        PluginKey::new(self.key)
    }

    fn inject(&self) -> Vec<cordis_core::ServiceId> {
        self.dependency
            .then_some(PRIVATE.id())
            .into_iter()
            .collect()
    }

    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        if self.provide_private {
            ctx.provide(PRIVATE, "private")?;
        }
        let disposals = self.disposals.clone();
        ctx.effect()?.on_dispose(move || {
            disposals.fetch_add(1, Ordering::SeqCst);
        });
        Ok(())
    }
}

struct TestPlugin {
    key: &'static str,
    dependency: bool,
    provide_private: bool,
    disposals: Arc<AtomicUsize>,
}

impl ExtensionFactory for TestFactory {
    type Config = serde_json::Value;

    fn id(&self) -> &'static str {
        self.id
    }

    fn build(&self, _: Self::Config) -> Result<Arc<dyn Plugin>, LoaderError> {
        Ok(Arc::new(TestPlugin {
            key: self.id,
            dependency: self.dependency,
            provide_private: self.provide_private,
            disposals: self.disposals.clone(),
        }))
    }
}

fn catalog(factories: Vec<TestFactory>) -> ExtensionCatalog {
    let mut catalog = ExtensionCatalog::new();
    for factory in factories {
        catalog.register(factory).unwrap();
    }
    catalog
}

fn config(entries: Vec<ExtensionEntry>) -> ExtensionsConfig {
    ExtensionsConfig {
        version: 1,
        extensions: entries,
    }
}

async fn mount(plugin: LoaderPlugin) -> (Runtime, Fiber, Loader) {
    let runtime = Runtime::new().unwrap();
    let root = runtime.root();
    let fiber = root.plugin(Arc::new(plugin)).await.unwrap();
    assert_ne!(
        fiber.state(),
        FiberState::Failed,
        "{:?}",
        fiber.last_error()
    );
    let loader = (*root.get(LOADER).unwrap()).clone();
    (runtime, fiber, loader)
}

#[tokio::test]
async fn loader_plugin_provides_service_and_unload_releases_its_tree() {
    let factory = TestFactory::new("demo.provider").provide_private();
    let disposals = factory.disposals.clone();
    let plugin = LoaderPlugin::new(
        catalog(vec![factory]),
        config(vec![ExtensionEntry::new("provider", "demo.provider", true)]),
    )
    .unwrap();
    let (runtime, mut loader_fiber, loader) = mount(plugin).await;

    assert_eq!(loader.await_idle().await.unwrap().instances.len(), 1);
    assert!(runtime.root().get(PRIVATE).is_err());

    loader_fiber.dispose_wait().await.unwrap();
    assert!(runtime.root().get(LOADER).is_err());
    assert!(matches!(
        loader.entries(),
        Err(LoaderControlError::LoaderUnavailable)
    ));
    assert_eq!(disposals.load(Ordering::SeqCst), 1);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn entries_use_isolated_child_contexts() {
    let plugin = LoaderPlugin::new(
        catalog(vec![
            TestFactory::new("demo.provider").provide_private(),
            TestFactory::new("demo.consumer").dependency(),
        ]),
        config(vec![
            ExtensionEntry::new("provider", "demo.provider", true),
            ExtensionEntry::new("consumer", "demo.consumer", true),
        ]),
    )
    .unwrap();
    let (runtime, _loader_fiber, loader) = mount(plugin).await;
    let snapshot = loader.await_idle().await.unwrap();

    assert_eq!(
        snapshot.instance("provider").unwrap().state,
        FiberState::Active
    );
    assert_eq!(
        snapshot.instance("consumer").unwrap().state,
        FiberState::Pending
    );
    assert!(runtime.root().get(PRIVATE).is_err());
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn persistent_changes_reload_and_conflict_keep_existing_semantics() {
    let directory = tempfile::tempdir().unwrap();
    let bootstrap = directory.path().join("bootstrap.toml");
    let extensions = directory.path().join("extensions.toml");
    fs::write(
        &bootstrap,
        "version = 1\n[config]\ndriver = \"file\"\npath = \"extensions.toml\"\n",
    )
    .unwrap();
    fs::write(&extensions, "version = 1\nextensions = []\n").unwrap();
    let plugin =
        LoaderPlugin::bootstrap(catalog(vec![TestFactory::new("demo.file")]), &bootstrap).unwrap();
    let (runtime, _loader_fiber, loader) = mount(plugin).await;

    loader
        .create(ExtensionEntry::new("one", "demo.file", true))
        .await
        .unwrap();
    assert_eq!(
        loader
            .await_idle()
            .await
            .unwrap()
            .instance("one")
            .unwrap()
            .state,
        FiberState::Active
    );
    assert!(
        fs::read_to_string(&extensions)
            .unwrap()
            .contains("instance = \"one\"")
    );

    fs::write(&extensions, "version = 1\nextensions = []\n# edited\n").unwrap();
    assert!(matches!(
        loader
            .create(ExtensionEntry::new("two", "demo.file", true))
            .await,
        Err(LoaderControlError::ConfigConflict { .. })
    ));
    loader.reload().await.unwrap();
    assert!(loader.await_idle().await.unwrap().instances.is_empty());
    runtime.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn persist_failure_keeps_the_applied_snapshot() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().unwrap();
    let bootstrap = directory.path().join("bootstrap.toml");
    fs::write(
        &bootstrap,
        "version = 1\n[config]\ndriver = \"file\"\npath = \"extensions.toml\"\n",
    )
    .unwrap();
    fs::write(
        directory.path().join("extensions.toml"),
        "version = 1\nextensions = []\n",
    )
    .unwrap();
    let plugin =
        LoaderPlugin::bootstrap(catalog(vec![TestFactory::new("demo.persist")]), &bootstrap)
            .unwrap();
    let (runtime, _loader_fiber, loader) = mount(plugin).await;
    let original = fs::metadata(directory.path()).unwrap().permissions();
    let mut read_only = original.clone();
    read_only.set_mode(0o500);
    fs::set_permissions(directory.path(), read_only).unwrap();

    let result = loader
        .create(ExtensionEntry::new("persist", "demo.persist", true))
        .await;

    fs::set_permissions(directory.path(), original).unwrap();
    match result.unwrap_err() {
        LoaderControlError::Persist { snapshot, .. } => {
            assert_eq!(
                snapshot.instance("persist").unwrap().state,
                FiberState::Active
            );
        }
        error => panic!("expected persist error, got {error}"),
    }
    assert_eq!(
        loader
            .await_idle()
            .await
            .unwrap()
            .instance("persist")
            .unwrap()
            .state,
        FiberState::Active
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn await_idle_does_not_wait_for_unrelated_runtime_work() {
    let runtime = Runtime::new().unwrap();
    let root = runtime.root();
    let gate = Arc::new(Notify::new());
    let waiter = gate.clone();
    root.inject([BLOCK.id()], move |_, _| {
        let waiter = waiter.clone();
        async move {
            waiter.notified().await;
            Ok(())
        }
    })
    .unwrap();
    root.provide(BLOCK, ()).unwrap();

    let plugin = LoaderPlugin::new(
        catalog(vec![TestFactory::new("demo.fast")]),
        config(vec![ExtensionEntry::new("fast", "demo.fast", true)]),
    )
    .unwrap();
    let _loader_fiber = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        root.plugin(Arc::new(plugin)),
    )
    .await
    .expect("Loader must not wait for unrelated work")
    .unwrap();
    let loader = root.get(LOADER).unwrap();
    assert_eq!(loader.await_idle().await.unwrap().instances.len(), 1);

    gate.notify_waiters();
    runtime.shutdown().await.unwrap();
}

#[test]
fn typed_factory_schema_is_available_before_plugin_mount() {
    #[derive(serde::Deserialize, JsonSchema)]
    struct Typed;
    struct TypedFactory;
    impl ExtensionFactory for TypedFactory {
        type Config = Typed;
        fn id(&self) -> &'static str {
            "demo.typed"
        }
        fn build(&self, _: Typed) -> Result<Arc<dyn Plugin>, LoaderError> {
            Err(LoaderError::invalid_config("not used"))
        }
    }
    let mut catalog = ExtensionCatalog::new();
    catalog.register(TypedFactory).unwrap();
    assert!(catalog.factories().unwrap()[0].schema.is_object());
}
