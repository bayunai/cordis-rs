//! 统一 Factory：延迟构造、能力/类型切换与预检错误。
use async_trait::async_trait;
use cordis_core::{Context, CoreError, Fiber, FiberState, Plugin, PluginKey, Runtime, ServiceKey};
use cordis_loader::{
    ConfigRef, EntryOptions, EntryUpdate, ExtensionCatalog, ExtensionFactory, ExtensionsConfig,
    LOADER, Loader, LoaderControlError, LoaderError, LoaderPlugin, PreparedExtension,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{
    fs,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Clone, Copy, Deserialize, Serialize, JsonSchema)]
enum Mode {
    Ordinary,
    Hot,
    OtherStatic,
    OtherLive,
    BadKey,
    PreparePanic,
    BuildPanic,
    BuildError,
    Invalid,
    CompareSafe,
    ComparePanic,
}

#[derive(Clone)]
struct Comparing(bool);
impl PartialEq for Comparing {
    fn eq(&self, other: &Self) -> bool {
        assert!(!self.0 && !other.0, "config comparison panic");
        true
    }
}
#[derive(Deserialize, Serialize, JsonSchema)]
struct Config {
    mode: Mode,
    value: u32,
}

#[derive(Clone)]
enum View {
    Plain(u32),
    U32(ConfigRef<u32>),
    U64(ConfigRef<u64>),
}
impl View {
    fn snapshot(&self) -> (u64, u64) {
        match self {
            Self::Plain(value) => (0, u64::from(*value)),
            Self::U32(reference) => {
                let snapshot = reference.get();
                (snapshot.revision, u64::from(*snapshot.value))
            }
            Self::U64(reference) => {
                let snapshot = reference.get();
                (snapshot.revision, *snapshot.value)
            }
        }
    }
}
static VIEW: ServiceKey<View> = ServiceKey::new("test.prepared.view@1");
#[derive(Default)]
struct Counts {
    prepares: AtomicUsize,
    builds: AtomicUsize,
    applies: AtomicUsize,
}
struct Factory(Arc<Counts>);
struct Instance {
    view: View,
    counts: Arc<Counts>,
    alt_key: bool,
}
#[async_trait]
impl Plugin for Instance {
    fn key(&self) -> PluginKey {
        PluginKey::new(if self.alt_key {
            "test.prepared.alt"
        } else {
            "test.prepared"
        })
    }
    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        self.counts.applies.fetch_add(1, Ordering::SeqCst);
        ctx.provide(VIEW, self.view.clone())
    }
}
impl ExtensionFactory for Factory {
    type Config = Config;
    fn id(&self) -> &'static str {
        "prepared"
    }
    fn prepare(&self, config: Config) -> Result<PreparedExtension, LoaderError> {
        self.0.prepares.fetch_add(1, Ordering::SeqCst);
        if matches!(config.mode, Mode::PreparePanic) {
            panic!("prepare panic");
        }
        if matches!(config.mode, Mode::Invalid) {
            return Err(LoaderError::invalid_config("invalid config"));
        }
        let counts = self.0.clone();
        let construct = move |view| -> Result<Arc<dyn Plugin>, LoaderError> {
            counts.builds.fetch_add(1, Ordering::SeqCst);
            if matches!(config.mode, Mode::BuildPanic) {
                panic!("construct panic");
            }
            if matches!(config.mode, Mode::BuildError) {
                return Err(LoaderError::invalid_config("construct error"));
            }
            Ok(Arc::new(Instance {
                view,
                counts,
                alt_key: matches!(config.mode, Mode::BadKey),
            }))
        };
        Ok(match config.mode {
            Mode::Hot => PreparedExtension::volatile(
                String::from("settings"),
                config.value,
                move |_, live| construct(View::U32(live)),
            ),
            Mode::OtherStatic => {
                PreparedExtension::volatile(42_u32, config.value, move |_, live| {
                    construct(View::U32(live))
                })
            }
            Mode::OtherLive => {
                PreparedExtension::volatile(42_u32, u64::from(config.value), move |_, live| {
                    construct(View::U64(live))
                })
            }
            Mode::CompareSafe | Mode::ComparePanic => PreparedExtension::volatile(
                Comparing(matches!(config.mode, Mode::ComparePanic)),
                config.value,
                move |_, live| construct(View::U32(live)),
            ),
            _ => PreparedExtension::ordinary(move || construct(View::Plain(config.value))),
        })
    }
}
fn raw(mode: Mode, value: u32) -> toml::Value {
    toml::Value::try_from(Config { mode, value }).unwrap()
}

struct Harness {
    runtime: Runtime,
    loader: Loader,
    _fiber: Fiber,
    directory: tempfile::TempDir,
    counts: Arc<Counts>,
}
impl Harness {
    async fn new(mode: Mode, disabled: bool) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let mut entry = EntryOptions::new("a", "prepared").with_config(raw(mode, 1));
        entry.disabled = disabled;
        let config = ExtensionsConfig {
            version: cordis_loader::CONFIG_VERSION,
            extensions: vec![entry],
        };
        fs::write(
            directory.path().join("extensions.toml"),
            toml::to_string(&config).unwrap(),
        )
        .unwrap();
        fs::write(
            directory.path().join("bootstrap.toml"),
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
        let fiber = mount_file(&runtime, catalog, directory.path().join("bootstrap.toml")).await;
        let loader = (*runtime.root().get(LOADER).unwrap()).clone();
        Self {
            runtime,
            loader,
            _fiber: fiber,
            directory,
            counts,
        }
    }
    async fn update(&self, mode: Mode, value: u32) -> Result<(), LoaderControlError> {
        self.loader
            .subtree("include")
            .unwrap()
            .update(
                "a",
                EntryUpdate {
                    config: Some(raw(mode, value)),
                    ..Default::default()
                },
                None,
                None,
            )
            .await
            .map(|_| ())
    }
    fn view(&self) -> Arc<View> {
        self.runtime.root().get(VIEW).unwrap()
    }
}

#[test]
fn prepare_does_not_construct_even_when_closure_will_fail() {
    let counts = Arc::new(Counts::default());
    let factory = Factory(counts.clone());
    for mode in [
        Mode::Ordinary,
        Mode::Hot,
        Mode::BuildPanic,
        Mode::BuildError,
    ] {
        drop(factory.prepare(Config { mode, value: 1 }).unwrap());
    }
    assert_eq!(counts.prepares.load(Ordering::SeqCst), 4);
    assert_eq!(counts.builds.load(Ordering::SeqCst), 0);
    assert_eq!(counts.applies.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn create_factory_precheck_failure_keeps_loader_active_without_applying_entries() {
    for mode in [
        Mode::Invalid,
        Mode::PreparePanic,
        Mode::BuildError,
        Mode::BuildPanic,
    ] {
        let counts = Arc::new(Counts::default());
        let mut catalog = ExtensionCatalog::new();
        catalog.register(Factory(counts.clone())).unwrap();
        let runtime = Runtime::new().unwrap();
        let plugin = LoaderPlugin::new(catalog);
        let fiber = runtime.root().plugin(Arc::new(plugin)).await.unwrap();
        let loader = runtime.root().get(LOADER).unwrap();
        assert!(
            loader
                .create(
                    EntryOptions::new("a", "prepared").with_config(raw(mode, 1)),
                    None,
                    None
                )
                .await
                .is_err()
        );
        assert_eq!(fiber.state(), FiberState::Active);
        assert!(fiber.last_error().is_none());
        assert!(loader.await_idle().await.unwrap().entries.is_empty());
        assert_eq!(counts.applies.load(Ordering::SeqCst), 0);
        runtime.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn capability_and_binding_types_replace_but_hot_values_do_not_construct() {
    let h = Harness::new(Mode::Ordinary, false).await;
    let original = h.view();
    let fiber_id = h
        .loader
        .await_idle()
        .await
        .unwrap()
        .entry("include:a")
        .unwrap()
        .fiber_id;
    h.update(Mode::Ordinary, 2).await.unwrap();
    assert!(!Arc::ptr_eq(&original, &h.view()));
    assert_eq!(h.counts.builds.load(Ordering::SeqCst), 2);
    h.update(Mode::Hot, 3).await.unwrap();
    let hot = h.view();
    assert_eq!(hot.snapshot(), (0, 3));
    h.update(Mode::Hot, 4).await.unwrap();
    assert!(Arc::ptr_eq(&hot, &h.view()));
    assert_eq!(hot.snapshot(), (1, 4));
    assert_eq!(h.counts.builds.load(Ordering::SeqCst), 3);
    let View::U32(reference) = &*hot else {
        panic!("expected u32 config");
    };
    let mut closed = reference.subscribe();
    h.update(Mode::OtherStatic, 4).await.unwrap();
    assert!(closed.changed().await.is_err());
    let other_static = h.view();
    assert_eq!(other_static.snapshot(), (0, 4));
    assert!(!Arc::ptr_eq(&hot, &other_static));
    h.update(Mode::OtherStatic, 5).await.unwrap();
    assert_eq!(other_static.snapshot(), (1, 5));
    let View::U32(reference) = &*other_static else {
        panic!("expected u32 config");
    };
    let mut closed = reference.subscribe();
    h.update(Mode::OtherLive, 5).await.unwrap();
    assert!(closed.changed().await.is_err());
    let other_live = h.view();
    assert_eq!(other_live.snapshot(), (0, 5));
    h.update(Mode::OtherLive, 6).await.unwrap();
    assert_eq!(other_live.snapshot(), (1, 6));
    let View::U64(reference) = &*other_live else {
        panic!("expected u64 config");
    };
    let mut closed = reference.subscribe();
    h.update(Mode::Ordinary, 7).await.unwrap();
    assert!(closed.changed().await.is_err());
    assert_eq!(h.view().snapshot(), (0, 7));
    assert_eq!(h.counts.builds.load(Ordering::SeqCst), 6);
    assert_eq!(h.counts.applies.load(Ordering::SeqCst), 6);
    assert_eq!(
        fiber_id,
        h.loader
            .await_idle()
            .await
            .unwrap()
            .entry("include:a")
            .unwrap()
            .fiber_id
    );
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn prepare_construct_and_key_failures_are_zero_runtime_change() {
    let h = Harness::new(Mode::Hot, false).await;
    let before = h.view();
    let file = h.directory.path().join("extensions.toml");
    let text = fs::read_to_string(&file).unwrap();
    for mode in [
        Mode::PreparePanic,
        Mode::BuildPanic,
        Mode::BuildError,
        Mode::Invalid,
        Mode::BadKey,
    ] {
        let result = h.update(mode, 2).await;
        if matches!(mode, Mode::PreparePanic | Mode::BuildPanic) {
            assert!(matches!(
                result,
                Err(LoaderControlError::Loader(LoaderError::FactoryPanic { .. }))
            ));
        } else if matches!(mode, Mode::BadKey) {
            assert!(matches!(
                result,
                Err(LoaderControlError::Loader(
                    LoaderError::PluginKeyChanged { .. }
                ))
            ));
        } else {
            assert!(result.is_err());
        }
        assert!(Arc::ptr_eq(&before, &h.view()));
        assert_eq!(before.snapshot(), (0, 1));
        assert_eq!(h.counts.applies.load(Ordering::SeqCst), 1);
        assert_eq!(fs::read_to_string(&file).unwrap(), text);
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
async fn disabled_entry_does_not_prepare_or_construct() {
    let h = Harness::new(Mode::Ordinary, true).await;
    assert_eq!(h.counts.prepares.load(Ordering::SeqCst), 0);
    assert_eq!(h.counts.builds.load(Ordering::SeqCst), 0);
    assert!(h.runtime.root().get(VIEW).is_err());
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
    assert_eq!(h.counts.builds.load(Ordering::SeqCst), 1);
    h.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn comparison_panic_is_rejected_before_any_runtime_or_file_change() {
    let h = Harness::new(Mode::CompareSafe, false).await;
    let before = h.view();
    let path = h.directory.path().join("extensions.toml");
    let file = fs::read_to_string(&path).unwrap();
    assert!(matches!(
        h.update(Mode::ComparePanic, 2).await,
        Err(LoaderControlError::Loader(LoaderError::FactoryPanic { .. }))
    ));
    assert!(Arc::ptr_eq(&before, &h.view()));
    assert_eq!(before.snapshot(), (0, 1));
    assert_eq!(h.counts.builds.load(Ordering::SeqCst), 1);
    assert_eq!(h.counts.applies.load(Ordering::SeqCst), 1);
    assert_eq!(fs::read_to_string(path).unwrap(), file);
    h.runtime.shutdown().await.unwrap();
}

async fn mount_file(
    runtime: &Runtime,
    catalog: ExtensionCatalog,
    bootstrap: impl AsRef<std::path::Path>,
) -> Fiber {
    let entry = cordis_plugin_include::bootstrap_entry("include", bootstrap)
        .await
        .unwrap();
    let fiber = runtime
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
    fiber
}
