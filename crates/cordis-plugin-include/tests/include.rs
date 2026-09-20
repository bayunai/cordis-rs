//! Include 插件集成测试。

use async_trait::async_trait;
use cordis_core::{Context, CoreError, FiberState, Plugin, PluginKey, Runtime, ServiceKey};
use cordis_loader::{
    EntryOptions, EntryUpdate, ExtensionCatalog, ExtensionFactory, ExtensionsConfig, IsolateValue,
    IsolationDescriptor, LOADER, Loader, LoaderControlError, LoaderError, LoaderPlugin,
};
use cordis_plugin_include::IncludeFactory;
use std::{fs, path::PathBuf, sync::Arc};
use tokio::sync::Barrier;

static VALUE: ServiceKey<String> = ServiceKey::new("include.test.value@1");

#[derive(Clone, serde::Deserialize, schemars::JsonSchema)]
struct Empty {}

struct ValueFactory {
    id: &'static str,
    label: &'static str,
}

impl ExtensionFactory for ValueFactory {
    type Config = Empty;
    fn id(&self) -> &'static str {
        self.id
    }
    fn build(&self, _: Empty) -> Result<Arc<dyn Plugin>, LoaderError> {
        Ok(Arc::new(ValuePlugin {
            id: self.id,
            label: self.label,
        }))
    }
}

struct ValuePlugin {
    id: &'static str,
    label: &'static str,
}

#[async_trait]
impl Plugin for ValuePlugin {
    fn key(&self) -> PluginKey {
        PluginKey::new(self.id)
    }
    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        if !self.label.is_empty() {
            ctx.provide(VALUE, self.label.into())?;
        }
        Ok(())
    }
}

fn catalog() -> ExtensionCatalog {
    let mut catalog = ExtensionCatalog::new();
    catalog.register(IncludeFactory).unwrap();
    catalog
        .register(ValueFactory {
            id: "demo.value",
            label: "from-include",
        })
        .unwrap();
    catalog
        .register(ValueFactory {
            id: "demo.root",
            label: "from-root",
        })
        .unwrap();
    catalog
        .register(ValueFactory {
            id: "demo.noop",
            label: "",
        })
        .unwrap();
    catalog
        .register_isolation(IsolationDescriptor::new("value", VALUE))
        .unwrap();
    catalog
}

fn write_tree(
    dir: &std::path::Path,
    root: &ExtensionsConfig,
    include: &ExtensionsConfig,
) -> PathBuf {
    let bootstrap = dir.join("bootstrap.toml");
    let extensions = dir.join("extensions.toml");
    let nested = dir.join("nested.toml");
    fs::write(
        &bootstrap,
        "version = 3\n[config]\ndriver = \"file\"\npath = \"extensions.toml\"\n",
    )
    .unwrap();
    fs::write(&extensions, toml::to_string_pretty(root).unwrap()).unwrap();
    fs::write(&nested, toml::to_string_pretty(include).unwrap()).unwrap();
    bootstrap
}

fn include_entry(path: &str) -> EntryOptions {
    let mut cfg = toml::Table::new();
    cfg.insert("path".into(), toml::Value::String(path.into()));
    EntryOptions::new("reports", "cordis:include")
        .with_config(toml::Value::Table(cfg))
        .with_isolate([("value".into(), IsolateValue::Flag(true))].into())
}

async fn mount(bootstrap: &std::path::Path) -> (Runtime, cordis_core::Fiber, Loader) {
    let runtime = Runtime::new().unwrap();
    let fiber = runtime
        .root()
        .plugin(Arc::new(
            LoaderPlugin::bootstrap(catalog(), bootstrap).unwrap(),
        ))
        .await
        .unwrap();
    let loader = (*runtime.root().get(LOADER).unwrap()).clone();
    (runtime, fiber, loader)
}

#[tokio::test]
async fn include_mounts_children_with_prefixed_paths() {
    let dir = tempfile::tempdir().unwrap();
    let include = ExtensionsConfig {
        version: 3,
        extensions: vec![EntryOptions::new("child", "demo.value")],
    };
    let root = ExtensionsConfig {
        version: 3,
        extensions: vec![
            EntryOptions::new("root", "demo.root"),
            include_entry("nested.toml"),
        ],
    };
    let bootstrap = write_tree(dir.path(), &root, &include);
    let (runtime, _fiber, loader) = mount(&bootstrap).await;
    let snapshot = loader.await_idle().await.unwrap();
    assert_eq!(
        snapshot.entry("reports:child").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(
        snapshot
            .entry("reports:child")
            .unwrap()
            .parent
            .as_ref()
            .map(cordis_loader::EntryId::as_str),
        Some("reports")
    );
    assert_eq!(
        &*loader
            .entry_context("reports:child")
            .unwrap()
            .get(VALUE)
            .unwrap(),
        "from-include"
    );
    assert_eq!(
        &*loader.entry_context("root").unwrap().get(VALUE).unwrap(),
        "from-root"
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn include_detach_on_disable_clears_children() {
    let dir = tempfile::tempdir().unwrap();
    let include = ExtensionsConfig {
        version: 3,
        extensions: vec![EntryOptions::new("child", "demo.value")],
    };
    let root = ExtensionsConfig {
        version: 3,
        extensions: vec![include_entry("nested.toml")],
    };
    let bootstrap = write_tree(dir.path(), &root, &include);
    let (runtime, _fiber, loader) = mount(&bootstrap).await;
    loader.await_idle().await.unwrap();
    assert!(loader.entry_context("reports:child").is_ok());

    loader
        .update(
            "reports",
            EntryUpdate {
                disabled: Some(true),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap();
    loader.await_idle().await.unwrap();
    assert!(matches!(
        loader.entry_context("reports:child"),
        Err(LoaderControlError::Loader(
            LoaderError::UnknownInstance { .. }
        ))
    ));
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn subtree_create_writes_only_include_file() {
    let dir = tempfile::tempdir().unwrap();
    let include = ExtensionsConfig {
        version: 3,
        extensions: vec![EntryOptions::new("child", "demo.value")],
    };
    let root = ExtensionsConfig {
        version: 3,
        extensions: vec![include_entry("nested.toml")],
    };
    let bootstrap = write_tree(dir.path(), &root, &include);
    let root_text_before = fs::read_to_string(dir.path().join("extensions.toml")).unwrap();
    let (runtime, _fiber, loader) = mount(&bootstrap).await;
    loader.await_idle().await.unwrap();

    let subtree = loader.subtree("reports").unwrap();
    subtree
        .create(EntryOptions::new("extra", "demo.noop"), None, None)
        .await
        .unwrap();
    let snapshot = loader.await_idle().await.unwrap();
    assert_eq!(
        snapshot.entry("reports:extra").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(
        fs::read_to_string(dir.path().join("extensions.toml")).unwrap(),
        root_text_before
    );
    let nested: ExtensionsConfig =
        toml::from_str(&fs::read_to_string(dir.path().join("nested.toml")).unwrap()).unwrap();
    assert_eq!(nested.extensions.len(), 2);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn subtree_conflict_requires_reload() {
    let dir = tempfile::tempdir().unwrap();
    let include = ExtensionsConfig {
        version: 3,
        extensions: vec![EntryOptions::new("child", "demo.value")],
    };
    let root = ExtensionsConfig {
        version: 3,
        extensions: vec![include_entry("nested.toml")],
    };
    let bootstrap = write_tree(dir.path(), &root, &include);
    let (runtime, _fiber, loader) = mount(&bootstrap).await;
    loader.await_idle().await.unwrap();
    let subtree = loader.subtree("reports").unwrap();

    fs::write(
        dir.path().join("nested.toml"),
        toml::to_string_pretty(&ExtensionsConfig {
            version: 3,
            extensions: vec![
                EntryOptions::new("child", "demo.value"),
                EntryOptions::new("external", "demo.noop"),
            ],
        })
        .unwrap(),
    )
    .unwrap();

    let err = subtree
        .create(EntryOptions::new("extra", "demo.noop"), None, None)
        .await
        .unwrap_err();
    assert!(matches!(err, LoaderControlError::ConfigConflict { .. }));
    assert!(loader.entry_context("reports:external").is_err());

    subtree.reload().await.unwrap();
    let snapshot = loader.await_idle().await.unwrap();
    assert_eq!(
        snapshot.entry("reports:external").unwrap().state,
        Some(FiberState::Active)
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_include_fails_apply_without_residue() {
    let dir = tempfile::tempdir().unwrap();
    let root = ExtensionsConfig {
        version: 3,
        extensions: vec![include_entry("nested.toml")],
    };
    let bootstrap = write_tree(
        dir.path(),
        &root,
        &ExtensionsConfig {
            version: 3,
            extensions: vec![],
        },
    );
    fs::write(
        dir.path().join("nested.toml"),
        "version = 2\nextensions = []\n",
    )
    .unwrap();
    let runtime = Runtime::new().unwrap();
    let fiber = runtime
        .root()
        .plugin(Arc::new(
            LoaderPlugin::bootstrap(catalog(), &bootstrap).unwrap(),
        ))
        .await
        .expect("handle returned");
    assert_eq!(fiber.state(), FiberState::Failed);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn cross_tree_move_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let include = ExtensionsConfig {
        version: 3,
        extensions: vec![EntryOptions::new("child", "demo.value")],
    };
    let root = ExtensionsConfig {
        version: 3,
        extensions: vec![
            EntryOptions::group("app", vec![EntryOptions::new("local", "demo.root")]).unwrap(),
            include_entry("nested.toml"),
        ],
    };
    let bootstrap = write_tree(dir.path(), &root, &include);
    let (runtime, _fiber, loader) = mount(&bootstrap).await;
    loader.await_idle().await.unwrap();
    let err = loader
        .update(
            "app:local",
            EntryUpdate::default(),
            Some(Some("reports:child")),
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, LoaderControlError::CrossTreeMove { .. }));
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn duplicate_include_file_fails() {
    let dir = tempfile::tempdir().unwrap();
    let include = ExtensionsConfig {
        version: 3,
        extensions: vec![EntryOptions::new("child", "demo.value")],
    };
    let mut cfg = toml::Table::new();
    cfg.insert("path".into(), toml::Value::String("nested.toml".into()));
    let isolate = [("value".into(), IsolateValue::Flag(true))];
    let root = ExtensionsConfig {
        version: 3,
        extensions: vec![
            EntryOptions::new("a", "cordis:include")
                .with_config(toml::Value::Table(cfg.clone()))
                .with_isolate(isolate.clone().into()),
            EntryOptions::new("b", "cordis:include")
                .with_config(toml::Value::Table(cfg))
                .with_isolate(isolate.into()),
        ],
    };
    let bootstrap = write_tree(dir.path(), &root, &include);
    let runtime = Runtime::new().unwrap();
    let fiber = runtime
        .root()
        .plugin(Arc::new(
            LoaderPlugin::bootstrap(catalog(), &bootstrap).unwrap(),
        ))
        .await
        .expect("handle");
    assert_eq!(fiber.state(), FiberState::Failed);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn include_cycle_reports_include_cycle_without_registered_subtree() {
    let dir = tempfile::tempdir().unwrap();
    let mut back_config = toml::Table::new();
    back_config.insert("path".into(), toml::Value::String("extensions.toml".into()));
    let nested = ExtensionsConfig {
        version: 3,
        extensions: vec![
            EntryOptions::new("back", "cordis:include")
                .with_config(toml::Value::Table(back_config))
                .with_isolate([("value".into(), IsolateValue::Flag(true))].into()),
        ],
    };
    let root = ExtensionsConfig {
        version: 3,
        extensions: vec![include_entry("nested.toml")],
    };
    let bootstrap = write_tree(dir.path(), &root, &nested);
    let runtime = Runtime::new().unwrap();
    let fiber = runtime
        .root()
        .plugin(Arc::new(
            LoaderPlugin::bootstrap(catalog(), &bootstrap).unwrap(),
        ))
        .await
        .expect("handle returned");
    assert_eq!(fiber.state(), FiberState::Failed);
    assert!(
        fiber
            .last_error()
            .as_deref()
            .is_some_and(|message| message.contains("循环引用"))
    );
    assert!(runtime.root().get(LOADER).is_err());
    runtime.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_attach_reserves_one_subtree_source() {
    let dir = tempfile::tempdir().unwrap();
    let include = ExtensionsConfig {
        version: 3,
        extensions: vec![EntryOptions::new("child", "demo.noop")],
    };
    let root = ExtensionsConfig {
        version: 3,
        extensions: vec![
            EntryOptions::new("a", "demo.noop"),
            EntryOptions::new("b", "demo.noop"),
        ],
    };
    let bootstrap = write_tree(dir.path(), &root, &include);
    let (runtime, _fiber, loader) = mount(&bootstrap).await;
    let left_context = loader.entry_context("a").unwrap();
    let right_context = loader.entry_context("b").unwrap();
    let barrier = Arc::new(Barrier::new(2));

    let left_loader = loader.clone();
    let left_barrier = barrier.clone();
    let left = tokio::spawn(async move {
        left_barrier.wait().await;
        left_loader
            .attach_file_subtree(&left_context, "nested.toml")
            .await
    });
    let right_loader = loader.clone();
    let right_barrier = barrier.clone();
    let right = tokio::spawn(async move {
        right_barrier.wait().await;
        right_loader
            .attach_file_subtree(&right_context, "nested.toml")
            .await
    });

    let left = left.await.unwrap();
    let right = right.await.unwrap();
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    let error = left.err().or_else(|| right.err()).unwrap();
    assert!(matches!(
        error,
        LoaderControlError::Loader(LoaderError::DuplicateSubtreeSource { .. })
    ));
    let snapshot = loader.await_idle().await.unwrap();
    assert_eq!(
        usize::from(snapshot.entry("a:child").is_some())
            + usize::from(snapshot.entry("b:child").is_some()),
        1
    );

    let subtree = ["a", "b"]
        .into_iter()
        .find_map(|prefix| loader.subtree(prefix).ok())
        .expect("one attached subtree");
    subtree.detach().await.unwrap();
    assert!(loader.entry_context("a:child").is_err());
    assert!(loader.entry_context("b:child").is_err());
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn nested_include_prefixes_paths() {
    let dir = tempfile::tempdir().unwrap();
    let inner = ExtensionsConfig {
        version: 3,
        extensions: vec![EntryOptions::new("leaf", "demo.value")],
    };
    fs::write(
        dir.path().join("inner.toml"),
        toml::to_string_pretty(&inner).unwrap(),
    )
    .unwrap();
    let mut inner_cfg = toml::Table::new();
    inner_cfg.insert("path".into(), toml::Value::String("inner.toml".into()));
    let mid = ExtensionsConfig {
        version: 3,
        extensions: vec![
            EntryOptions::new("mid", "cordis:include")
                .with_config(toml::Value::Table(inner_cfg))
                .with_isolate([("value".into(), IsolateValue::Flag(true))].into()),
        ],
    };
    let root = ExtensionsConfig {
        version: 3,
        extensions: vec![include_entry("nested.toml")],
    };
    let bootstrap = write_tree(dir.path(), &root, &mid);
    let (runtime, _fiber, loader) = mount(&bootstrap).await;
    let snapshot = loader.await_idle().await.unwrap();
    assert_eq!(
        snapshot.entry("reports:mid:leaf").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(
        snapshot
            .entry("reports:mid")
            .unwrap()
            .parent
            .as_ref()
            .map(cordis_loader::EntryId::as_str),
        Some("reports")
    );
    assert_eq!(
        snapshot
            .entry("reports:mid:leaf")
            .unwrap()
            .parent
            .as_ref()
            .map(cordis_loader::EntryId::as_str),
        Some("reports:mid")
    );
    runtime.shutdown().await.unwrap();
}
