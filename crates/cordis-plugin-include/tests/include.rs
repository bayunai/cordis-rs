//! Include 插件集成测试。

use async_trait::async_trait;
use cordis_core::{Context, CoreError, FiberState, Plugin, PluginKey, Runtime, ServiceKey};
use cordis_loader::{
    EntryOptions, EntryUpdate, ExtensionCatalog, ExtensionFactory, ExtensionsConfig, IsolateValue,
    IsolationDescriptor, LOADER, Loader, LoaderControlError, LoaderError, LoaderPlugin,
    PreparedExtension,
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
    fn prepare(&self, _: Empty) -> Result<PreparedExtension, LoaderError> {
        let id = self.id;
        let label = self.label;
        Ok(PreparedExtension::ordinary(move || {
            Ok(Arc::new(ValuePlugin { id, label }))
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
        .plugin(Arc::new(file_plugin(catalog(), bootstrap).unwrap()))
        .await
        .unwrap();
    let loader = (*runtime.root().get(LOADER).unwrap()).clone();
    (runtime, fiber, loader)
}

#[tokio::test]
async fn root_include_id_is_application_owned_and_has_no_path_aliases() {
    for root_id in ["include", "workspace-config"] {
        let dir = tempfile::tempdir().unwrap();
        let bootstrap = write_tree(
            dir.path(),
            &ExtensionsConfig {
                version: 3,
                extensions: vec![EntryOptions::new("worker", "demo.noop")],
            },
            &ExtensionsConfig {
                version: 3,
                extensions: vec![],
            },
        );
        let runtime = Runtime::new().unwrap();
        let plugin = LoaderPlugin::new(
            catalog(),
            ExtensionsConfig {
                version: 3,
                extensions: vec![
                    cordis_plugin_include::bootstrap_entry(root_id, &bootstrap).unwrap(),
                ],
            },
        )
        .unwrap();
        let _fiber = runtime.root().plugin(Arc::new(plugin)).await.unwrap();
        let loader = runtime.root().get(LOADER).unwrap();
        let snapshot = loader.await_idle().await.unwrap();
        let full_path = format!("{root_id}:worker");
        assert_eq!(
            snapshot.entry(&full_path).unwrap().state,
            Some(FiberState::Active)
        );
        assert!(loader.entry_context(&full_path).is_ok());
        assert!(loader.describe_config(&full_path).await.is_ok());
        assert!(loader.subtree(root_id).is_ok());
        for alias in ["worker", "main:worker", "include:worker"] {
            if alias != full_path {
                assert!(snapshot.entry(alias).is_none());
                assert!(loader.entry_context(alias).is_err());
                assert!(loader.describe_config(alias).await.is_err());
            }
        }
        runtime.shutdown().await.unwrap();
    }
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
        snapshot.entry("include:reports:child").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(
        snapshot
            .entry("include:reports:child")
            .unwrap()
            .parent
            .as_ref()
            .map(cordis_loader::EntryId::as_str),
        Some("include:reports")
    );
    assert_eq!(
        &*loader
            .entry_context("include:reports:child")
            .unwrap()
            .get(VALUE)
            .unwrap(),
        "from-include"
    );
    assert_eq!(
        &*loader
            .entry_context("include:root")
            .unwrap()
            .get(VALUE)
            .unwrap(),
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
    assert!(loader.entry_context("include:reports:child").is_ok());

    loader
        .subtree("include")
        .unwrap()
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
        loader.entry_context("include:reports:child"),
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

    let subtree = loader.subtree("include:reports").unwrap();
    subtree
        .create(EntryOptions::new("extra", "demo.noop"), None, None)
        .await
        .unwrap();
    let snapshot = loader.await_idle().await.unwrap();
    assert_eq!(
        snapshot.entry("include:reports:extra").unwrap().state,
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
    let subtree = loader.subtree("include:reports").unwrap();

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
    assert!(loader.entry_context("include:reports:external").is_err());

    subtree.reload().await.unwrap();
    let snapshot = loader.await_idle().await.unwrap();
    assert_eq!(
        snapshot.entry("include:reports:external").unwrap().state,
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
        .plugin(Arc::new(file_plugin(catalog(), &bootstrap).unwrap()))
        .await
        .expect("handle returned");
    assert_eq!(fiber.state(), FiberState::Active);
    let loader = runtime.root().get(LOADER).unwrap();
    let snapshot = loader.await_idle().await.unwrap();
    assert_eq!(
        snapshot.entry("include:reports").unwrap().state,
        Some(FiberState::Failed)
    );
    assert!(
        snapshot
            .entry("include:reports")
            .unwrap()
            .last_error
            .as_deref()
            .unwrap()
            .contains("配置版本")
    );
    assert!(loader.subtree("include:reports").is_err());
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_child_keeps_include_control_plane_and_independent_sibling() {
    struct Broken;
    #[async_trait]
    impl Plugin for Broken {
        fn key(&self) -> PluginKey {
            PluginKey::new("demo.broken")
        }
        async fn apply(&self, _: &Context) -> Result<(), CoreError> {
            Err(CoreError::PluginApply("child apply failed".into()))
        }
    }
    struct BrokenFactory;
    impl ExtensionFactory for BrokenFactory {
        type Config = Empty;
        fn id(&self) -> &'static str {
            "demo.broken"
        }
        fn prepare(&self, _: Empty) -> Result<PreparedExtension, LoaderError> {
            Ok(PreparedExtension::ordinary(|| Ok(Arc::new(Broken))))
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let nested = ExtensionsConfig {
        version: 3,
        extensions: vec![
            EntryOptions::new("broken", "demo.broken"),
            EntryOptions::new("stable", "demo.value"),
        ],
    };
    let bootstrap = write_tree(
        dir.path(),
        &ExtensionsConfig {
            version: 3,
            extensions: vec![include_entry("nested.toml")],
        },
        &nested,
    );
    let mut catalog = catalog();
    catalog.register(BrokenFactory).unwrap();
    let runtime = Runtime::new().unwrap();
    let mut fiber = runtime
        .root()
        .plugin(Arc::new(file_plugin(catalog, bootstrap).unwrap()))
        .await
        .unwrap();
    let loader = runtime.root().get(LOADER).unwrap();
    let snapshot = loader.await_idle().await.unwrap();
    assert_eq!(fiber.state(), FiberState::Active);
    assert_eq!(
        snapshot.entry("include:reports").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(
        snapshot.entry("include:reports:broken").unwrap().state,
        Some(FiberState::Failed)
    );
    let stable = snapshot.entry("include:reports:stable").unwrap();
    assert_eq!(stable.state, Some(FiberState::Active));
    let subtree = loader.subtree("include:reports").unwrap();
    assert_eq!(subtree.snapshot().unwrap().entries.len(), 2);
    assert!(subtree.reload().await.is_err());
    subtree.remove("broken").await.unwrap();
    let recovered = loader.await_idle().await.unwrap();
    assert!(recovered.entry("include:reports:broken").is_none());
    assert_eq!(
        recovered.entry("include:reports:stable").unwrap().fiber_id,
        stable.fiber_id
    );
    fiber.dispose_wait().await.unwrap();
    assert!(runtime.diagnostics().plugin_fibers.is_empty());
    assert!(loader.subtree("include:reports").is_err());
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
        .subtree("include")
        .unwrap()
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
        .plugin(Arc::new(file_plugin(catalog(), &bootstrap).unwrap()))
        .await
        .expect("handle");
    assert_eq!(fiber.state(), FiberState::Active);
    let loader = runtime.root().get(LOADER).unwrap();
    let snapshot = loader.await_idle().await.unwrap();
    assert_eq!(
        snapshot.entry("include:a").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(
        snapshot.entry("include:b").unwrap().state,
        Some(FiberState::Failed)
    );
    assert!(
        snapshot
            .entry("include:b")
            .unwrap()
            .last_error
            .as_deref()
            .unwrap()
            .contains("其他树附着")
    );
    assert!(loader.subtree("include:a").is_ok());
    assert!(loader.subtree("include:b").is_err());
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn include_cycle_fails_only_cyclic_entry_without_registering_another_tree() {
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
        .plugin(Arc::new(file_plugin(catalog(), &bootstrap).unwrap()))
        .await
        .expect("handle returned");
    assert_eq!(fiber.state(), FiberState::Active);
    let loader = runtime.root().get(LOADER).unwrap();
    let snapshot = loader.await_idle().await.unwrap();
    let cyclic = snapshot.entry("include:reports:back").unwrap();
    assert_eq!(cyclic.state, Some(FiberState::Failed));
    assert!(
        cyclic
            .last_error
            .as_deref()
            .is_some_and(|message| message.contains("循环引用"))
    );
    assert_eq!(snapshot.entries.len(), 3);
    assert!(loader.subtree("include:reports").is_ok());
    assert!(loader.subtree("include:reports:back").is_err());
    let mut fiber = fiber;
    fiber.dispose_wait().await.unwrap();
    assert!(runtime.root().get(LOADER).is_err());
    assert!(runtime.diagnostics().plugin_fibers.is_empty());
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
    let left_context = loader.entry_context("include:a").unwrap();
    let right_context = loader.entry_context("include:b").unwrap();
    let barrier = Arc::new(Barrier::new(2));

    let left_loader = loader.clone();
    let left_barrier = barrier.clone();
    let left = tokio::spawn(async move {
        left_barrier.wait().await;
        left_loader
            .attach_subtree(
                &left_context,
                cordis_plugin_include::FileSource::from_config(
                    cordis_plugin_include::IncludeConfig::new("nested.toml"),
                    &left_context,
                )
                .unwrap(),
            )
            .await
    });
    let right_loader = loader.clone();
    let right_barrier = barrier.clone();
    let right = tokio::spawn(async move {
        right_barrier.wait().await;
        right_loader
            .attach_subtree(
                &right_context,
                cordis_plugin_include::FileSource::from_config(
                    cordis_plugin_include::IncludeConfig::new("nested.toml"),
                    &right_context,
                )
                .unwrap(),
            )
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
        usize::from(snapshot.entry("include:a:child").is_some())
            + usize::from(snapshot.entry("include:b:child").is_some()),
        1
    );

    let subtree = ["a", "b"]
        .into_iter()
        .find_map(|prefix| loader.subtree(format!("include:{prefix}")).ok())
        .expect("one attached subtree");
    subtree.detach().await.unwrap();
    assert!(loader.entry_context("include:a:child").is_err());
    assert!(loader.entry_context("include:b:child").is_err());
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
        snapshot.entry("include:reports:mid:leaf").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(
        snapshot
            .entry("include:reports:mid")
            .unwrap()
            .parent
            .as_ref()
            .map(cordis_loader::EntryId::as_str),
        Some("include:reports")
    );
    assert_eq!(
        snapshot
            .entry("include:reports:mid:leaf")
            .unwrap()
            .parent
            .as_ref()
            .map(cordis_loader::EntryId::as_str),
        Some("include:reports:mid")
    );
    runtime.shutdown().await.unwrap();
}

fn file_plugin(
    catalog: ExtensionCatalog,
    bootstrap: impl AsRef<std::path::Path>,
) -> Result<LoaderPlugin, LoaderError> {
    let entry = cordis_plugin_include::bootstrap_entry("include", bootstrap)
        .map_err(|error| LoaderError::invalid_config(error.to_string()))?;
    LoaderPlugin::new(
        catalog,
        ExtensionsConfig {
            version: 3,
            extensions: vec![entry],
        },
    )
}
