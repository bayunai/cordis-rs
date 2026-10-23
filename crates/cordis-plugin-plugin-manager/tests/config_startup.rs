//! 配置启动编排与控制面生命周期隔离：业务失败必须可见且仍可修复。

use async_trait::async_trait;
use cordis_core::{Context, CoreError, Fiber, FiberState, Plugin, PluginKey, Runtime};
use cordis_loader::{
    EntryOptions, ExtensionCatalog, ExtensionFactory, LOADER, LoaderError, LoaderPlugin,
    PreparedExtension,
};
use cordis_plugin_config_editor::{
    CONFIG_EDITOR, ConfigEditorConfig, ConfigEditorPlugin, PATCH_FILE_VERSION, PatchFileDocument,
    prepare_root_include,
};
use cordis_plugin_include::{EntryPatch, IncludeFactory, include_root_entry};
use cordis_plugin_plugin_manager::{PLUGIN_MANAGER, PluginManagerPlugin};
use cordis_plugin_settings::{ConfigOperation, SETTINGS, SettingsPlugin};
use schemars::JsonSchema;
use serde::Deserialize;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(serde::Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Input {
    fail: bool,
}

struct Factory(Arc<AtomicUsize>);
struct Instance {
    fail: bool,
    applies: Arc<AtomicUsize>,
}

impl ExtensionFactory for Factory {
    type Config = Input;

    fn id(&self) -> &'static str {
        "startup.business"
    }

    fn prepare(&self, config: Input) -> Result<PreparedExtension, LoaderError> {
        let applies = self.0.clone();
        Ok(PreparedExtension::ordinary(move || {
            Ok(Arc::new(Instance {
                fail: config.fail,
                applies,
            }))
        }))
    }
}

#[async_trait]
impl Plugin for Instance {
    fn key(&self) -> PluginKey {
        PluginKey::new("startup.business")
    }

    async fn apply(&self, _: &Context) -> Result<(), CoreError> {
        self.applies.fetch_add(1, Ordering::SeqCst);
        if self.fail {
            return Err(CoreError::PluginApply("controlled business failure".into()));
        }
        Ok(())
    }
}

fn write_config(dir: &std::path::Path) -> ConfigEditorConfig {
    std::fs::write(dir.join("cordis.toml"), "version = 3\nextensions = []\n").unwrap();
    let patch = PatchFileDocument {
        version: PATCH_FILE_VERSION,
        patches: vec![EntryPatch {
            insert: Some(vec![
                EntryOptions::new("broken", "startup.business")
                    .with_config(toml::toml! { fail = true }.into()),
                EntryOptions::new("healthy", "startup.business")
                    .with_config(toml::toml! { fail = false }.into()),
            ]),
            ..Default::default()
        }],
    };
    std::fs::write(
        dir.join("deploy.patches.toml"),
        toml::to_string(&patch).unwrap(),
    )
    .unwrap();
    std::fs::write(dir.join("home.patches.toml"), "version = 1\n").unwrap();
    ConfigEditorConfig::new(
        vec![dir.join("deploy.patches.toml")],
        dir.join("user.patches.toml"),
        dir.join("home.patches.toml"),
    )
}

async fn mount_loader(runtime: &Runtime, applies: Arc<AtomicUsize>) -> Fiber {
    let mut catalog = ExtensionCatalog::new();
    catalog.register(Factory(applies)).unwrap();
    catalog.register(IncludeFactory).unwrap();
    runtime
        .root()
        .plugin(Arc::new(LoaderPlugin::new(catalog)))
        .await
        .unwrap()
}

async fn mount_controls(runtime: &Runtime, config: ConfigEditorConfig) -> Vec<Fiber> {
    vec![
        runtime
            .root()
            .plugin(Arc::new(ConfigEditorPlugin::new(config)))
            .await
            .unwrap(),
        runtime
            .root()
            .plugin(Arc::new(SettingsPlugin))
            .await
            .unwrap(),
        runtime
            .root()
            .plugin(Arc::new(PluginManagerPlugin))
            .await
            .unwrap(),
    ]
}

fn assert_controls_active(fibers: &[Fiber]) {
    for fiber in fibers {
        assert_eq!(fiber.state(), FiberState::Active);
    }
}

#[tokio::test]
async fn initial_business_failure_keeps_complete_control_plane_available_for_repair() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_config(dir.path());
    let applies = Arc::new(AtomicUsize::new(0));
    let runtime = Runtime::new().unwrap();
    let loader_fiber = mount_loader(&runtime, applies).await;
    let loader = runtime.root().get(LOADER).unwrap();
    let entry = prepare_root_include(dir.path().join("cordis.toml"), &config)
        .await
        .unwrap();
    loader.create(entry, None, None).await.unwrap();
    let before = loader.await_idle().await.unwrap();
    assert_eq!(
        before.entry("include:broken").unwrap().state,
        Some(FiberState::Failed)
    );
    let healthy = before.entry("include:healthy").unwrap().fiber_id;
    assert!(healthy.is_some());
    let controls = mount_controls(&runtime, config).await;
    assert_eq!(loader_fiber.state(), FiberState::Active);
    assert_controls_active(&controls);
    let settings = runtime.root().get(SETTINGS).unwrap();
    let view = settings.describe("include:broken").await.unwrap();
    assert_eq!(view.config["fail"].as_bool(), Some(true));
    settings
        .mutate(
            "include:broken",
            view.revision,
            vec![ConfigOperation::Set {
                path: vec!["fail".into()],
                value: false.into(),
            }],
        )
        .await
        .unwrap();
    let repaired = loader.await_idle().await.unwrap();
    assert_eq!(
        repaired.entry("include:broken").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(repaired.entry("include:healthy").unwrap().fiber_id, healthy);

    // 后续编辑失败必须返回错误，而非因控制面需要可用就伪装成功。
    let view = settings.describe("include:broken").await.unwrap();
    assert!(
        settings
            .mutate(
                "include:broken",
                view.revision,
                vec![ConfigOperation::Set {
                    path: vec!["fail".into()],
                    value: true.into(),
                }],
            )
            .await
            .is_err()
    );
    assert_controls_active(&controls);
    assert_eq!(loader_fiber.state(), FiberState::Active);
    let manager = runtime.root().get(PLUGIN_MANAGER).unwrap();
    manager
        .set_plugin_enabled("include:broken", false)
        .await
        .unwrap();
    // 禁用时写入候选配置不会执行 apply，重新启用时业务错误仍须上报。
    let view = settings.describe("include:broken").await.unwrap();
    settings
        .mutate(
            "include:broken",
            view.revision,
            vec![ConfigOperation::Set {
                path: vec!["fail".into()],
                value: true.into(),
            }],
        )
        .await
        .unwrap();
    assert!(
        manager
            .set_plugin_enabled("include:broken", true)
            .await
            .is_err()
    );
    assert_controls_active(&controls);
    assert_eq!(
        loader
            .await_idle()
            .await
            .unwrap()
            .entry("include:healthy")
            .unwrap()
            .fiber_id,
        healthy
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn mounting_editor_does_not_apply_deploy_business_entries() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_config(dir.path());
    let applies = Arc::new(AtomicUsize::new(0));
    let runtime = Runtime::new().unwrap();
    let _loader_fiber = mount_loader(&runtime, applies.clone()).await;
    let loader = runtime.root().get(LOADER).unwrap();
    loader
        .create(
            include_root_entry("include", dir.path().join("cordis.toml")).unwrap(),
            None,
            None,
        )
        .await
        .unwrap();
    // 无效 deploy 文件证明 apply 不负责读取配置层或触发业务构造。
    std::fs::write(&config.deploy_patch_paths[0], "not valid TOML = [").unwrap();
    let controls = mount_controls(&runtime, config.clone()).await;
    assert_controls_active(&controls);
    assert!(runtime.root().get(CONFIG_EDITOR).is_ok());
    assert_eq!(applies.load(Ordering::SeqCst), 0);
    assert!(
        loader
            .await_idle()
            .await
            .unwrap()
            .entry("include:broken")
            .is_none()
    );
    assert!(
        prepare_root_include(dir.path().join("cordis.toml"), &config)
            .await
            .is_err()
    );
    assert_eq!(applies.load(Ordering::SeqCst), 0);
    runtime.shutdown().await.unwrap();
}
