//! 受信任 Host 将配置描述交给管理 UI；传输与认证不属于 Loader。
//! 表单写入本机 user patch，不 bake 进空根或 deploy patch。
use async_trait::async_trait;
use cordis_core::{Context, CoreError, Plugin, PluginKey, Runtime};
use cordis_loader::{
    EntryOptions, ExtensionCatalog, ExtensionFactory, LOADER, LoaderError, LoaderPlugin,
    PreparedExtension,
};
use cordis_plugin_config_editor::{
    ConfigEditorConfig, ConfigEditorPlugin, PATCH_FILE_VERSION, PatchFileDocument,
};
use cordis_plugin_include::{EntryPatch, IncludeFactory, include_root_entry};
use cordis_plugin_settings::{ConfigOperation, SETTINGS, SettingsPlugin};
use schemars::JsonSchema;
use serde::Deserialize;
use std::{fs, sync::Arc};

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Config {
    /// OCR 服务器的完整上传地址。
    endpoint: String,
    #[serde(default)]
    #[schemars(extend("writeOnly" = true))]
    token: Option<String>,
}
struct Factory;
struct ExamplePlugin;
impl ExtensionFactory for Factory {
    type Config = Config;
    fn id(&self) -> &'static str {
        "example.config"
    }
    fn prepare(&self, config: Config) -> Result<PreparedExtension, LoaderError> {
        if !config.endpoint.starts_with("http://") && !config.endpoint.starts_with("https://") {
            return Err(LoaderError::invalid_config(
                "endpoint 必须使用 HTTP 或 HTTPS",
            ));
        }
        let _ = config.token;
        Ok(PreparedExtension::ordinary(|| Ok(Arc::new(ExamplePlugin))))
    }
}
#[async_trait]
impl Plugin for ExamplePlugin {
    fn key(&self) -> PluginKey {
        PluginKey::new("example.config")
    }
    async fn apply(&self, _: &Context) -> Result<(), CoreError> {
        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let mut entry = EntryOptions::new("ocr", "example.config");
    entry.disabled = true;
    fs::write(
        directory.path().join("cordis.toml"),
        "version = 3\n\nextensions = []\n",
    )?;
    let deploy = PatchFileDocument {
        version: PATCH_FILE_VERSION,
        patches: vec![EntryPatch {
            insert: Some(vec![entry]),
            ..Default::default()
        }],
    };
    let deploy_path = directory.path().join("deploy.patches.toml");
    fs::write(&deploy_path, toml::to_string_pretty(&deploy)?)?;
    fs::write(directory.path().join("home.patches.toml"), "version = 1\n")?;
    let deploy_before = fs::read(&deploy_path)?;
    let mut catalog = ExtensionCatalog::new();
    catalog.register(Factory)?;
    catalog.register(IncludeFactory)?;
    let runtime = Runtime::new()?;
    let _loader = runtime
        .root()
        .plugin(Arc::new(LoaderPlugin::new(catalog)))
        .await?;
    let loader = runtime.root().get(LOADER)?;
    loader
        .create(
            include_root_entry("include", directory.path().join("cordis.toml"))?,
            None,
            None,
        )
        .await?;
    let _editor = runtime
        .root()
        .plugin(Arc::new(ConfigEditorPlugin::new(ConfigEditorConfig::new(
            vec![deploy_path.clone()],
            directory.path().join("user.patches.toml"),
            directory.path().join("home.patches.toml"),
            directory.path(),
        ))))
        .await?;
    let _settings = runtime.root().plugin(Arc::new(SettingsPlugin)).await?;
    let settings = runtime.root().get(SETTINGS)?;
    let before = settings.describe("include:ocr").await?;
    assert!(before.user.is_none());
    let expected = before.revision.to_string().parse()?;
    let after = settings
        .mutate(
            "include:ocr",
            expected,
            vec![
                ConfigOperation::Set {
                    path: vec!["endpoint".into()],
                    value: "http://127.0.0.1:3000/ocr".into(),
                },
                ConfigOperation::Set {
                    path: vec!["token".into()],
                    value: "example-token-not-a-real-credential".into(),
                },
            ],
        )
        .await?;
    assert!(after.user.is_some());
    assert!(after.value.get("token").is_none());
    assert!(
        after
            .secrets
            .iter()
            .any(|field| field.path == ["token"] && field.is_set)
    );
    assert_eq!(
        fs::read(&deploy_path)?,
        deploy_before,
        "表单不得 bake 进 deploy patch"
    );
    assert!(
        directory.path().join("user.patches.toml").is_file(),
        "应写入本机 user patch"
    );
    println!("已保存到 user patch；Token 已脱敏，deploy 未改，实例仍保持禁用。");
    runtime.shutdown().await?;
    Ok(())
}
