//! 受信任 Host 将配置描述交给管理 UI；传输与认证不属于 Loader。
use async_trait::async_trait;
use cordis_core::{Context, CoreError, Plugin, PluginKey, Runtime};
use cordis_loader::{
    ConfigOperation, EntryOptions, ExtensionCatalog, ExtensionFactory, ExtensionsConfig, LOADER,
    LoaderError, LoaderPlugin, PreparedExtension,
};
use cordis_plugin_include::{IncludeFactory, bootstrap_entry};
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
        // 实际插件在闭包内捕获配置；绝不把 Token 写进日志或错误。
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
    // 禁用且未配置的实例仍然可描述；表单可在启用前补齐必填字段。
    let mut entry = EntryOptions::new("ocr", "example.config");
    entry.disabled = true;
    fs::write(
        directory.path().join("extensions.toml"),
        toml::to_string(&ExtensionsConfig {
            version: 3,
            extensions: vec![entry],
        })?,
    )?;
    fs::write(
        directory.path().join("bootstrap.toml"),
        "version = 3\n[config]\ndriver = 'file'\npath = 'extensions.toml'\n",
    )?;
    let mut catalog = ExtensionCatalog::new();
    catalog.register(Factory)?;
    catalog.register(IncludeFactory)?;
    let runtime = Runtime::new()?;
    let _fiber = runtime
        .root()
        .plugin(Arc::new(LoaderPlugin::new(catalog)))
        .await?;
    let loader = runtime.root().get(LOADER)?;
    loader
        .create(
            bootstrap_entry("include", directory.path().join("bootstrap.toml")).await?,
            None,
            None,
        )
        .await?;
    let before = loader.describe_config("include:ocr").await?;
    // 应用可以用字符串 DTO 往返版本令牌；它不是访问凭据。
    let expected = before.revision.to_string().parse()?;
    let after = loader
        .mutate_config(
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
    assert!(after.value.get("token").is_none());
    assert!(
        after
            .secrets
            .iter()
            .any(|field| field.path == ["token"] && field.is_set)
    );
    println!("已保存配置；Token 已脱敏，实例仍保持禁用。");
    runtime.shutdown().await?;
    Ok(())
}
