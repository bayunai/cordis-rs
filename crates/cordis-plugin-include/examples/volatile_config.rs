//! cargo run -p cordis-plugin-include --example volatile_config
use async_trait::async_trait;
use cordis_core::{Context, CoreError, Plugin, PluginKey, Runtime, ServiceKey};
use cordis_loader::{
    ConfigRef, EntryOptions, EntryUpdate, ExtensionCatalog, ExtensionFactory, ExtensionsConfig,
    LOADER, LoaderError, LoaderPlugin, PreparedExtension,
};
use cordis_plugin_include::{IncludeFactory, bootstrap_entry};
use schemars::JsonSchema;
use serde::Deserialize;
use std::{error::Error, fs, sync::Arc};

#[derive(Deserialize, JsonSchema)]
struct Config {
    address: String,
    live: Limits,
}
#[derive(Deserialize, JsonSchema, PartialEq)]
struct Limits {
    rate_limit: u32,
    timeout_ms: u64,
}

static LIMITS: ServiceKey<ConfigRef<Limits>> = ServiceKey::new("example.limits@1");
struct Factory;
struct Gateway {
    address: String,
    live: ConfigRef<Limits>,
}

impl ExtensionFactory for Factory {
    type Config = Config;
    fn id(&self) -> &'static str {
        "gateway"
    }
    fn prepare(&self, config: Config) -> Result<PreparedExtension, LoaderError> {
        if config.address.is_empty() || config.live.rate_limit == 0 || config.live.timeout_ms == 0 {
            return Err(LoaderError::invalid_config(
                "address、rate_limit 与 timeout_ms 必须有效",
            ));
        }
        Ok(PreparedExtension::volatile(
            config.address,
            config.live,
            |address, live| Ok(Arc::new(Gateway { address, live })),
        ))
    }
}

#[async_trait]
impl Plugin for Gateway {
    fn key(&self) -> PluginKey {
        PluginKey::new("example.gateway")
    }
    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        ctx.provide(LIMITS, self.live.clone())?;
        println!("apply: {}", self.address);
        let mut changes = self.live.subscribe();
        ctx.effect()?.spawn(move |cancel| async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    value = changes.changed() => match value {
                        Ok(snapshot) => println!("热配置 revision={}: limit={}, timeout={}", snapshot.revision, snapshot.value.rate_limit, snapshot.value.timeout_ms),
                        Err(_) => break,
                    }
                }
            }
        })?;
        Ok(())
    }
}

fn config(address: &str, limit: u32) -> Result<toml::Value, toml::de::Error> {
    toml::from_str(&format!(
        "address = {address:?}\n[live]\nrate_limit = {limit}\ntimeout_ms = 5000"
    ))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let bootstrap = directory.path().join("bootstrap.toml");
    fs::write(
        &bootstrap,
        "version = 3\n[config]\ndriver = 'file'\npath = 'extensions.toml'\n",
    )?;
    let initial = ExtensionsConfig {
        version: cordis_loader::CONFIG_VERSION,
        extensions: vec![
            EntryOptions::new("gateway", "gateway").with_config(config("db://primary", 100)?),
        ],
    };
    fs::write(
        directory.path().join("extensions.toml"),
        toml::to_string(&initial)?,
    )?;
    let mut catalog = ExtensionCatalog::new();
    catalog.register(Factory)?;
    catalog.register(IncludeFactory)?;
    let runtime = Runtime::new()?;
    let mut loader_fiber = runtime
        .root()
        .plugin(Arc::new(LoaderPlugin::new(catalog)))
        .await?;
    let loader = runtime.root().get(LOADER)?;
    loader
        .create(bootstrap_entry("include", &bootstrap).await?, None, None)
        .await?;
    let before = loader
        .await_idle()
        .await?
        .entry("include:gateway")
        .expect("gateway entry")
        .fiber_id;
    let reference = loader.entry_context("include:gateway")?.get(LIMITS)?;
    let file_tree = loader.subtree("include")?;
    file_tree
        .update(
            "gateway",
            EntryUpdate {
                config: Some(config("db://primary", 200)?),
                ..Default::default()
            },
            None,
            None,
        )
        .await?;
    assert_eq!(
        before,
        loader
            .await_idle()
            .await?
            .entry("include:gateway")
            .expect("gateway entry")
            .fiber_id
    );
    let snapshot = reference.get(); // 一次操作读取一次完整快照。
    assert_eq!(snapshot.revision, 1);
    assert_eq!(snapshot.value.rate_limit, 200);
    file_tree
        .update(
            "gateway",
            EntryUpdate {
                config: Some(config("db://secondary", 300)?),
                ..Default::default()
            },
            None,
            None,
        )
        .await?;
    let replacement = loader.entry_context("include:gateway")?.get(LIMITS)?;
    assert_eq!(reference.get().value.rate_limit, 200);
    assert_eq!(replacement.get().revision, 0);
    assert_eq!(replacement.get().value.rate_limit, 300);
    loader_fiber.dispose_wait().await?;
    runtime.shutdown().await?;
    Ok(())
}
