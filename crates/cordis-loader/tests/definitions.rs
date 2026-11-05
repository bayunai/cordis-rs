use async_trait::async_trait;
use cordis_core::{Context, CoreError, FiberState, Plugin, PluginDefinition, Runtime};
use cordis_loader::{
    EntryOptions, EntryUpdate, ExtensionCatalog, ExtensionFactory, LOADER, LoaderError,
    LoaderPlugin, PreparedExtension,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Deserialize, Serialize, JsonSchema)]
struct Config {
    value: u32,
}
struct Factory(&'static str);
struct Instance;
#[async_trait]
impl Plugin for Instance {
    async fn apply(&self, _: &Context) -> Result<(), CoreError> {
        Ok(())
    }
}
impl ExtensionFactory for Factory {
    type Config = Config;
    fn id(&self) -> &'static str {
        self.0
    }
    fn prepare(&self, _: Config) -> Result<PreparedExtension, LoaderError> {
        Ok(PreparedExtension::ordinary(|| Ok(Arc::new(Instance))))
    }
}
fn entry(id: &str, factory: &str) -> EntryOptions {
    EntryOptions::new(id, factory).with_config(toml::toml! { value = 1 }.into())
}

#[tokio::test]
async fn catalog_definition_survives_disable_replace_and_aliases_without_using_names_as_identity() {
    let definition = PluginDefinition::named("same display name");
    let distinct = PluginDefinition::named("same display name");
    let mut catalog = ExtensionCatalog::new();
    catalog.register(&definition, Factory("first")).unwrap();
    catalog
        .register(&definition.clone(), Factory("alias"))
        .unwrap();
    catalog.register(&distinct, Factory("distinct")).unwrap();
    let cloned = catalog.clone();
    assert_eq!(cloned.definition("first").unwrap().id(), definition.id());
    assert_eq!(cloned.definition("alias").unwrap().id(), definition.id());
    assert_ne!(cloned.definition("distinct").unwrap().id(), definition.id());
    let runtime = Runtime::new().unwrap();
    let _loader_fiber = runtime
        .root()
        .plugin(
            &cordis_loader::PLUGIN_DEFINITION,
            Arc::new(LoaderPlugin::new(catalog)),
        )
        .await
        .unwrap();
    let loader = runtime.root().get(LOADER).unwrap();
    let mut disabled = entry("one", "first");
    disabled.disabled = true;
    let snapshot = loader.create(disabled, None, None).await.unwrap();
    assert_eq!(
        snapshot.entry("one").unwrap().definition_id,
        definition.id()
    );
    assert_eq!(snapshot.entry("one").unwrap().fiber_id, None);
    loader
        .update(
            "one",
            EntryUpdate {
                disabled: Some(false),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap();
    let before = loader
        .await_idle()
        .await
        .unwrap()
        .entry("one")
        .unwrap()
        .clone();
    let after = loader
        .update(
            "one",
            EntryUpdate {
                config: Some(toml::toml! { value = 2 }.into()),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(after.entry("one").unwrap().definition_id, definition.id());
    assert_eq!(after.entry("one").unwrap().fiber_id, before.fiber_id);
    loader
        .create(entry("two", "alias"), None, None)
        .await
        .unwrap();
    loader
        .create(entry("three", "distinct"), None, None)
        .await
        .unwrap();
    assert_eq!(runtime.unmount(definition.id()).await.unwrap(), 2);
    let after = loader.await_idle().await.unwrap();
    assert_eq!(
        after.entry("one").unwrap().state,
        Some(FiberState::Disposed)
    );
    assert_eq!(
        after.entry("two").unwrap().state,
        Some(FiberState::Disposed)
    );
    assert_eq!(
        after.entry("three").unwrap().state,
        Some(FiberState::Active)
    );
    assert_eq!(after.entry("one").unwrap().definition_id, definition.id());
    runtime.shutdown().await.unwrap();
}
