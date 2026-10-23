use cordis_core::Runtime;
use cordis_loader::{
    EntryOptions, EntryUpdate, ExtensionCatalog, ExtensionsConfig, LOADER, LoaderPlugin,
};
use cordis_plugin_include::{IncludeConfig, IncludeFactory};
use std::{fs, sync::Arc};

#[tokio::test]
async fn stale_parent_detach_preserves_recreated_descendants_and_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let nested = dir.path().join("nested.toml");
    let root = dir.path().join("root.toml");
    fs::write(&nested, "version=3\nextensions=[]\n").unwrap();
    let child = EntryOptions::new("nested", "cordis:include")
        .with_config(toml::Value::try_from(IncludeConfig::new(&nested)).unwrap());
    fs::write(
        &root,
        toml::to_string(&ExtensionsConfig {
            version: 3,
            extensions: vec![child],
        })
        .unwrap(),
    )
    .unwrap();
    let mut catalog = ExtensionCatalog::new();
    catalog.register(IncludeFactory).unwrap();
    let runtime = Runtime::new().unwrap();
    let entry = EntryOptions::new("include", "cordis:include")
        .with_config(toml::Value::try_from(IncludeConfig::new(&root)).unwrap());
    let _fiber = runtime
        .root()
        .plugin(Arc::new(LoaderPlugin::new(catalog)))
        .await
        .unwrap();
    let loader = runtime.root().get(LOADER).unwrap();
    loader.create(entry, None, None).await.unwrap();
    loader.await_idle().await.unwrap();
    let stale_parent = loader.subtree("include").unwrap();
    let stale_child = loader.subtree("include:nested").unwrap();
    loader
        .update(
            "include",
            EntryUpdate {
                disabled: Some(true),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap();
    loader
        .update(
            "include",
            EntryUpdate {
                disabled: Some(false),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .unwrap();
    let before = loader.await_idle().await.unwrap();
    assert!(loader.subtree("include:nested").is_ok());
    let (parent, child) = tokio::join!(stale_parent.clone().detach(), stale_child.detach());
    parent.unwrap();
    child.unwrap();
    stale_parent.detach().await.unwrap();
    let after = loader.await_idle().await.unwrap();
    assert!(loader.subtree("include:nested").is_ok());
    assert_eq!(
        before.entry("include:nested").unwrap().fiber_id,
        after.entry("include:nested").unwrap().fiber_id
    );
    assert!(
        loader
            .subtree("include:nested")
            .unwrap()
            .entries()
            .unwrap()
            .extensions
            .is_empty()
    );
    runtime.shutdown().await.unwrap();
}
