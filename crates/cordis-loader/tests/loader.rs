//! Loader：根服务、静态 Factory 管理、持久化与冲突语义。

#[path = "common/mod.rs"]
mod common;
use common::*;

use cordis_core::FiberState;
use cordis_loader::{
    CordisLoader, ExtensionEntry, ExtensionUpdate, LOADER, LoaderControlError, LoaderError, toml,
};
use std::{fs, sync::atomic::Ordering};

#[tokio::test]
async fn loader_is_available_from_host_and_root_service() {
    let directory = tempfile::tempdir().unwrap();
    let bootstrap = write_pair(
        directory.path(),
        "version = 1\n[config]\ndriver = \"file\"\npath = \"extensions.toml\"\n",
        "version = 1\nextensions = []\n",
    );
    let host = CordisLoader::bootstrap(catalog_with(TestFactory::new("demo.loader")), &bootstrap)
        .await
        .unwrap();

    let from_host = host.loader();
    let from_root = host.root().get(LOADER).unwrap();
    assert_eq!(from_host.entries().unwrap().extensions.len(), 0);
    assert_eq!(from_root.entries().unwrap().extensions.len(), 0);
    assert_eq!(from_host.factories().unwrap()[0].id, "demo.loader");
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn loader_returns_factory_json_schema() {
    let directory = tempfile::tempdir().unwrap();
    let bootstrap = write_pair(
        directory.path(),
        "version = 1\n[config]\ndriver = \"file\"\npath = \"extensions.toml\"\n",
        "version = 1\nextensions = []\n",
    );
    let host = CordisLoader::bootstrap(catalog_with(TestFactory::new("demo.schema")), &bootstrap)
        .await
        .unwrap();

    let descriptor = host.loader().factories().unwrap().remove(0);
    assert_eq!(descriptor.id, "demo.schema");
    assert!(descriptor.schema.is_object());
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn loader_create_update_disable_remove_persists_target_config() {
    let directory = tempfile::tempdir().unwrap();
    let bootstrap = write_pair(
        directory.path(),
        "version = 1\n[config]\ndriver = \"file\"\npath = \"extensions.toml\"\n",
        "version = 1\nextensions = []\n",
    );
    let factory = TestFactory::new("demo.managed");
    let applies = factory.applies();
    let host = CordisLoader::bootstrap(catalog_with(factory), &bootstrap)
        .await
        .unwrap();
    let loader = host.loader();

    let created = loader
        .create(ExtensionEntry::new("primary", "demo.managed", true))
        .await
        .unwrap();
    assert_eq!(
        created.instance("primary").unwrap().state,
        FiberState::Active
    );
    assert_eq!(applies.load(Ordering::SeqCst), 1);

    loader
        .update(
            "primary",
            ExtensionUpdate {
                enabled: false,
                config: table(&[("message", toml::Value::String("saved".into()))]),
            },
        )
        .await
        .unwrap();
    assert!(host.snapshot().instances.is_empty());
    let saved = fs::read_to_string(directory.path().join("extensions.toml")).unwrap();
    assert!(saved.contains("enabled = false"));
    assert!(saved.contains("message = \"saved\""));

    loader.set_enabled("primary", true).await.unwrap();
    assert_eq!(
        host.snapshot().instance("primary").unwrap().state,
        FiberState::Active
    );
    loader.remove("primary").await.unwrap();
    assert!(host.snapshot().instances.is_empty());
    assert!(loader.entries().unwrap().extensions.is_empty());
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn loader_rejects_invalid_changes_without_runtime_or_file_mutation() {
    let directory = tempfile::tempdir().unwrap();
    let bootstrap = write_pair(
        directory.path(),
        "version = 1\n[config]\ndriver = \"file\"\npath = \"extensions.toml\"\n",
        "version = 1\nextensions = []\n",
    );
    let host = CordisLoader::bootstrap(catalog_with(TestFactory::new("demo.known")), &bootstrap)
        .await
        .unwrap();
    let loader = host.loader();
    let before = fs::read_to_string(directory.path().join("extensions.toml")).unwrap();

    let error = loader
        .create(ExtensionEntry::new("missing", "demo.missing", true))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        LoaderControlError::Loader(LoaderError::UnknownFactory { .. })
    ));
    assert!(host.snapshot().instances.is_empty());
    assert_eq!(
        fs::read_to_string(directory.path().join("extensions.toml")).unwrap(),
        before
    );

    loader
        .create(ExtensionEntry::new("known", "demo.known", false))
        .await
        .unwrap();
    let before_duplicate = fs::read_to_string(directory.path().join("extensions.toml")).unwrap();
    let error = loader
        .create(ExtensionEntry::new("known", "demo.known", true))
        .await
        .unwrap_err();
    assert!(matches!(error, LoaderControlError::InstanceExists { .. }));
    assert_eq!(
        fs::read_to_string(directory.path().join("extensions.toml")).unwrap(),
        before_duplicate
    );
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn loader_requires_reload_after_external_file_change() {
    let directory = tempfile::tempdir().unwrap();
    let bootstrap = write_pair(
        directory.path(),
        "version = 1\n[config]\ndriver = \"file\"\npath = \"extensions.toml\"\n",
        "version = 1\nextensions = []\n",
    );
    let host = CordisLoader::bootstrap(catalog_with(TestFactory::new("demo.conflict")), &bootstrap)
        .await
        .unwrap();
    fs::write(
        directory.path().join("extensions.toml"),
        "version = 1\nextensions = []\n# external\n",
    )
    .unwrap();

    let error = host
        .loader()
        .create(ExtensionEntry::new("one", "demo.conflict", true))
        .await
        .unwrap_err();
    assert!(matches!(error, LoaderControlError::ConfigConflict { .. }));
    assert!(host.snapshot().instances.is_empty());
    host.loader().reload().await.unwrap();
    host.loader()
        .create(ExtensionEntry::new("one", "demo.conflict", true))
        .await
        .unwrap();
    assert_eq!(
        host.snapshot().instance("one").unwrap().state,
        FiberState::Active
    );
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn loader_without_file_source_rejects_persistent_mutation() {
    let host = CordisLoader::new(catalog_with(TestFactory::new("demo.memory"))).unwrap();
    let error = host
        .loader()
        .create(ExtensionEntry::new("one", "demo.memory", true))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        LoaderControlError::Loader(LoaderError::NoConfigSource)
    ));
    host.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn loader_reports_active_but_unsaved_when_atomic_write_fails() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().unwrap();
    let bootstrap = write_pair(
        directory.path(),
        "version = 1\n[config]\ndriver = \"file\"\npath = \"extensions.toml\"\n",
        "version = 1\nextensions = []\n",
    );
    let host = CordisLoader::bootstrap(catalog_with(TestFactory::new("demo.persist")), &bootstrap)
        .await
        .unwrap();
    let original_permissions = fs::metadata(directory.path()).unwrap().permissions();
    let mut read_only = original_permissions.clone();
    read_only.set_mode(0o500);
    fs::set_permissions(directory.path(), read_only).unwrap();

    let result = host
        .loader()
        .create(ExtensionEntry::new("one", "demo.persist", true))
        .await;

    fs::set_permissions(directory.path(), original_permissions).unwrap();
    match result.unwrap_err() {
        LoaderControlError::Persist { snapshot, .. } => {
            assert_eq!(snapshot.instance("one").unwrap().state, FiberState::Active);
        }
        error => panic!("expected persist error, got {error}"),
    }
    assert_eq!(
        host.snapshot().instance("one").unwrap().state,
        FiberState::Active
    );
    host.shutdown().await.unwrap();
}
