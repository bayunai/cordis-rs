//! bootstrap.toml 解析、未知字段与相对路径。

#[path = "common/mod.rs"]
mod common;
use common::*;

use cordis_loader::{
    BootstrapConfig, ConfigDriver, CordisLoader, ExtensionCatalog, LoaderError, load_bootstrap,
};
use std::fs;

#[test]
fn bootstrap_parses_file_driver() {
    let config = BootstrapConfig::from_toml_str(
        r#"
version = 1
[config]
driver = "file"
path = "extensions.toml"
"#,
    )
    .unwrap();
    assert_eq!(config.version, 1);
    assert_eq!(config.config.driver, ConfigDriver::File);
}

#[test]
fn bootstrap_rejects_unknown_field() {
    let error = BootstrapConfig::from_toml_str(
        r#"
version = 1
log_level = "info"
[config]
driver = "file"
path = "extensions.toml"
"#,
    )
    .unwrap_err();
    assert!(matches!(error, LoaderError::Toml { .. }));
    assert!(error.to_string().contains("log_level"));
}

#[test]
fn bootstrap_rejects_missing_path() {
    let error = BootstrapConfig::from_toml_str(
        r#"
version = 1
[config]
driver = "file"
"#,
    )
    .unwrap_err();
    assert!(matches!(error, LoaderError::Toml { .. }));
}

#[test]
fn bootstrap_rejects_unsupported_version() {
    let error = BootstrapConfig::from_toml_str(
        r#"
version = 2
[config]
driver = "file"
path = "extensions.toml"
"#,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        LoaderError::UnsupportedVersion { found: 2 }
    ));
}

#[test]
fn bootstrap_rejects_unknown_driver() {
    let error = BootstrapConfig::from_toml_str(
        r#"
version = 1
[config]
driver = "sqlite"
path = "extensions.toml"
"#,
    )
    .unwrap_err();
    assert!(matches!(error, LoaderError::Toml { .. }));
    assert!(error.to_string().contains("sqlite"));
}

#[test]
fn load_bootstrap_resolves_path_relative_to_file() {
    let dir = tempfile::tempdir().unwrap();
    let nested = dir.path().join("nested");
    fs::create_dir_all(&nested).unwrap();
    let bootstrap = dir.path().join("bootstrap.toml");
    fs::write(
        &bootstrap,
        r#"
version = 1
[config]
driver = "file"
path = "nested/extensions.toml"
"#,
    )
    .unwrap();
    fs::write(
        nested.join("extensions.toml"),
        r#"
version = 1
[[extensions]]
instance = "rel"
factory = "demo.noop"
enabled = true
"#,
    )
    .unwrap();

    let (_, extensions_path) = load_bootstrap(&bootstrap).unwrap();
    assert_eq!(extensions_path, nested.join("extensions.toml"));
}

#[tokio::test]
async fn bootstrap_loads_relative_extensions() {
    let dir = tempfile::tempdir().unwrap();
    let bootstrap = write_pair(
        dir.path(),
        r#"
version = 1
[config]
driver = "file"
path = "extensions.toml"
"#,
        r#"
version = 1
[[extensions]]
instance = "rel"
factory = "demo.noop"
enabled = true
"#,
    );
    let factory = TestFactory::new("demo.noop");
    let mut catalog = ExtensionCatalog::new();
    catalog.register(factory).unwrap();
    let host = CordisLoader::bootstrap(catalog, &bootstrap).await.unwrap();
    let snapshot = host.snapshot();
    assert_eq!(snapshot.instances.len(), 1);
    assert_eq!(snapshot.instance("rel").unwrap().factory, "demo.noop");
    host.shutdown().await.unwrap();
}

#[tokio::test]
async fn bootstrap_missing_extensions_file_does_not_start_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let bootstrap = dir.path().join("bootstrap.toml");
    fs::write(
        &bootstrap,
        r#"
version = 1
[config]
driver = "file"
path = "missing.toml"
"#,
    )
    .unwrap();
    match CordisLoader::bootstrap(ExtensionCatalog::new(), &bootstrap).await {
        Err(error) => assert!(matches!(error, LoaderError::Io { .. })),
        Ok(host) => {
            let _ = host.shutdown().await;
            panic!("expected missing extensions file");
        }
    }
}
