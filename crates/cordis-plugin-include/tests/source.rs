use cordis_core::Runtime;
use cordis_loader::{EntryOptions, EntryTreeSource, ExtensionsConfig, SourceErrorKind};
use cordis_plugin_include::{
    EntryPatch, FileSource, INCLUDE_LOCATION, IncludeConfig, apply_patches, bootstrap_entry,
    load_bootstrap, load_extensions,
};
use std::{fs, path::Path};

fn entry(id: &str, value: i64) -> EntryOptions {
    EntryOptions::new(id, "test").with_config(toml::toml! {value=value}.into())
}
fn document(entries: Vec<EntryOptions>) -> ExtensionsConfig {
    ExtensionsConfig {
        version: 3,
        extensions: entries,
    }
}
fn save(path: &Path, config: &ExtensionsConfig) {
    let content = match path.extension().unwrap().to_str().unwrap() {
        "toml" => toml::to_string(config).unwrap(),
        "json" => serde_json::to_string(config).unwrap(),
        "yaml" | "yml" => serde_yaml_ng::to_string(config).unwrap(),
        _ => panic!("test extension"),
    };
    fs::write(path, content).unwrap();
}

#[tokio::test]
async fn formats_roundtrip_and_write_is_revision_checked() {
    let runtime = Runtime::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    for extension in ["toml", "json", "yaml", "yml"] {
        let path = dir.path().join(format!("config.{extension}"));
        save(&path, &document(vec![entry("one", 1)]));
        let source = FileSource::open(IncludeConfig::new(&path), &runtime.root()).unwrap();
        let initial = source.read().unwrap();
        let target = document(vec![entry("one", 2)]);
        source.validate_write(&target).unwrap();
        let revision = source.write(&target, &initial.revision).unwrap();
        assert_eq!(source.read().unwrap().config, target);
        assert_eq!(source.read().unwrap().revision, revision);
        assert_eq!(
            source.write(&target, &initial.revision).unwrap_err().kind,
            SourceErrorKind::Conflict
        );
        assert!(!format!("{revision:?}").contains("value"));
    }
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn explicit_initial_only_creates_missing_files() {
    let runtime = Runtime::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    for extension in ["toml", "json", "yaml"] {
        let path = dir.path().join(format!("initial.{extension}"));
        let mut config = IncludeConfig::new(&path);
        config.initial = Some(vec![entry("initial", 7)]);
        let source = FileSource::open(config, &runtime.root()).unwrap();
        assert_eq!(
            source.read().unwrap().config,
            document(vec![entry("initial", 7)])
        );
        fs::write(&path, "invalid-existing-file secret-payload").unwrap();
        let error = source.read().err().unwrap();
        assert_eq!(error.kind, SourceErrorKind::InvalidData);
        assert!(!error.to_string().contains("secret-payload"));
        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            "invalid-existing-file secret-payload"
        );
        fs::write(&path, "").unwrap();
        assert!(source.read().is_err());
    }
    let missing = dir.path().join("missing-parent/config.toml");
    let mut config = IncludeConfig::new(missing);
    config.initial = Some(vec![]);
    assert!(FileSource::open(config, &runtime.root()).is_err());
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn initial_does_not_overwrite_a_file_created_after_open() {
    let runtime = Runtime::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("new.toml");
    let mut config = IncludeConfig::new(&path);
    config.initial = Some(vec![entry("initial", 1)]);
    let source = FileSource::open(config, &runtime.root()).unwrap();
    save(&path, &document(vec![entry("other", 2)]));
    assert_eq!(
        source.read().unwrap().config,
        document(vec![entry("other", 2)])
    );
    runtime.shutdown().await.unwrap();
}

#[test]
fn sequential_patches_resolve_full_local_paths_and_preserve_base() {
    let base = document(vec![
        EntryOptions::group("group", vec![entry("same", 1)]).unwrap(),
        entry("same", 2),
    ]);
    let patches = vec![
        EntryPatch {
            id: Some("group".into()),
            insert: Some(vec![entry("inserted", 3)]),
            ..Default::default()
        },
        EntryPatch {
            id: Some("group:inserted".into()),
            name: Some("test".into()),
            disabled: Some(true),
            ..Default::default()
        },
        EntryPatch {
            id: Some("group:same".into()),
            config: Some(toml::toml! {value=4}.into()),
            ..Default::default()
        },
    ];
    let next = apply_patches(&base, &patches).unwrap();
    let children = next.extensions[0].children().unwrap();
    assert_eq!(children[0].config["value"].as_integer(), Some(4));
    assert!(children[1].disabled);
    assert_eq!(next.extensions[1].config["value"].as_integer(), Some(2));
    assert_eq!(
        base.extensions[0].children().unwrap()[0].config["value"].as_integer(),
        Some(1)
    );
}

#[test]
fn invalid_patches_fail_instead_of_skipping() {
    let base = document(vec![entry("one", 1)]);
    for patch in [
        EntryPatch {
            id: Some("missing".into()),
            disabled: Some(true),
            ..Default::default()
        },
        EntryPatch {
            id: Some("one".into()),
            name: Some("wrong".into()),
            ..Default::default()
        },
        EntryPatch {
            id: Some("one".into()),
            insert: Some(vec![]),
            ..Default::default()
        },
        EntryPatch {
            insert: Some(vec![entry("one", 3)]),
            ..Default::default()
        },
        EntryPatch {
            disabled: Some(true),
            ..Default::default()
        },
        EntryPatch {
            insert: Some(vec![]),
            disabled: Some(true),
            ..Default::default()
        },
        EntryPatch {
            id: Some("one:".into()),
            ..Default::default()
        },
    ] {
        assert!(apply_patches(&base, &[patch]).is_err());
    }
}

#[tokio::test]
async fn write_materializes_overrides_but_rejects_non_roundtrippable_patches() {
    let runtime = Runtime::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("patch.toml");
    save(&path, &document(vec![entry("one", 1)]));
    let mut config = IncludeConfig::new(&path);
    config.patches = vec![EntryPatch {
        id: Some("one".into()),
        disabled: Some(true),
        ..Default::default()
    }];
    let source = FileSource::open(config, &runtime.root()).unwrap();
    let initial = source.read().unwrap();
    assert!(initial.config.extensions[0].disabled);
    let revision = source.write(&initial.config, &initial.revision).unwrap();
    assert!(load_extensions(&path).unwrap().extensions[0].disabled);
    let mut impossible = initial.config;
    impossible.extensions[0].disabled = false;
    assert_eq!(
        source.write(&impossible, &revision).unwrap_err().kind,
        SourceErrorKind::Conflict
    );
    let mut config = IncludeConfig::new(&path);
    config.patches = vec![EntryPatch {
        insert: Some(vec![entry("inserted", 2)]),
        ..Default::default()
    }];
    let source = FileSource::open(config, &runtime.root()).unwrap();
    let initial = source.read().unwrap();
    assert_eq!(
        source.validate_write(&initial.config).unwrap_err().kind,
        SourceErrorKind::Conflict
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn source_context_controls_relative_paths_and_log_inheritance() {
    let runtime = Runtime::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("main.toml");
    save(&path, &document(vec![]));
    let mut config = IncludeConfig::new(&path);
    config.enable_logs = Some(true);
    let source = FileSource::open(config, &runtime.root()).unwrap();
    assert!(FileSource::open(IncludeConfig::new("child.toml"), &runtime.root()).is_err());
    let context = source.context(&runtime.root()).unwrap();
    let child = FileSource::open(IncludeConfig::new("child.toml"), &context).unwrap();
    assert_eq!(
        child.path(),
        dir.path().canonicalize().unwrap().join("child.toml")
    );
    assert!(
        child
            .context(&context)
            .unwrap()
            .config(INCLUDE_LOCATION)
            .unwrap()
            .enable_logs()
    );
    let mut disabled = IncludeConfig::new("child.toml");
    disabled.enable_logs = Some(false);
    assert!(
        !FileSource::open(disabled, &context)
            .unwrap()
            .context(&context)
            .unwrap()
            .config(INCLUDE_LOCATION)
            .unwrap()
            .enable_logs()
    );
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_values_and_secrets_never_become_parser_errors() {
    let runtime = Runtime::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    for (name, text) in [
        (
            "null.json",
            r#"{"version":3,"extensions":[{"id":"a","name":"a","config":{"secret":null}}]}"#,
        ),
        (
            "huge.json",
            r#"{"version":3,"extensions":[{"id":"a","name":"a","config":{"secret":18446744073709551615}}]}"#,
        ),
        (
            "key.yaml",
            "version: 3\nextensions: []\n123: secret-payload\n",
        ),
        (
            "tag.yaml",
            "version: 3\nextensions: !custom secret-payload\n",
        ),
        (
            "nan.yaml",
            "version: 3\nextensions: [{id: a, name: a, config: {secret: .nan}}]\n",
        ),
    ] {
        let path = dir.path().join(name);
        fs::write(&path, text).unwrap();
        let source = FileSource::open(IncludeConfig::new(path), &runtime.root()).unwrap();
        let error = source.read().err().unwrap();
        assert_eq!(error.kind, SourceErrorKind::InvalidData);
        assert!(!error.to_string().contains("secret-payload"));
    }
    runtime.shutdown().await.unwrap();
}

#[test]
fn bootstrap_is_owned_by_include_and_does_not_hide_invalid_input() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bootstrap.toml");
    fs::write(
        &path,
        "version=3\n[config]\ndriver='file'\npath='extensions.yaml'\n",
    )
    .unwrap();
    let (_, resolved) = load_bootstrap(&path).unwrap();
    assert_eq!(
        resolved,
        dir.path().canonicalize().unwrap().join("extensions.yaml")
    );
    let entry = bootstrap_entry("include", &path).unwrap();
    assert_eq!(entry.id, "include");
    assert_eq!(entry.name, "cordis:include");
    let config: IncludeConfig = entry.config.try_into().unwrap();
    assert_eq!(config.path, resolved);
    fs::write(
        &path,
        "version=3\n[config]\ndriver='remote'\npath='secret-payload'\n",
    )
    .unwrap();
    assert!(
        !load_bootstrap(&path)
            .err()
            .unwrap()
            .to_string()
            .contains("secret-payload")
    );
}

#[cfg(unix)]
#[tokio::test]
async fn canonical_identity_and_atomic_writes_have_private_permissions() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let runtime = Runtime::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("main.toml");
    let mut config = IncludeConfig::new(&path);
    config.initial = Some(vec![]);
    let source = FileSource::open(config, &runtime.root()).unwrap();
    let initial = source.read().unwrap();
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    source.write(&initial.config, &initial.revision).unwrap();
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let alias = dir.path().join("alias.toml");
    symlink(&path, &alias).unwrap();
    let alias = FileSource::open(IncludeConfig::new(alias), &runtime.root()).unwrap();
    assert_eq!(alias.identity(), source.identity());
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 2);
    runtime.shutdown().await.unwrap();
}
