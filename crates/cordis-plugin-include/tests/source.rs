use cordis_core::Runtime;
use cordis_loader::{EntryOptions, EntryTreeSource, ExtensionsConfig, SourceErrorKind};
use cordis_plugin_include::{
    EntryPatch, FileSource, INCLUDE_LOCATION, IncludeConfig, apply_patches, include_root_entry,
    load_extensions,
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
        let source = FileSource::open(IncludeConfig::new(&path), &runtime.root())
            .await
            .unwrap();
        let initial = source.read().await.unwrap();
        let target = document(vec![entry("one", 2)]);
        source.validate_write(&target).await.unwrap();
        let revision = source.write(&target, &initial.revision).await.unwrap();
        assert_eq!(source.read().await.unwrap().config, target);
        assert_eq!(source.read().await.unwrap().revision, revision);
        assert_eq!(
            source
                .write(&target, &initial.revision)
                .await
                .unwrap_err()
                .kind,
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
        let source = FileSource::open(config, &runtime.root()).await.unwrap();
        assert_eq!(
            source.read().await.unwrap().config,
            document(vec![entry("initial", 7)])
        );
        fs::write(&path, "invalid-existing-file secret-payload").unwrap();
        let error = source.read().await.err().unwrap();
        assert_eq!(error.kind, SourceErrorKind::InvalidData);
        assert!(!error.to_string().contains("secret-payload"));
        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            "invalid-existing-file secret-payload"
        );
        fs::write(&path, "").unwrap();
        assert!(source.read().await.is_err());
    }
    let missing = dir.path().join("missing-parent/config.toml");
    let mut config = IncludeConfig::new(missing);
    config.initial = Some(vec![]);
    assert!(FileSource::open(config, &runtime.root()).await.is_err());
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn initial_does_not_overwrite_a_file_created_after_open() {
    let runtime = Runtime::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("new.toml");
    let mut config = IncludeConfig::new(&path);
    config.initial = Some(vec![entry("initial", 1)]);
    let source = FileSource::open(config, &runtime.root()).await.unwrap();
    save(&path, &document(vec![entry("other", 2)]));
    assert_eq!(
        source.read().await.unwrap().config,
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
    let source = FileSource::open(config, &runtime.root()).await.unwrap();
    let initial = source.read().await.unwrap();
    assert!(initial.config.extensions[0].disabled);
    let revision = source
        .write(&initial.config, &initial.revision)
        .await
        .unwrap();
    assert!(load_extensions(&path).await.unwrap().extensions[0].disabled);
    let mut impossible = initial.config;
    impossible.extensions[0].disabled = false;
    assert_eq!(
        source.write(&impossible, &revision).await.unwrap_err().kind,
        SourceErrorKind::Conflict
    );
    let mut config = IncludeConfig::new(&path);
    config.patches = vec![EntryPatch {
        insert: Some(vec![entry("inserted", 2)]),
        ..Default::default()
    }];
    let source = FileSource::open(config, &runtime.root()).await.unwrap();
    let initial = source.read().await.unwrap();
    assert_eq!(
        source
            .validate_write(&initial.config)
            .await
            .unwrap_err()
            .kind,
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
    let source = FileSource::open(config, &runtime.root()).await.unwrap();
    assert!(
        FileSource::open(IncludeConfig::new("child.toml"), &runtime.root())
            .await
            .is_err()
    );
    let context = source.context(&runtime.root()).unwrap();
    let child = FileSource::open(IncludeConfig::new("child.toml"), &context)
        .await
        .unwrap();
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
            .await
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
        let source = FileSource::open(IncludeConfig::new(path), &runtime.root())
            .await
            .unwrap();
        let error = source.read().await.err().unwrap();
        assert_eq!(error.kind, SourceErrorKind::InvalidData);
        assert!(!error.to_string().contains("secret-payload"));
    }
    runtime.shutdown().await.unwrap();
}

#[test]
fn include_root_entry_resolves_path_and_rejects_unknown_extension() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cordis.toml");
    fs::write(&path, "version = 3\n\nextensions = []\n").unwrap();
    let entry = include_root_entry("include", &path).unwrap();
    assert_eq!(entry.id, "include");
    assert_eq!(entry.name, "cordis:include");
    let config: IncludeConfig = entry.config.try_into().unwrap();
    assert_eq!(config.path, path.canonicalize().unwrap());
    let bad = dir.path().join("secret-payload.bin");
    fs::write(&bad, b"x").unwrap();
    assert!(
        !include_root_entry("include", &bad)
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
    let source = FileSource::open(config, &runtime.root()).await.unwrap();
    let initial = source.read().await.unwrap();
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    source
        .write(&initial.config, &initial.revision)
        .await
        .unwrap();
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let alias = dir.path().join("alias.toml");
    symlink(&path, &alias).unwrap();
    let alias = FileSource::open(IncludeConfig::new(alias), &runtime.root())
        .await
        .unwrap();
    assert_eq!(alias.identity(), source.identity());
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 2);
    runtime.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn readonly_file_is_readable_but_management_writes_are_rejected_without_changes() {
    use std::os::unix::fs::PermissionsExt;
    let runtime = Runtime::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    let original = document(vec![entry("one", 1)]);
    save(&path, &original);
    let original_bytes = fs::read(&path).unwrap();
    let source = FileSource::open(IncludeConfig::new(&path), &runtime.root())
        .await
        .unwrap();
    let before = source.read().await.unwrap();
    let target = document(vec![entry("one", 2)]);

    fs::set_permissions(&path, fs::Permissions::from_mode(0o444)).unwrap();
    let validation = source.validate_write(&target).await;
    let writing = source.write(&target, &before.revision).await;
    let readonly_read = source.read().await;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();

    assert_eq!(validation.unwrap_err().kind, SourceErrorKind::ReadOnly);
    assert_eq!(writing.unwrap_err().kind, SourceErrorKind::ReadOnly);
    let readonly_read = readonly_read.unwrap();
    assert_eq!(readonly_read.config, original);
    assert_eq!(readonly_read.revision, before.revision);
    assert_eq!(fs::read(&path).unwrap(), original_bytes);
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    // 不缓存永久只读标记，同一来源恢复权限后可立即编辑。
    source.validate_write(&target).await.unwrap();
    assert_eq!(fs::read(&path).unwrap(), original_bytes);
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    source.write(&target, &before.revision).await.unwrap();
    assert_eq!(source.read().await.unwrap().config, target);
    runtime.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn parent_directory_preflight_observes_actual_process_permissions() {
    use std::os::unix::fs::PermissionsExt;
    let runtime = Runtime::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    let config = document(vec![entry("one", 1)]);
    save(&path, &config);
    let source = FileSource::open(IncludeConfig::new(&path), &runtime.root())
        .await
        .unwrap();
    let bytes = fs::read(&path).unwrap();
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o555)).unwrap();
    let actual_probe = dir.path().join("actual-access-check");
    let access = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&actual_probe);
    let validation = source.validate_write(&config).await;
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o755)).unwrap();
    match access {
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            assert_eq!(validation.unwrap_err().kind, SourceErrorKind::ReadOnly);
        }
        Ok(file) => {
            drop(file);
            fs::remove_file(actual_probe).unwrap();
            eprintln!("当前进程可绕过目录模式位；本环境仅验证实际可写探测，不宣称已覆盖目录拒绝。");
            validation.unwrap();
        }
        Err(error) => panic!("无法验证目录写权限：{error}"),
    }
    assert_eq!(fs::read(&path).unwrap(), bytes);
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    source.validate_write(&config).await.unwrap();
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    runtime.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn write_rechecks_readonly_after_successful_validation() {
    use std::os::unix::fs::PermissionsExt;
    let runtime = Runtime::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    save(&path, &document(vec![entry("one", 1)]));
    let source = FileSource::open(IncludeConfig::new(&path), &runtime.root())
        .await
        .unwrap();
    let before = source.read().await.unwrap();
    let target = document(vec![entry("one", 2)]);
    source.validate_write(&target).await.unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o444)).unwrap();
    let result = source.write(&target, &before.revision).await;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!(result.unwrap_err().kind, SourceErrorKind::ReadOnly);
    let after = source.read().await.unwrap();
    assert_eq!(after.config, before.config);
    assert_eq!(after.revision, before.revision);
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    runtime.shutdown().await.unwrap();
}
