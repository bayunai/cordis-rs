//! 磁盘上的 patch 文档（deploy / home）：`version` + `[[patches]]`（完整 [`EntryPatch`]）。

use cordis_loader::{CONFIG_VERSION, ExtensionsConfig};
use cordis_plugin_include::{EntryPatch, apply_patches};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{fs, path::Path};

/// 文件事务作为完整单元执行；JoinError 不回显可能包含配置的 panic 载荷。
pub(crate) async fn file_task<T: Send + 'static>(
    task: impl FnOnce() -> Result<T, String> + Send + 'static,
) -> Result<T, String> {
    tokio::task::spawn_blocking(task)
        .await
        .map_err(|_| "配置文件事务异常退出".to_owned())?
}

pub const PATCH_FILE_VERSION: u32 = 1;

/// 解析诊断只保留文件位置与安全分类，绝不格式化可能含源文本/秘密值的 Serde 错误。
pub(crate) fn parse_patch_document<T: DeserializeOwned>(
    path: &Path,
    text: &str,
) -> Result<T, String> {
    toml::from_str(text).map_err(|error| {
        let location = match error.span() {
            Some(span) => {
                let (line, column) = text
                    .char_indices()
                    .take_while(|(offset, _)| *offset < span.start)
                    .fold((1, 1), |(line, column), (_, ch)| {
                        if ch == '\n' {
                            (line + 1, 1)
                        } else {
                            (line, column + 1)
                        }
                    });
                format!("第 {line} 行，第 {column} 列")
            }
            None => "无位置信息".to_owned(),
        };
        format!(
            "无法解析配置文件 {}：TOML 语法或类型错误（{location}）",
            path.display()
        )
    })
}

/// 有序 patch 文件（deploy 包文件、home 覆盖等）。
#[derive(Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PatchFileDocument {
    pub version: u32,
    #[serde(default)]
    pub patches: Vec<EntryPatch>,
}

impl PatchFileDocument {
    pub fn empty() -> Self {
        Self {
            version: PATCH_FILE_VERSION,
            patches: Vec::new(),
        }
    }

    pub async fn load(path: impl AsRef<Path>) -> Result<Self, String> {
        let path = path.as_ref().to_owned();
        file_task(move || Self::read(&path)).await
    }

    fn read(path: &Path) -> Result<Self, String> {
        match fs::read_to_string(path) {
            Ok(text) if text.trim().is_empty() => Ok(Self::empty()),
            Ok(text) => {
                let document: Self = parse_patch_document(path, &text)?;
                if document.version != PATCH_FILE_VERSION {
                    return Err(format!(
                        "不支持的 patch 版本 {}（{}）；当前要求 {PATCH_FILE_VERSION}",
                        document.version,
                        path.display()
                    ));
                }
                Ok(document)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Self::empty()),
            Err(error) => Err(format!("无法读取 patch {}: {error}", path.display())),
        }
    }
}

/// 依次加载有序 patch 文件并扁平为 EntryPatch 列表。
pub async fn load_ordered_patches(paths: &[impl AsRef<Path>]) -> Result<Vec<EntryPatch>, String> {
    let paths: Vec<_> = paths.iter().map(|path| path.as_ref().to_owned()).collect();
    file_task(move || {
        let mut patches = Vec::new();
        for path in paths {
            patches.extend(PatchFileDocument::read(&path)?.patches);
        }
        Ok(patches)
    })
    .await
}

/// 对空根应用 patch 列表，得到生效条目树。
pub fn compose_entries(patches: &[EntryPatch]) -> Result<ExtensionsConfig, String> {
    let empty = ExtensionsConfig {
        version: CONFIG_VERSION,
        extensions: Vec::new(),
    };
    apply_patches(&empty, patches).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cordis_loader::EntryOptions;
    use std::io::Write;

    #[tokio::test]
    async fn public_load_errors_keep_locations_without_source_or_secret_values() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("deploy.toml");
        for text in [
            "version=1\n[[patches]]\nid='one'\n[patches.config]\ntoken=\"AUDIT-PRIVATE-SECRET\n",
            "version='AUDIT-PRIVATE-SECRET'\n",
        ] {
            fs::write(&path, text).unwrap();
            let error = PatchFileDocument::load(&path).await.err().unwrap();
            assert!(error.contains(path.to_str().unwrap()));
            assert!(error.contains("TOML 语法或类型错误"));
            assert!(error.contains("行，第"));
            assert!(!error.contains("AUDIT-PRIVATE-SECRET"));
            assert!(!error.contains("token="));
            let error = load_ordered_patches(std::slice::from_ref(&path))
                .await
                .err()
                .unwrap();
            assert!(!error.contains("AUDIT-PRIVATE-SECRET"));
        }
    }

    #[test]
    fn custom_deserializer_diagnostics_never_enter_public_errors() {
        struct Reject;
        impl<'de> Deserialize<'de> for Reject {
            fn deserialize<D: serde::Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
                Err(serde::de::Error::custom("private-custom-serde-error"))
            }
        }
        let error = parse_patch_document::<Reject>(Path::new("custom.toml"), "value=1")
            .err()
            .unwrap();
        assert!(error.contains("custom.toml"));
        assert!(error.contains("TOML 语法或类型错误"));
        assert!(!error.contains("private-custom-serde-error"));
    }

    #[test]
    fn safe_parse_locations_use_one_based_character_columns() {
        let error = parse_patch_document::<PatchFileDocument>(
            Path::new("unicode.toml"),
            "# 中文注释\nversion = 'private-invalid-version'\n",
        )
        .err()
        .unwrap();
        assert!(error.contains("第 2 行，第 11 列"));
        assert!(!error.contains("private-invalid-version"));
    }

    #[tokio::test]
    async fn compose_deploy_user_home_order() {
        let dir = tempfile::tempdir().unwrap();
        let deploy = dir.path().join("a.patch.toml");
        let mut file = std::fs::File::create(&deploy).unwrap();
        write!(
            file,
            r#"
version = 1
[[patches]]
[[patches.insert]]
id = "demo"
name = "demo"
disabled = false
[patches.insert.config]
value = 1
"#
        )
        .unwrap();
        let home = dir.path().join("home.patch.toml");
        std::fs::write(
            &home,
            r#"
version = 1
[[patches]]
id = "demo"
name = "demo"
[patches.config]
value = 9
"#,
        )
        .unwrap();
        let deploy_patches = load_ordered_patches(&[&deploy]).await.unwrap();
        let mut all = deploy_patches;
        all.extend(PatchFileDocument::load(&home).await.unwrap().patches);
        let tree = compose_entries(&all).unwrap();
        assert_eq!(tree.extensions.len(), 1);
        assert_eq!(tree.extensions[0].config["value"].as_integer(), Some(9));
    }

    #[test]
    fn insert_then_override_disabled() {
        let mut entry = EntryOptions::new("x", "x");
        entry.disabled = false;
        let patches = vec![
            EntryPatch {
                insert: Some(vec![entry]),
                ..Default::default()
            },
            EntryPatch {
                id: Some("x".into()),
                name: Some("x".into()),
                disabled: Some(true),
                ..Default::default()
            },
        ];
        let tree = compose_entries(&patches).unwrap();
        assert!(tree.extensions[0].disabled);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn blocking_transaction_keeps_async_runtime_responsive() {
        let (started, ready) = tokio::sync::oneshot::channel();
        let (release, barrier) = std::sync::mpsc::channel();
        let transaction = tokio::spawn(file_task(move || {
            started.send(()).unwrap();
            barrier.recv().unwrap();
            Ok(())
        }));
        ready.await.unwrap();
        // 文件事务仍停在屏障内，单线程 Runtime 仍能运行另一个异步任务。
        let progress = tokio::spawn(async { true });
        assert!(progress.await.unwrap());
        assert!(!transaction.is_finished());
        release.send(()).unwrap();
        transaction.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn blocking_transaction_panic_is_an_explicit_error() {
        let error = file_task::<()>(|| panic!("private payload"))
            .await
            .unwrap_err();
        assert_eq!(error, "配置文件事务异常退出");
        assert!(!error.contains("private payload"));
    }
}
