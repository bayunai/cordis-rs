//! 磁盘上的 patch 文档（deploy / home）：`version` + `[[patches]]`（完整 [`EntryPatch`]）。

use cordis_loader::{CONFIG_VERSION, ExtensionsConfig};
use cordis_plugin_include::{EntryPatch, apply_patches};
use serde::{Deserialize, Serialize};
use std::{fs, path::Path};

pub const PATCH_FILE_VERSION: u32 = 1;

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

    pub fn load(path: &Path) -> Result<Self, String> {
        match fs::read_to_string(path) {
            Ok(text) if text.trim().is_empty() => Ok(Self::empty()),
            Ok(text) => {
                let document: Self = toml::from_str(&text)
                    .map_err(|error| format!("无法解析 patch {}: {error}", path.display()))?;
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
pub fn load_ordered_patches(paths: &[impl AsRef<Path>]) -> Result<Vec<EntryPatch>, String> {
    let mut patches = Vec::new();
    for path in paths {
        patches.extend(PatchFileDocument::load(path.as_ref())?.patches);
    }
    Ok(patches)
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

    #[test]
    fn compose_deploy_user_home_order() {
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
        let deploy_patches = load_ordered_patches(&[&deploy]).unwrap();
        let mut all = deploy_patches;
        all.extend(PatchFileDocument::load(&home).unwrap().patches);
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
}
