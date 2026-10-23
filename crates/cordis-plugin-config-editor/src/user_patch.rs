//! 本机 user 覆盖文档：权威在独立文件，不写进 Include path 源文件。

use cordis_plugin_include::EntryPatch;
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};

pub const USER_PATCH_VERSION: u32 = 1;

/// 磁盘上的 user patch 文档。
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct UserPatchDocument {
    pub version: u32,
    #[serde(default)]
    pub patches: Vec<UserPatch>,
}

/// 单条覆盖；`id` 为 Include 文件内局部路径（不含根 Include 前缀）。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UserPatch {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<toml::Value>,
    /// 启停覆盖；与 base 相同时应省略。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disabled: Option<bool>,
}

impl UserPatchDocument {
    pub fn empty() -> Self {
        Self {
            version: USER_PATCH_VERSION,
            patches: Vec::new(),
        }
    }

    pub fn to_entry_patches(&self) -> Vec<EntryPatch> {
        self.patches
            .iter()
            .map(|patch| EntryPatch {
                id: Some(patch.id.clone()),
                name: patch.name.clone(),
                config: patch.config.clone(),
                disabled: patch.disabled,
                ..Default::default()
            })
            .collect()
    }

    pub fn config_for(&self, local_id: &str) -> Option<&toml::Value> {
        self.patches
            .iter()
            .rev()
            .find(|patch| patch.id == local_id)
            .and_then(|patch| patch.config.as_ref())
    }

    pub fn disabled_for(&self, local_id: &str) -> Option<bool> {
        self.patches
            .iter()
            .rev()
            .find(|patch| patch.id == local_id)
            .and_then(|patch| patch.disabled)
    }

    /// 更新或清除 config 覆盖；保留同条目的 `disabled`。
    pub fn upsert_config(
        &mut self,
        local_id: &str,
        factory: &str,
        config: Option<toml::Value>,
    ) {
        let disabled = self.disabled_for(local_id);
        self.write_patch(local_id, factory, config, disabled);
    }

    /// 更新或清除 disabled 覆盖；保留同条目的 `config`。
    ///
    /// `disabled = None` 表示与 base 一致，去掉该字段。
    pub fn upsert_disabled(
        &mut self,
        local_id: &str,
        factory: &str,
        disabled: Option<bool>,
    ) {
        let config = self.config_for(local_id).cloned();
        self.write_patch(local_id, factory, config, disabled);
    }

    fn write_patch(
        &mut self,
        local_id: &str,
        factory: &str,
        config: Option<toml::Value>,
        disabled: Option<bool>,
    ) {
        self.patches.retain(|patch| patch.id != local_id);
        if config.is_none() && disabled.is_none() {
            return;
        }
        self.patches.push(UserPatch {
            id: local_id.to_owned(),
            name: Some(factory.to_owned()),
            config,
            disabled,
        });
    }
}

/// 进程内串行 + 原子写回的 user patch 存储。
#[derive(Debug)]
pub struct UserPatchStore {
    path: PathBuf,
    lock: tokio::sync::Mutex<()>,
}

impl UserPatchStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            lock: tokio::sync::Mutex::new(()),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub async fn load(&self) -> Result<UserPatchDocument, String> {
        let _guard = self.lock.lock().await;
        load_document(&self.path)
    }

    pub async fn store(&self, document: &UserPatchDocument) -> Result<(), String> {
        let _guard = self.lock.lock().await;
        save_document(&self.path, document)
    }
}

fn load_document(path: &Path) -> Result<UserPatchDocument, String> {
    match fs::read_to_string(path) {
        Ok(text) if text.trim().is_empty() => Ok(UserPatchDocument::empty()),
        Ok(text) => {
            let document: UserPatchDocument = toml::from_str(&text)
                .map_err(|error| format!("无法解析 user patch：{error}"))?;
            if document.version != USER_PATCH_VERSION {
                return Err(format!(
                    "不支持的 user patch 版本 {}；当前要求 {USER_PATCH_VERSION}",
                    document.version
                ));
            }
            Ok(document)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(UserPatchDocument::empty())
        }
        Err(error) => Err(format!("无法读取 user patch {}: {error}", path.display())),
    }
}

fn save_document(path: &Path, document: &UserPatchDocument) -> Result<(), String> {
    if document.patches.is_empty() {
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(format!("无法清除 user patch {}: {error}", path.display())),
        }
    } else {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| format!("无法创建 user patch 目录：{error}"))?;
        }
        let mut document = document.clone();
        document.version = USER_PATCH_VERSION;
        let text = toml::to_string_pretty(&document)
            .map_err(|error| format!("无法序列化 user patch：{error}"))?;
        let temporary = path.with_extension("toml.tmp");
        {
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&temporary)
                .map_err(|error| format!("无法创建临时 user patch：{error}"))?;
            file.write_all(text.as_bytes())
                .map_err(|error| format!("无法写入临时 user patch：{error}"))?;
            file.sync_all()
                .map_err(|error| format!("无法同步临时 user patch：{error}"))?;
        }
        fs::rename(&temporary, path).map_err(|error| {
            let _ = fs::remove_file(&temporary);
            format!("无法替换 user patch：{error}")
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn roundtrip_config_and_disabled() {
        let dir = tempfile::tempdir().unwrap();
        let store = UserPatchStore::new(dir.path().join("user.patches.toml"));
        let mut doc = store.load().await.unwrap();
        doc.upsert_config(
            "ocr",
            "example.config",
            Some(toml::toml! { endpoint = "http://x" }.into()),
        );
        doc.upsert_disabled("ocr", "example.config", Some(true));
        store.store(&doc).await.unwrap();
        let loaded = store.load().await.unwrap();
        assert_eq!(loaded.patches.len(), 1);
        assert_eq!(
            loaded.config_for("ocr").unwrap()["endpoint"].as_str(),
            Some("http://x")
        );
        assert_eq!(loaded.disabled_for("ocr"), Some(true));

        let mut cleared_disabled = loaded;
        cleared_disabled.upsert_disabled("ocr", "example.config", None);
        assert!(cleared_disabled.config_for("ocr").is_some());
        assert_eq!(cleared_disabled.disabled_for("ocr"), None);

        let mut cleared_all = cleared_disabled;
        cleared_all.upsert_config("ocr", "example.config", None);
        assert!(cleared_all.patches.is_empty());
    }
}
