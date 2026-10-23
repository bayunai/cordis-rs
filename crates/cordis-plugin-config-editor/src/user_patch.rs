//! 本机 user 覆盖文档：权威在独立文件，不写进 Include path 源文件。

use crate::patch_file::file_task;
use cordis_plugin_include::EntryPatch;
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
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
    pub fn upsert_config(&mut self, local_id: &str, factory: &str, config: Option<toml::Value>) {
        let disabled = self.disabled_for(local_id);
        self.write_patch(local_id, factory, config, disabled);
    }

    /// 更新或清除 disabled 覆盖；保留同条目的 `config`。
    ///
    /// `disabled = None` 表示与 base 一致，去掉该字段。
    pub fn upsert_disabled(&mut self, local_id: &str, factory: &str, disabled: Option<bool>) {
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
    lock: Arc<tokio::sync::Mutex<()>>,
}

impl UserPatchStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub async fn load(&self) -> Result<UserPatchDocument, String> {
        self.transaction(|path| load_document(&path)).await
    }

    pub async fn store(&self, document: &UserPatchDocument) -> Result<(), String> {
        let document = document.clone();
        self.transaction(move |path| save_document(&path, &document))
            .await
    }

    async fn transaction<T: Send + 'static>(
        &self,
        operation: impl FnOnce(PathBuf) -> Result<T, String> + Send + 'static,
    ) -> Result<T, String> {
        let guard = self.lock.clone().lock_owned().await;
        let path = self.path.clone();
        file_task(move || {
            // 阻塞事务启动后不能取消；锁随事务结算，不随调用方取消等待释放。
            let _guard = guard;
            operation(path)
        })
        .await
    }
}

fn load_document(path: &Path) -> Result<UserPatchDocument, String> {
    match fs::read_to_string(path) {
        Ok(text) if text.trim().is_empty() => Ok(UserPatchDocument::empty()),
        Ok(text) => {
            let document: UserPatchDocument =
                toml::from_str(&text).map_err(|error| format!("无法解析 user patch：{error}"))?;
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
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let name = path
            .file_name()
            .ok_or_else(|| "user patch 路径没有文件名".to_owned())?;
        let mut directory = fs::DirBuilder::new();
        directory.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            // 只限制本次创建的目录，不修改已有共享父目录权限。
            directory.mode(0o700);
        }
        directory
            .create(parent)
            .map_err(|error| format!("无法创建 user patch 目录：{error}"))?;
        let mut document = document.clone();
        document.version = USER_PATCH_VERSION;
        let text = toml::to_string_pretty(&document)
            .map_err(|error| format!("无法序列化 user patch：{error}"))?;
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let temporary = parent.join(format!(
            ".{}.{}.{}.tmp",
            name.to_string_lossy(),
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        publish_user_patch(path, &temporary, |file| {
            file.write_all(text.as_bytes())
                .map_err(|error| format!("无法写入临时 user patch：{error}"))?;
            file.sync_all()
                .map_err(|error| format!("无法同步临时 user patch：{error}"))
        })
    }
}

/// 仅成功 create_new 后才拥有临时文件；发布前的任何失败都在关闭句柄后清理。
fn publish_user_patch(
    path: &Path,
    temporary: &Path,
    write: impl FnOnce(&mut fs::File) -> Result<(), String>,
) -> Result<(), String> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        // 创建时即限制权限，不能先写秘密再 chmod。
        options.mode(0o600);
    }
    let mut file = options
        .open(temporary)
        .map_err(|error| format!("无法创建临时 user patch：{error}"))?;
    let result = write(&mut file);
    drop(file);
    if let Err(error) = result {
        return Err(cleanup_user_temporary(temporary, error));
    }
    fs::rename(temporary, path)
        .map_err(|error| cleanup_user_temporary(temporary, format!("无法替换 user patch：{error}")))
}

fn cleanup_user_temporary(temporary: &Path, original: String) -> String {
    match fs::remove_file(temporary) {
        Ok(()) => original,
        Err(error) => format!("{original}；临时 user patch 清理失败：{error}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_waiter_keeps_store_locked_until_blocking_work_settles() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(UserPatchStore::new(dir.path().join("user.toml")));
        let (started, ready) = tokio::sync::oneshot::channel();
        let (release, barrier) = std::sync::mpsc::channel();
        let writer = tokio::spawn({
            let store = store.clone();
            async move {
                store
                    .transaction(move |path| {
                        started.send(()).unwrap();
                        barrier.recv().unwrap();
                        save_document(&path, &document())
                    })
                    .await
            }
        });
        ready.await.unwrap();
        assert!(store.lock.try_lock().is_err());
        writer.abort();
        assert!(writer.await.unwrap_err().is_cancelled());
        assert!(store.lock.try_lock().is_err());
        release.send(()).unwrap();
        // load 必须等待前一事务完成；完成后读到已落盘值，而不是并发读取半成品。
        let loaded = store.load().await.unwrap();
        assert_eq!(
            loaded.config_for("demo").unwrap()["token"].as_str(),
            Some("secret")
        );
    }

    fn document() -> UserPatchDocument {
        let mut document = UserPatchDocument::empty();
        document.upsert_config(
            "demo",
            "demo",
            Some(toml::toml! { token = "secret" }.into()),
        );
        document
    }

    #[test]
    fn existing_temporary_is_never_overwritten_or_removed() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("user.toml");
        let temporary = dir.path().join("occupied.tmp");
        fs::write(&target, "original target").unwrap();
        fs::write(&temporary, "unrelated file").unwrap();
        let error = publish_user_patch(&target, &temporary, |_| panic!("must not write"));
        assert!(error.is_err());
        assert_eq!(fs::read_to_string(&target).unwrap(), "original target");
        assert_eq!(fs::read_to_string(&temporary).unwrap(), "unrelated file");
    }

    #[test]
    fn failed_write_keeps_target_and_cleans_only_owned_temporary() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("user.toml");
        let temporary = dir.path().join("ours.tmp");
        fs::write(&target, "original target").unwrap();
        let error = publish_user_patch(&target, &temporary, |file| {
            file.write_all(b"partial secret").unwrap();
            Err("write failure".into())
        })
        .unwrap_err();
        assert_eq!(error, "write failure");
        assert_eq!(fs::read_to_string(&target).unwrap(), "original target");
        assert!(!temporary.exists());
    }

    #[test]
    fn failed_publication_cleans_owned_temporary_and_preserves_target() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target-directory");
        fs::create_dir(&target).unwrap();
        fs::write(target.join("marker"), "preserved").unwrap();
        let temporary = dir.path().join("ours.tmp");
        let error = publish_user_patch(&target, &temporary, |file| {
            file.write_all(b"secret").map_err(|error| error.to_string())
        })
        .unwrap_err();
        assert!(error.starts_with("无法替换 user patch"));
        assert!(!temporary.exists());
        assert_eq!(
            fs::read_to_string(target.join("marker")).unwrap(),
            "preserved"
        );
    }

    #[test]
    fn cleanup_failure_preserves_original_error() {
        let dir = tempfile::tempdir().unwrap();
        // 模拟清理时临时文件已消失；原始错误不得被清理错误取代。
        let error =
            cleanup_user_temporary(&dir.path().join("missing.tmp"), "original failure".into());
        assert!(error.starts_with("original failure；"));
        assert!(error.contains("清理失败"));
    }

    #[cfg(unix)]
    #[test]
    fn old_fixed_temporary_symlink_cannot_redirect_writes() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("user.toml");
        let victim = dir.path().join("victim");
        let old_temporary = target.with_extension("toml.tmp");
        fs::write(&victim, "unrelated secret").unwrap();
        symlink(&victim, &old_temporary).unwrap();
        save_document(&target, &document()).unwrap();
        assert_eq!(fs::read_to_string(&victim).unwrap(), "unrelated secret");
        assert!(
            fs::symlink_metadata(&old_temporary)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert!(
            !fs::symlink_metadata(&target)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        // create_new 也拒绝候选唯一临时路径上的符号链接。
        assert!(publish_user_patch(&target, &old_temporary, |_| panic!("must not write")).is_err());
        assert_eq!(fs::read_to_string(&victim).unwrap(), "unrelated secret");
        assert!(
            fs::symlink_metadata(&old_temporary)
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }

    #[cfg(unix)]
    #[test]
    fn private_files_and_new_directories_do_not_chmod_existing_parent() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o755)).unwrap();
        let parent = dir.path().join("new").join("nested");
        let target = parent.join("user.toml");
        save_document(&target, &document()).unwrap();
        assert_eq!(
            fs::metadata(&target).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(&parent).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(parent.parent().unwrap())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(dir.path()).unwrap().permissions().mode() & 0o777,
            0o755
        );
        // 替换已有宽权限文件也不能继承 0644。
        fs::set_permissions(&target, fs::Permissions::from_mode(0o644)).unwrap();
        save_document(&target, &document()).unwrap();
        assert_eq!(
            fs::metadata(&target).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

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
