//! 本机 user 覆盖文档：权威在独立文件，不写进 Include path 源文件。

use crate::patch_file::{file_task, parse_patch_document};
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

    /// 按字段读取最后一份显式覆盖；后续只改 disabled 的行不会清除 config。
    pub fn config_for(&self, local_id: &str) -> Option<&toml::Value> {
        self.patches
            .iter()
            .rev()
            .filter(|patch| patch.id == local_id)
            .find_map(|patch| patch.config.as_ref())
    }

    /// 按字段读取最后一份显式覆盖；Some(false) 同样是有效覆盖。
    pub fn disabled_for(&self, local_id: &str) -> Option<bool> {
        self.patches
            .iter()
            .rev()
            .filter(|patch| patch.id == local_id)
            .find_map(|patch| patch.disabled)
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

/// 进程内串行、跨进程排他锁与原子写回的 user patch 存储。
#[derive(Debug)]
pub struct UserPatchStore {
    path: PathBuf,
    lock: Arc<tokio::sync::Mutex<()>>,
}

/// 一次完整读改写的排他事务。必须覆盖读取、协调及失败回写的整个期间。
///
/// 锁文件是持久锚点，不随事务删除，避免等待者锁住已经脱离路径的旧 inode。
#[derive(Debug)]
pub struct UserPatchEdit {
    path: PathBuf,
    held: Arc<HeldEdit>,
}

#[derive(Debug)]
struct HeldEdit {
    _process: tokio::sync::OwnedMutexGuard<()>,
    // 关闭文件释放 OS 锁；不显式 unlock，确保最后一个在途操作结算后才释放。
    _file: fs::File,
}

impl UserPatchEdit {
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
        let held = self.held.clone();
        let path = self.path.clone();
        file_task(move || {
            let _held = held;
            operation(path)
        })
        .await
    }
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
        self.begin_edit().await?.load().await
    }

    pub async fn store(&self, document: &UserPatchDocument) -> Result<(), String> {
        self.begin_edit().await?.store(document).await
    }

    pub async fn begin_edit(&self) -> Result<UserPatchEdit, String> {
        let guard = self.lock.clone().lock_owned().await;
        let path = self.path.clone();
        file_task(move || {
            // 阻塞等待 OS 锁不占用 Tokio 工作线程；调用者取消后结果自动释放。
            let file = acquire_file_lock(&path)?;
            Ok(UserPatchEdit {
                path,
                held: Arc::new(HeldEdit {
                    _process: guard,
                    _file: file,
                }),
            })
        })
        .await
    }
}

fn ensure_parent(path: &Path) -> Result<&Path, String> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut directory = fs::DirBuilder::new();
    directory.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        directory.mode(0o700);
    }
    directory
        .create(parent)
        .map_err(|error| format!("无法创建 user patch 目录：{error}"))?;
    Ok(parent)
}

fn acquire_file_lock(path: &Path) -> Result<fs::File, String> {
    let parent = ensure_parent(path)?;
    let name = path
        .file_name()
        .ok_or_else(|| "user patch 路径没有文件名".to_owned())?;
    let mut lock_name = std::ffi::OsString::from(".");
    lock_name.push(name);
    lock_name.push(".lock");
    let lock_path = parent.join(lock_name);
    // 不截断现有锚点，不允许符号链接或非普通文件作为锁。
    match fs::symlink_metadata(&lock_path) {
        Ok(metadata) if !metadata.is_file() => return Err("user patch 锁必须是普通文件".into()),
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(format!("无法检查 user patch 锁：{error}")),
    }
    let mut options = fs::OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options
        .open(&lock_path)
        .map_err(|error| format!("无法打开 user patch 锁：{error}"))?;
    let metadata = fs::symlink_metadata(&lock_path)
        .map_err(|error| format!("无法检查 user patch 锁：{error}"))?;
    if !metadata.is_file() {
        return Err("user patch 锁必须是普通文件".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let opened = file
            .metadata()
            .map_err(|error| format!("无法检查 user patch 锁句柄：{error}"))?;
        if metadata.dev() != opened.dev() || metadata.ino() != opened.ino() {
            return Err("user patch 锁文件在打开期间被替换".into());
        }
    }
    file.lock()
        .map_err(|error| format!("无法锁定 user patch：{error}"))?;
    Ok(file)
}

fn load_document(path: &Path) -> Result<UserPatchDocument, String> {
    match fs::read_to_string(path) {
        Ok(text) if text.trim().is_empty() => Ok(UserPatchDocument::empty()),
        Ok(text) => {
            let document: UserPatchDocument = parse_patch_document(path, &text)?;
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
        let parent = ensure_parent(path)?;
        let name = path
            .file_name()
            .ok_or_else(|| "user patch 路径没有文件名".to_owned())?;
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

    #[tokio::test]
    async fn public_load_parse_errors_redact_syntax_and_type_failures() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("user.toml");
        let store = UserPatchStore::new(&path);
        for text in [
            "version=1\n[[patches]]\nid='one'\n[patches.config]\ntoken=\"AUDIT-PRIVATE-SECRET\n",
            "version=1\n[[patches]]\nid='one'\ndisabled='AUDIT-PRIVATE-SECRET'\n",
        ] {
            fs::write(&path, text).unwrap();
            let error = store.load().await.unwrap_err();
            assert!(error.contains(path.to_str().unwrap()));
            assert!(error.contains("TOML 语法或类型错误"));
            assert!(error.contains("行，第"));
            assert!(!error.contains("AUDIT-PRIVATE-SECRET"));
            assert!(!error.contains("token="));
            assert_eq!(fs::read_to_string(&path).unwrap(), text);
        }
    }

    #[tokio::test]
    async fn repeated_rows_keep_last_override_per_field_when_updating() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("user.toml");
        fs::write(
            &path,
            r#"
version = 1
[[patches]]
id = "demo"
config = { timeout = 30 }
[[patches]]
id = "other"
config = { timeout = 99 }
[[patches]]
id = "demo"
disabled = true
[[patches]]
id = "demo"
config = { timeout = 40 }
[[patches]]
id = "demo"
disabled = false
[[patches]]
id = "demo"
"#,
        )
        .unwrap();
        let store = UserPatchStore::new(&path);
        let mut doc = store.load().await.unwrap();
        assert_eq!(
            doc.config_for("demo").unwrap()["timeout"].as_integer(),
            Some(40)
        );
        assert_eq!(doc.disabled_for("demo"), Some(false));
        assert!(doc.config_for("missing").is_none());
        assert!(doc.disabled_for("missing").is_none());

        doc.upsert_disabled("demo", "example", Some(true));
        assert_eq!(
            doc.config_for("demo").unwrap()["timeout"].as_integer(),
            Some(40)
        );
        assert_eq!(doc.disabled_for("demo"), Some(true));
        assert_eq!(
            doc.patches
                .iter()
                .filter(|patch| patch.id == "demo")
                .count(),
            1
        );
        doc.upsert_config("demo", "example", Some(toml::toml! { timeout = 50 }.into()));
        assert_eq!(doc.disabled_for("demo"), Some(true));
        assert_eq!(
            doc.config_for("demo").unwrap()["timeout"].as_integer(),
            Some(50)
        );
        store.store(&doc).await.unwrap();
        let mut doc = store.load().await.unwrap();
        assert_eq!(doc.disabled_for("demo"), Some(true));
        doc.upsert_disabled("demo", "example", None);
        assert_eq!(
            doc.config_for("demo").unwrap()["timeout"].as_integer(),
            Some(50)
        );
        assert_eq!(doc.disabled_for("demo"), None);
        doc.upsert_config("demo", "example", None);
        assert!(doc.config_for("demo").is_none());
        assert_eq!(doc.patches.len(), 1);
        assert_eq!(
            doc.config_for("other").unwrap()["timeout"].as_integer(),
            Some(99)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_waiter_keeps_store_locked_until_blocking_work_settles() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(UserPatchStore::new(dir.path().join("user.toml")));
        let (started, ready) = tokio::sync::oneshot::channel();
        let (release, barrier) = std::sync::mpsc::channel();
        let writer = tokio::spawn({
            let store = store.clone();
            async move {
                let edit = store.begin_edit().await.unwrap();
                edit.transaction(move |path| {
                    started.send(()).unwrap();
                    barrier.recv().unwrap();
                    save_document(&path, &document())
                })
                .await
            }
        });
        ready.await.unwrap();
        let anchor = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(dir.path().join(".user.toml.lock"))
            .unwrap();
        assert!(store.lock.try_lock().is_err());
        assert!(anchor.try_lock().is_err());
        writer.abort();
        assert!(writer.await.unwrap_err().is_cancelled());
        assert!(store.lock.try_lock().is_err());
        assert!(anchor.try_lock().is_err());
        release.send(()).unwrap();
        // load 必须等待前一事务完成；完成后读到已落盘值，而不是并发读取半成品。
        let loaded = store.load().await.unwrap();
        assert_eq!(
            loaded.config_for("demo").unwrap()["token"].as_str(),
            Some("secret")
        );
        anchor.try_lock().unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn independent_stores_serialize_complete_read_modify_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("user.toml");
        let first = UserPatchStore::new(&path);
        let second = Arc::new(UserPatchStore::new(&path));
        let edit = first.begin_edit().await.unwrap();
        let mut first_document = edit.load().await.unwrap();
        let (started, ready) = tokio::sync::oneshot::channel();
        let other = tokio::spawn(async move {
            started.send(()).unwrap();
            let edit = second.begin_edit().await.unwrap();
            let mut doc = edit.load().await.unwrap();
            doc.upsert_disabled("second", "demo", Some(true));
            edit.store(&doc).await.unwrap();
        });
        // 在单线程 runtime 上，等待文件锁不得阻止该任务继续运行。
        ready.await.unwrap();
        first_document.upsert_disabled("first", "demo", Some(true));
        edit.store(&first_document).await.unwrap();
        drop(edit);
        other.await.unwrap();
        let doc = first.load().await.unwrap();
        assert_eq!(doc.disabled_for("first"), Some(true));
        assert_eq!(doc.disabled_for("second"), Some(true));
        assert!(dir.path().join(".user.toml.lock").is_file());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_lock_waiter_releases_lock_after_acquisition() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("user.toml");
        let holder = UserPatchStore::new(&path);
        let store = UserPatchStore::new(&path);
        let edit = holder.begin_edit().await.unwrap();
        {
            let waiting = store.begin_edit();
            tokio::pin!(waiting);
            // 首次轮询开始阻塞锁任务后立即取消，不依赖固定睡眠。
            tokio::select! {
                biased;
                result = &mut waiting => panic!("排他锁未生效：{result:?}"),
                () = std::future::ready(()) => {},
            }
            assert!(store.lock.try_lock().is_err());
        }
        drop(edit);
        // 被取消任务最终取得的句柄必须自动关闭，否则该次读取永久阻塞。
        assert_eq!(store.load().await.unwrap(), UserPatchDocument::empty());
    }

    #[test]
    fn child_process_lock_holder() {
        let Some(path) = std::env::var_os("CORDIS_USER_LOCK_TEST_PATH") else {
            return;
        };
        let _lock = acquire_file_lock(Path::new(&path)).unwrap();
        use std::io::Read;
        println!("user-lock-acquired");
        std::io::stdout().flush().unwrap();
        // 父进程杀死子进程，验证 OS 自动释放，而非依赖 Rust Drop。
        let mut byte = [0];
        std::io::stdin().read_exact(&mut byte).unwrap();
    }

    #[test]
    fn os_lock_is_exclusive_across_processes_and_released_on_exit() {
        use std::io::BufRead;
        use std::process::{Command, Stdio};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("user.toml");
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "user_patch::tests::child_process_lock_holder",
                "--nocapture",
            ])
            .env("CORDIS_USER_LOCK_TEST_PATH", &path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut output = std::io::BufReader::new(child.stdout.take().unwrap());
        loop {
            let mut line = String::new();
            assert_ne!(
                output.read_line(&mut line).unwrap(),
                0,
                "child exited before locking"
            );
            if line.contains("user-lock-acquired") {
                break;
            }
        }
        let anchor = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(dir.path().join(".user.toml.lock"))
            .unwrap();
        assert!(anchor.try_lock().is_err());
        child.kill().unwrap();
        child.wait().unwrap();
        anchor.try_lock().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn lock_anchor_rejects_symlinks_without_modifying_target() {
        let dir = tempfile::tempdir().unwrap();
        let victim = dir.path().join("victim");
        fs::write(&victim, "preserve").unwrap();
        std::os::unix::fs::symlink(&victim, dir.path().join(".user.toml.lock")).unwrap();
        assert!(acquire_file_lock(&dir.path().join("user.toml")).is_err());
        assert_eq!(fs::read_to_string(victim).unwrap(), "preserve");
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
