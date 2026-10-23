use crate::{IncludeConfig, apply_patches, codec::Format};
use cordis_core::{ConfigKey, Context, CoreError, Logger};
use cordis_loader::{
    CONFIG_VERSION, EntryTreeSource, ExtensionsConfig, SourceDocument, SourceError,
    SourceErrorKind, SourceRevision,
};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicU64, Ordering},
    },
};

/// 来源目录只由文件提供方解释，Loader 元数据不再承载文件系统语义。
#[derive(Clone)]
pub struct IncludeLocation {
    pub directory: PathBuf,
    identity: String,
    logs: Arc<RwLock<LogPolicy>>,
}
impl IncludeLocation {
    pub fn enable_logs(&self) -> bool {
        log_enabled(&self.logs)
    }
}
#[derive(Clone)]
struct LogPolicy {
    explicit: Option<bool>,
    parent: Option<Arc<RwLock<LogPolicy>>>,
}
fn log_enabled(policy: &Arc<RwLock<LogPolicy>>) -> bool {
    let policy = policy.read().expect("include log policy").clone();
    match policy.explicit {
        Some(value) => value,
        None => policy.parent.as_ref().is_some_and(log_enabled),
    }
}
pub static INCLUDE_LOCATION: ConfigKey<IncludeLocation> =
    ConfigKey::new("cordis.include.location@1");

pub struct FileSource {
    filename: PathBuf,
    format: Format,
    config: IncludeConfig,
    logs: Mutex<Arc<RwLock<LogPolicy>>>,
    logger: Logger,
}

/// 一个任务拥有一份完整事务的输入。磁盘、编解码和 patches 不在 Tokio 调度线程执行。
#[derive(Clone)]
struct FileWork {
    filename: PathBuf,
    format: Format,
    config: IncludeConfig,
}

pub(crate) async fn file_task<T: Send + 'static>(
    operation: impl FnOnce() -> Result<T, SourceError> + Send + 'static,
) -> Result<T, SourceError> {
    tokio::task::spawn_blocking(operation).await.map_err(|_| {
        // JoinError 可能携带 panic 载荷，不能把它或原始配置传播到控制面。
        SourceError::new(SourceErrorKind::Unavailable, "Include 文件事务任务异常终止")
    })?
}

fn io_error(path: &Path, error: &std::io::Error) -> SourceError {
    // 使用错误分类而不是嵌套解析信息；不能让配置正文经错误链回显。
    SourceError::new(
        SourceErrorKind::Io,
        format!(
            "Include 文件操作失败（{}，{:?}）",
            path.display(),
            error.kind()
        ),
    )
}
impl FileSource {
    pub async fn from_config(
        config: IncludeConfig,
        ctx: &Context,
    ) -> Result<Arc<dyn EntryTreeSource>, SourceError> {
        Self::open(config, ctx)
            .await
            .map(|source| Arc::new(source) as Arc<dyn EntryTreeSource>)
    }
    pub async fn open(config: IncludeConfig, ctx: &Context) -> Result<Self, SourceError> {
        config.validate()?;
        let parent = match ctx.config(INCLUDE_LOCATION) {
            Ok(parent) => Some(parent),
            Err(CoreError::ConfigUnavailable { .. }) => None,
            Err(_) => {
                return Err(SourceError::new(
                    SourceErrorKind::Unavailable,
                    "Include 来源上下文无效",
                ));
            }
        };
        let configured = if config.path.is_absolute() {
            config.path.clone()
        } else {
            let parent = parent.as_ref().ok_or_else(|| {
                SourceError::new(
                    SourceErrorKind::InvalidData,
                    "相对 Include 路径必须位于另一个 Include 文件来源内",
                )
            })?;
            parent.directory.join(&config.path)
        };
        let format = Format::for_path(&configured)?;
        // 规范化父目录，确保创建 initial 时也能以同一身份占用尚不存在的文件。
        let filename = file_task(move || {
            let filename = match configured.canonicalize() {
                Ok(path) => path,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    let parent = configured.parent().ok_or_else(|| {
                        SourceError::new(SourceErrorKind::InvalidData, "Include 文件路径没有父目录")
                    })?;
                    let parent = parent
                        .canonicalize()
                        .map_err(|error| io_error(parent, &error))?;
                    let name = configured.file_name().ok_or_else(|| {
                        SourceError::new(SourceErrorKind::InvalidData, "Include 路径没有文件名")
                    })?;
                    parent.join(name)
                }
                Err(error) => return Err(io_error(&configured, &error)),
            };
            if filename.to_str().is_none() {
                return Err(SourceError::new(
                    SourceErrorKind::InvalidData,
                    "Include 来源路径必须是有效 UTF-8",
                ));
            }
            Ok(filename)
        })
        .await?;
        let logs = Arc::new(RwLock::new(LogPolicy {
            explicit: config.enable_logs,
            parent: parent.as_ref().map(|parent| parent.logs.clone()),
        }));
        let logger = ctx.logger_named("include").map_err(|_| {
            SourceError::new(
                SourceErrorKind::Unavailable,
                "Include 所属 Context 已不可用",
            )
        })?;
        Ok(Self {
            filename,
            format,
            config,
            logs: Mutex::new(logs),
            logger,
        })
    }
    pub fn path(&self) -> &Path {
        &self.filename
    }
    fn work(&self) -> FileWork {
        FileWork {
            filename: self.filename.clone(),
            format: self.format,
            config: self.config.clone(),
        }
    }
    fn log(&self, message: &str) {
        if log_enabled(&self.logs.lock().expect("include logs")) {
            self.logger
                .info(format!("{message}: {}", self.filename.display()));
        }
    }
}

#[async_trait::async_trait]
impl EntryTreeSource for FileSource {
    fn identity(&self) -> String {
        self.filename
            .to_str()
            .expect("validated UTF-8 source path")
            .to_owned()
    }
    fn context(&self, owner: &Context) -> Result<Context, SourceError> {
        let directory = self
            .filename
            .parent()
            .ok_or_else(|| {
                SourceError::new(SourceErrorKind::InvalidData, "Include 来源缺少父目录")
            })?
            .to_path_buf();
        // 同一来源定义提交后复用共享策略，使已有嵌套 Include 的继承关系立即生效，
        // 不需要重建它们的 Context/Fiber。候选 open/read 阶段不修改旧策略。
        let identity = self.identity();
        let candidate = self.logs.lock().expect("include logs").clone();
        let logs = match owner.config(INCLUDE_LOCATION) {
            Ok(previous) if previous.identity == identity => {
                if !Arc::ptr_eq(&previous.logs, &candidate) {
                    *previous.logs.write().expect("include log policy") =
                        candidate.read().expect("include log policy").clone();
                }
                previous.logs.clone()
            }
            Ok(_) | Err(CoreError::ConfigUnavailable { .. }) => candidate,
            Err(_) => {
                return Err(SourceError::new(
                    SourceErrorKind::Unavailable,
                    "Include 来源上下文无效",
                ));
            }
        };
        *self.logs.lock().expect("include logs") = logs.clone();
        owner
            .intercept(
                INCLUDE_LOCATION,
                IncludeLocation {
                    directory,
                    identity,
                    logs,
                },
            )
            .map_err(|_| {
                SourceError::new(SourceErrorKind::Unavailable, "无法建立 Include 来源上下文")
            })
    }
    async fn read(&self) -> Result<SourceDocument, SourceError> {
        let work = self.work();
        let document = file_task(move || work.read()).await?;
        self.log("已读取配置树");
        Ok(document)
    }
    async fn check_revision(&self, expected: &SourceRevision) -> Result<(), SourceError> {
        let work = self.work();
        let expected = expected.clone();
        file_task(move || work.check_revision(&expected)).await
    }
    async fn validate_write(&self, config: &ExtensionsConfig) -> Result<(), SourceError> {
        let work = self.work();
        let config = config.clone();
        file_task(move || work.bytes_for_write(&config).map(|_| ())).await
    }
    async fn write(
        &self,
        config: &ExtensionsConfig,
        expected: &SourceRevision,
    ) -> Result<SourceRevision, SourceError> {
        let work = self.work();
        let config = config.clone();
        let expected = expected.clone();
        let revision = file_task(move || {
            let bytes = work.bytes_for_write(&config)?;
            work.check_revision(&expected)?;
            atomic_write(&work.filename, &bytes, true)?;
            Ok(SourceRevision::new(bytes))
        })
        .await?;
        self.log("已写回配置树");
        Ok(revision)
    }
}

impl FileWork {
    fn read(&self) -> Result<SourceDocument, SourceError> {
        let bytes = match fs::read(&self.filename) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let initial = self
                    .config
                    .initial
                    .as_ref()
                    .ok_or_else(|| io_error(&self.filename, &error))?;
                let initial = ExtensionsConfig {
                    version: CONFIG_VERSION,
                    extensions: initial.clone(),
                };
                let bytes = self.format.serialize(&initial)?;
                // 先验证 initial 结构与 patches，再创建文件；Factory 业务预检仍由 Loader 执行。
                apply_patches(&initial, &self.config.patches)?;
                atomic_write(&self.filename, &bytes, false)?;
                bytes
            }
            Err(error) => return Err(io_error(&self.filename, &error)),
        };
        let config = apply_patches(&self.format.parse(&bytes)?, &self.config.patches)?;
        Ok(SourceDocument {
            config,
            revision: SourceRevision::new(bytes),
        })
    }
    fn check_revision(&self, expected: &SourceRevision) -> Result<(), SourceError> {
        let bytes = fs::read(&self.filename).map_err(|error| io_error(&self.filename, &error))?;
        if bytes.as_slice() != expected.as_bytes() {
            return Err(SourceError::new(
                SourceErrorKind::Conflict,
                "Include 配置文件已在外部修改；请先 reload",
            ));
        }
        Ok(())
    }
    fn bytes_for_write(&self, config: &ExtensionsConfig) -> Result<Vec<u8>, SourceError> {
        let bytes = self.format.serialize(config)?;
        let reconstructed = apply_patches(&self.format.parse(&bytes)?, &self.config.patches)
            .map_err(|_| {
                SourceError::new(
                    SourceErrorKind::Conflict,
                    "写回后再次应用 patches 将产生无效条目；请修改 Include 的覆盖声明",
                )
            })?;
        if reconstructed != *config {
            return Err(SourceError::new(
                SourceErrorKind::Conflict,
                "写回结果被 patches 覆盖，无法保留目标配置",
            ));
        }
        Ok(bytes)
    }
}

/// initial 使用 hard_link 的原子“仅不存在才创建”语义，不覆盖竞争者刚建立的文件。
/// 普通写回使用同目录 rename。临时文件在打开时即设置 0600，正文没有宽权限窗口。
fn atomic_write(path: &Path, bytes: &[u8], replace: bool) -> Result<(), SourceError> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let parent = path.parent().ok_or_else(|| {
        SourceError::new(SourceErrorKind::InvalidData, "Include 文件路径没有父目录")
    })?;
    let name = path.file_name().ok_or_else(|| {
        SourceError::new(SourceErrorKind::InvalidData, "Include 文件路径没有文件名")
    })?;
    let temporary = parent.join(format!(
        ".{}.{}.{}.tmp",
        name.to_string_lossy(),
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let result = (|| {
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&temporary)
            .map_err(|error| io_error(&temporary, &error))?;
        file.write_all(bytes)
            .map_err(|error| io_error(&temporary, &error))?;
        file.sync_all()
            .map_err(|error| io_error(&temporary, &error))?;
        if replace {
            fs::rename(&temporary, path).map_err(|error| io_error(path, &error))?;
        } else {
            fs::hard_link(&temporary, path).map_err(|error| io_error(path, &error))?;
            fs::remove_file(&temporary).map_err(|error| io_error(&temporary, &error))?;
        }
        Ok(())
    })();
    if result.is_err() && temporary.exists() {
        fs::remove_file(&temporary).map_err(|error| io_error(&temporary, &error))?;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::file_task;
    use cordis_loader::SourceErrorKind;

    #[tokio::test(flavor = "current_thread")]
    async fn blocking_file_transaction_leaves_the_scheduler_available() {
        // 所有文件工作单元共用同一执行边界。用屏障而非磁盘速度验证调度行为。
        for phase in ["open", "read", "validate_write", "write"] {
            let (started, waiting) = tokio::sync::oneshot::channel();
            let (release, blocked) = std::sync::mpsc::channel();
            let caller = std::thread::current().id();
            let task = tokio::spawn(file_task(move || {
                assert_ne!(std::thread::current().id(), caller, "{phase}");
                started.send(()).unwrap();
                blocked.recv().unwrap();
                Ok(())
            }));
            waiting.await.unwrap();
            // 阻塞工作尚未释放时，单线程 runtime 仍可执行另一个异步任务。
            assert_eq!(tokio::spawn(async { 42 }).await.unwrap(), 42);
            assert!(!task.is_finished());
            release.send(()).unwrap();
            task.await.unwrap().unwrap();
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn file_transaction_panic_is_a_redacted_source_error() {
        let error = file_task::<()>(|| panic!("private configuration payload"))
            .await
            .unwrap_err();
        assert_eq!(error.kind, SourceErrorKind::Unavailable);
        assert_eq!(error.message, "Include 文件事务任务异常终止");
        assert!(!format!("{error:?}").contains("private configuration payload"));
    }
}
