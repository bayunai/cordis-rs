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
    pub fn from_config(
        config: IncludeConfig,
        ctx: &Context,
    ) -> Result<Arc<dyn EntryTreeSource>, SourceError> {
        Self::open(config, ctx).map(|source| Arc::new(source) as Arc<dyn EntryTreeSource>)
    }
    pub fn open(config: IncludeConfig, ctx: &Context) -> Result<Self, SourceError> {
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
    fn read_bytes(&self) -> Result<Vec<u8>, SourceError> {
        fs::read(&self.filename).map_err(|error| io_error(&self.filename, &error))
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
    fn log(&self, message: &str) {
        if log_enabled(&self.logs.lock().expect("include logs")) {
            self.logger
                .info(format!("{message}: {}", self.filename.display()));
        }
    }
}

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
                // 先完整验证 initial 与 patches，再创建文件。非法初始值不落盘。
                apply_patches(&initial, &self.config.patches)?;
                atomic_write(&self.filename, &bytes, false)?;
                bytes
            }
            Err(error) => return Err(io_error(&self.filename, &error)),
        };
        let config = apply_patches(&self.format.parse(&bytes)?, &self.config.patches)?;
        self.log("已读取配置树");
        Ok(SourceDocument {
            config,
            revision: SourceRevision::new(bytes),
        })
    }
    fn check_revision(&self, expected: &SourceRevision) -> Result<(), SourceError> {
        if self.read_bytes()?.as_slice() != expected.as_bytes() {
            return Err(SourceError::new(
                SourceErrorKind::Conflict,
                "Include 配置文件已在外部修改；请先 reload",
            ));
        }
        Ok(())
    }
    fn validate_write(&self, config: &ExtensionsConfig) -> Result<(), SourceError> {
        self.bytes_for_write(config).map(|_| ())
    }
    fn write(
        &self,
        config: &ExtensionsConfig,
        expected: &SourceRevision,
    ) -> Result<SourceRevision, SourceError> {
        let bytes = self.bytes_for_write(config)?;
        self.check_revision(expected)?;
        atomic_write(&self.filename, &bytes, true)?;
        self.log("已写回配置树");
        Ok(SourceRevision::new(bytes))
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
