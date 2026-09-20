//! Runtime 全局结构化日志总线（不经 ServiceKey / isolate）。
//!
//! 全局 Logger **不过滤**等级：每条记录都进入缓冲区并分发给全部 exporter；
//! 等级阈值由各 exporter 自行解释（可参考 [`LogRecord::default_level`]）。

use crate::{ConfigKey, PluginKey};
use std::{
    collections::{HashMap, VecDeque},
    fmt::Display,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::SystemTime,
};

/// 环形缓冲区容量。
pub const LOG_BUFFER_CAPACITY: usize = 1000;

/// 日志级别（序值越高越严重；阈值表示「不低于该级才输出」）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LogLevel {
    Debug = 0,
    Info = 1,
    Warn = 2,
    Error = 3,
}

impl LogLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }
}

/// 一条已格式化的结构化日志。
#[derive(Debug, Clone)]
pub struct LogRecord {
    pub seq: u64,
    pub timestamp: SystemTime,
    pub level: LogLevel,
    pub target: String,
    pub message: String,
    pub fiber_id: Option<u64>,
    pub plugin_key: Option<PluginKey>,
    /// 调用 Context 的默认阈值（来自 `LOGGER_CONFIG.level`，缺省 `Info`）。
    /// 供 exporter 在无专用配置时回退，**不**决定是否写入全局缓冲。
    pub default_level: LogLevel,
}

/// 同步导出器；I/O 错误由实现者自行处理，不得影响业务控制流。
pub trait LogExporter: Send + Sync {
    fn export(&self, record: &LogRecord);
}

type ExporterId = u64;

/// Runtime 唯一的日志服务：缓冲、序号与 exporter 分发。
pub struct LoggerService {
    next_seq: AtomicU64,
    next_exporter: AtomicU64,
    state: Mutex<LoggerState>,
}

struct LoggerState {
    buffer: VecDeque<LogRecord>,
    exporters: HashMap<ExporterId, Arc<dyn LogExporter>>,
}

impl LoggerService {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            next_seq: AtomicU64::new(1),
            next_exporter: AtomicU64::new(1),
            state: Mutex::new(LoggerState {
                buffer: VecDeque::with_capacity(LOG_BUFFER_CAPACITY),
                exporters: HashMap::new(),
            }),
        })
    }

    pub fn emit(
        &self,
        level: LogLevel,
        target: impl Into<String>,
        message: impl Display,
        fiber_id: Option<u64>,
        plugin_key: Option<PluginKey>,
        default_level: LogLevel,
    ) {
        let record = LogRecord {
            seq: self.next_seq.fetch_add(1, Ordering::Relaxed),
            timestamp: SystemTime::now(),
            level,
            target: target.into(),
            message: message.to_string(),
            fiber_id,
            plugin_key,
            default_level,
        };
        let exporters = {
            let mut state = self.state.lock().expect("logger state");
            if state.buffer.len() >= LOG_BUFFER_CAPACITY {
                state.buffer.pop_front();
            }
            state.buffer.push_back(record.clone());
            state.exporters.values().cloned().collect::<Vec<_>>()
        };
        for exporter in exporters {
            exporter.export(&record);
        }
    }

    pub(crate) fn register_exporter(&self, exporter: Arc<dyn LogExporter>) -> ExporterId {
        let id = self.next_exporter.fetch_add(1, Ordering::Relaxed);
        self.state
            .lock()
            .expect("logger state")
            .exporters
            .insert(id, exporter);
        id
    }

    pub(crate) fn unregister_exporter(&self, id: ExporterId) {
        self.state
            .lock()
            .expect("logger state")
            .exporters
            .remove(&id);
    }

    /// 诊断：当前缓冲快照（不影响 exporter）。
    pub fn buffer_snapshot(&self) -> Vec<LogRecord> {
        self.state
            .lock()
            .expect("logger state")
            .buffer
            .iter()
            .cloned()
            .collect()
    }

    /// 诊断：已注册 exporter 数量（生命周期 / 卸载验证）。
    pub fn exporter_count(&self) -> usize {
        self.state.lock().expect("logger state").exporters.len()
    }
}

/// 调用上下文上的日志门面（不过滤；每条均送达 [`LoggerService`]）。
#[derive(Clone)]
pub struct Logger {
    service: Arc<LoggerService>,
    target: String,
    default_level: LogLevel,
    fiber_id: Option<u64>,
    plugin_key: Option<PluginKey>,
}

impl Logger {
    pub(crate) fn new(
        service: Arc<LoggerService>,
        target: String,
        default_level: LogLevel,
        fiber_id: Option<u64>,
        plugin_key: Option<PluginKey>,
    ) -> Self {
        Self {
            service,
            target,
            default_level,
            fiber_id,
            plugin_key,
        }
    }

    pub fn target(&self) -> &str {
        &self.target
    }

    /// 写入记录时附带的 Context 默认阈值（供 exporter 回退）。
    pub fn default_level(&self) -> LogLevel {
        self.default_level
    }

    pub fn service(&self) -> &Arc<LoggerService> {
        &self.service
    }

    pub fn log(&self, level: LogLevel, message: impl Display) {
        self.service.emit(
            level,
            self.target.clone(),
            message,
            self.fiber_id,
            self.plugin_key,
            self.default_level,
        );
    }

    pub fn debug(&self, message: impl Display) {
        self.log(LogLevel::Debug, message);
    }

    pub fn info(&self, message: impl Display) {
        self.log(LogLevel::Info, message);
    }

    pub fn warn(&self, message: impl Display) {
        self.log(LogLevel::Warn, message);
    }

    pub fn error(&self, message: impl Display) {
        self.log(LogLevel::Error, message);
    }
}

/// 可经 [`crate::Context::intercept`] 覆盖的默认目标名与默认等级。
///
/// `level` 写入 [`LogRecord::default_level`]，不阻止低等级记录进入全局总线。
#[derive(Debug, Clone, Default)]
pub struct LoggerConfig {
    pub name: Option<String>,
    pub level: Option<LogLevel>,
}

/// 日志门面默认配置 ConfigKey。
pub static LOGGER_CONFIG: ConfigKey<LoggerConfig> = ConfigKey::new("cordis.logger.config@1");
