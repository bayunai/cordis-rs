//! [`HostError`]：配置、工厂目录与编排失败契约。

use cordis_core::{CoreError, PluginKey};
use std::{any::Any, io, path::PathBuf};

/// 将扩展边界捕获到的 panic 转换为可诊断、但不泄漏非字符串载荷的错误文本。
pub(crate) fn format_panic_message(boundary: &str, payload: Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        format!("{boundary} panicked: {message}")
    } else if let Some(message) = payload.downcast_ref::<String>() {
        format!("{boundary} panicked: {message}")
    } else {
        format!("{boundary} panicked")
    }
}

/// Host 公开失败类型。
///
/// 配置与构造错误发生在变更之前；生命周期错误表示至少一步编排已经提交。
#[derive(Debug, Clone, thiserror::Error)]
pub enum HostError {
    #[error("读取配置失败 {path}: {message}", path = path.display())]
    Io { path: PathBuf, message: String },
    #[error("解析配置失败 {path}: {message}", path = path.display())]
    Toml { path: PathBuf, message: String },
    #[error("不支持的配置版本 {found}，当前仅支持 1")]
    UnsupportedVersion { found: u32 },
    #[error("实例 {instance} 重复")]
    DuplicateInstance { instance: String },
    #[error("实例 {instance} 引用未注册工厂 {factory}")]
    UnknownFactory { instance: String, factory: String },
    #[error("工厂 {id} 已注册")]
    DuplicateFactory { id: String },
    #[error("实例 {instance} 不允许更换工厂：{from} → {to}")]
    FactoryChanged {
        instance: String,
        from: String,
        to: String,
    },
    #[error("实例 {instance} 的 PluginKey 不允许更换：期望 {expected}，实际 {actual}")]
    PluginKeyChanged {
        instance: String,
        expected: PluginKey,
        actual: PluginKey,
    },
    #[error("插件配置无效: {message}")]
    InvalidConfig { message: String },
    #[error("实例 {instance}（工厂 {factory}）构造失败: {message}")]
    PluginBuild {
        instance: String,
        factory: String,
        message: String,
    },
    #[error("工厂边界 panic: {message}")]
    FactoryPanic { message: String },
    #[error("插件元数据 panic: {message}")]
    PluginPanic { message: String },
    #[error("实例 {instance} 生命周期失败: {source}")]
    Lifecycle {
        instance: String,
        #[source]
        source: CoreError,
    },
    #[error("运行时错误: {source}")]
    Runtime {
        #[source]
        source: CoreError,
    },
    #[error("未配置文件配置源；只能对 bootstrap 启动的 Host 调用 reload")]
    NoConfigSource,
    #[error("Host reconcile 正在执行；同一时刻只允许一个编排操作")]
    ReconcileBusy,
    #[error("Host reconcile 协调器异常终止: {reason}")]
    ReconcileAborted { reason: String },
}

impl HostError {
    /// 工厂在 `build` 时报告的配置错误。
    pub fn invalid_config(message: impl Into<String>) -> Self {
        Self::InvalidConfig {
            message: message.into(),
        }
    }

    pub(crate) fn io(path: PathBuf, source: io::Error) -> Self {
        Self::Io {
            path,
            message: source.to_string(),
        }
    }

    pub(crate) fn toml(path: PathBuf, source: toml::de::Error) -> Self {
        Self::Toml {
            path,
            message: source.to_string(),
        }
    }
}
