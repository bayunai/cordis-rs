//! Loader 诊断快照：把配置实例与 Core Fiber 关联起来。

use cordis_core::{FiberState, PluginKey, RuntimeSnapshot};
use std::fmt;

/// 配置层实例身份，例如 `primary-database`。
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct InstanceId(String);

impl InstanceId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for InstanceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl From<&str> for InstanceId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<String> for InstanceId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

/// Loader + Runtime 联合快照。
#[derive(Debug, Clone)]
pub struct LoaderSnapshot {
    pub instances: Vec<InstanceSnapshot>,
    pub runtime: RuntimeSnapshot,
}

/// 单个已编排实例。
#[derive(Debug, Clone)]
pub struct InstanceSnapshot {
    pub instance: InstanceId,
    pub factory: String,
    pub plugin_key: PluginKey,
    pub fiber_id: u64,
    pub state: FiberState,
    pub last_error: Option<String>,
}

impl LoaderSnapshot {
    pub fn instance(&self, id: &str) -> Option<&InstanceSnapshot> {
        self.instances
            .iter()
            .find(|item| item.instance.as_str() == id)
    }
}
