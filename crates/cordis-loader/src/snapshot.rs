//! EntryTree 诊断快照。

use cordis_core::{FiberState, PluginDefinitionId};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct EntryId(String);
impl EntryId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl fmt::Display for EntryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl From<String> for EntryId {
    fn from(value: String) -> Self {
        Self(value)
    }
}
impl From<&str> for EntryId {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

#[derive(Debug, Clone)]
pub struct LoaderSnapshot {
    pub entries: Vec<EntrySnapshot>,
}
#[derive(Debug, Clone)]
pub struct EntrySnapshot {
    /// Include 树前缀与本地 ID 组成的地址，不含 Group 层级。
    pub path: EntryId,
    /// 真实父 Group，或承载所属树的 Include；不通过地址前缀推导。
    pub parent: Option<EntryId>,
    pub name: String,
    pub group: bool,
    pub enabled: bool,
    pub definition_id: PluginDefinitionId,
    pub fiber_id: Option<u64>,
    pub state: Option<FiberState>,
    pub last_error: Option<String>,
}
impl LoaderSnapshot {
    pub fn entry(&self, path: &str) -> Option<&EntrySnapshot> {
        self.entries.iter().find(|item| item.path.as_str() == path)
    }
}
