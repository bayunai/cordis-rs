//! 只读运行时诊断快照。
//!
//! 导出 Context / Fiber / Effect 等结构信息；不包含服务载荷。

use crate::{FiberState, PluginDefinitionId, ProviderAvailability};

/// 非敏感运行时诊断快照。
#[derive(Debug, Clone, Default)]
pub struct RuntimeSnapshot {
    pub isolations: Vec<IsolationSnapshot>,
    pub providers: Vec<ProviderSnapshot>,
    pub fibers: Vec<FiberSnapshot>,
    pub definitions: Vec<PluginDefinitionSnapshot>,
    pub effects: Vec<EffectSnapshot>,
}

#[derive(Debug, Clone)]
pub struct IsolationSnapshot {
    pub id: u64,
}

#[derive(Debug, Clone)]
pub struct ProviderSnapshot {
    pub context_depth: usize,
    pub isolation: Option<u64>,
    pub service: &'static str,
    pub provider_id: u64,
    pub effect_id: Option<u64>,
    /// 严格解析下的有效状态，包含 Provider check 与所属 Plugin Fiber 状态。
    pub availability: ProviderAvailability,
}

/// 声明式依赖已注册但当前不能严格解析的受控诊断信息。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnavailableDependencySnapshot {
    pub service: &'static str,
    pub reason: std::sync::Arc<str>,
}

/// Plugin Fiber 诊断快照。
#[derive(Debug, Clone)]
pub struct FiberSnapshot {
    pub id: u64,
    pub definition_id: PluginDefinitionId,
    pub parent_fiber_id: Option<u64>,
    pub name: Option<std::sync::Arc<str>>,
    pub context_depth: usize,
    pub state: FiberState,
    pub dependencies: Vec<&'static str>,
    pub missing_dependencies: Vec<&'static str>,
    pub unavailable_dependencies: Vec<UnavailableDependencySnapshot>,
    pub last_error: Option<String>,
    pub root_effect: Option<u64>,
}

/// 同一定义在本 Runtime 内的实例分组；状态来自统一 fibers 列表。
#[derive(Debug, Clone)]
pub struct PluginDefinitionSnapshot {
    pub definition_id: PluginDefinitionId,
    pub name: Option<std::sync::Arc<str>>,
    pub unmounting: bool,
    pub fiber_ids: Vec<u64>,
}

#[derive(Debug, Clone)]
pub struct EffectSnapshot {
    pub id: u64,
    pub name: String,
    pub parent: Option<u64>,
    pub context_depth: Option<usize>,
    pub fiber_id: Option<u64>,
    pub cancelled: bool,
    pub disposed: bool,
    pub child_count: usize,
    pub task_count: usize,
    pub cleanup_count: usize,
}
