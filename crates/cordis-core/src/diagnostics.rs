//! 只读运行时诊断快照。
//!
//! 导出 Context / Fiber / Effect 等结构信息；不包含服务载荷。

use crate::{ProviderAvailability, registry::InjectionPhase};

/// 非敏感运行时诊断快照。
#[derive(Debug, Clone, Default)]
pub struct RuntimeSnapshot {
    pub isolations: Vec<IsolationSnapshot>,
    pub providers: Vec<ProviderSnapshot>,
    pub plugin_fibers: Vec<PluginFiberSnapshot>,
    pub plugin_registry: Vec<PluginRegistrySnapshot>,
    pub inject_fibers: Vec<InjectFiberSnapshot>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FiberStateSnapshot {
    Pending,
    Loading,
    Active,
    Failed,
    Unloading,
    Disposed,
}

impl From<InjectionPhase> for FiberStateSnapshot {
    fn from(value: InjectionPhase) -> Self {
        match value {
            InjectionPhase::Pending => Self::Pending,
            InjectionPhase::Active => Self::Active,
            InjectionPhase::Failed => Self::Failed,
            InjectionPhase::Disposed => Self::Disposed,
        }
    }
}

/// 派生 inject 的诊断快照（非 Plugin Fiber）。
#[derive(Debug, Clone)]
pub struct InjectFiberSnapshot {
    pub id: u64,
    pub context_depth: usize,
    pub phase: FiberStateSnapshot,
    pub dependencies: Vec<&'static str>,
    pub missing_dependencies: Vec<&'static str>,
    pub unavailable_dependencies: Vec<UnavailableDependencySnapshot>,
    pub last_error: Option<String>,
}

/// Plugin Fiber 诊断快照。
#[derive(Debug, Clone)]
pub struct PluginFiberSnapshot {
    pub id: u64,
    pub plugin_key: &'static str,
    pub context_depth: usize,
    pub state: FiberStateSnapshot,
    pub dependencies: Vec<&'static str>,
    pub missing_dependencies: Vec<&'static str>,
    pub unavailable_dependencies: Vec<UnavailableDependencySnapshot>,
    pub last_error: Option<String>,
    pub root_effect: Option<u64>,
}

/// Plugin Registry 分组诊断（仅 Key + Fiber id/state）。
#[derive(Debug, Clone)]
pub struct PluginRegistrySnapshot {
    pub plugin_key: &'static str,
    pub unmounting: bool,
    pub fibers: Vec<PluginRegistryFiberSnapshot>,
}

#[derive(Debug, Clone)]
pub struct PluginRegistryFiberSnapshot {
    pub id: u64,
    pub state: FiberStateSnapshot,
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
