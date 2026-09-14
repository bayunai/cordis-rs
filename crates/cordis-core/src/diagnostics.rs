use crate::inject::InjectionPhase;

/// 非敏感运行时诊断快照。
#[derive(Debug, Clone, Default)]
pub struct RuntimeSnapshot {
    pub contexts: Vec<ContextSnapshot>,
    pub isolations: Vec<IsolationSnapshot>,
    pub providers: Vec<ProviderSnapshot>,
    pub plugin_fibers: Vec<PluginFiberSnapshot>,
    pub inject_fibers: Vec<InjectFiberSnapshot>,
    pub effects: Vec<EffectSnapshot>,
}

#[derive(Debug, Clone)]
pub struct ContextSnapshot {
    pub id: u64,
    pub parent: Option<u64>,
    pub isolations: Vec<ContextIsolationSnapshot>,
}

#[derive(Debug, Clone)]
pub struct ContextIsolationSnapshot {
    pub service: &'static str,
    pub label_id: u64,
}

#[derive(Debug, Clone)]
pub struct IsolationSnapshot {
    pub id: u64,
}

#[derive(Debug, Clone)]
pub struct ProviderSnapshot {
    pub node: Option<u64>,
    pub isolation: Option<u64>,
    pub service: &'static str,
    pub provider_id: u64,
    pub effect_id: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FiberStateSnapshot {
    Pending,
    Loading,
    Active,
    Failed,
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
    pub node: u64,
    pub phase: FiberStateSnapshot,
    pub dependencies: Vec<&'static str>,
    pub missing_dependencies: Vec<&'static str>,
    pub last_error: Option<String>,
}

/// Plugin Fiber 诊断快照。
#[derive(Debug, Clone)]
pub struct PluginFiberSnapshot {
    pub id: u64,
    pub node: u64,
    pub state: FiberStateSnapshot,
    pub dependencies: Vec<&'static str>,
    pub missing_dependencies: Vec<&'static str>,
    pub last_error: Option<String>,
    pub root_effect: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct EffectSnapshot {
    pub id: u64,
    pub name: String,
    pub parent: Option<u64>,
    pub node: Option<u64>,
    pub fiber_id: Option<u64>,
    pub cancelled: bool,
    pub disposed: bool,
    pub child_count: usize,
    pub task_count: usize,
    pub cleanup_count: usize,
}
