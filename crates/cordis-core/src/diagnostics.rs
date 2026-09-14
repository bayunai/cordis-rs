use crate::inject::InjectionPhase;

/// 非敏感运行时诊断快照。
#[derive(Debug, Clone, Default)]
pub struct RuntimeSnapshot {
    pub contexts: Vec<ContextSnapshot>,
    pub providers: Vec<ProviderSnapshot>,
    pub fibers: Vec<FiberSnapshot>,
    pub plugins: Vec<PluginSnapshot>,
}

#[derive(Debug, Clone)]
pub struct ContextSnapshot {
    pub id: u64,
    pub parent: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct ProviderSnapshot {
    pub node: u64,
    pub service: &'static str,
    pub provider_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InjectionPhaseSnapshot {
    Pending,
    Active,
    Failed,
    Disposed,
}

impl From<InjectionPhase> for InjectionPhaseSnapshot {
    fn from(value: InjectionPhase) -> Self {
        match value {
            InjectionPhase::Pending => Self::Pending,
            InjectionPhase::Active => Self::Active,
            InjectionPhase::Failed => Self::Failed,
            InjectionPhase::Disposed => Self::Disposed,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FiberSnapshot {
    pub id: u64,
    pub node: u64,
    pub phase: InjectionPhaseSnapshot,
    pub dependencies: Vec<&'static str>,
    pub missing_dependencies: Vec<&'static str>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PluginSnapshot {
    pub id: u64,
    pub node: u64,
}
