use crate::{Context, CoreError, ServiceId};
use async_trait::async_trait;
use std::fmt;

/// 稳定的插件身份；同 Runtime 内按 Key 归组 Fiber。
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PluginKey(&'static str);

impl PluginKey {
    pub const fn new(id: &'static str) -> Self {
        Self(id)
    }

    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

impl fmt::Display for PluginKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl fmt::Debug for PluginKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("PluginKey").field(&self.0).finish()
    }
}

/// 可挂载到 Context 的极简插件。
///
/// 宿主负责构造插件对象；Core 只负责挂载与 Fiber 生命周期。
#[async_trait]
pub trait Plugin: Send + Sync {
    /// 稳定插件身份；`Runtime::unmount` / Fiber 归组按此 Key。
    fn key(&self) -> PluginKey;

    /// 插件级依赖声明；未齐时 Fiber 保持 [`crate::FiberState::Pending`]。
    fn inject(&self) -> Vec<ServiceId> {
        Vec::new()
    }

    async fn apply(&self, ctx: &Context) -> Result<(), CoreError>;
}
