use crate::{Context, CoreError, ServiceId};
use async_trait::async_trait;

/// 可挂载到 Context 的极简插件。
///
/// 宿主负责构造插件对象；Core 只负责挂载与 Fiber 生命周期。
#[async_trait]
pub trait Plugin: Send + Sync {
    /// 插件级依赖声明；未齐时 Fiber 保持 [`crate::FiberState::Pending`]。
    fn inject(&self) -> Vec<ServiceId> {
        Vec::new()
    }

    async fn apply(&self, ctx: &Context) -> Result<(), CoreError>;
}
