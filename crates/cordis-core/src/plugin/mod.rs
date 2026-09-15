//! [`Plugin`] trait：Core 的挂载契约；公开 [`PluginKey`]。
//!
//! 配置、Schema、发现与热更新属 Host；子模块 `group` 负责按 Key 归组与统一卸载。

pub(crate) mod group;

use crate::{Context, CoreError, ServiceId, error::format_panic_message};
use async_trait::async_trait;
use std::{
    fmt,
    panic::{AssertUnwindSafe, catch_unwind},
};

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

/// 已在用户边界读取并通过 panic 隔离的 Plugin 静态元数据。
pub(crate) struct PluginMetadata {
    pub(crate) key: PluginKey,
    pub(crate) dependencies: Vec<ServiceId>,
}

pub(crate) fn read_metadata(plugin: &dyn Plugin) -> Result<PluginMetadata, CoreError> {
    let key = catch_unwind(AssertUnwindSafe(|| plugin.key()))
        .map_err(|payload| CoreError::PluginApply(format_panic_message("plugin key", payload)))?;
    let dependencies = catch_unwind(AssertUnwindSafe(|| plugin.inject())).map_err(|payload| {
        CoreError::PluginApply(format_panic_message("plugin inject", payload))
    })?;
    Ok(PluginMetadata { key, dependencies })
}

/// 可挂载到 Context 的极简插件实例。
///
/// 宿主负责校验配置并据此构造新的、配置不可变的插件实例；Core 只负责挂载与
/// Fiber 生命周期，不保存 JSON、Schema 或可变插件配置。配置变更应由宿主构造
/// 新实例后调用 [`crate::Fiber::replace`]，而不是修改已挂载实例后 `restart()`。
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
