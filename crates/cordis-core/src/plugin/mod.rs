//! 插件定义身份与实例执行契约；配置构造由宿主和 Loader 负责。

pub(crate) mod group;

use crate::{Context, CoreError, ServiceId, error::format_panic_message};
use async_trait::async_trait;
use std::{
    fmt,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

static NEXT_DEFINITION: AtomicU64 = AtomicU64::new(1);

/// 进程内插件定义身份；不用于持久配置或跨进程寻址。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PluginDefinitionId(u64);

impl PluginDefinitionId {
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for PluginDefinitionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug)]
struct Definition {
    id: PluginDefinitionId,
    name: Option<Arc<str>>,
}

/// 不可变插件定义句柄；克隆共享身份，同名新定义仍具有独立身份。
#[derive(Clone, Debug)]
pub struct PluginDefinition(Arc<Definition>);

impl PluginDefinition {
    pub fn named(name: impl Into<Arc<str>>) -> Self {
        Self::new(Some(name.into()))
    }
    pub fn anonymous() -> Self {
        Self::new(None)
    }
    fn new(name: Option<Arc<str>>) -> Self {
        let id = NEXT_DEFINITION
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .expect("plugin definition identity exhausted");
        Self(Arc::new(Definition {
            id: PluginDefinitionId(id),
            name,
        }))
    }
    pub fn id(&self) -> PluginDefinitionId {
        self.0.id
    }
    pub fn name(&self) -> Option<&str> {
        self.0.name.as_deref()
    }
    pub(crate) fn shared_name(&self) -> Option<Arc<str>> {
        self.0.name.clone()
    }
}

pub(crate) struct PluginMetadata {
    pub(crate) dependencies: Vec<ServiceId>,
}

pub(crate) fn read_metadata(plugin: &dyn Plugin) -> Result<PluginMetadata, CoreError> {
    let dependencies = catch_unwind(AssertUnwindSafe(|| plugin.inject())).map_err(|payload| {
        CoreError::PluginApply(format_panic_message("plugin inject", payload))
    })?;
    Ok(PluginMetadata { dependencies })
}

/// 插件实例的执行内容；身份由挂载时传入的 PluginDefinition 决定。
#[async_trait]
pub trait Plugin: Send + Sync {
    fn inject(&self) -> Vec<ServiceId> {
        Vec::new()
    }
    async fn apply(&self, ctx: &Context) -> Result<(), CoreError>;
}
