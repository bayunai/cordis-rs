//! [`ConfigKey`] / [`ConfigId`]：可拦截配置身份。
//!
//! 与 ServiceKey 分表；仅标识与类型绑定，不持久化配置内容。

use std::{any::TypeId, fmt, marker::PhantomData, sync::Arc};

/// 稳定的配置标识。扩展应只通过 [`ConfigKey`] 创建它。
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConfigId(&'static str);

impl ConfigId {
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

impl fmt::Display for ConfigId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl fmt::Debug for ConfigId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("ConfigId").field(&self.0).finish()
    }
}

/// 类型化的稳定配置 Key；不与 [`crate::ServiceKey`] 共用表。
pub struct ConfigKey<T: Send + Sync + 'static> {
    id: ConfigId,
    marker: PhantomData<fn() -> T>,
}

impl<T: Send + Sync + 'static> ConfigKey<T> {
    pub const fn new(id: &'static str) -> Self {
        Self {
            id: ConfigId(id),
            marker: PhantomData,
        }
    }

    pub const fn id(&self) -> ConfigId {
        self.id
    }

    pub(crate) fn type_id(&self) -> TypeId {
        TypeId::of::<T>()
    }
}

impl<T: Send + Sync + 'static> Copy for ConfigKey<T> {}

impl<T: Send + Sync + 'static> Clone for ConfigKey<T> {
    fn clone(&self) -> Self {
        *self
    }
}

#[derive(Clone)]
pub(crate) struct ErasedConfig {
    pub(crate) type_id: TypeId,
    pub(crate) value: Arc<dyn std::any::Any + Send + Sync>,
}
