//! [`ServiceKey`] / [`ServiceId`] 类型安全与 [`Services`] 快照。
//!
//! 定义稳定服务标识；层级/隔离解析在 `resolver`，不包含业务作用域语义。

pub(crate) mod resolver;

use crate::CoreError;
use std::{
    any::{Any, TypeId},
    collections::HashMap,
    fmt,
    marker::PhantomData,
    sync::Arc,
};

/// 稳定的 Service 标识。扩展应只通过 [`ServiceKey`] 创建它。
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ServiceId(&'static str);

impl ServiceId {
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

impl fmt::Display for ServiceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl fmt::Debug for ServiceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("ServiceId").field(&self.0).finish()
    }
}

/// 类型化的稳定 Service Key。
///
/// 同一个字符串 ID 不得用于不同的 `T`。Core 会在运行时明确拒绝该错误。
pub struct ServiceKey<T: Send + Sync + 'static> {
    id: ServiceId,
    marker: PhantomData<fn() -> T>,
}

impl<T: Send + Sync + 'static> ServiceKey<T> {
    pub const fn new(id: &'static str) -> Self {
        Self {
            id: ServiceId(id),
            marker: PhantomData,
        }
    }

    pub const fn id(&self) -> ServiceId {
        self.id
    }

    pub(crate) fn type_id(&self) -> TypeId {
        TypeId::of::<T>()
    }
}

impl<T: Send + Sync + 'static> Copy for ServiceKey<T> {}

impl<T: Send + Sync + 'static> Clone for ServiceKey<T> {
    fn clone(&self) -> Self {
        *self
    }
}

#[derive(Clone)]
pub(crate) struct ErasedService {
    pub(crate) type_id: TypeId,
    pub(crate) value: Arc<dyn Any + Send + Sync>,
}

/// 一次 `inject()` 回调捕获的不可变依赖集合。
#[derive(Clone)]
pub struct Services {
    pub(crate) values: HashMap<ServiceId, ErasedService>,
}

impl Services {
    pub fn get<T: Send + Sync + 'static>(&self, key: ServiceKey<T>) -> Result<Arc<T>, CoreError> {
        let Some(service) = self.values.get(&key.id()) else {
            return Err(CoreError::ServiceUnavailable { service: key.id() });
        };
        if service.type_id != key.type_id() {
            return Err(CoreError::ServiceTypeMismatch { service: key.id() });
        }
        Arc::downcast::<T>(service.value.clone())
            .map_err(|_| CoreError::ServiceTypeMismatch { service: key.id() })
    }
}
