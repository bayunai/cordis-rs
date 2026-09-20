//! [`IsolationLabel`] 与 Runtime 绑定的隔离令牌。
//!
//! 标签不可跨 Runtime 伪造；用于服务解析的隔离边界，不含业务租户语义。

use crate::CoreError;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

/// Runtime 身份令牌；[`IsolationLabel`] 绑定其上，不可跨 Runtime 伪造使用。
#[derive(Clone)]
pub(crate) struct RuntimeToken {
    inner: Arc<RuntimeTokenInner>,
}

struct RuntimeTokenInner {
    next_label: AtomicU64,
}

impl RuntimeToken {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(RuntimeTokenInner {
                next_label: AtomicU64::new(1),
            }),
        }
    }

    pub(crate) fn allocate_label(&self) -> IsolationLabel {
        IsolationLabel {
            token: self.inner.clone(),
            id: self.inner.next_label.fetch_add(1, Ordering::Relaxed),
        }
    }
}

/// Runtime 所属的不可伪造隔离标签。
#[derive(Clone)]
pub struct IsolationLabel {
    token: Arc<RuntimeTokenInner>,
    id: u64,
}

impl IsolationLabel {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub(crate) fn ensure_runtime(&self, token: &RuntimeToken) -> Result<(), CoreError> {
        if Arc::ptr_eq(&self.token, &token.inner) {
            Ok(())
        } else {
            Err(CoreError::IsolationRuntimeMismatch)
        }
    }
}

impl PartialEq for IsolationLabel {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && Arc::ptr_eq(&self.token, &other.token)
    }
}

impl Eq for IsolationLabel {}

impl std::hash::Hash for IsolationLabel {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        // token identity is checked separately; hash by id only within same runtime maps
    }
}

impl std::fmt::Debug for IsolationLabel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IsolationLabel")
            .field("id", &self.id)
            .finish()
    }
}
