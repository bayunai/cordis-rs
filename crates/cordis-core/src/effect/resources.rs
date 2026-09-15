//! Effect 资源集合与受控工作记录。
//!
//! 保存子 Scope、同步/异步清理器与受管任务；处置协调逻辑在 `dispose`。

use std::{future::Future, pin::Pin};

use tokio::task::JoinHandle;

use crate::CoreError;

use super::EffectScope;

pub(super) type BoxFuture = Pin<Box<dyn Future<Output = Result<(), CoreError>> + Send>>;
pub(super) type AsyncDisposer = Box<dyn FnOnce() -> BoxFuture + Send>;

pub(super) enum ManagedWork {
    Task(JoinHandle<()>),
    /// 等待子 Scope `DisposeCompletion`，并把聚合错误带回父协调任务。
    ChildWait(JoinHandle<Result<(), Vec<String>>>),
}

#[derive(Default)]
pub(super) struct Resources {
    pub(super) children: Vec<EffectScope>,
    pub(super) cleanups: Vec<Box<dyn FnOnce() + Send>>,
    pub(super) async_disposers: Vec<AsyncDisposer>,
    pub(super) work: Vec<ManagedWork>,
}

impl ManagedWork {
    pub(super) fn abort(self) {
        match self {
            Self::Task(handle) => handle.abort(),
            Self::ChildWait(handle) => handle.abort(),
        }
    }
}

pub(super) fn abort_work(work: Vec<ManagedWork>) {
    for item in work {
        item.abort();
    }
}
