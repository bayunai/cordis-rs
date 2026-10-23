//! 来源树释放绑定具体实例；取消等待或同路径重建不改变已认领的目标。
use crate::{LoaderError, SourceError, SourceErrorKind, plugin::LoaderInner, tree::RuntimeTree};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, atomic::Ordering},
};
use tokio::{sync::Notify, task::JoinHandle};

pub(crate) struct ReleaseCompletion {
    result: Mutex<Option<Result<(), LoaderError>>>,
    notify: Notify,
    retain: Mutex<Option<JoinHandle<()>>>,
}
impl ReleaseCompletion {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            result: Mutex::new(None),
            notify: Notify::new(),
            retain: Mutex::new(None),
        })
    }
    fn finish(&self, result: Result<(), LoaderError>) {
        *self.result.lock().expect("release result") = Some(result);
        self.notify.notify_waiters();
    }
    async fn wait(&self) -> Result<(), LoaderError> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(result) = self.result.lock().expect("release result").clone() {
                return result;
            }
            notified.await;
        }
    }
}

fn belongs_to(candidate: &Arc<RuntimeTree>, ancestor: &Arc<RuntimeTree>) -> bool {
    let mut cursor = candidate.parent_tree.upgrade();
    while let Some(parent) = cursor {
        if Arc::ptr_eq(&parent, ancestor) {
            return true;
        }
        cursor = parent.parent_tree.upgrade();
    }
    false
}

pub(crate) fn detach(
    loader: Arc<LoaderInner>,
    tree: Arc<RuntimeTree>,
) -> Pin<Box<dyn Future<Output = Result<(), LoaderError>> + Send>> {
    Box::pin(async move {
        let completion = {
            // 先认领原实例的结果，再验证注册身份；旧句柄只能读回旧结果。
            let registry = loader.subtrees.lock().expect("subtree registry");
            let mut slot = tree.release.lock().expect("release slot");
            if let Some(completion) = slot.as_ref() {
                completion.clone()
            } else {
                if !registry
                    .by_prefix
                    .get(&tree.prefix)
                    .is_some_and(|current| Arc::ptr_eq(current, &tree))
                {
                    return Err(SourceError::new(
                        SourceErrorKind::Unavailable,
                        "来源树实例已不在当前注册表中",
                    )
                    .into());
                }
                let children: Vec<_> = registry
                    .by_prefix
                    .values()
                    .filter(|candidate| {
                        candidate
                            .parent_tree
                            .upgrade()
                            .is_some_and(|parent| Arc::ptr_eq(&parent, &tree))
                    })
                    .cloned()
                    .collect();
                tree.closing.store(true, Ordering::Release);
                for candidate in registry
                    .by_prefix
                    .values()
                    .filter(|candidate| belongs_to(candidate, &tree))
                {
                    candidate.closing.store(true, Ordering::Release);
                }
                let completion = ReleaseCompletion::new();
                *slot = Some(completion.clone());
                let task_completion = completion.clone();
                let worker_loader = loader.clone();
                let worker_tree = tree.clone();
                let task = tokio::spawn(async move {
                    let result =
                        match tokio::spawn(run_release(worker_loader, worker_tree, children)).await
                        {
                            Ok(result) => result,
                            Err(_) => Err(LoaderError::ReconcileAborted {
                                reason: "来源树释放任务异常结束".into(),
                            }),
                        };
                    task_completion.finish(result);
                });
                *completion.retain.lock().expect("release retain") = Some(task);
                completion
            }
        };
        completion.wait().await
    })
}

async fn run_release(
    loader: Arc<LoaderInner>,
    tree: Arc<RuntimeTree>,
    children: Vec<Arc<RuntimeTree>>,
) -> Result<(), LoaderError> {
    // 先让父树已接受的操作结算，不能持父操作锁等待子 Fiber 的清理回调。
    {
        let _operation = tree.operations.lock().await;
    }
    let mut first_error = None;
    for child in children {
        if let Err(error) = loader.detach_subtree(&child).await {
            tree.lifecycle_logs.error(&error);
            first_error.get_or_insert(error);
        }
    }
    if let Err(error) = tree.dispose_entries().await {
        tree.lifecycle_logs.error(&error);
        first_error.get_or_insert(error);
    }
    loader.unregister_subtree(&tree);
    loader.publish_snapshot_changed();
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}
