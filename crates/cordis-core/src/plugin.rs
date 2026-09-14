use crate::{Context, CoreError, effect::EffectScope};
use async_trait::async_trait;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// 可挂载到 Context 的极简插件。
///
/// 宿主负责构造插件对象；Core 只负责挂载与释放。
#[async_trait]
pub trait Plugin: Send + Sync {
    async fn apply(&self, ctx: &Context) -> Result<(), CoreError>;
}

/// 已挂载插件的释放句柄；重复释放幂等。
///
/// - [`dispose`](Self::dispose)：同步释放，任务上收到父 Scope（供最终 `shutdown` 等待）。
/// - [`dispose_wait`](Self::dispose_wait)：热卸载推荐；取消并等待本插件受控任务结束后再返回。
/// - `Drop` 走同步 [`dispose`](Self::dispose)。
pub struct PluginHandle {
    disposed: Arc<AtomicBool>,
    scope: Option<EffectScope>,
}

impl PluginHandle {
    pub(crate) fn from_scope(scope: EffectScope) -> Self {
        Self {
            disposed: Arc::new(AtomicBool::new(false)),
            scope: Some(scope),
        }
    }

    pub fn is_disposed(&self) -> bool {
        self.disposed.load(Ordering::Acquire)
    }

    /// 同步释放插件资源；任务上收到父 Scope。
    pub fn dispose(&mut self) {
        if self.disposed.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(scope) = self.scope.take() {
            scope.dispose();
        }
    }

    /// 释放并等待本插件受控任务退出（无超时）。
    ///
    /// 不停机热加载应在挂载替换实例前调用此方法。
    pub async fn dispose_wait(&mut self) {
        if self.disposed.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(scope) = self.scope.take() {
            scope.dispose_wait().await;
        }
    }
}

impl Drop for PluginHandle {
    fn drop(&mut self) {
        self.dispose();
    }
}
