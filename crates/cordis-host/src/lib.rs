//! 应用 Host 外壳：负责 Runtime 的创建、访问与关闭。
//!
//! LoaderPlugin 由应用入口显式挂载；此 crate 不假定某个插件一定存在。

use cordis_core::{Context, Runtime};
use cordis_loader::LoaderError;

/// 应用进程的薄 Host。
///
/// 它持有唯一 Runtime。应用入口负责按需挂载 LoaderPlugin 或其他根插件，运行自身事件循环，
/// 并在退出时调用 [`Self::shutdown`]。
#[derive(Clone)]
pub struct CordisHost {
    runtime: Runtime,
}

impl CordisHost {
    /// 创建应用唯一的 Runtime。
    pub fn new() -> Result<Self, LoaderError> {
        Ok(Self {
            runtime: Runtime::new().map_err(|source| LoaderError::Runtime { source })?,
        })
    }

    /// 返回完整根 Context；应用代码和受信任进程内插件拥有完整 Cordis 权限。
    pub fn root(&self) -> Context {
        self.runtime.root()
    }

    /// 返回当前应用唯一 Runtime。
    pub fn runtime(&self) -> &Runtime {
        &self.runtime
    }

    /// 关闭 Runtime 及其全部根插件。
    pub async fn shutdown(self) -> Result<(), LoaderError> {
        self.runtime
            .shutdown()
            .await
            .map_err(|source| LoaderError::Runtime { source })
    }
}
