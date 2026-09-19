//! 应用 Host 外壳：负责应用进程对 Loader 的启动、访问与关闭。
//!
//! 插件 Factory Catalog、配置解析、reconcile 与管理服务由 [`cordis_loader`] 提供。
//! 此 crate 不重复实现插件管理逻辑；一个可执行程序通常只创建一个 [`CordisHost`]。

use cordis_core::{Context, Runtime};
use cordis_loader::{CordisLoader, ExtensionCatalog, Loader, LoaderError, LoaderSnapshot};
use std::path::Path;

/// 应用进程的薄 Host。
///
/// 它持有唯一的 [`CordisLoader`]。应用入口负责构造静态 Catalog、启动 Host、运行自身事件循环，
/// 并在退出时调用 [`Self::shutdown`]。
#[derive(Clone)]
pub struct CordisHost {
    loader: CordisLoader,
}

impl CordisHost {
    /// 创建没有配置源的应用 Host。
    pub fn new(catalog: ExtensionCatalog) -> Result<Self, LoaderError> {
        Ok(Self {
            loader: CordisLoader::new(catalog)?,
        })
    }

    /// 按 bootstrap 配置启动应用 Host 与其 Loader。
    pub async fn bootstrap(
        catalog: ExtensionCatalog,
        bootstrap_path: impl AsRef<Path>,
    ) -> Result<Self, LoaderError> {
        Ok(Self {
            loader: CordisLoader::bootstrap(catalog, bootstrap_path).await?,
        })
    }

    /// 返回完整根 Context；应用代码和受信任进程内插件拥有完整 Cordis 权限。
    pub fn root(&self) -> Context {
        self.loader.root()
    }

    /// 返回当前应用唯一 Runtime。
    pub fn runtime(&self) -> &Runtime {
        self.loader.runtime()
    }

    /// 返回插件管理控制面。
    pub fn loader(&self) -> Loader {
        self.loader.loader()
    }

    /// 返回当前由 Loader 收敛的实例快照。
    pub fn snapshot(&self) -> LoaderSnapshot {
        self.loader.snapshot()
    }

    /// 关闭 Loader 管理的插件和 Runtime。
    pub async fn shutdown(self) -> Result<(), LoaderError> {
        self.loader.shutdown().await
    }
}
