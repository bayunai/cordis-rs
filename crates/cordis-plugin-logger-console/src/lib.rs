//! Host 显式挂载的控制台日志 exporter。
//!
//! 不进入 Loader builtin；仅依赖 `cordis-core`，通过 [`Context::register_log_exporter`]
//! 注册，卸载后自动停止输出。

mod exporter;

pub use exporter::{ColorMode, ConsoleExporter, ConsoleLoggerConfig, LabelAlign};

use async_trait::async_trait;
use cordis_core::{Context, CoreError, Plugin};
use std::sync::Arc;

/// 控制台日志插件实例；使用 [`PLUGIN_DEFINITION`] 挂载。
pub struct ConsoleLoggerPlugin {
    config: ConsoleLoggerConfig,
}

impl ConsoleLoggerPlugin {
    pub fn new(config: ConsoleLoggerConfig) -> Self {
        Self { config }
    }

    pub fn default_config() -> Self {
        Self::new(ConsoleLoggerConfig::default())
    }
}

impl Default for ConsoleLoggerPlugin {
    fn default() -> Self {
        Self::default_config()
    }
}

#[async_trait]
impl Plugin for ConsoleLoggerPlugin {
    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        let exporter = Arc::new(ConsoleExporter::new(self.config.clone()));
        ctx.register_log_exporter(exporter)?;
        Ok(())
    }
}

/// 此插件的共享定义；展示名称不参与身份比较。
pub static PLUGIN_DEFINITION: std::sync::LazyLock<cordis_core::PluginDefinition> =
    std::sync::LazyLock::new(|| cordis_core::PluginDefinition::named("cordis.logger-console"));
