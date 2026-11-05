//! 共享契约：ServiceKey / PluginDefinition。消费方只依赖这些类型与公开 API。

use cordis_core::{PluginDefinition, ServiceKey};
use std::sync::LazyLock;

use crate::db::Db;
use crate::http_client::HttpCaller;

pub static DB: ServiceKey<Db> = ServiceKey::new("demo.stack.db@1");
pub static HTTP: ServiceKey<HttpCaller> = ServiceKey::new("demo.stack.http@1");

pub static DEFINITION_DB: LazyLock<PluginDefinition> =
    LazyLock::new(|| PluginDefinition::named("demo.stack.db"));
pub static DEFINITION_LOG_STORE: LazyLock<PluginDefinition> =
    LazyLock::new(|| PluginDefinition::named("demo.stack.log-store"));
pub static DEFINITION_HTTP: LazyLock<PluginDefinition> =
    LazyLock::new(|| PluginDefinition::named("demo.stack.http"));
pub static DEFINITION_SIDE: LazyLock<PluginDefinition> =
    LazyLock::new(|| PluginDefinition::named("demo.stack.side"));
