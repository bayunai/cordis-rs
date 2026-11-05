//! 共享契约：ServiceKey / PluginDefinition。消费方只依赖这些类型与公开 API。

use cordis_core::ServiceKey;

use crate::db::Db;
use crate::http_client::HttpCaller;

pub static DB: ServiceKey<Db> = ServiceKey::new("demo.stack.db@1");
pub static HTTP: ServiceKey<HttpCaller> = ServiceKey::new("demo.stack.http@1");

cordis_core::plugin_definition!(pub static DEFINITION_DB = "demo.stack.db";);
cordis_core::plugin_definition!(pub static DEFINITION_LOG_STORE = "demo.stack.log-store";);
cordis_core::plugin_definition!(pub static DEFINITION_HTTP = "demo.stack.http";);
cordis_core::plugin_definition!(pub static DEFINITION_SIDE = "demo.stack.side";);
