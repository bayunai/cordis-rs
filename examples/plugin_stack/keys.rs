//! 共享契约：ServiceKey / PluginKey。消费方只依赖这些类型与公开 API。

use cordis_core::{PluginKey, ServiceKey};

use crate::db::Db;
use crate::http_client::HttpCaller;

pub static DB: ServiceKey<Db> = ServiceKey::new("demo.stack.db@1");
pub static HTTP: ServiceKey<HttpCaller> = ServiceKey::new("demo.stack.http@1");

pub static KEY_DB: PluginKey = PluginKey::new("demo.stack.db");
pub static KEY_LOG_STORE: PluginKey = PluginKey::new("demo.stack.log-store");
pub static KEY_HTTP: PluginKey = PluginKey::new("demo.stack.http");
pub static KEY_SIDE: PluginKey = PluginKey::new("demo.stack.side");
