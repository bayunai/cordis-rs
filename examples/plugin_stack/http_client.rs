//! HTTP 插件：对外提供自由请求方法，内部只依赖 Logger。

use async_trait::async_trait;
use cordis_core::{Context, CoreError, Plugin, PluginKey, ServiceId};
use reqwest::Client;
use std::time::Duration;

use crate::db::LogTx;
use crate::keys::{HTTP, KEY_HTTP, LOGGER};
use crate::logger::Logger;

fn bus(tx: &LogTx, msg: impl Into<String>) {
    let _ = tx.send(msg.into());
}

/// Loader 可替换配置；仅改 label 时应走 `Fiber::replace` 且保留 Fiber ID。
#[derive(Clone, Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct HttpConfig {
    #[serde(default = "default_http_label")]
    pub label: String,
}

fn default_http_label() -> String {
    "default".into()
}

#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status: u16,
    pub body: String,
}

/// 对外契约：自由发 HTTP 请求；每次调用都会打日志。
#[derive(Clone)]
pub struct HttpCaller {
    logger: Logger,
    client: Client,
}

impl HttpCaller {
    pub async fn request(
        &self,
        method: &str,
        url: &str,
        body: Option<&str>,
    ) -> Result<HttpResponse, String> {
        self.logger.info(format!("http → {method} {url}"));
        let method = method
            .parse::<reqwest::Method>()
            .map_err(|e| format!("bad method: {e}"))?;
        let mut builder = self.client.request(method, url);
        if let Some(body) = body.filter(|s| !s.is_empty()) {
            builder = builder
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body.to_string());
        }
        let response = match builder.send().await {
            Ok(response) => response,
            Err(e) => {
                let msg = format!("request failed: {e}");
                self.logger.info(format!("http ✗ {msg}"));
                return Err(msg);
            }
        };
        let status = response.status().as_u16();
        let text = response
            .text()
            .await
            .map_err(|e| format!("read body failed: {e}"))?;
        let preview: String = text.chars().take(240).collect();
        self.logger
            .info(format!("http ← {status} ({} chars)", text.len()));
        if !preview.is_empty() {
            self.logger.info(format!("http body preview: {preview}"));
        }
        Ok(HttpResponse { status, body: text })
    }
}

pub struct HttpPlugin {
    pub bus: LogTx,
    pub label: String,
}

#[async_trait]
impl Plugin for HttpPlugin {
    fn key(&self) -> PluginKey {
        KEY_HTTP
    }

    fn inject(&self) -> Vec<ServiceId> {
        vec![LOGGER.id()]
    }

    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        let logger = ctx.get(LOGGER)?.as_ref().clone();
        let client = Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .map_err(|e| CoreError::PluginApply(e.to_string()))?;
        ctx.provide(HTTP, HttpCaller { logger, client })?;
        bus(
            &self.bus,
            format!("sys: http apply (label={})", self.label),
        );
        let bus_d = self.bus.clone();
        ctx.effect()?.on_dispose(move || {
            bus(&bus_d, "sys: http disposed");
        });
        Ok(())
    }
}
