//! HTTP 插件：对外提供自由请求方法；请求生命周期写入 Runtime Logger。
//!
//! 日志只记录方法、脱敏 URL（去掉 query/fragment）与状态/长度，**不**写入完整 URL
//! 或响应正文（正文仅经 HTTP API 返回页面）。

use async_trait::async_trait;
use cordis_core::{Context, CoreError, Logger, Plugin, PluginKey};
use reqwest::Client;
use std::time::Duration;

use crate::keys::{HTTP, KEY_HTTP};
use crate::url_redact::redact_url;

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

/// 对外契约：自由发 HTTP 请求；每次调用都会打（脱敏）日志。
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
        let safe_url = redact_url(url);
        let method = match method.parse::<reqwest::Method>() {
            Ok(method) => method,
            Err(e) => {
                let msg = format!("bad method: {e}");
                self.logger.error(format!("http ✗ {msg} → {safe_url}"));
                return Err(msg);
            }
        };
        self.logger.info(format!("http → {method} {safe_url}"));

        let mut builder = self.client.request(method.clone(), url);
        if let Some(body) = body.filter(|s| !s.is_empty()) {
            builder = builder
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body.to_string());
        }
        let response = match builder.send().await {
            Ok(response) => response,
            Err(e) => {
                let msg = format!("request failed: {e}");
                self.logger
                    .error(format!("http ✗ {msg} → {method} {safe_url}"));
                return Err(msg);
            }
        };
        let status = response.status().as_u16();
        let text = match response.text().await {
            Ok(text) => text,
            Err(e) => {
                let msg = format!("read body failed: {e}");
                self.logger
                    .error(format!("http ✗ {msg} ← {status} ({method} {safe_url})"));
                return Err(msg);
            }
        };
        self.logger.info(format!(
            "http ← {status} ({} chars) ← {method} {safe_url}",
            text.len()
        ));
        Ok(HttpResponse { status, body: text })
    }
}

pub struct HttpPlugin {
    pub label: String,
}

#[async_trait]
impl Plugin for HttpPlugin {
    fn key(&self) -> PluginKey {
        KEY_HTTP
    }

    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        let logger = ctx.logger()?;
        let client = Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .map_err(|e| CoreError::PluginApply(e.to_string()))?;
        ctx.provide(
            HTTP,
            HttpCaller {
                logger: logger.clone(),
                client,
            },
        )?;
        logger.info(format!("sys: http apply (label={})", self.label));
        let logger_d = logger.clone();
        ctx.effect()?.on_dispose(move || {
            logger_d.info("sys: http disposed");
        });
        Ok(())
    }
}
