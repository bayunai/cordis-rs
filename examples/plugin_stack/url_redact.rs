//! HTTP 示例日志用的 URL 脱敏（去掉 query / fragment）。

/// 去掉 query / fragment，避免 token 等敏感参数进入全局日志。
pub fn redact_url(url: &str) -> String {
    match reqwest::Url::parse(url) {
        Ok(parsed) => {
            let host = parsed.host_str().unwrap_or("?");
            let mut out = format!("{}://{}", parsed.scheme(), host);
            if let Some(port) = parsed.port() {
                out.push(':');
                out.push_str(&port.to_string());
            }
            let path = if parsed.path().is_empty() {
                "/"
            } else {
                parsed.path()
            };
            out.push_str(path);
            if parsed.query().is_some() || parsed.fragment().is_some() {
                out.push_str("?[redacted]");
            }
            out
        }
        Err(_) => "(invalid-url)".into(),
    }
}
