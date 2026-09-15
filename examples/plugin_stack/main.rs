//! DB → Logger → HTTP 插件栈网页 Demo。
//!
//! ```bash
//! cargo run -p cordis-core --example plugin_stack
//! # 打开 http://127.0.0.1:3002
//! ```

mod db;
mod http_client;
mod keys;
mod logger;

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{
        Html, Sse,
        sse::{Event, KeepAlive},
    },
    routing::{get, post},
};
use cordis_core::{Fiber, FiberStateSnapshot, Plugin, PluginKey, Runtime};
use db::{DbPlugin, LogTx};
use futures_util::stream::{Stream, unfold};
use http_client::HttpPlugin;
use keys::{DB, HTTP, KEY_DB, KEY_HTTP, KEY_LOGGER};
use logger::LoggerPlugin;
use serde::{Deserialize, Serialize};
use std::{convert::Infallible, sync::Arc};
use tokio::sync::{Mutex as AsyncMutex, broadcast};

struct AppState {
    runtime: Runtime,
    db: AsyncMutex<Option<Fiber>>,
    logger: AsyncMutex<Option<Fiber>>,
    http: AsyncMutex<Option<Fiber>>,
    bus: LogTx,
}

#[derive(Deserialize)]
struct PluginBody {
    plugin: String,
}

#[derive(Deserialize)]
struct RequestBody {
    method: String,
    url: String,
    #[serde(default)]
    body: String,
}

#[derive(Serialize)]
struct ErrJson {
    error: String,
}

#[derive(Serialize)]
struct StateJson {
    plugins: PluginsJson,
    db_logs: Vec<String>,
    registry: Vec<GroupJson>,
}

#[derive(Serialize)]
struct PluginsJson {
    db: String,
    logger: String,
    http: String,
}

#[derive(Serialize)]
struct GroupJson {
    key: String,
    fibers: Vec<FiberJson>,
}

#[derive(Serialize)]
struct FiberJson {
    id: u64,
    state: String,
}

#[derive(Serialize)]
struct RequestJson {
    status: u16,
    body: String,
}

fn snapshot_name(state: FiberStateSnapshot) -> &'static str {
    match state {
        FiberStateSnapshot::Pending => "Pending",
        FiberStateSnapshot::Loading => "Loading",
        FiberStateSnapshot::Active => "Active",
        FiberStateSnapshot::Failed => "Failed",
        FiberStateSnapshot::Unloading => "Unloading",
        FiberStateSnapshot::Disposed => "Disposed",
    }
}

fn plugin_state(runtime: &Runtime, key: &PluginKey) -> String {
    runtime
        .diagnostics()
        .plugin_registry
        .iter()
        .find(|g| g.plugin_key == key.as_str())
        .and_then(|g| g.fibers.first())
        .map(|f| snapshot_name(f.state).to_string())
        .unwrap_or_else(|| "off".into())
}

fn build_state(app: &AppState) -> StateJson {
    let db_logs = app
        .runtime
        .root()
        .get(DB)
        .map(|db| db.recent(80))
        .unwrap_or_default();
    let registry = app
        .runtime
        .diagnostics()
        .plugin_registry
        .iter()
        .map(|g| GroupJson {
            key: g.plugin_key.to_string(),
            fibers: g
                .fibers
                .iter()
                .map(|f| FiberJson {
                    id: f.id,
                    state: snapshot_name(f.state).into(),
                })
                .collect(),
        })
        .collect();
    StateJson {
        plugins: PluginsJson {
            db: plugin_state(&app.runtime, &KEY_DB),
            logger: plugin_state(&app.runtime, &KEY_LOGGER),
            http: plugin_state(&app.runtime, &KEY_HTTP),
        },
        db_logs,
        registry,
    }
}

fn err(status: StatusCode, msg: impl Into<String>) -> (StatusCode, Json<ErrJson>) {
    (status, Json(ErrJson { error: msg.into() }))
}

async fn index() -> Html<&'static str> {
    Html(include_str!("index.html"))
}

async fn api_state(State(app): State<Arc<AppState>>) -> Json<StateJson> {
    Json(build_state(&app))
}

async fn api_logs(
    State(app): State<Arc<AppState>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = app.bus.subscribe();
    let stream = unfold(rx, |mut rx| async move {
        match rx.recv().await {
            Ok(msg) => Some((Ok(Event::default().data(msg)), rx)),
            Err(broadcast::error::RecvError::Closed) => None,
            Err(broadcast::error::RecvError::Lagged(_)) => Some((
                Ok(Event::default().data("(bus lagged; skipped some lines)")),
                rx,
            )),
        }
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}

async fn api_mount(
    State(app): State<Arc<AppState>>,
    Json(body): Json<PluginBody>,
) -> Result<Json<StateJson>, (StatusCode, Json<ErrJson>)> {
    let root = app.runtime.root();
    match body.plugin.as_str() {
        "db" => {
            let mut slot = app.db.lock().await;
            if slot.is_some() {
                return Err(err(StatusCode::BAD_REQUEST, "db already mounted"));
            }
            let fiber = root
                .plugin(Arc::new(DbPlugin {
                    bus: app.bus.clone(),
                }) as Arc<dyn Plugin>)
                .await
                .map_err(|e| err(StatusCode::BAD_REQUEST, e.to_string()))?;
            *slot = Some(fiber);
        }
        "logger" => {
            let mut slot = app.logger.lock().await;
            if slot.is_some() {
                return Err(err(StatusCode::BAD_REQUEST, "logger already mounted"));
            }
            let fiber = root
                .plugin(Arc::new(LoggerPlugin {
                    bus: app.bus.clone(),
                }) as Arc<dyn Plugin>)
                .await
                .map_err(|e| err(StatusCode::BAD_REQUEST, e.to_string()))?;
            *slot = Some(fiber);
        }
        "http" => {
            let mut slot = app.http.lock().await;
            if slot.is_some() {
                return Err(err(StatusCode::BAD_REQUEST, "http already mounted"));
            }
            let fiber = root
                .plugin(Arc::new(HttpPlugin {
                    bus: app.bus.clone(),
                }) as Arc<dyn Plugin>)
                .await
                .map_err(|e| err(StatusCode::BAD_REQUEST, e.to_string()))?;
            *slot = Some(fiber);
        }
        other => {
            return Err(err(
                StatusCode::BAD_REQUEST,
                format!("unknown plugin: {other}"),
            ));
        }
    }
    app.runtime.settle().await;
    let _ = app.bus.send(format!("sys: mounted {}", body.plugin));
    Ok(Json(build_state(&app)))
}

async fn api_unmount(
    State(app): State<Arc<AppState>>,
    Json(body): Json<PluginBody>,
) -> Result<Json<StateJson>, (StatusCode, Json<ErrJson>)> {
    let fiber = match body.plugin.as_str() {
        "db" => app.db.lock().await.take(),
        "logger" => app.logger.lock().await.take(),
        "http" => app.http.lock().await.take(),
        other => {
            return Err(err(
                StatusCode::BAD_REQUEST,
                format!("unknown plugin: {other}"),
            ));
        }
    };
    let Some(mut fiber) = fiber else {
        return Err(err(
            StatusCode::BAD_REQUEST,
            format!("{} not mounted", body.plugin),
        ));
    };
    fiber
        .dispose_wait()
        .await
        .map_err(|e| err(StatusCode::BAD_REQUEST, e.to_string()))?;
    app.runtime.settle().await;
    let _ = app.bus.send(format!("sys: unmounted {}", body.plugin));
    Ok(Json(build_state(&app)))
}

async fn api_request(
    State(app): State<Arc<AppState>>,
    Json(body): Json<RequestBody>,
) -> Result<Json<RequestJson>, (StatusCode, Json<ErrJson>)> {
    let http = app.runtime.root().get(HTTP).map_err(|_| {
        err(
            StatusCode::BAD_REQUEST,
            "HTTP 插件未 Active（请先挂载 DB → Logger → HTTP）",
        )
    })?;
    let body_opt = if body.body.trim().is_empty() {
        None
    } else {
        Some(body.body.as_str())
    };
    let result = http
        .request(&body.method, &body.url, body_opt)
        .await
        .map_err(|e| err(StatusCode::BAD_GATEWAY, e))?;
    // 给异步落库一点时间
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    Ok(Json(RequestJson {
        status: result.status,
        body: result.body,
    }))
}

async fn api_clear_db(
    State(app): State<Arc<AppState>>,
    Json(_body): Json<serde_json::Value>,
) -> Result<Json<StateJson>, (StatusCode, Json<ErrJson>)> {
    match app.runtime.root().get(DB) {
        Ok(db) => {
            db.clear();
            let _ = app.bus.send("sys: db logs cleared".into());
            Ok(Json(build_state(&app)))
        }
        Err(_) => Err(err(StatusCode::BAD_REQUEST, "DB 未挂载")),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (bus, _) = broadcast::channel(256);
    let runtime = Runtime::new()?;
    let app = Arc::new(AppState {
        runtime,
        db: AsyncMutex::new(None),
        logger: AsyncMutex::new(None),
        http: AsyncMutex::new(None),
        bus: bus.clone(),
    });

    let router = Router::new()
        .route("/", get(index))
        .route("/api/state", get(api_state))
        .route("/api/logs", get(api_logs))
        .route("/api/mount", post(api_mount))
        .route("/api/unmount", post(api_unmount))
        .route("/api/request", post(api_request))
        .route("/api/clear-db", post(api_clear_db))
        .with_state(app);

    let addr = "127.0.0.1:3002";
    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!("Plugin stack demo → http://{addr}");
    let _ = bus.send(format!("sys: listening on http://{addr}"));
    axum::serve(listener, router).await?;
    Ok(())
}
