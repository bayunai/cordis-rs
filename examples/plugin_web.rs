//! 网页版插件插拔 Demo（Axum + 内嵌 HTML）。
//!
//! ```bash
//! cargo run -p cordis-core --example plugin_web
//! # 打开 http://127.0.0.1:3001
//! ```

use async_trait::async_trait;
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
use cordis_core::{
    Context, CoreError, EventKey, Fiber, FiberStateSnapshot, Plugin, PluginKey, Runtime, ServiceKey,
};
use futures_util::stream::{Stream, unfold};
use serde::{Deserialize, Serialize};
use std::{
    convert::Infallible,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Mutex as AsyncMutex, broadcast};

// --- Cordis keys -------------------------------------------------------------

#[derive(Debug)]
struct Logger;

#[derive(Debug, Clone)]
struct Greeting(String);

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct Counter(u64);

static LOGGER: ServiceKey<Logger> = ServiceKey::new("demo.logger@1");
static GREETING: ServiceKey<Greeting> = ServiceKey::new("demo.greeting@1");
static COUNTER: ServiceKey<Counter> = ServiceKey::new("demo.counter@1");
static NOTE: EventKey<String> = EventKey::new("demo.note@1");

static KEY_LOGGER: PluginKey = PluginKey::new("demo.logger");
static KEY_GREETER: PluginKey = PluginKey::new("demo.greeter");
static KEY_COUNTER: PluginKey = PluginKey::new("demo.counter");

type LogTx = broadcast::Sender<String>;

fn log(tx: &LogTx, msg: impl Into<String>) {
    let _ = tx.send(msg.into());
}

// --- Plugins -----------------------------------------------------------------

struct LoggerPlugin {
    label: String,
    logs: LogTx,
}

#[async_trait]
impl Plugin for LoggerPlugin {
    fn key(&self) -> PluginKey {
        KEY_LOGGER
    }
    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        let label = self.label.clone();
        let logs = self.logs.clone();
        log(&logs, format!("[{label}] logger apply"));
        ctx.provide(LOGGER, Logger)?;
        let logs_on = logs.clone();
        let label_on = label.clone();
        ctx.on(NOTE, move |msg| {
            log(&logs_on, format!("[{label_on}] note: {msg}"));
            Ok(())
        })?;
        let logs_d = logs.clone();
        ctx.effect()?.on_dispose(move || {
            log(&logs_d, format!("[{label}] logger disposed"));
        });
        Ok(())
    }
}

struct GreeterPlugin {
    name: String,
    logs: LogTx,
}

#[async_trait]
impl Plugin for GreeterPlugin {
    fn key(&self) -> PluginKey {
        KEY_GREETER
    }
    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        let text = format!("hello from {}", self.name);
        log(&self.logs, format!("[greeter] apply → {text}"));
        ctx.provide(GREETING, Greeting(text))?;
        ctx.emit(NOTE, &format!("greeter online: {}", self.name))?;
        let name = self.name.clone();
        let logs = self.logs.clone();
        ctx.effect()?.on_dispose(move || {
            log(&logs, format!("[greeter:{name}] disposed"));
        });
        Ok(())
    }
}

struct CounterPlugin {
    ticks: Arc<AtomicU64>,
    logs: LogTx,
}

#[async_trait]
impl Plugin for CounterPlugin {
    fn key(&self) -> PluginKey {
        KEY_COUNTER
    }
    fn inject(&self) -> Vec<cordis_core::ServiceId> {
        vec![LOGGER.id(), GREETING.id()]
    }
    async fn apply(&self, ctx: &Context) -> Result<(), CoreError> {
        let _ = ctx.get(LOGGER)?;
        let greeting = ctx.get(GREETING)?;
        log(
            &self.logs,
            format!("[counter] apply (seen greeting: {})", greeting.0),
        );
        let ticks = self.ticks.clone();
        ctx.provide(COUNTER, Counter(ticks.load(Ordering::SeqCst)))?;
        let effect = ctx.effect()?;
        let ticks_bg = ticks.clone();
        let logs = self.logs.clone();
        effect.spawn(move |cancel| async move {
            let mut n = 0u64;
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        log(&logs, format!("[counter] tick loop cancelled at {n}"));
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_millis(700)) => {
                        n += 1;
                        ticks_bg.store(n, Ordering::SeqCst);
                        log(&logs, format!("[counter] tick {n}"));
                    }
                }
            }
        })?;
        let logs_d = self.logs.clone();
        effect.on_dispose(move || log(&logs_d, "[counter] disposed"));
        Ok(())
    }
}

// --- App state ---------------------------------------------------------------

struct AppState {
    runtime: Runtime,
    logger: AsyncMutex<Option<Fiber>>,
    greeter: AsyncMutex<Option<Fiber>>,
    counter: AsyncMutex<Option<Fiber>>,
    ticks: Arc<AtomicU64>,
    logger_gen: AtomicU32,
    greeter_seq: AtomicU32,
    greeter_name: Mutex<String>,
    logs: LogTx,
}

#[derive(Deserialize)]
struct PluginBody {
    plugin: String,
}

#[derive(Deserialize)]
struct EmitBody {
    text: String,
}

#[derive(Serialize)]
struct FiberJson {
    id: u64,
    state: String,
}

#[derive(Serialize)]
struct GroupJson {
    key: String,
    fibers: Vec<FiberJson>,
}

#[derive(Serialize)]
struct StateJson {
    ticks: u64,
    logger_gen: u32,
    greeter_name: String,
    plugins: PluginsJson,
    registry: Vec<GroupJson>,
}

#[derive(Serialize)]
struct PluginsJson {
    logger: String,
    greeter: String,
    counter: String,
}

#[derive(Serialize)]
struct ErrJson {
    error: String,
}

fn snapshot_name(s: FiberStateSnapshot) -> &'static str {
    match s {
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
        ticks: app.ticks.load(Ordering::SeqCst),
        logger_gen: app.logger_gen.load(Ordering::SeqCst),
        greeter_name: app.greeter_name.lock().expect("name").clone(),
        plugins: PluginsJson {
            logger: plugin_state(&app.runtime, &KEY_LOGGER),
            greeter: plugin_state(&app.runtime, &KEY_GREETER),
            counter: plugin_state(&app.runtime, &KEY_COUNTER),
        },
        registry,
    }
}

fn err(status: StatusCode, msg: impl Into<String>) -> (StatusCode, Json<ErrJson>) {
    (
        status,
        Json(ErrJson {
            error: msg.into(),
        }),
    )
}

async fn index() -> Html<&'static str> {
    Html(include_str!("plugin_web.html"))
}

async fn api_state(State(app): State<Arc<AppState>>) -> Json<StateJson> {
    Json(build_state(&app))
}

async fn api_logs(
    State(app): State<Arc<AppState>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = app.logs.subscribe();
    let stream = unfold(rx, |mut rx| async move {
        loop {
            match rx.recv().await {
                Ok(msg) => return Some((Ok(Event::default().data(msg)), rx)),
                Err(broadcast::error::RecvError::Closed) => return None,
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    return Some((
                        Ok(Event::default().data("(log lagged; skipped some lines)")),
                        rx,
                    ));
                }
            }
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
        "logger" => {
            let mut slot = app.logger.lock().await;
            if slot.is_some() {
                return Err(err(StatusCode::CONFLICT, "logger already mounted"));
            }
            let ver = app.logger_gen.fetch_add(1, Ordering::SeqCst) + 1;
            let fiber = root
                .plugin(Arc::new(LoggerPlugin {
                    label: format!("v{ver}"),
                    logs: app.logs.clone(),
                }))
                .await
                .map_err(|e| err(StatusCode::BAD_REQUEST, e.to_string()))?;
            *slot = Some(fiber);
        }
        "greeter" => {
            let mut slot = app.greeter.lock().await;
            if slot.is_some() {
                return Err(err(StatusCode::CONFLICT, "greeter already mounted"));
            }
            let seq = app.greeter_seq.fetch_add(1, Ordering::SeqCst) + 1;
            let name = if seq % 2 == 1 { "Alice" } else { "Bob" };
            *app.greeter_name.lock().expect("name") = name.into();
            let fiber = root
                .plugin(Arc::new(GreeterPlugin {
                    name: name.into(),
                    logs: app.logs.clone(),
                }))
                .await
                .map_err(|e| err(StatusCode::BAD_REQUEST, e.to_string()))?;
            *slot = Some(fiber);
        }
        "counter" => {
            let mut slot = app.counter.lock().await;
            if slot.is_some() {
                return Err(err(StatusCode::CONFLICT, "counter already mounted"));
            }
            let fiber = root
                .plugin(Arc::new(CounterPlugin {
                    ticks: app.ticks.clone(),
                    logs: app.logs.clone(),
                }))
                .await
                .map_err(|e| err(StatusCode::BAD_REQUEST, e.to_string()))?;
            *slot = Some(fiber);
        }
        other => return Err(err(StatusCode::BAD_REQUEST, format!("unknown plugin: {other}"))),
    }
    app.runtime.settle().await;
    log(&app.logs, format!("sys: mounted {}", body.plugin));
    Ok(Json(build_state(&app)))
}

async fn api_unmount(
    State(app): State<Arc<AppState>>,
    Json(body): Json<PluginBody>,
) -> Result<Json<StateJson>, (StatusCode, Json<ErrJson>)> {
    match body.plugin.as_str() {
        "logger" => {
            let mut slot = app.logger.lock().await;
            if let Some(mut fiber) = slot.take() {
                fiber
                    .dispose_wait()
                    .await
                    .map_err(|e| err(StatusCode::BAD_REQUEST, e.to_string()))?;
            } else {
                return Err(err(StatusCode::NOT_FOUND, "logger not mounted"));
            }
        }
        "greeter" => {
            let mut slot = app.greeter.lock().await;
            if let Some(mut fiber) = slot.take() {
                fiber
                    .dispose_wait()
                    .await
                    .map_err(|e| err(StatusCode::BAD_REQUEST, e.to_string()))?;
            } else {
                return Err(err(StatusCode::NOT_FOUND, "greeter not mounted"));
            }
        }
        "counter" => {
            let mut slot = app.counter.lock().await;
            if let Some(mut fiber) = slot.take() {
                fiber
                    .dispose_wait()
                    .await
                    .map_err(|e| err(StatusCode::BAD_REQUEST, e.to_string()))?;
            } else {
                // 也可能已被 unmount(key)
                let _ = app.runtime.unmount(KEY_COUNTER).await;
            }
        }
        other => return Err(err(StatusCode::BAD_REQUEST, format!("unknown plugin: {other}"))),
    }
    app.runtime.settle().await;
    log(&app.logs, format!("sys: unmounted {}", body.plugin));
    Ok(Json(build_state(&app)))
}

async fn api_replace(
    State(app): State<Arc<AppState>>,
    Json(body): Json<PluginBody>,
) -> Result<Json<StateJson>, (StatusCode, Json<ErrJson>)> {
    if body.plugin != "logger" {
        return Err(err(
            StatusCode::BAD_REQUEST,
            "only logger supports replace in this demo",
        ));
    }
    let mut slot = app.logger.lock().await;
    let Some(fiber) = slot.as_mut() else {
        return Err(err(StatusCode::NOT_FOUND, "logger not mounted"));
    };
    let ver = app.logger_gen.fetch_add(1, Ordering::SeqCst) + 1;
    fiber
        .replace(Arc::new(LoggerPlugin {
            label: format!("v{ver}"),
            logs: app.logs.clone(),
        }))
        .await
        .map_err(|e| err(StatusCode::BAD_REQUEST, e.to_string()))?;
    app.runtime.settle().await;
    log(&app.logs, format!("sys: logger replaced → v{ver}"));
    Ok(Json(build_state(&app)))
}

async fn api_emit(
    State(app): State<Arc<AppState>>,
    Json(body): Json<EmitBody>,
) -> Result<Json<StateJson>, (StatusCode, Json<ErrJson>)> {
    app.runtime
        .root()
        .emit(NOTE, &body.text)
        .map_err(|e| err(StatusCode::BAD_REQUEST, e.to_string()))?;
    Ok(Json(build_state(&app)))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (logs, _) = broadcast::channel(256);
    let runtime = Runtime::new()?;
    let app = Arc::new(AppState {
        runtime,
        logger: AsyncMutex::new(None),
        greeter: AsyncMutex::new(None),
        counter: AsyncMutex::new(None),
        ticks: Arc::new(AtomicU64::new(0)),
        logger_gen: AtomicU32::new(0),
        greeter_seq: AtomicU32::new(0),
        greeter_name: Mutex::new(String::new()),
        logs: logs.clone(),
    });

    let router = Router::new()
        .route("/", get(index))
        .route("/api/state", get(api_state))
        .route("/api/logs", get(api_logs))
        .route("/api/mount", post(api_mount))
        .route("/api/unmount", post(api_unmount))
        .route("/api/replace", post(api_replace))
        .route("/api/emit", post(api_emit))
        .with_state(app);

    let addr = "127.0.0.1:3001";
    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!("Cordis plugin web demo → http://{addr}");
    log(&logs, format!("sys: listening on http://{addr}"));
    axum::serve(listener, router).await?;
    Ok(())
}
