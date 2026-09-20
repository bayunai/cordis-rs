//! Loader EntryTree 子树差分 reconcile 网页 Demo。
//!
//! ```bash
//! cargo run -p cordis-core --example plugin_stack
//! # 打开 http://127.0.0.1:3002
//! ```
//!
//! 初始树：
//! - `side`：根级旁路，观察 `app` 子树变更时 Fiber ID 是否保持
//! - `app` Group：`db` → `logger` → `http`（同组共享服务域）

mod db;
mod http_client;
mod keys;
mod logger;
mod side;

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
use cordis_core::{FiberState, FiberStateSnapshot, Plugin, Runtime};
use cordis_loader::{
    EntryOptions, EntryUpdate, ExtensionCatalog, ExtensionFactory, ExtensionsConfig, LOADER,
    Loader, LoaderError, LoaderPlugin, LoaderSnapshot,
};
use db::{DbPlugin, LogTx};
use futures_util::stream::{Stream, unfold};
use http_client::{HttpConfig, HttpPlugin};
use keys::{DB, HTTP, KEY_DB, KEY_HTTP, KEY_LOGGER, KEY_SIDE};
use logger::LoggerPlugin;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use side::SidePlugin;
use std::{convert::Infallible, fs, path::PathBuf, sync::Arc};
use tokio::sync::broadcast;

#[derive(Clone, Deserialize, JsonSchema)]
struct Empty {}

struct AppState {
    runtime: Runtime,
    loader: Loader,
    _loader_fiber: cordis_core::Fiber,
    bus: LogTx,
    _config_dir: PathBuf,
}

#[derive(Deserialize)]
struct PathBody {
    path: String,
}

#[derive(Deserialize)]
struct DisableBody {
    path: String,
    disabled: bool,
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
struct EntryJson {
    path: String,
    name: String,
    group: bool,
    enabled: bool,
    fiber_id: Option<u64>,
    state: Option<String>,
    last_error: Option<String>,
}

#[derive(Serialize)]
struct StateJson {
    entries: Vec<EntryJson>,
    db_logs: Vec<String>,
    registry: Vec<GroupJson>,
    graph: GraphJson,
    note: String,
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
struct GraphJson {
    view: String,
    stack_depth: usize,
    fibers: Vec<GraphFiberJson>,
    providers: Vec<GraphProviderJson>,
    effects: Vec<GraphEffectJson>,
}

#[derive(Serialize)]
struct GraphFiberJson {
    id: u64,
    plugin: String,
    label: String,
    state: String,
    dependencies: Vec<String>,
    missing_dependencies: Vec<String>,
    root_effect: Option<u64>,
    provides: Vec<String>,
    last_error: Option<String>,
}

#[derive(Serialize)]
struct GraphProviderJson {
    provider_id: u64,
    service: String,
    effect_id: Option<u64>,
    context_depth: usize,
    owner_fiber: Option<u64>,
    owner_plugin: Option<String>,
}

#[derive(Serialize)]
struct GraphEffectJson {
    id: u64,
    name: String,
    parent: Option<u64>,
    fiber_id: Option<u64>,
    disposed: bool,
    cancelled: bool,
    task_count: usize,
    cleanup_count: usize,
    child_count: usize,
}

#[derive(Serialize)]
struct RequestJson {
    status: u16,
    body: String,
}

struct BusFactory<F> {
    id: &'static str,
    bus: LogTx,
    build: F,
}

impl<F> ExtensionFactory for BusFactory<F>
where
    F: Fn(LogTx) -> Arc<dyn Plugin> + Send + Sync + 'static,
{
    type Config = Empty;
    fn id(&self) -> &'static str {
        self.id
    }
    fn build(&self, _: Empty) -> Result<Arc<dyn Plugin>, LoaderError> {
        Ok((self.build)(self.bus.clone()))
    }
}

struct HttpFactory {
    bus: LogTx,
}

impl ExtensionFactory for HttpFactory {
    type Config = HttpConfig;
    fn id(&self) -> &'static str {
        "demo.stack.http"
    }
    fn build(&self, config: HttpConfig) -> Result<Arc<dyn Plugin>, LoaderError> {
        Ok(Arc::new(HttpPlugin {
            bus: self.bus.clone(),
            label: config.label,
        }))
    }
}

fn fiber_state_name(state: FiberState) -> &'static str {
    match state {
        FiberState::Pending => "Pending",
        FiberState::Loading => "Loading",
        FiberState::Active => "Active",
        FiberState::Failed => "Failed",
        FiberState::Unloading => "Unloading",
        FiberState::Disposed => "Disposed",
    }
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

fn plugin_label(key: &str) -> &'static str {
    if key == KEY_DB.as_str() {
        "DB"
    } else if key == KEY_LOGGER.as_str() {
        "Logger"
    } else if key == KEY_HTTP.as_str() {
        "HTTP"
    } else if key == KEY_SIDE.as_str() {
        "Side"
    } else if key == "cordis.loader.group" {
        "Group"
    } else if key.starts_with("cordis.loader") {
        "Loader"
    } else {
        "Plugin"
    }
}

fn build_graph(runtime: &Runtime) -> GraphJson {
    let snap = runtime.diagnostics();
    let stack_depth = snap
        .providers
        .iter()
        .map(|p| p.context_depth)
        .max()
        .or_else(|| snap.plugin_fibers.iter().map(|f| f.context_depth).max())
        .unwrap_or(1);

    let fibers: Vec<GraphFiberJson> = snap
        .plugin_fibers
        .iter()
        .map(|f| {
            let provides = snap
                .providers
                .iter()
                .filter(|p| {
                    p.effect_id
                        .zip(f.root_effect)
                        .is_some_and(|(pe, re)| pe == re)
                        || snap
                            .effects
                            .iter()
                            .any(|e| e.fiber_id == Some(f.id) && p.effect_id == Some(e.id))
                })
                .map(|p| p.service.to_string())
                .collect();
            GraphFiberJson {
                id: f.id,
                plugin: f.plugin_key.to_string(),
                label: plugin_label(f.plugin_key).into(),
                state: snapshot_name(f.state).into(),
                dependencies: f.dependencies.iter().map(|s| (*s).to_string()).collect(),
                missing_dependencies: f
                    .missing_dependencies
                    .iter()
                    .map(|s| (*s).to_string())
                    .collect(),
                root_effect: f.root_effect,
                provides,
                last_error: f.last_error.clone(),
            }
        })
        .collect();

    let providers: Vec<GraphProviderJson> = snap
        .providers
        .iter()
        .map(|p| {
            let owner_fiber = p.effect_id.and_then(|eid| {
                snap.effects
                    .iter()
                    .find(|e| e.id == eid)
                    .and_then(|e| e.fiber_id)
            });
            let owner_plugin = owner_fiber.and_then(|fid| {
                snap.plugin_fibers
                    .iter()
                    .find(|f| f.id == fid)
                    .map(|f| f.plugin_key.to_string())
            });
            GraphProviderJson {
                provider_id: p.provider_id,
                service: p.service.to_string(),
                effect_id: p.effect_id,
                context_depth: p.context_depth,
                owner_fiber,
                owner_plugin,
            }
        })
        .collect();

    let effects: Vec<GraphEffectJson> = snap
        .effects
        .iter()
        .map(|e| GraphEffectJson {
            id: e.id,
            name: e.name.clone(),
            parent: e.parent,
            fiber_id: e.fiber_id,
            disposed: e.disposed,
            cancelled: e.cancelled,
            task_count: e.task_count,
            cleanup_count: e.cleanup_count,
            child_count: e.child_count,
        })
        .collect();

    GraphJson {
        view: "Loader EntryTree · app Group 共享服务域 · side 为根级旁路".into(),
        stack_depth,
        fibers,
        providers,
        effects,
    }
}

async fn collect_state(app: &AppState) -> StateJson {
    let snap = app
        .loader
        .await_idle()
        .await
        .unwrap_or(LoaderSnapshot { entries: vec![] });
    let db_logs = app
        .loader
        .entry_context("app:db")
        .ok()
        .and_then(|ctx| ctx.get(DB).ok())
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
    let entries = snap
        .entries
        .iter()
        .map(|e| EntryJson {
            path: e.path.to_string(),
            name: e.name.clone(),
            group: e.group,
            enabled: e.enabled,
            fiber_id: e.fiber_id,
            state: e.state.map(fiber_state_name).map(str::to_string),
            last_error: e.last_error.clone(),
        })
        .collect();
    StateJson {
        entries,
        db_logs,
        registry,
        graph: build_graph(&app.runtime),
        note: "对比 side 与 app:* 的 fiber_id：子树操作不应改动无关 Entry。".into(),
    }
}

fn err(status: StatusCode, msg: impl Into<String>) -> (StatusCode, Json<ErrJson>) {
    (status, Json(ErrJson { error: msg.into() }))
}

fn map_loader(e: impl std::fmt::Display) -> (StatusCode, Json<ErrJson>) {
    err(StatusCode::BAD_REQUEST, e.to_string())
}

async fn index() -> Html<&'static str> {
    Html(include_str!("index.html"))
}

async fn api_state(State(app): State<Arc<AppState>>) -> Json<StateJson> {
    Json(collect_state(&app).await)
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

async fn api_disable(
    State(app): State<Arc<AppState>>,
    Json(body): Json<DisableBody>,
) -> Result<Json<StateJson>, (StatusCode, Json<ErrJson>)> {
    app.loader
        .update(
            &body.path,
            EntryUpdate {
                disabled: Some(body.disabled),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .map_err(map_loader)?;
    let _ = app
        .bus
        .send(format!("sys: {} disabled={}", body.path, body.disabled));
    Ok(Json(collect_state(&app).await))
}

async fn api_reorder(
    State(app): State<Arc<AppState>>,
) -> Result<Json<StateJson>, (StatusCode, Json<ErrJson>)> {
    let snap = app.loader.await_idle().await.map_err(map_loader)?;
    let children: Vec<_> = snap
        .entries
        .iter()
        .filter(|e| e.parent.as_ref().is_some_and(|p| p.as_str() == "app") && !e.group)
        .map(|e| e.path.as_str().to_string())
        .collect();
    let http_index = children
        .iter()
        .position(|p| p == "app:http")
        .ok_or_else(|| err(StatusCode::BAD_REQUEST, "app:http 不存在，请先重建"))?;
    // 在同父内把 http 挪到另一端，演示纯排序不改 Fiber ID。
    let target = if http_index == 0 {
        children.len().saturating_sub(1)
    } else {
        0
    };
    app.loader
        .update(
            "app:http",
            EntryUpdate::default(),
            Some(Some("app")),
            Some(target),
        )
        .await
        .map_err(map_loader)?;
    let _ = app.bus.send(format!(
        "sys: reorder app:http → position {target} (pure sort)"
    ));
    Ok(Json(collect_state(&app).await))
}

async fn api_replace_http_config(
    State(app): State<Arc<AppState>>,
) -> Result<Json<StateJson>, (StatusCode, Json<ErrJson>)> {
    let label = format!("v{}", stamp());
    app.loader
        .update(
            "app:http",
            EntryUpdate {
                config: Some(toml::Value::Table({
                    let mut table = toml::Table::new();
                    table.insert("label".into(), toml::Value::String(label.clone()));
                    table
                })),
                ..Default::default()
            },
            None,
            None,
        )
        .await
        .map_err(map_loader)?;
    let _ = app
        .bus
        .send(format!("sys: http config replace label={label}"));
    Ok(Json(collect_state(&app).await))
}

fn stamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() % 10_000)
        .unwrap_or(0)
}

async fn api_remove(
    State(app): State<Arc<AppState>>,
    Json(body): Json<PathBody>,
) -> Result<Json<StateJson>, (StatusCode, Json<ErrJson>)> {
    app.loader.remove(&body.path).await.map_err(map_loader)?;
    let _ = app.bus.send(format!("sys: removed {}", body.path));
    Ok(Json(collect_state(&app).await))
}

async fn api_create_http(
    State(app): State<Arc<AppState>>,
) -> Result<Json<StateJson>, (StatusCode, Json<ErrJson>)> {
    let entry = EntryOptions::new("http", "demo.stack.http").with_config({
        let mut table = toml::Table::new();
        table.insert("label".into(), toml::Value::String("recreated".into()));
        toml::Value::Table(table)
    });
    app.loader
        .create(entry, Some("app"), None)
        .await
        .map_err(map_loader)?;
    let _ = app.bus.send("sys: created app:http".into());
    Ok(Json(collect_state(&app).await))
}

async fn api_request(
    State(app): State<Arc<AppState>>,
    Json(body): Json<RequestBody>,
) -> Result<Json<RequestJson>, (StatusCode, Json<ErrJson>)> {
    let http = app
        .loader
        .entry_context("app:http")
        .map_err(|_| {
            err(
                StatusCode::BAD_REQUEST,
                "HTTP 未 Active（检查 app Group / db / logger 是否启用）",
            )
        })?
        .get(HTTP)
        .map_err(|_| {
            err(
                StatusCode::BAD_REQUEST,
                "HTTP 未 Active（检查 app Group / db / logger 是否启用）",
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
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    Ok(Json(RequestJson {
        status: result.status,
        body: result.body,
    }))
}

async fn api_clear_db(
    State(app): State<Arc<AppState>>,
) -> Result<Json<StateJson>, (StatusCode, Json<ErrJson>)> {
    let ctx = app
        .loader
        .entry_context("app:db")
        .map_err(|_| err(StatusCode::BAD_REQUEST, "DB 未挂载"))?;
    let db = ctx
        .get(DB)
        .map_err(|_| err(StatusCode::BAD_REQUEST, "DB 未挂载"))?;
    db.clear();
    let _ = app.bus.send("sys: db logs cleared".into());
    Ok(Json(collect_state(&app).await))
}

fn build_catalog(bus: LogTx) -> Result<ExtensionCatalog, LoaderError> {
    let mut catalog = ExtensionCatalog::new();
    catalog.register(BusFactory {
        id: "demo.stack.side",
        bus: bus.clone(),
        build: |bus| Arc::new(SidePlugin { bus }) as Arc<dyn Plugin>,
    })?;
    catalog.register(BusFactory {
        id: "demo.stack.db",
        bus: bus.clone(),
        build: |bus| Arc::new(DbPlugin { bus }) as Arc<dyn Plugin>,
    })?;
    catalog.register(BusFactory {
        id: "demo.stack.logger",
        bus: bus.clone(),
        build: |bus| Arc::new(LoggerPlugin { bus }) as Arc<dyn Plugin>,
    })?;
    catalog.register(HttpFactory { bus })?;
    Ok(catalog)
}

fn initial_extensions() -> Result<ExtensionsConfig, LoaderError> {
    let mut http_cfg = toml::Table::new();
    http_cfg.insert("label".into(), toml::Value::String("default".into()));
    let app = EntryOptions::group(
        "app",
        vec![
            EntryOptions::new("db", "demo.stack.db"),
            EntryOptions::new("logger", "demo.stack.logger"),
            EntryOptions::new("http", "demo.stack.http").with_config(toml::Value::Table(http_cfg)),
        ],
    )?;
    Ok(ExtensionsConfig {
        version: 2,
        extensions: vec![EntryOptions::new("side", "demo.stack.side"), app],
    })
}

fn write_bootstrap_dir() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let dir = std::env::temp_dir().join(format!("cordis-plugin-stack-{}", std::process::id()));
    fs::create_dir_all(&dir)?;
    fs::write(
        dir.join("bootstrap.toml"),
        "version = 2\n[config]\ndriver = \"file\"\npath = \"extensions.toml\"\n",
    )?;
    fs::write(
        dir.join("extensions.toml"),
        toml::to_string_pretty(&initial_extensions()?)?,
    )?;
    Ok(dir)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (bus, _) = broadcast::channel(256);
    let config_dir = write_bootstrap_dir()?;
    let bootstrap = config_dir.join("bootstrap.toml");

    let runtime = Runtime::new()?;
    let catalog = build_catalog(bus.clone())?;
    let plugin = LoaderPlugin::bootstrap(catalog, &bootstrap)?;
    let loader_fiber = runtime.root().plugin(Arc::new(plugin)).await?;
    let loader = (*runtime.root().get(LOADER)?).clone();
    let _ = loader.await_idle().await?;
    let _ = bus.send("sys: Loader EntryTree ready (side + app/{db,logger,http})".into());

    let app = Arc::new(AppState {
        runtime,
        loader,
        _loader_fiber: loader_fiber,
        bus: bus.clone(),
        _config_dir: config_dir,
    });

    let router = Router::new()
        .route("/", get(index))
        .route("/api/state", get(api_state))
        .route("/api/logs", get(api_logs))
        .route("/api/disable", post(api_disable))
        .route("/api/reorder", post(api_reorder))
        .route("/api/replace-http-config", post(api_replace_http_config))
        .route("/api/remove", post(api_remove))
        .route("/api/create-http", post(api_create_http))
        .route("/api/request", post(api_request))
        .route("/api/clear-db", post(api_clear_db))
        .with_state(app);

    let addr = "127.0.0.1:3002";
    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!("Plugin stack (Loader subtree reconcile) → http://{addr}");
    let _ = bus.send(format!("sys: listening on http://{addr}"));
    axum::serve(listener, router).await?;
    Ok(())
}
