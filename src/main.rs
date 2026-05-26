mod agents;
mod app;
mod dashboard;
mod detection;
mod maps;
mod riot;

use anyhow::{Context, Result, anyhow};
use app::{AppState, Config};
use std::{
    env,
    net::SocketAddr,
    path::{Path, PathBuf},
};
use tokio::net::TcpListener;
use tracing::{info, warn};
use tracing_subscriber::{EnvFilter, fmt};

const DEFAULT_DASHBOARD_PORT: u16 = 8787;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let base_dir = base_dir()?;
    load_dotenv(&base_dir);

    let config_path = base_dir.join("lockin-config.json");
    let config = Config::load(&config_path).await;
    let app_state = AppState::new(config_path, config);
    tokio::spawn(agents::load_public_agents(app_state.clone()));
    tokio::spawn(maps::load_public_maps(app_state.clone()));

    let app = dashboard::router(app_state);
    let (listener, addr) = bind_dashboard().await?;
    info!("dashboard listening on http://{addr}");
    println!("LOCKIN dashboard: http://{addr}");

    axum::serve(listener, app).await.context("server failed")
}

async fn bind_dashboard() -> Result<(TcpListener, SocketAddr)> {
    for port in DEFAULT_DASHBOARD_PORT..DEFAULT_DASHBOARD_PORT + 100 {
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        match TcpListener::bind(addr).await {
            Ok(listener) => return Ok((listener, addr)),
            Err(err) => warn!(port, error = ?err, "dashboard port unavailable"),
        }
    }
    Err(anyhow!("no available localhost dashboard port found"))
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("lockin=info,tower_http=warn,axum=warn"));

    fmt().with_env_filter(filter).with_target(false).init();
}

fn base_dir() -> Result<PathBuf> {
    if let Ok(cargo_dir) = env::var("CARGO_MANIFEST_DIR") {
        return Ok(PathBuf::from(cargo_dir));
    }

    let exe = env::current_exe().context("failed to locate current executable")?;
    exe.parent()
        .map(Path::to_path_buf)
        .context("executable has no parent directory")
}

fn load_dotenv(exe_dir: &Path) {
    let env_path = exe_dir.join(".env");
    if env_path.exists()
        && let Err(err) = dotenvy::from_path(&env_path)
    {
        warn!(error = ?err, path = %env_path.display(), "failed to load .env");
    }
}
