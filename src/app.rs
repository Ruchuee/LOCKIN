use crate::detection;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    fs,
    sync::{Mutex, watch},
    task::JoinHandle,
};
use tracing::warn;

pub const DEFAULT_POLL_MS: u64 = 250;

#[derive(Clone)]
pub struct AppState {
    pub(crate) inner: Arc<Mutex<InnerState>>,
    updates: watch::Sender<u64>,
}

impl AppState {
    pub fn new(config_path: PathBuf, config: Config) -> Self {
        let state = UiState {
            armed: false,
            rearm_pending: false,
            phase: UiPhase::Idle,
            message: "Idle. Press Arm to refresh live data.".to_string(),
            selected_agent_uuid: config.selected_agent_uuid.clone(),
            selected_map_urls: config.selected_map_urls.clone(),
            select_before_lock: config.select_before_lock,
            auto_rearm: config.auto_rearm,
            poll_ms: config.poll_ms,
            detection_mode: config.detection_mode,
            glz_route: None,
            lock_attempts: 0,
            last_match_id: None,
            last_lock_agent_uuid: None,
        };
        let (updates, _) = watch::channel(0);

        Self {
            inner: Arc::new(Mutex::new(InnerState {
                config_path,
                config,
                state,
                agents: Vec::new(),
                maps: Vec::new(),
                events: Vec::new(),
                poller: None,
            })),
            updates,
        }
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.updates.subscribe()
    }

    pub async fn snapshot(&self) -> StateResponse {
        let inner = self.inner.lock().await;
        snapshot(&inner)
    }

    pub fn notify(&self) {
        let next = self.updates.borrow().wrapping_add(1);
        let _ = self.updates.send(next);
    }
}

pub(crate) struct InnerState {
    pub config_path: PathBuf,
    pub config: Config,
    pub state: UiState,
    pub agents: Vec<Agent>,
    pub maps: Vec<Map>,
    pub events: Vec<AppEvent>,
    pub poller: Option<JoinHandle<()>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub selected_agent_uuid: Option<String>,
    pub selected_map_urls: Vec<String>,
    pub select_before_lock: bool,
    pub auto_rearm: bool,
    pub poll_ms: u64,
    pub detection_mode: DetectionMode,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            selected_agent_uuid: None,
            selected_map_urls: Vec::new(),
            select_before_lock: false,
            auto_rearm: true,
            poll_ms: DEFAULT_POLL_MS,
            detection_mode: DetectionMode::Hybrid,
        }
    }
}

impl Config {
    pub async fn load(path: &Path) -> Self {
        match fs::read_to_string(path).await {
            Ok(contents) => serde_json::from_str(&contents).unwrap_or_else(|err| {
                warn!(error = ?err, path = %path.display(), "failed to parse config; using defaults");
                Config::default()
            }),
            Err(_) => Config::default(),
        }
    }

    pub async fn save(path: &Path, config: &Config) -> Result<()> {
        let body = serde_json::to_string_pretty(config)?;
        fs::write(path, body)
            .await
            .with_context(|| format!("failed to write {}", path.display()))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DetectionMode {
    Websocket,
    Polling,
    Hybrid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UiPhase {
    Idle,
    Arming,
    Detecting,
    ResolvingMatch,
    Locking,
    Locked,
    LockedOtherAgent,
    SkippedMap,
    WaitingMenus,
    Warning,
    Error,
}

#[derive(Clone, Debug, Serialize)]
pub struct UiState {
    pub armed: bool,
    pub rearm_pending: bool,
    pub phase: UiPhase,
    pub message: String,
    pub selected_agent_uuid: Option<String>,
    pub selected_map_urls: Vec<String>,
    pub select_before_lock: bool,
    pub auto_rearm: bool,
    pub poll_ms: u64,
    pub detection_mode: DetectionMode,
    pub glz_route: Option<String>,
    pub lock_attempts: u64,
    pub last_match_id: Option<String>,
    pub last_lock_agent_uuid: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct AppEvent {
    pub ts: u64,
    pub level: String,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Agent {
    pub uuid: String,
    pub display_name: String,
    pub display_icon: Option<String>,
    pub role: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Map {
    pub uuid: String,
    pub display_name: String,
    pub display_icon: Option<String>,
    pub list_view_icon: Option<String>,
    pub splash: Option<String>,
    pub map_url: String,
    pub in_ranked_pool: bool,
}

#[derive(Debug, Deserialize)]
pub struct ConfigRequest {
    pub selected_agent_uuid: Option<String>,
    pub selected_map_urls: Option<Vec<String>>,
    pub select_before_lock: Option<bool>,
    pub auto_rearm: Option<bool>,
    pub detection_mode: Option<DetectionMode>,
}

#[derive(Serialize)]
pub struct StateResponse {
    pub state: UiState,
    pub agents: Vec<Agent>,
    pub maps: Vec<Map>,
    pub events: Vec<AppEvent>,
}

pub async fn apply_config(app: &AppState, req: ConfigRequest) -> Result<StateResponse> {
    let response = {
        let mut inner = app.inner.lock().await;
        if let Some(agent_uuid) = req.selected_agent_uuid {
            inner.config.selected_agent_uuid = if agent_uuid.trim().is_empty() {
                None
            } else {
                Some(agent_uuid)
            };
        }
        if let Some(map_urls) = req.selected_map_urls {
            inner.config.selected_map_urls = clean_map_urls(map_urls);
        }
        if let Some(select_before_lock) = req.select_before_lock {
            inner.config.select_before_lock = select_before_lock;
        }
        if let Some(auto_rearm) = req.auto_rearm {
            inner.config.auto_rearm = auto_rearm;
        }
        if let Some(detection_mode) = req.detection_mode {
            inner.config.detection_mode = detection_mode;
        }

        inner.state.selected_agent_uuid = inner.config.selected_agent_uuid.clone();
        inner.state.selected_map_urls = inner.config.selected_map_urls.clone();
        inner.state.select_before_lock = inner.config.select_before_lock;
        inner.state.auto_rearm = inner.config.auto_rearm;
        inner.state.detection_mode = inner.config.detection_mode;
        Config::save(&inner.config_path, &inner.config).await?;
        snapshot(&inner)
    };
    app.notify();
    Ok(response)
}

pub async fn arm(app: AppState) -> Result<StateResponse> {
    detection::arm(app).await
}

pub async fn disarm(app: &AppState) -> StateResponse {
    let response = {
        let mut inner = app.inner.lock().await;
        if let Some(handle) = inner.poller.take() {
            handle.abort();
        }
        inner.state.armed = false;
        inner.state.rearm_pending = false;
        inner.state.phase = UiPhase::Idle;
        inner.state.message = "Disarmed. Live sensitive data cleared.".to_string();
        inner.state.glz_route = None;
        inner.state.last_match_id = None;
        inner.state.last_lock_agent_uuid = None;
        push_event(&mut inner, "info", "Disarmed.");
        snapshot(&inner)
    };
    app.notify();
    response
}

pub(crate) fn snapshot(inner: &InnerState) -> StateResponse {
    StateResponse {
        state: inner.state.clone(),
        agents: inner.agents.clone(),
        maps: inner.maps.clone(),
        events: inner.events.clone(),
    }
}

fn clean_map_urls(map_urls: Vec<String>) -> Vec<String> {
    let mut cleaned = Vec::new();
    for map_url in map_urls {
        let map_url = map_url.trim();
        if !map_url.is_empty()
            && !cleaned
                .iter()
                .any(|existing: &String| existing.eq_ignore_ascii_case(map_url))
        {
            cleaned.push(map_url.to_string());
        }
    }
    cleaned
}

pub(crate) fn push_event(inner: &mut InnerState, level: &str, message: &str) {
    inner.events.push(AppEvent {
        ts: now_ts(),
        level: level.to_string(),
        message: redact(message),
    });
    if inner.events.len() > 80 {
        let overflow = inner.events.len() - 80;
        inner.events.drain(0..overflow);
    }
}

fn redact(input: &str) -> String {
    input
        .replace("Authorization", "[auth-header]")
        .replace("X-Riot-Entitlements-JWT", "[entitlement-header]")
}

fn now_ts() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_detection_mode_is_hybrid() {
        assert_eq!(Config::default().detection_mode, DetectionMode::Hybrid);
    }

    #[test]
    fn ui_phase_serializes_as_snake_case_string() {
        assert_eq!(
            serde_json::to_string(&UiPhase::ResolvingMatch).unwrap(),
            r#""resolving_match""#
        );
        assert_eq!(
            serde_json::to_string(&UiPhase::LockedOtherAgent).unwrap(),
            r#""locked_other_agent""#
        );
    }

    #[test]
    fn loads_old_config_without_detection_mode_as_hybrid() {
        let config: Config = serde_json::from_str(
            r#"{
                "selected_agent_uuid": "agent",
                "select_before_lock": true,
                "poll_ms": 250
            }"#,
        )
        .unwrap();

        assert_eq!(config.detection_mode, DetectionMode::Hybrid);
    }

    #[test]
    fn loads_old_config_without_selected_maps_as_empty_list() {
        let config: Config = serde_json::from_str(
            r#"{
                "selected_agent_uuid": "agent",
                "select_before_lock": true,
                "poll_ms": 250
            }"#,
        )
        .unwrap();

        assert!(config.selected_map_urls.is_empty());
    }

    #[test]
    fn loads_old_config_without_auto_rearm_as_enabled() {
        let config: Config = serde_json::from_str(
            r#"{
                "selected_agent_uuid": "agent",
                "select_before_lock": true,
                "poll_ms": 250
            }"#,
        )
        .unwrap();

        assert!(config.auto_rearm);
    }

    #[test]
    fn cleans_selected_map_urls() {
        let map_urls = clean_map_urls(vec![
            " /Game/Maps/Ascent/Ascent ".to_string(),
            "/game/maps/ascent/ascent".to_string(),
            "".to_string(),
            "/Game/Maps/Haven/Haven".to_string(),
        ]);

        assert_eq!(
            map_urls,
            vec![
                "/Game/Maps/Ascent/Ascent".to_string(),
                "/Game/Maps/Haven/Haven".to_string()
            ]
        );
    }
}
