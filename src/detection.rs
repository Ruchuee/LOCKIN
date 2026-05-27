use crate::{
    app::{AppState, DetectionMode, StateResponse, UiPhase, push_event, snapshot},
    riot::{self, LiveContext, PregameApiError, PregameMatch},
};
use anyhow::{Context, Result, anyhow};
use futures_util::{SinkExt, StreamExt};
use tokio::{
    sync::mpsc,
    time::{Duration, Instant, sleep},
};
use tokio_tungstenite::{tungstenite::client::IntoClientRequest, tungstenite::protocol::Message};
use tracing::error;

const HYBRID_FALLBACK_MS: u64 = 1000;
const LOCK_RETRY_MS: u64 = 250;
const LOCK_SEQUENCE_TIMEOUT_MS: u64 = 8000;

pub async fn arm(app: AppState) -> Result<StateResponse> {
    let selected_agent_uuid = {
        let mut inner = app.inner.lock().await;
        if inner.state.armed {
            return Ok(snapshot(&inner));
        }

        inner.state.armed = false;
        inner.state.rearm_pending = false;
        inner.state.phase = UiPhase::Arming;
        inner.state.message = "Refreshing live Riot and agent data.".to_string();
        inner.state.lock_attempts = 0;
        inner.state.last_match_id = None;
        inner.state.last_lock_agent_uuid = None;
        inner.state.glz_route = None;
        push_event(&mut inner, "info", "Arming: refreshing live data.");
        inner.config.selected_agent_uuid.clone()
    };
    app.notify();

    let Some(selected_agent_uuid) = selected_agent_uuid else {
        let response = {
            let mut inner = app.inner.lock().await;
            inner.state.armed = false;
            inner.state.rearm_pending = false;
            inner.state.phase = UiPhase::Idle;
            inner.state.message = "Select an agent before arming.".to_string();
            snapshot(&inner)
        };
        app.notify();
        return Ok(response);
    };

    let agent_is_known = {
        let inner = app.inner.lock().await;
        inner.agents.is_empty()
            || inner
                .agents
                .iter()
                .any(|agent| agent.uuid == selected_agent_uuid)
    };
    if !agent_is_known {
        let message = "selected agent is not present in fetched playable agent data";
        rollback_arm_failure(&app, message).await;
        return Err(anyhow!(message));
    }

    let (select_before_lock, detection_mode, poll_ms) = {
        let inner = app.inner.lock().await;
        (
            inner.config.select_before_lock,
            inner.config.detection_mode,
            inner.config.poll_ms,
        )
    };
    let mut live = match riot::build_live_context().await {
        Ok(live) => live,
        Err(err) => {
            rollback_arm_failure(&app, &err.to_string()).await;
            return Err(err);
        }
    };
    live.select_before_lock = select_before_lock;

    {
        let mut inner = app.inner.lock().await;
        inner.state.armed = true;
        inner.state.phase = UiPhase::Detecting;
        inner.state.message = "Armed. Waiting for pre-game lobby.".to_string();
        inner.state.glz_route = Some(live.glz_base.clone());
        push_event(
            &mut inner,
            "info",
            &format!("Using GLZ route: {}", live.glz_base),
        );
        push_event(
            &mut inner,
            "info",
            &format!("Armed. Detection mode: {detection_mode:?}."),
        );
    }
    app.notify();

    let poller_app = app.clone();
    let handle = tokio::spawn(async move {
        if let Err(err) = detect_lobbies(
            poller_app.clone(),
            live,
            selected_agent_uuid,
            poll_ms,
            detection_mode,
        )
        .await
        {
            error!(error = ?err, "poller failed");
            let mut inner = poller_app.inner.lock().await;
            inner.state.armed = false;
            inner.state.rearm_pending = false;
            inner.state.phase = UiPhase::Error;
            inner.state.message = err.to_string();
            inner.poller = None;
            push_event(&mut inner, "error", &err.to_string());
            poller_app.notify();
        }
    });

    let response = {
        let mut inner = app.inner.lock().await;
        inner.poller = Some(handle);
        snapshot(&inner)
    };
    app.notify();
    Ok(response)
}

async fn rollback_arm_failure(app: &AppState, message: &str) {
    let mut inner = app.inner.lock().await;
    inner.state.armed = false;
    inner.state.rearm_pending = false;
    inner.state.phase = UiPhase::Idle;
    inner.state.message = format!("Arm failed: {message}");
    inner.state.glz_route = None;
    inner.state.last_match_id = None;
    inner.state.last_lock_agent_uuid = None;
    push_event(&mut inner, "error", &format!("Arm failed: {message}"));
    app.notify();
}

async fn detect_lobbies(
    app: AppState,
    mut live: LiveContext,
    mut selected_agent_uuid: String,
    mut poll_ms: u64,
    mut detection_mode: DetectionMode,
) -> Result<()> {
    loop {
        match detection_mode {
            DetectionMode::Websocket => {
                websocket_lobbies(app.clone(), live.clone(), selected_agent_uuid.clone()).await?
            }
            DetectionMode::Polling => {
                polling_lobbies(
                    app.clone(),
                    live.clone(),
                    selected_agent_uuid.clone(),
                    poll_ms,
                )
                .await?
            }
            DetectionMode::Hybrid => {
                hybrid_lobbies(app.clone(), live.clone(), selected_agent_uuid.clone()).await?
            }
        }

        if !is_rearm_pending(&app).await {
            return Ok(());
        }

        wait_for_menu_ws(&live).await?;
        let Some(next) = prepare_auto_rearm(&app, &mut live).await else {
            return Ok(());
        };
        selected_agent_uuid = next.selected_agent_uuid;
        poll_ms = next.poll_ms;
        detection_mode = next.detection_mode;
    }
}

async fn websocket_lobbies(
    app: AppState,
    live: LiveContext,
    selected_agent_uuid: String,
) -> Result<()> {
    if let Some(candidate) = detect_current_pregame_candidate_or_warn(&app, &live).await?
        && process_pregame_candidate(&app, &live, &selected_agent_uuid, candidate).await?
    {
        return Ok(());
    }
    if !is_armed(&app).await {
        return Ok(());
    }

    let (pregame_tx, mut pregame_rx) = mpsc::channel::<()>(8);
    let ws_live = live.clone();
    let ws_handle = tokio::spawn(async move { listen_for_pregame_ws(ws_live, pregame_tx).await });
    tokio::pin!(ws_handle);

    loop {
        tokio::select! {
            maybe_event = pregame_rx.recv() => {
                if maybe_event.is_none() {
                    return Err(anyhow!("Riot Client websocket event channel closed"));
                }
                if process_pregame_candidate(&app, &live, &selected_agent_uuid, PregameCandidate::from_signal()).await? {
                    return Ok(());
                }
            }
            ws_result = &mut ws_handle => {
                return ws_result.context("websocket task panicked")?;
            }
        }
    }
}

async fn polling_lobbies(
    app: AppState,
    live: LiveContext,
    selected_agent_uuid: String,
    poll_ms: u64,
) -> Result<()> {
    loop {
        sleep(Duration::from_millis(poll_ms)).await;
        if let Some(candidate) = detect_current_pregame_candidate_or_warn(&app, &live).await?
            && process_pregame_candidate(&app, &live, &selected_agent_uuid, candidate).await?
        {
            return Ok(());
        }

        if !is_armed(&app).await {
            return Ok(());
        }
    }
}

async fn hybrid_lobbies(
    app: AppState,
    live: LiveContext,
    selected_agent_uuid: String,
) -> Result<()> {
    let (pregame_tx, mut pregame_rx) = mpsc::channel::<()>(8);
    let ws_live = live.clone();
    let ws_app = app.clone();
    tokio::spawn(async move {
        if let Err(err) = listen_for_pregame_ws(ws_live, pregame_tx).await {
            let mut inner = ws_app.inner.lock().await;
            push_event(
                &mut inner,
                "warn",
                &format!("WS state detection unavailable: {err}"),
            );
            ws_app.notify();
        }
    });

    loop {
        let candidate = tokio::select! {
            maybe_event = pregame_rx.recv() => {
                if maybe_event.is_none() {
                    continue;
                }
                PregameCandidate::from_signal()
            }
            _ = sleep(Duration::from_millis(HYBRID_FALLBACK_MS)) => {
                let Some(candidate) = detect_current_pregame_candidate_or_warn(&app, &live).await? else {
                    continue;
                };
                candidate
            }
        };

        if process_pregame_candidate(&app, &live, &selected_agent_uuid, candidate).await? {
            return Ok(());
        }

        if !is_armed(&app).await {
            return Ok(());
        }
    }
}

async fn detect_current_pregame_candidate(live: &LiveContext) -> Result<Option<PregameCandidate>> {
    Ok(riot::get_pregame_player(live)
        .await?
        .map(PregameCandidate::with_match_id))
}

async fn detect_current_pregame_candidate_or_warn(
    app: &AppState,
    live: &LiveContext,
) -> Result<Option<PregameCandidate>> {
    match detect_current_pregame_candidate(live).await {
        Ok(candidate @ Some(_)) => Ok(candidate),
        Ok(None) => {
            let mut inner = app.inner.lock().await;
            if inner.state.armed && inner.state.phase == UiPhase::Warning {
                inner.state.phase = UiPhase::Detecting;
                inner.state.message = "Armed. Waiting for pre-game lobby.".to_string();
                app.notify();
            }
            Ok(None)
        }
        Err(err) => {
            let mut inner = app.inner.lock().await;
            inner.state.phase = UiPhase::Warning;
            inner.state.message = "Pregame check failed; will retry.".to_string();
            push_event(&mut inner, "warn", &format!("Pregame check failed: {err}"));
            app.notify();
            Ok(None)
        }
    }
}

async fn process_pregame_candidate(
    app: &AppState,
    live: &LiveContext,
    selected_agent_uuid: &str,
    candidate: PregameCandidate,
) -> Result<bool> {
    match run_lock_sequence(app, live, selected_agent_uuid, candidate).await? {
        LockSequenceResult::Finished => Ok(true),
        LockSequenceResult::Expired => handle_lock_sequence_expired(app, live).await,
    }
}

async fn handle_lock_sequence_expired(app: &AppState, live: &LiveContext) -> Result<bool> {
    {
        let mut inner = app.inner.lock().await;
        push_event(
            &mut inner,
            "warn",
            "Pregame candidate expired before match data or lock was ready.",
        );
    }
    app.notify();

    if riot::get_local_player_session_loop_state(live)
        .await?
        .is_some_and(|state| state.eq_ignore_ascii_case("MENUS"))
    {
        let mut inner = app.inner.lock().await;
        if inner.state.armed {
            inner.state.phase = UiPhase::Detecting;
            inner.state.message = "Returned to menu. Waiting for pre-game lobby.".to_string();
            inner.state.last_match_id = None;
            inner.state.last_lock_agent_uuid = None;
        }
        app.notify();
        return Ok(false);
    }

    {
        let mut inner = app.inner.lock().await;
        inner.state.armed = false;
        inner.state.rearm_pending = true;
        inner.state.phase = UiPhase::WaitingMenus;
        inner.state.message =
            "Pregame candidate expired. Waiting for menu before retrying.".to_string();
        push_event(
            &mut inner,
            "info",
            "Waiting for menu before retrying detection.",
        );
    }
    app.notify();
    Ok(true)
}

async fn run_lock_sequence(
    app: &AppState,
    live: &LiveContext,
    selected_agent_uuid: &str,
    mut candidate: PregameCandidate,
) -> Result<LockSequenceResult> {
    if !is_armed(app).await {
        return Ok(LockSequenceResult::Finished);
    }

    {
        let mut inner = app.inner.lock().await;
        inner.state.phase = UiPhase::ResolvingMatch;
        inner.state.message = "Pre-game detected. Resolving match data.".to_string();
        inner.state.lock_attempts += 1;
        inner.state.last_match_id = candidate.initial_match_id.clone();
        inner.state.last_lock_agent_uuid = None;
        push_event(
            &mut inner,
            "info",
            "Pre-game detected. Starting lock sequence.",
        );
    }
    app.notify();

    let deadline = Instant::now() + Duration::from_millis(LOCK_SEQUENCE_TIMEOUT_MS);
    let mut use_initial_match_id = true;

    loop {
        if !is_armed(app).await {
            return Ok(LockSequenceResult::Finished);
        }

        let Some(match_id) = resolve_match_id(live, &mut candidate, use_initial_match_id).await?
        else {
            if !sleep_until_next_lock_retry(deadline).await {
                return Ok(LockSequenceResult::Expired);
            }
            use_initial_match_id = false;
            continue;
        };
        use_initial_match_id = false;

        {
            let mut inner = app.inner.lock().await;
            inner.state.phase = UiPhase::ResolvingMatch;
            inner.state.message = "Pre-game detected. Resolving match data.".to_string();
            inner.state.last_match_id = Some(match_id.clone());
        }
        app.notify();

        let match_state = match riot::get_pregame_match(live, &match_id).await {
            Ok(match_state) => match_state,
            Err(err) if is_transient_pregame_error(&err) => {
                if !sleep_until_next_lock_retry(deadline).await {
                    return Ok(LockSequenceResult::Expired);
                }
                continue;
            }
            Err(err) => return Err(err),
        };

        if !map_is_allowed(app, &match_state.map_id).await {
            skip_unselected_map(app, match_id, &match_state.map_id).await;
            return Ok(LockSequenceResult::Finished);
        }

        match locked_status_for_player(&match_state, &live.puuid, selected_agent_uuid) {
            PlayerLockStatus::LockedSelected => {
                finish_happy_path(
                    app,
                    UiPhase::Locked,
                    "Already locked selected agent for this game.",
                    "info",
                    "Already locked selected agent for this game.",
                    Some(match_id),
                    Some(selected_agent_uuid.to_string()),
                )
                .await;
                return Ok(LockSequenceResult::Finished);
            }
            PlayerLockStatus::LockedOther(agent_uuid) => {
                let message = format!("Already locked a different agent: {agent_uuid}.");
                finish_happy_path(
                    app,
                    UiPhase::LockedOtherAgent,
                    &message,
                    "warn",
                    "Already locked a different agent for this game.",
                    Some(match_id),
                    Some(agent_uuid),
                )
                .await;
                return Ok(LockSequenceResult::Finished);
            }
            PlayerLockStatus::NotLocked | PlayerLockStatus::PlayerMissing => {}
        }

        {
            let mut inner = app.inner.lock().await;
            inner.state.phase = UiPhase::Locking;
            inner.state.message = "Match data ready. Sending lock request.".to_string();
            inner.state.last_match_id = Some(match_id.clone());
            inner.state.last_lock_agent_uuid = Some(selected_agent_uuid.to_string());
            push_event(&mut inner, "info", "Attempting agent lock.");
        }
        app.notify();

        if live.select_before_lock {
            match riot::select_agent(live, &match_id, selected_agent_uuid).await {
                Ok(()) => {}
                Err(err) if is_transient_pregame_error(&err) => {
                    if !sleep_until_next_lock_retry(deadline).await {
                        return Ok(LockSequenceResult::Expired);
                    }
                    continue;
                }
                Err(err) => return Err(err),
            }
        }

        match riot::lock_agent(live, &match_id, selected_agent_uuid).await {
            Ok(()) => {
                finish_happy_path(
                    app,
                    UiPhase::Locked,
                    "Agent lock request succeeded.",
                    "info",
                    "Agent lock request succeeded.",
                    Some(match_id),
                    Some(selected_agent_uuid.to_string()),
                )
                .await;
                return Ok(LockSequenceResult::Finished);
            }
            Err(err) if is_transient_pregame_error(&err) => {
                if !sleep_until_next_lock_retry(deadline).await {
                    return Ok(LockSequenceResult::Expired);
                }
            }
            Err(err) => return Err(err),
        }
    }
}

#[derive(Clone, Debug)]
struct PregameCandidate {
    initial_match_id: Option<String>,
}

impl PregameCandidate {
    fn from_signal() -> Self {
        Self {
            initial_match_id: None,
        }
    }

    fn with_match_id(match_id: String) -> Self {
        Self {
            initial_match_id: Some(match_id),
        }
    }
}

enum LockSequenceResult {
    Finished,
    Expired,
}

async fn resolve_match_id(
    live: &LiveContext,
    candidate: &mut PregameCandidate,
    use_initial_match_id: bool,
) -> Result<Option<String>> {
    if use_initial_match_id && let Some(match_id) = candidate.initial_match_id.take() {
        return Ok(Some(match_id));
    }

    riot::get_pregame_player(live).await
}

fn is_transient_pregame_error(err: &anyhow::Error) -> bool {
    err.downcast_ref::<PregameApiError>()
        .is_some_and(PregameApiError::is_transient)
}

async fn sleep_until_next_lock_retry(deadline: Instant) -> bool {
    let now = Instant::now();
    if now >= deadline {
        return false;
    }

    let retry_delay = Duration::from_millis(LOCK_RETRY_MS);
    let remaining = deadline.saturating_duration_since(now);
    sleep(retry_delay.min(remaining)).await;
    Instant::now() < deadline
}

async fn map_is_allowed(app: &AppState, map_id: &str) -> bool {
    let inner = app.inner.lock().await;
    inner.config.selected_map_urls.is_empty()
        || inner
            .config
            .selected_map_urls
            .iter()
            .any(|selected| selected.eq_ignore_ascii_case(map_id))
}

async fn skip_unselected_map(app: &AppState, match_id: String, map_id: &str) {
    let map_name = {
        let inner = app.inner.lock().await;
        map_display_name(&inner.maps, map_id)
    };
    finish_happy_path(
        app,
        UiPhase::SkippedMap,
        &format!("Skipped {map_name} because it is not selected."),
        "warn",
        &format!("Skipped {map_name}; map is not selected."),
        Some(match_id),
        None,
    )
    .await;
}

async fn finish_happy_path(
    app: &AppState,
    phase: UiPhase,
    message: &str,
    event_level: &str,
    event_message: &str,
    match_id: Option<String>,
    lock_agent_uuid: Option<String>,
) {
    {
        let mut inner = app.inner.lock().await;
        let should_auto_rearm = inner.config.auto_rearm;
        inner.state.armed = false;
        inner.state.rearm_pending = should_auto_rearm;
        inner.state.phase = if should_auto_rearm {
            UiPhase::WaitingMenus
        } else {
            phase
        };
        inner.state.message = if should_auto_rearm {
            format!("{message} Waiting for menu to auto re-arm.")
        } else {
            message.to_string()
        };
        inner.state.last_match_id = match_id;
        inner.state.last_lock_agent_uuid = lock_agent_uuid;
        if !should_auto_rearm {
            inner.poller = None;
        }
        push_event(&mut inner, event_level, event_message);
        if should_auto_rearm {
            push_event(
                &mut inner,
                "info",
                "Auto re-arm pending until Valorant returns to menu.",
            );
        }
    }
    app.notify();
}

async fn wait_for_menu_ws(live: &LiveContext) -> Result<()> {
    if riot::get_local_player_session_loop_state(live)
        .await?
        .is_some_and(|state| state.eq_ignore_ascii_case("MENUS"))
    {
        return Ok(());
    }

    let events = riot::get_local_event_names(live).await?;
    if events.is_empty() {
        return Err(anyhow!("local Riot Client returned no websocket events"));
    }

    let ws_url = live.local_base.replacen("https://", "wss://", 1);
    let mut request = ws_url
        .into_client_request()
        .context("failed to build local websocket request")?;
    request
        .headers_mut()
        .insert("Authorization", live.local_auth.parse()?);

    let (mut ws, _) = riot::connect_local_ws(request).await?;

    for event in events {
        ws.send(Message::Text(format!(r#"[5,"{event}"]"#).into()))
            .await
            .with_context(|| format!("failed to subscribe to websocket event {event}"))?;
    }

    while let Some(message) = ws.next().await {
        let message = message.context("failed to read Riot Client websocket message")?;
        let text = match message {
            Message::Text(text) => text.to_string(),
            Message::Binary(bytes) => String::from_utf8_lossy(&bytes).to_string(),
            Message::Close(_) => return Err(anyhow!("Riot Client websocket closed")),
            _ => continue,
        };

        if riot::websocket_message_session_loop_state_for_player(&text, &live.puuid)
            .is_some_and(|state| state.eq_ignore_ascii_case("MENUS"))
        {
            return Ok(());
        }
    }

    Err(anyhow!("Riot Client websocket ended"))
}

fn map_display_name(maps: &[crate::app::Map], map_id: &str) -> String {
    maps.iter()
        .find(|map| map.map_url.eq_ignore_ascii_case(map_id))
        .map(|map| map.display_name.clone())
        .unwrap_or_else(|| map_id.rsplit('/').next().unwrap_or(map_id).to_string())
}

async fn is_armed(app: &AppState) -> bool {
    app.inner.lock().await.state.armed
}

async fn is_rearm_pending(app: &AppState) -> bool {
    app.inner.lock().await.state.rearm_pending
}

struct AutoRearmConfig {
    selected_agent_uuid: String,
    poll_ms: u64,
    detection_mode: DetectionMode,
}

async fn prepare_auto_rearm(app: &AppState, live: &mut LiveContext) -> Option<AutoRearmConfig> {
    let next = {
        let mut inner = app.inner.lock().await;
        if !inner.state.rearm_pending || inner.state.phase == UiPhase::Idle {
            inner.poller = None;
            return None;
        }
        let Some(selected_agent_uuid) = inner.config.selected_agent_uuid.clone() else {
            inner.state.armed = false;
            inner.state.rearm_pending = false;
            inner.state.phase = UiPhase::Idle;
            inner.state.message = "Select an agent before arming.".to_string();
            inner.poller = None;
            return None;
        };

        inner.state.armed = true;
        inner.state.rearm_pending = false;
        inner.state.phase = UiPhase::Detecting;
        inner.state.message = "Armed. Waiting for pre-game lobby.".to_string();
        inner.state.lock_attempts = 0;
        inner.state.last_match_id = None;
        inner.state.last_lock_agent_uuid = None;
        inner.state.glz_route = Some(live.glz_base.clone());
        push_event(&mut inner, "info", "Returned to menu. Auto re-armed.");

        live.select_before_lock = inner.config.select_before_lock;
        AutoRearmConfig {
            selected_agent_uuid,
            poll_ms: inner.config.poll_ms,
            detection_mode: inner.config.detection_mode,
        }
    };
    app.notify();
    Some(next)
}

async fn listen_for_pregame_ws(live: LiveContext, pregame_tx: mpsc::Sender<()>) -> Result<()> {
    let events = riot::get_local_event_names(&live).await?;
    if events.is_empty() {
        return Err(anyhow!("local Riot Client returned no websocket events"));
    }

    let ws_url = live.local_base.replacen("https://", "wss://", 1);
    let mut request = ws_url
        .into_client_request()
        .context("failed to build local websocket request")?;
    request
        .headers_mut()
        .insert("Authorization", live.local_auth.parse()?);

    let (mut ws, _) = riot::connect_local_ws(request).await?;

    for event in events {
        ws.send(Message::Text(format!(r#"[5,"{event}"]"#).into()))
            .await
            .with_context(|| format!("failed to subscribe to websocket event {event}"))?;
    }

    while let Some(message) = ws.next().await {
        let message = message.context("failed to read Riot Client websocket message")?;
        let text = match message {
            Message::Text(text) => text.to_string(),
            Message::Binary(bytes) => String::from_utf8_lossy(&bytes).to_string(),
            Message::Close(_) => return Err(anyhow!("Riot Client websocket closed")),
            _ => continue,
        };

        if riot::websocket_message_is_pregame_for_player(&text, &live.puuid) {
            let _ = pregame_tx.send(()).await;
        }
    }

    Err(anyhow!("Riot Client websocket ended"))
}

#[derive(Debug, Eq, PartialEq)]
enum PlayerLockStatus {
    LockedSelected,
    LockedOther(String),
    NotLocked,
    PlayerMissing,
}

fn locked_status_for_player(
    pregame_match: &PregameMatch,
    puuid: &str,
    selected_agent_uuid: &str,
) -> PlayerLockStatus {
    let Some(player) = pregame_match
        .ally_team
        .players
        .iter()
        .find(|player| player.subject.eq_ignore_ascii_case(puuid))
    else {
        return PlayerLockStatus::PlayerMissing;
    };

    if !player
        .character_selection_state
        .eq_ignore_ascii_case("locked")
    {
        return PlayerLockStatus::NotLocked;
    }

    if player
        .character_id
        .eq_ignore_ascii_case(selected_agent_uuid)
    {
        PlayerLockStatus::LockedSelected
    } else {
        PlayerLockStatus::LockedOther(player.character_id.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::riot::{AllyTeam, PregamePlayer};
    use reqwest::{Client, StatusCode};

    fn pregame_match(subject: &str, character_id: &str, state: &str) -> PregameMatch {
        PregameMatch {
            ally_team: AllyTeam {
                players: vec![PregamePlayer {
                    subject: subject.to_string(),
                    character_id: character_id.to_string(),
                    character_selection_state: state.to_string(),
                }],
            },
            map_id: "/Game/Maps/Ascent/Ascent".to_string(),
        }
    }

    fn live_context() -> LiveContext {
        LiveContext {
            local_base: "https://127.0.0.1:1".to_string(),
            local_auth: "Basic token".to_string(),
            glz_base: "https://glz-na-1.na.a.pvp.net".to_string(),
            puuid: "me".to_string(),
            access_token: "access".to_string(),
            entitlement_token: "entitlement".to_string(),
            client_version: "version".to_string(),
            select_before_lock: false,
            riot_client: Client::new(),
        }
    }

    #[test]
    fn detects_already_locked_selected_agent() {
        let state =
            locked_status_for_player(&pregame_match("me", "agent", "locked"), "me", "agent");

        assert_eq!(state, PlayerLockStatus::LockedSelected);
    }

    #[test]
    fn detects_already_locked_other_agent() {
        let state =
            locked_status_for_player(&pregame_match("me", "other", "locked"), "me", "agent");

        assert_eq!(state, PlayerLockStatus::LockedOther("other".to_string()));
    }

    #[test]
    fn selected_agent_is_not_treated_as_locked() {
        let state =
            locked_status_for_player(&pregame_match("me", "agent", "selected"), "me", "agent");

        assert_eq!(state, PlayerLockStatus::NotLocked);
    }

    #[test]
    fn missing_current_player_is_reported() {
        let state = locked_status_for_player(
            &pregame_match("someone-else", "agent", "locked"),
            "me",
            "agent",
        );

        assert_eq!(state, PlayerLockStatus::PlayerMissing);
    }

    #[tokio::test]
    async fn initial_candidate_match_id_is_used_once() {
        let live = live_context();
        let mut candidate = PregameCandidate::with_match_id("stale-or-current".to_string());

        let match_id = resolve_match_id(&live, &mut candidate, true).await.unwrap();

        assert_eq!(match_id.as_deref(), Some("stale-or-current"));
        assert!(candidate.initial_match_id.is_none());
    }

    #[test]
    fn only_not_found_pregame_errors_are_transient() {
        let not_found = anyhow!(PregameApiError::MatchRequest(StatusCode::NOT_FOUND));
        let forbidden = anyhow!(PregameApiError::MatchRequest(StatusCode::FORBIDDEN));

        assert!(is_transient_pregame_error(&not_found));
        assert!(!is_transient_pregame_error(&forbidden));
    }

    #[test]
    fn resolves_known_map_display_name() {
        let maps = vec![crate::app::Map {
            uuid: "uuid".to_string(),
            display_name: "Ascent".to_string(),
            display_icon: None,
            list_view_icon: None,
            splash: None,
            map_url: "/Game/Maps/Ascent/Ascent".to_string(),
            in_ranked_pool: true,
        }];

        assert_eq!(
            map_display_name(&maps, "/game/maps/ascent/ascent"),
            "Ascent"
        );
    }

    #[test]
    fn falls_back_to_map_id_tail_for_unknown_map() {
        assert_eq!(map_display_name(&[], "/Game/Maps/Foo/Foo"), "Foo");
    }
}
