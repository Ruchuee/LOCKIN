use anyhow::{Context, Result, anyhow};
use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD as BASE64, STANDARD_NO_PAD as BASE64_NO_PAD},
};
use reqwest::{Client, StatusCode};
use rustls::{
    ClientConfig, DigitallySignedStruct, Error as RustlsError, SignatureScheme,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    pki_types::{CertificateDer, ServerName, UnixTime},
};
use serde::Deserialize;
use serde_json::Value;
use std::{env, fmt, path::PathBuf, sync::Arc};
use tokio::fs;
use tokio_tungstenite::{Connector, connect_async_tls_with_config};

pub const CLIENT_PLATFORM: &str = "ew0KCSJwbGF0Zm9ybVR5cGUiOiAiUEMiLA0KCSJwbGF0Zm9ybU9TIjogIldpbmRvd3MiLA0KCSJwbGF0Zm9ybU9TVmVyc2lvbiI6ICIxMC4wLjE5MDQyLjEuMjU2LjY0Yml0IiwNCgkicGxhdGZvcm1DaGlwc2V0IjogIlVua25vd24iDQp9";

#[derive(Clone)]
pub struct LiveContext {
    pub local_base: String,
    pub local_auth: String,
    pub glz_base: String,
    pub puuid: String,
    pub access_token: String,
    pub entitlement_token: String,
    pub client_version: String,
    pub select_before_lock: bool,
    pub riot_client: Client,
}

pub async fn build_live_context() -> Result<LiveContext> {
    let lockfile = read_lockfile().await?;
    let riot_client = Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .context("failed to build Riot local HTTP client")?;
    let local_base = format!("https://127.0.0.1:{}", lockfile.port);
    let local_auth = format!(
        "Basic {}",
        BASE64.encode(format!("riot:{}", lockfile.password))
    );

    let entitlements: EntitlementsResponse = local_get(
        &riot_client,
        &local_base,
        &local_auth,
        "entitlements/v1/token",
    )
    .await?;
    let region: RegionResponse = local_get(
        &riot_client,
        &local_base,
        &local_auth,
        "riotclient/region-locale",
    )
    .await?;
    let sessions: Value = local_get(
        &riot_client,
        &local_base,
        &local_auth,
        "product-session/v1/external-sessions",
    )
    .await?;

    let valorant_session = find_valorant_session(&sessions)?;
    let client_version = valorant_session
        .get("version")
        .and_then(Value::as_str)
        .context("Valorant session did not include client version")?
        .to_string();
    let args = valorant_session
        .get("launchConfiguration")
        .and_then(|cfg| cfg.get("arguments"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let glz_base = match read_shooter_log()
        .await
        .ok()
        .and_then(|log| extract_glz_url_from_log(&log))
    {
        Some(url) => url,
        None => {
            let (glz_region, shard) = parse_region_shard(&args, &region.region)?;
            format!("https://glz-{glz_region}-1.{shard}.a.pvp.net")
        }
    };

    Ok(LiveContext {
        local_base,
        local_auth,
        glz_base,
        puuid: entitlements.subject,
        access_token: entitlements.access_token,
        entitlement_token: entitlements.token,
        client_version,
        select_before_lock: false,
        riot_client,
    })
}

pub async fn get_pregame_player(live: &LiveContext) -> Result<Option<String>> {
    let url = format!("{}/pregame/v1/players/{}", live.glz_base, live.puuid);
    let resp = riot_headers(live.riot_client.get(url), live).send().await?;
    if resp.status() == StatusCode::NOT_FOUND {
        return Ok(None);
    }
    if !resp.status().is_success() {
        return Err(anyhow!(PregameApiError::PlayerRequest(resp.status())));
    }
    let body: Value = resp.json().await?;
    Ok(body
        .get("MatchID")
        .and_then(Value::as_str)
        .map(ToString::to_string))
}

pub async fn get_pregame_match(live: &LiveContext, match_id: &str) -> Result<PregameMatch> {
    let url = format!("{}/pregame/v1/matches/{}", live.glz_base, match_id);
    let resp = riot_headers(live.riot_client.get(url), live).send().await?;
    if !resp.status().is_success() {
        return Err(anyhow!(PregameApiError::MatchRequest(resp.status())));
    }
    resp.json()
        .await
        .context("failed to parse pregame match response")
}

pub async fn get_local_event_names(live: &LiveContext) -> Result<Vec<String>> {
    let body: Value = local_get(
        &live.riot_client,
        &live.local_base,
        &live.local_auth,
        "help",
    )
    .await?;
    Ok(body
        .get("events")
        .and_then(Value::as_object)
        .map(|events| events.keys().cloned().collect())
        .unwrap_or_default())
}

pub async fn get_local_player_session_loop_state(live: &LiveContext) -> Result<Option<String>> {
    let body: Value = local_get(
        &live.riot_client,
        &live.local_base,
        &live.local_auth,
        "chat/v4/presences",
    )
    .await?;
    Ok(body
        .get("presences")
        .and_then(Value::as_array)
        .and_then(|presences| {
            presences
                .iter()
                .filter(|presence| {
                    presence
                        .get("product")
                        .and_then(Value::as_str)
                        .is_none_or(|product| product.eq_ignore_ascii_case("valorant"))
                })
                .find_map(|presence| {
                    let private = find_private_presence_for_puuid(presence, &live.puuid)?;
                    decode_presence_private(private)
                        .ok()
                        .and_then(|presence| serde_json::from_str::<Value>(&presence).ok())
                        .and_then(|presence| {
                            presence_session_loop_state(&presence).map(ToString::to_string)
                        })
                })
        }))
}

pub async fn select_agent(live: &LiveContext, match_id: &str, agent_uuid: &str) -> Result<()> {
    let url = format!(
        "{}/pregame/v1/matches/{}/select/{}",
        live.glz_base, match_id, agent_uuid
    );
    post_agent_action(live, url, "select").await
}

pub async fn lock_agent(live: &LiveContext, match_id: &str, agent_uuid: &str) -> Result<()> {
    let url = format!(
        "{}/pregame/v1/matches/{}/lock/{}",
        live.glz_base, match_id, agent_uuid
    );
    post_agent_action(live, url, "lock").await
}

pub fn local_ws_connector() -> Connector {
    let tls_config = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAnyServerCert))
        .with_no_client_auth();
    Connector::Rustls(Arc::new(tls_config))
}

pub async fn connect_local_ws(
    request: tokio_tungstenite::tungstenite::http::Request<()>,
) -> Result<(
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    tokio_tungstenite::tungstenite::handshake::client::Response,
)> {
    connect_async_tls_with_config(request, None, false, Some(local_ws_connector()))
        .await
        .context("failed to connect to Riot Client websocket")
}

pub fn websocket_message_is_pregame_for_player(message: &str, puuid: &str) -> bool {
    let Ok(value) = serde_json::from_str::<Value>(message) else {
        return false;
    };
    find_private_presence_for_puuid(&value, puuid)
        .and_then(|private| decode_presence_private(private).ok())
        .and_then(|presence| serde_json::from_str::<Value>(&presence).ok())
        .is_some_and(|presence| presence_has_pregame_state(&presence))
}

pub fn websocket_message_session_loop_state_for_player(
    message: &str,
    puuid: &str,
) -> Option<String> {
    let value = serde_json::from_str::<Value>(message).ok()?;
    find_private_presence_for_puuid(&value, puuid)
        .and_then(|private| decode_presence_private(private).ok())
        .and_then(|presence| serde_json::from_str::<Value>(&presence).ok())
        .and_then(|presence| presence_session_loop_state(&presence).map(ToString::to_string))
}

async fn post_agent_action(live: &LiveContext, url: String, action: &str) -> Result<()> {
    let resp = riot_headers(live.riot_client.post(url), live)
        .send()
        .await?;
    if !resp.status().is_success() {
        return Err(anyhow!(PregameApiError::AgentAction {
            action: action.to_string(),
            status: resp.status(),
        }));
    }
    Ok(())
}

#[derive(Debug)]
pub enum PregameApiError {
    PlayerRequest(StatusCode),
    MatchRequest(StatusCode),
    AgentAction { action: String, status: StatusCode },
}

impl PregameApiError {
    pub fn is_transient(&self) -> bool {
        match self {
            PregameApiError::PlayerRequest(status) | PregameApiError::MatchRequest(status) => {
                *status == StatusCode::NOT_FOUND
            }
            PregameApiError::AgentAction { status, .. } => {
                matches!(*status, StatusCode::NOT_FOUND | StatusCode::BAD_REQUEST)
            }
        }
    }
}

impl fmt::Display for PregameApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PregameApiError::PlayerRequest(status) => {
                write!(f, "Pregame player request failed with {status}")
            }
            PregameApiError::MatchRequest(status) => {
                write!(f, "Pregame match request failed with {status}")
            }
            PregameApiError::AgentAction { action, status } => {
                write!(f, "{action} request failed with {status}")
            }
        }
    }
}

impl std::error::Error for PregameApiError {}

fn riot_headers(builder: reqwest::RequestBuilder, live: &LiveContext) -> reqwest::RequestBuilder {
    builder
        .bearer_auth(&live.access_token)
        .header("X-Riot-Entitlements-JWT", &live.entitlement_token)
        .header("X-Riot-ClientVersion", &live.client_version)
        .header("X-Riot-ClientPlatform", CLIENT_PLATFORM)
}

async fn local_get<T: for<'de> Deserialize<'de>>(
    client: &Client,
    base: &str,
    auth: &str,
    suffix: &str,
) -> Result<T> {
    let url = format!("{base}/{suffix}");
    let resp = client.get(url).header("Authorization", auth).send().await?;
    let status = resp.status();
    if !status.is_success() {
        return Err(anyhow!("local endpoint {suffix} failed with {status}"));
    }
    resp.json::<T>()
        .await
        .with_context(|| format!("failed to parse local endpoint {suffix}"))
}

#[derive(Debug, Deserialize)]
pub struct PregameMatch {
    #[serde(rename = "AllyTeam")]
    pub ally_team: AllyTeam,
    #[serde(rename = "MapID")]
    pub map_id: String,
}

#[derive(Debug, Deserialize)]
pub struct AllyTeam {
    #[serde(rename = "Players")]
    pub players: Vec<PregamePlayer>,
}

#[derive(Debug, Deserialize)]
pub struct PregamePlayer {
    #[serde(rename = "Subject")]
    pub subject: String,
    #[serde(rename = "CharacterID")]
    pub character_id: String,
    #[serde(rename = "CharacterSelectionState")]
    pub character_selection_state: String,
}

#[derive(Deserialize)]
struct EntitlementsResponse {
    #[serde(rename = "accessToken")]
    access_token: String,
    subject: String,
    token: String,
}

#[derive(Deserialize)]
struct RegionResponse {
    region: String,
}

struct Lockfile {
    port: u16,
    password: String,
}

async fn read_lockfile() -> Result<Lockfile> {
    let local_app_data = env::var("LOCALAPPDATA").context("LOCALAPPDATA is not set")?;
    let path = PathBuf::from(local_app_data)
        .join("Riot Games")
        .join("Riot Client")
        .join("Config")
        .join("lockfile");
    let contents = fs::read_to_string(&path)
        .await
        .with_context(|| format!("failed to read {}", path.display()))?;
    parse_lockfile(contents.trim())
}

async fn read_shooter_log() -> Result<String> {
    let local_app_data = env::var("LOCALAPPDATA").context("LOCALAPPDATA is not set")?;
    let path = PathBuf::from(local_app_data)
        .join("VALORANT")
        .join("Saved")
        .join("Logs")
        .join("ShooterGame.log");
    fs::read_to_string(&path)
        .await
        .with_context(|| format!("failed to read {}", path.display()))
}

fn parse_lockfile(raw: &str) -> Result<Lockfile> {
    let parts: Vec<&str> = raw.split(':').collect();
    if parts.len() != 5 {
        return Err(anyhow!("Riot lockfile had unexpected format"));
    }
    Ok(Lockfile {
        port: parts[2].parse().context("lockfile port was invalid")?,
        password: parts[3].to_string(),
    })
}

fn find_valorant_session(sessions: &Value) -> Result<&Value> {
    sessions
        .as_object()
        .and_then(|map| {
            map.values().find(|session| {
                session
                    .get("productId")
                    .and_then(Value::as_str)
                    .is_some_and(|product| product == "valorant")
            })
        })
        .context("Valorant session was not found; start Valorant before arming")
}

fn extract_glz_url_from_log(log: &str) -> Option<String> {
    let start = log.rfind("https://glz")?;
    let rest = &log[start..];
    let end = rest
        .find(".a.pvp.net")
        .map(|index| index + ".a.pvp.net".len())?;
    Some(rest[..end].trim_end_matches('/').to_string())
}

fn parse_region_shard(args: &[Value], fallback_region: &str) -> Result<(String, String)> {
    let arg_strings: Vec<&str> = args.iter().filter_map(Value::as_str).collect();
    for arg in &arg_strings {
        if let Some((region, shard)) = parse_glz_host(arg) {
            return Ok((region, shard));
        }
    }

    let region = find_arg_value(&arg_strings, "-ares-region=")
        .or_else(|| find_arg_value(&arg_strings, "ares-region="))
        .unwrap_or_else(|| fallback_region.to_string());
    let shard = find_arg_value(&arg_strings, "-ares-deployment=")
        .or_else(|| find_arg_value(&arg_strings, "ares-deployment="))
        .unwrap_or_else(|| shard_for_region(&region).to_string());

    let region = normalize_glz_region(&region, &shard, fallback_region);

    if region.is_empty() || shard.is_empty() {
        return Err(anyhow!("could not determine Valorant region/shard"));
    }
    Ok((region, shard))
}

fn parse_glz_host(text: &str) -> Option<(String, String)> {
    let glz = text.split("glz-").nth(1)?;
    let (region, rest) = glz.split_once("-1.")?;
    let shard = rest.split(".a.pvp.net").next()?;
    Some((region.to_string(), shard.to_string()))
}

fn find_arg_value(args: &[&str], prefix: &str) -> Option<String> {
    args.iter()
        .find_map(|arg| arg.strip_prefix(prefix).map(ToString::to_string))
}

fn shard_for_region(region: &str) -> &'static str {
    match region {
        "na" | "latam" | "br" => "na",
        "eu" => "eu",
        "ap" => "ap",
        "kr" => "kr",
        _ => "na",
    }
}

fn normalize_glz_region(region: &str, shard: &str, fallback_region: &str) -> String {
    match shard {
        "eu" | "ap" | "kr" => shard.to_string(),
        "na" => match region {
            "na" | "latam" | "br" => region.to_string(),
            _ => match fallback_region {
                "na" | "latam" | "br" => fallback_region.to_string(),
                _ => "na".to_string(),
            },
        },
        "pbe" => "na".to_string(),
        _ => region.to_string(),
    }
}

fn find_private_presence_for_puuid<'a>(value: &'a Value, puuid: &str) -> Option<&'a str> {
    match value {
        Value::Object(object) => {
            let has_puuid = object
                .get("puuid")
                .or_else(|| object.get("Puuid"))
                .or_else(|| object.get("PUUID"))
                .or_else(|| object.get("pid"))
                .and_then(Value::as_str)
                .is_some_and(|value| value.eq_ignore_ascii_case(puuid));
            if has_puuid && let Some(private) = object.get("private").and_then(Value::as_str) {
                return Some(private);
            }
            object
                .values()
                .find_map(|value| find_private_presence_for_puuid(value, puuid))
        }
        Value::Array(array) => array
            .iter()
            .find_map(|value| find_private_presence_for_puuid(value, puuid)),
        _ => None,
    }
}

fn decode_presence_private(private: &str) -> Result<String> {
    let bytes = BASE64
        .decode(private)
        .or_else(|_| BASE64_NO_PAD.decode(private))
        .context("failed to decode base64 presence")?;
    String::from_utf8(bytes).context("presence was not valid UTF-8")
}

fn presence_has_pregame_state(presence: &Value) -> bool {
    presence_session_loop_state(presence).is_some_and(|state| state.eq_ignore_ascii_case("PREGAME"))
}

fn presence_session_loop_state(presence: &Value) -> Option<&str> {
    presence
        .get("sessionLoopState")
        .or_else(|| {
            presence
                .get("matchPresenceData")
                .and_then(|data| data.get("sessionLoopState"))
        })
        .and_then(Value::as_str)
}

#[derive(Debug)]
struct AcceptAnyServerCert;

impl ServerCertVerifier for AcceptAnyServerCert {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, RustlsError> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, RustlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, RustlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ED25519,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn normalizes_invalid_na_region_with_eu_shard() {
        let args = vec![json!("-ares-region=na"), json!("-ares-deployment=eu")];

        let (region, shard) = parse_region_shard(&args, "eu").unwrap();

        assert_eq!(region, "eu");
        assert_eq!(shard, "eu");
    }

    #[test]
    fn preserves_na_shard_subregions() {
        let args = vec![json!("-ares-region=latam"), json!("-ares-deployment=na")];

        let (region, shard) = parse_region_shard(&args, "latam").unwrap();

        assert_eq!(region, "latam");
        assert_eq!(shard, "na");
    }

    #[test]
    fn extracts_last_glz_url_from_shooter_log() {
        let log = "\
            request https://glz-na-1.na.a.pvp.net/pregame/v1/players/foo\n\
            request https://glz-eu-1.eu.a.pvp.net/pregame/v1/players/bar\n";

        let url = extract_glz_url_from_log(log).unwrap();

        assert_eq!(url, "https://glz-eu-1.eu.a.pvp.net");
    }

    #[test]
    fn detects_top_level_menus_presence_state() {
        let private = BASE64.encode(r#"{"sessionLoopState":"MENUS"}"#);
        let message = json!({"puuid":"me","private":private}).to_string();

        let state = websocket_message_session_loop_state_for_player(&message, "me");

        assert_eq!(state.as_deref(), Some("MENUS"));
    }

    #[test]
    fn detects_nested_ingame_presence_state() {
        let private = BASE64.encode(r#"{"matchPresenceData":{"sessionLoopState":"INGAME"}}"#);
        let message = json!({"puuid":"me","private":private}).to_string();

        let state = websocket_message_session_loop_state_for_player(&message, "me");

        assert_eq!(state.as_deref(), Some("INGAME"));
    }

    #[test]
    fn ignores_malformed_presence_state() {
        let message = json!({"puuid":"me","private":"not-base64"}).to_string();

        let state = websocket_message_session_loop_state_for_player(&message, "me");

        assert_eq!(state, None);
    }
}
