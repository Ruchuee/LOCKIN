use crate::app::{Agent, AppState, push_event};
use anyhow::{Context, Result};
use reqwest::Client;
use serde::Deserialize;

pub async fn load_public_agents(app: AppState) {
    {
        let mut inner = app.inner.lock().await;
        inner.state.message = "Loading public agent data.".to_string();
    }
    app.notify();

    let client = match Client::builder().build() {
        Ok(client) => client,
        Err(err) => {
            let mut inner = app.inner.lock().await;
            inner.state.message = "Failed to initialize public agent fetch.".to_string();
            push_event(
                &mut inner,
                "error",
                &format!("Agent data load failed: {err}"),
            );
            app.notify();
            return;
        }
    };

    match fetch_agents(&client).await {
        Ok(agents) => {
            let mut inner = app.inner.lock().await;
            inner.agents = agents;
            inner.state.message = "Idle. Select an agent, then press Arm.".to_string();
        }
        Err(err) => {
            let mut inner = app.inner.lock().await;
            inner.state.message = "Agent data unavailable. Restart the app to retry.".to_string();
            push_event(
                &mut inner,
                "error",
                &format!("Agent data load failed: {err}"),
            );
        }
    }
    app.notify();
}

async fn fetch_agents(client: &Client) -> Result<Vec<Agent>> {
    let resp: ValorantApiAgentsResponse = client
        .get("https://valorant-api.com/v1/agents?isPlayableCharacter=true")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
        .context("failed to parse Valorant-API agents response")?;
    let mut agents: Vec<Agent> = resp
        .data
        .into_iter()
        .map(|agent| Agent {
            uuid: agent.uuid,
            display_name: agent.display_name,
            display_icon: agent.display_icon,
            role: agent.role.map(|role| role.display_name),
        })
        .collect();
    agents.sort_by(|a, b| {
        a.role
            .cmp(&b.role)
            .then_with(|| a.display_name.cmp(&b.display_name))
    });
    Ok(agents)
}

#[derive(Deserialize)]
struct ValorantApiAgentsResponse {
    data: Vec<ValorantApiAgent>,
}

#[derive(Deserialize)]
struct ValorantApiAgent {
    uuid: String,
    #[serde(rename = "displayName")]
    display_name: String,
    #[serde(rename = "displayIcon")]
    display_icon: Option<String>,
    role: Option<ValorantApiRole>,
}

#[derive(Deserialize)]
struct ValorantApiRole {
    #[serde(rename = "displayName")]
    display_name: String,
}
