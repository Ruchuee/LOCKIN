use crate::app::{AppState, Map, push_event};
use anyhow::{Context, Result};
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashSet;

const MAPS_URL: &str = "https://valorant-api.com/v1/maps";
const ROTATION_URL: &str = "https://xiongaox.github.io/valorant-rotation-api/maps.json";

pub async fn load_public_maps(app: AppState) {
    let client = match Client::builder().build() {
        Ok(client) => client,
        Err(err) => {
            let mut inner = app.inner.lock().await;
            push_event(&mut inner, "error", &format!("Map data load failed: {err}"));
            app.notify();
            return;
        }
    };

    match fetch_maps(&client).await {
        Ok(maps) => {
            let mut inner = app.inner.lock().await;
            inner.maps = maps;
        }
        Err(err) => {
            let mut inner = app.inner.lock().await;
            push_event(&mut inner, "error", &format!("Map data load failed: {err}"));
        }
    }
    app.notify();
}

async fn fetch_maps(client: &Client) -> Result<Vec<Map>> {
    let mut maps = fetch_valorant_api_maps(client).await?;
    let ranked_names = fetch_ranked_pool_names(client).await.unwrap_or_default();

    for map in &mut maps {
        map.in_ranked_pool = ranked_names.contains(&map.display_name.to_ascii_lowercase());
    }
    maps.sort_by(|a, b| {
        b.in_ranked_pool
            .cmp(&a.in_ranked_pool)
            .then_with(|| a.display_name.cmp(&b.display_name))
    });
    Ok(maps)
}

async fn fetch_valorant_api_maps(client: &Client) -> Result<Vec<Map>> {
    let resp: ValorantApiMapsResponse = client
        .get(MAPS_URL)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
        .context("failed to parse Valorant-API maps response")?;

    Ok(resp
        .data
        .into_iter()
        .filter(|map| !map.map_url.trim().is_empty() && is_bomb_site_map(&map.tactical_description))
        .map(|map| Map {
            uuid: map.uuid,
            display_name: map.display_name,
            display_icon: map.display_icon,
            list_view_icon: map.list_view_icon,
            splash: map.splash,
            map_url: map.map_url,
            in_ranked_pool: false,
        })
        .collect())
}

fn is_bomb_site_map(tactical_description: &Option<String>) -> bool {
    tactical_description
        .as_deref()
        .is_some_and(|value| matches!(value, "A/B Sites" | "A/B/C Sites"))
}

async fn fetch_ranked_pool_names(client: &Client) -> Result<HashSet<String>> {
    let resp: RotationResponse = client
        .get(ROTATION_URL)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await
        .context("failed to parse map rotation response")?;

    Ok(ranked_pool_names(resp.maps))
}

fn ranked_pool_names(maps: Vec<RotationMap>) -> HashSet<String> {
    maps.into_iter()
        .filter(|map| !map.status.eq_ignore_ascii_case("rotated_out"))
        .map(|map| map.name_en.to_ascii_lowercase())
        .collect()
}

#[derive(Deserialize)]
struct ValorantApiMapsResponse {
    data: Vec<ValorantApiMap>,
}

#[derive(Deserialize)]
struct ValorantApiMap {
    uuid: String,
    #[serde(rename = "displayName")]
    display_name: String,
    #[serde(rename = "displayIcon")]
    display_icon: Option<String>,
    #[serde(rename = "listViewIcon")]
    list_view_icon: Option<String>,
    splash: Option<String>,
    #[serde(rename = "mapUrl")]
    map_url: String,
    #[serde(rename = "tacticalDescription")]
    tactical_description: Option<String>,
}

#[derive(Deserialize)]
struct RotationResponse {
    maps: Vec<RotationMap>,
}

#[derive(Deserialize)]
struct RotationMap {
    #[serde(rename = "name_en")]
    name_en: String,
    status: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn treats_non_rotated_out_statuses_as_ranked_pool() {
        let names = ranked_pool_names(vec![
            RotationMap {
                name_en: "Haven".to_string(),
                status: "in_pool".to_string(),
            },
            RotationMap {
                name_en: "Ascent".to_string(),
                status: "returning".to_string(),
            },
            RotationMap {
                name_en: "Bind".to_string(),
                status: "rotated_out".to_string(),
            },
        ]);

        assert!(names.contains("haven"));
        assert!(names.contains("ascent"));
        assert!(!names.contains("bind"));
    }

    #[test]
    fn identifies_bomb_site_maps() {
        assert!(is_bomb_site_map(&Some("A/B Sites".to_string())));
        assert!(is_bomb_site_map(&Some("A/B/C Sites".to_string())));
        assert!(!is_bomb_site_map(&None));
        assert!(!is_bomb_site_map(&Some("Event Map".to_string())));
    }
}
