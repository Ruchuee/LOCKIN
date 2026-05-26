use crate::app::{self, AppState, ConfigRequest, StateResponse};
use anyhow::Error;
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{
        Html, IntoResponse, Response,
        sse::{Event as SseEvent, KeepAlive, Sse},
    },
    routing::{get, post},
};
use futures_util::stream;
use serde_json::json;
use std::convert::Infallible;
use tower_http::trace::TraceLayer;

pub fn router(app_state: AppState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/api/state", get(api_state))
        .route("/api/agents", get(api_agents))
        .route("/api/config", post(api_config))
        .route("/api/arm", post(api_arm))
        .route("/api/disarm", post(api_disarm))
        .route("/api/events", get(api_events))
        .route("/api/stream", get(api_stream))
        .layer(TraceLayer::new_for_http())
        .with_state(app_state)
}

async fn index() -> Html<&'static str> {
    Html(include_str!("../static/index.html"))
}

async fn api_state(State(app): State<AppState>) -> Json<StateResponse> {
    Json(app.snapshot().await)
}

async fn api_agents(State(app): State<AppState>) -> Json<Vec<crate::app::Agent>> {
    let inner = app.inner.lock().await;
    Json(inner.agents.clone())
}

async fn api_events(State(app): State<AppState>) -> Json<Vec<crate::app::AppEvent>> {
    let inner = app.inner.lock().await;
    Json(inner.events.clone())
}

async fn api_config(
    State(app): State<AppState>,
    Json(req): Json<ConfigRequest>,
) -> ApiResult<Json<StateResponse>> {
    Ok(Json(app::apply_config(&app, req).await?))
}

async fn api_arm(State(app): State<AppState>) -> ApiResult<Json<StateResponse>> {
    Ok(Json(app::arm(app).await?))
}

async fn api_disarm(State(app): State<AppState>) -> Json<StateResponse> {
    Json(app::disarm(&app).await)
}

async fn api_stream(
    State(app): State<AppState>,
) -> Sse<impl futures_util::Stream<Item = Result<SseEvent, Infallible>>> {
    let rx = app.subscribe();
    let stream = stream::unfold((app, rx, true), |(app, mut rx, first)| async move {
        if !first && rx.changed().await.is_err() {
            return None;
        }

        let event = match serde_json::to_string(&app.snapshot().await) {
            Ok(data) => SseEvent::default().event("state").data(data),
            Err(err) => SseEvent::default()
                .event("error")
                .data(json!({ "error": err.to_string() }).to_string()),
        };

        Some((Ok(event), (app, rx, false)))
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

#[derive(Debug)]
struct AppError(Error);

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": self.0.to_string() })),
        )
            .into_response()
    }
}

impl<E> From<E> for AppError
where
    E: Into<Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

type ApiResult<T> = std::result::Result<T, AppError>;
