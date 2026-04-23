use std::net::SocketAddr;

use anyhow::Result;
use axum::{
    Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::{Html, IntoResponse},
    routing::get,
};
use tokio::sync::broadcast;
use tracing::info;

#[derive(Clone)]
struct DashboardState {
    snapshots: broadcast::Sender<String>,
}

pub async fn run_dashboard(addr: SocketAddr, snapshots: broadcast::Sender<String>) -> Result<()> {
    let app = Router::new()
        .route("/", get(index))
        .route("/ws", get(ws_handler))
        .with_state(DashboardState { snapshots });

    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("dashboard listening on http://{addr}");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn index() -> Html<&'static str> {
    Html(include_str!("dashboard.html"))
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<DashboardState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| stream_snapshots(socket, state.snapshots.subscribe()))
}

async fn stream_snapshots(mut socket: WebSocket, mut snapshots: broadcast::Receiver<String>) {
    while let Ok(snapshot) = snapshots.recv().await {
        if socket.send(Message::Text(snapshot.into())).await.is_err() {
            break;
        }
    }
}
