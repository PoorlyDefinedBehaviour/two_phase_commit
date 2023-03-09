//! The http server allows the user to interact with the system,
//! to start a new transaction for example.

use anyhow::Result;
use axum::{response::IntoResponse, routing::post, Router};
use tracing::info;

#[tracing::instrument(name = "http_server::start", skip_all)]
pub async fn start(port: u16) -> Result<()> {
    let app = Router::new().route("/", post(handle_request));
    let addr = format!("0.0.0.0:{port}").parse()?;

    info!(?addr, "starting server");
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}

#[tracing::instrument(name = "handle_request", skip_all)]
async fn handle_request() -> impl IntoResponse {
    todo!()
}
