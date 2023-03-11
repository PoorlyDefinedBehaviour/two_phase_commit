//! The http server allows the user to interact with the system,
//! to start a new transaction for example.

use anyhow::Result;
use axum::{
    http::{Response, StatusCode},
    routing::post,
    Extension, Json, Router,
};
use std::sync::Arc;
use tracing::{error, info};

use crate::transaction_manager::TransactionManager;

#[tracing::instrument(name = "http_server::start", skip_all)]
pub async fn start(port: u16, transaction_manager: Arc<TransactionManager>) -> Result<()> {
    let app = Router::new()
        .route("/", post(handle_request))
        .layer(Extension(transaction_manager));
    let addr = format!("0.0.0.0:{port}").parse()?;

    info!(?addr, "starting server");
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}

#[derive(Debug, serde::Deserialize)]
struct HandleRequestInput {
    pub op: u8,
}

#[tracing::instrument(name = "handle_request", skip_all)]
#[axum_macros::debug_handler]
async fn handle_request(
    Extension(transaction_manager): Extension<Arc<TransactionManager>>,
    Json(input): Json<HandleRequestInput>,
) -> Response<String> {
    match transaction_manager.handle_request(input.op).await {
        Ok(_) => Response::builder()
            .status(StatusCode::OK)
            .body("OK".to_owned())
            .unwrap(),
        Err(err) => {
            error!(?err, "unable to handle user request");
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(err.to_string())
                .unwrap()
        }
    }
}
