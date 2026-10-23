#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)
)]
#![forbid(unsafe_code)]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    local_link_server::run().await
}
