#[tokio::main]
async fn main() -> anyhow::Result<()> {
    local_link_server::run().await
}
