use braid_http::{protocol::format_update, types::Update};
use std::io::Read;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut input = String::new();
    std::io::stdin().read_to_string(&mut input)?;
    let updates: Vec<Update> = serde_json::from_str(&input)?;
    let frames = updates
        .iter()
        .map(|update| format_update(update).map(|b| b.to_vec()))
        .collect::<braid_http::Result<Vec<_>>>()?;
    println!("{}", serde_json::to_string(&frames)?);
    Ok(())
}
