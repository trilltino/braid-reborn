use crate::error::{BraidError, Result};
use crate::traits::BraidNetwork;
use crate::types::{BraidRequest, BraidResponse, Update};
use async_trait::async_trait;

pub struct WasmNetwork;

#[async_trait]
impl BraidNetwork for WasmNetwork {
    async fn fetch(&self, _url: &str, _request: BraidRequest) -> Result<BraidResponse> {
        Err(BraidError::Internal(
            "WasmNetwork::fetch not implemented yet".to_string(),
        ))
    }

    async fn subscribe(
        &self,
        _url: &str,
        _request: BraidRequest,
    ) -> Result<async_channel::Receiver<Result<Update>>> {
        Err(BraidError::Internal(
            "WasmNetwork::subscribe not implemented yet".to_string(),
        ))
    }
}
