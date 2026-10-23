//! Cancellable subscription lifetime with an application-committed resume frontier.
//! Call acknowledge only after materializing/persisting an update successfully.
use crate::{
    traits::BraidNetwork,
    types::{BraidRequest, Update, Version},
    BraidClient, BraidError, Result,
};

pub struct ReliableSubscription {
    updates: async_channel::Receiver<Result<Update>>,
    acknowledgments: async_channel::Sender<Vec<Version>>,
    task: tokio::task::JoinHandle<()>,
}
impl ReliableSubscription {
    pub async fn next(&mut self) -> Option<Result<Update>> {
        self.updates.recv().await.ok()
    }
    pub async fn acknowledge(&self, update: &Update) -> Result<()> {
        self.acknowledgments
            .send(update.version.clone())
            .await
            .map_err(|_| BraidError::SubscriptionClosed)
    }
}
impl Drop for ReliableSubscription {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl BraidClient {
    /// Reconnects from the last acknowledged frontier, with one update awaiting application.
    pub async fn subscribe_reliable(
        &self,
        url: &str,
        mut request: BraidRequest,
    ) -> Result<ReliableSubscription> {
        let url = url.to_owned();
        let client = self.clone();
        let (tx, updates) = async_channel::bounded(1);
        let (ack_tx, ack_rx) = async_channel::bounded::<Vec<Version>>(1);
        let task = tokio::spawn(async move {
            let config = request.retry.clone().unwrap_or_default();
            let mut retry = super::RetryState::new(config.clone());
            loop {
                let connected = tokio::select! {_=tx.closed()=>return,result=client.network.subscribe(&url,request.clone())=>result};
                let mut failure = None;
                match connected {
                    Ok(receiver) => {
                        while let Ok(update) = receiver.recv().await {
                            match update {
                                Ok(update) => {
                                    let delivered = update.version.clone();
                                    if tx.send(Ok(update)).await.is_err() {
                                        return;
                                    }
                                    // Resume advances only after the consumer confirms durable application.
                                    match ack_rx.recv().await {
                                        Ok(frontier) if frontier == delivered => {
                                            if !frontier.is_empty() {
                                                request.parents = Some(frontier);
                                            }
                                            retry.reset();
                                        }
                                        Ok(_) => {
                                            let _=tx.send(Err(BraidError::Protocol("Acknowledgment frontier does not match the delivered update".into()))).await;
                                            return;
                                        }
                                        Err(_) => return,
                                    }
                                }
                                Err(e) => {
                                    failure = Some(e);
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => failure = Some(e),
                }
                let decision = match &failure {
                    Some(BraidError::SubscriptionStatus {
                        status,
                        retry_after,
                    }) if matches!(status, 408 | 425 | 429 | 500 | 502 | 503 | 504) => {
                        // A 500 is transient for subscription establishment; never reinterpret a conflict as success.
                        if *status == 500 {
                            retry.should_retry_error(false)
                        } else {
                            retry.should_retry_status(*status, *retry_after)
                        }
                    }
                    Some(BraidError::Http(_) | BraidError::Timeout) | None => {
                        retry.should_retry_error(false)
                    }
                    Some(BraidError::Protocol(message))
                        if message.starts_with("Truncated Braid frame") =>
                    {
                        retry.should_retry_error(false)
                    }
                    _ => super::RetryDecision::DontRetry,
                };
                match decision {
                    super::RetryDecision::Retry(delay) => {
                        let jitter = std::time::Duration::from_millis(rand::random::<u64>() % 100);
                        tokio::select! {_=tx.closed()=>return,_=tokio::time::sleep(delay+jitter)=>{}}
                    }
                    super::RetryDecision::DontRetry => {
                        let _ = tx
                            .send(Err(failure.unwrap_or(BraidError::SubscriptionClosed)))
                            .await;
                        return;
                    }
                }
            }
        });
        Ok(ReliableSubscription {
            updates,
            acknowledgments: ack_tx,
            task,
        })
    }
}
