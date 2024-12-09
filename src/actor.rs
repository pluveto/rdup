use log::{error, info, trace, warn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex, RwLock};
use tokio::time::Duration;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message<T> {
    pub id: Uuid,
    pub data: T,
}

/// Ports for communication between actors
/// NOTE: never use two ends of a channel as peer params to communicate
pub(crate) struct Peer<T> {
    /// Sender for messages to other peers
    pub(crate) sender: broadcast::Sender<Message<T>>,
    /// Receiver for messages from other peers
    pub(crate) receiver: broadcast::Receiver<Message<T>>,
}

pub(crate) struct Actor<T> {
    id: String,
    peers: Arc<RwLock<HashMap<String, Peer<T>>>>,
    pending_requests: Arc<Mutex<HashMap<Uuid, tokio::sync::oneshot::Sender<T>>>>,
    request_handler: Arc<dyn Fn(Message<T>, String) -> T + Send + Sync>,
}

pub(crate) struct ActorBuilder<T> {
    id: String,
    request_handler: Option<Arc<dyn Fn(Message<T>, String) -> T + Send + Sync>>,
}

impl<T> ActorBuilder<T> {
    pub(crate) fn new(id: String) -> Self {
        Self {
            id,
            request_handler: None,
        }
    }

    pub(crate) fn with_request_handler<F>(mut self, handler: F) -> Self
    where
        F: Fn(Message<T>, String) -> T + Send + Sync + 'static,
    {
        self.request_handler = Some(Arc::new(handler));
        self
    }

    pub(crate) fn build(self) -> Actor<T> {
        Actor {
            id: self.id,
            peers: Arc::new(RwLock::new(HashMap::new())),
            pending_requests: Arc::new(Mutex::new(HashMap::new())),
            request_handler: self
                .request_handler
                .unwrap_or_else(|| Arc::new(|msg, _| msg.data)),
        }
    }
}

impl<T> Actor<T>
where
    T: Clone + Debug + Send + Sync + 'static,
{
    pub(crate) fn builder(id: String) -> ActorBuilder<T> {
        ActorBuilder::new(id)
    }

    pub(crate) async fn run(&self) {
        info!("Actor {} started", self.id);
        loop {
            let mut peer_receivers = Vec::new();
            {
                let peers = self.peers.read().await;
                for (peer_id, peer) in peers.iter() {
                    peer_receivers.push((peer_id.clone(), peer.receiver.resubscribe()));
                }
            }

            if peer_receivers.is_empty() {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }

            let mut select_futures = Vec::new();
            for (peer_id, mut receiver) in peer_receivers {
                select_futures.push(Box::pin(async move {
                    match receiver.recv().await {
                        Ok(msg) => Some((peer_id, msg)),
                        Err(e) => {
                            error!(
                                "{} failed to receive message from {}: {}",
                                self.id, peer_id, e
                            );
                            None
                        }
                    }
                }));
            }

            tokio::select! {
                result = futures_util::future::select_all(select_futures) => {
                    match result.0 {
                        Some((peer_id, msg)) => {
                            self.handle_message(peer_id, msg).await;
                        }
                        None => {
                            warn!("Received None from a peer, continuing...");
                        }
                    }
                }
            }
        }
    }

    pub(crate) async fn handle_message(&self, peer_id: String, msg: Message<T>) {
        let is_response = self.pending_requests.lock().await.contains_key(&msg.id);
        trace!("{} is_response: {}", self.id, is_response);
        if is_response {
            info!("{} received response from {}: {:?}", self.id, peer_id, msg);
            self.handle_response(msg).await;
        } else {
            info!("{} received request from {}: {:?}", self.id, peer_id, msg);
            let response_data = (self.request_handler)(msg.clone(), peer_id.clone());
            let response = Message {
                id: msg.id,
                data: response_data,
            };
            self.send_message(peer_id, response).await;
        }
    }

    async fn handle_response(&self, msg: Message<T>) {
        let mut pending = self.pending_requests.lock().await;
        if let Some(sender) = pending.remove(&msg.id) {
            let _ = sender.send(msg.data);
        } else {
            error!("{} couldn't find pending request {}", self.id, msg.id);
        }
    }

    async fn send_message(&self, target: String, message: Message<T>) {
        info!("{} sending message to {}: {:?}", self.id, target, message);
        let peers = self.peers.read().await;
        if let Some(peer) = peers.get(&target) {
            match peer.sender.send(message) {
                Ok(_) => info!("{} sent message to {}", self.id, target),
                Err(e) => error!("{} failed to send message to {}: {:?}", self.id, target, e),
            }
        } else {
            error!("{} couldn't find peer {}", self.id, target);
        }
    }

    pub(crate) async fn send(
        &self,
        target: &str,
        content: T,
        timeout: Option<Duration>,
    ) -> Result<T, Box<dyn std::error::Error>> {
        // check validity of target
        if target.is_empty() {
            return Err("Invalid target".into());
        }
        {
            let peers = self.peers.read().await;
            if !peers.contains_key(target) {
                // debug all peers
                for (peer_id, _) in peers.iter() {
                    println!("{} has peer: {}", self.id, peer_id);
                }
                return Err(format!("{} is not connected to {}", self.id, target).into());
            }
        }

        let request_id = Uuid::new_v4();
        info!("{} sending request to {}: {}", self.id, target, request_id);
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        {
            let mut pending = self.pending_requests.lock().await;
            pending.insert(request_id, response_tx);
        }

        let request = Message {
            id: request_id,
            data: content,
        };

        self.send_message(target.to_string(), request).await;

        if let Some(timeout) = timeout {
            match tokio::time::timeout(timeout, response_rx).await {
                Ok(Ok(response)) => {
                    info!("{} received response for request {}", self.id, request_id);
                    Ok(response)
                }
                Ok(Err(_)) => Err("Response channel closed unexpectedly".into()),
                Err(_) => Err("Request timed out".into()),
            }
        } else {
            match response_rx.await {
                Ok(response) => {
                    info!("{} received response for request {}", self.id, request_id);
                    Ok(response)
                }
                Err(_) => Err("Response channel closed unexpectedly".into()),
            }
        }
    }

    pub(crate) async fn connect(&self, peer_id: String, peer: Peer<T>) {
        let mut peers = self.peers.write().await;
        peers.insert(peer_id.clone(), peer);
        trace!("{} connected to peer {}", self.id, peer_id);
    }

    pub(crate) async fn disconnect(&self, peer_id: &str) {
        let mut peers = self.peers.write().await;
        peers.remove(peer_id);
        trace!("{} disconnected from peer {}", self.id, peer_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn create_actor(id: String) -> Arc<Actor<String>> {
        let actor = Arc::new(
            Actor::builder(id.clone())
                .with_request_handler(move |msg, _| {
                    format!("Response from {}: Processed {}", id, msg.data)
                })
                .build(),
        );
        let actor_clone = actor.clone();
        tokio::spawn(async move { actor_clone.run().await });
        actor
    }

    async fn connect_actors(actor1: &Arc<Actor<String>>, actor2: &Arc<Actor<String>>) {
        let (tx1, rx1) = broadcast::channel(100);
        let (tx2, rx2) = broadcast::channel(100);
        actor1
            .connect(
                actor2.id.clone(),
                Peer {
                    sender: tx1.clone(),
                    receiver: rx2.resubscribe(),
                },
            )
            .await;

        actor2
            .connect(
                actor1.id.clone(),
                Peer {
                    sender: tx2.clone(),
                    receiver: rx1.resubscribe(),
                },
            )
            .await;
    }

    #[tokio::test]
    async fn test_actor() {
        let _ = env_logger::Builder::new()
            .parse_env("RUST_LOG")
            .try_init();

        let actor1 = create_actor("actor1".to_string()).await;
        let actor2 = create_actor("actor2".to_string()).await;

        connect_actors(&actor1, &actor2).await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        let response = actor1.send("actor2", "Hello".to_string(), None).await;
        assert_eq!(response.unwrap(), "Response from actor2: Processed Hello");
    }
}
