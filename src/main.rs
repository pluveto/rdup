use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex, RwLock};
use tokio::time::Duration;
use uuid::Uuid;

#[derive(Clone, Serialize, Deserialize, Debug)]
struct Message {
    id: Uuid,
    content: String,
}

struct Peer {
    sender: broadcast::Sender<Message>,
    receiver: broadcast::Receiver<Message>,
}

#[derive(Clone)]
struct Actor {
    id: String,
    peers: Arc<RwLock<HashMap<String, Peer>>>,
    pending_requests: Arc<Mutex<HashMap<Uuid, tokio::sync::oneshot::Sender<String>>>>,
}

impl Actor {
    fn new(id: String) -> Self {
        Actor {
            id,
            peers: Arc::new(RwLock::new(HashMap::new())),
            pending_requests: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn run(&self) {
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
                            error!("Error receiving message: {:?}", e);
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
    async fn handle_message(&self, peer_id: String, msg: Message) {
        let is_response = self.pending_requests.lock().await.contains_key(&msg.id);
        if is_response {
            info!("{} received response from {}: {:?}", self.id, peer_id, msg);
            self.handle_response(msg).await;
        } else {
            info!("{} received request from {}: {:?}", self.id, peer_id, msg);
            let response = self.process_request(msg, peer_id.clone()).await;
            self.send_message(peer_id, response).await;
        }
    }

    async fn process_request(&self, msg: Message, sender: String) -> Message {
        info!("{} processing request from {}: {}", self.id, sender, msg.content);
        Message {
            id: msg.id,
            content: format!("Response from {}: Processed {}", self.id, msg.content),
        }
    }

    async fn handle_response(&self, msg: Message) {
        let mut pending = self.pending_requests.lock().await;
        if let Some(sender) = pending.remove(&msg.id) {
            let _ = sender.send(msg.content);
        } else {
            error!("{} couldn't find pending request {}", self.id, msg.id);
        }
    }

    async fn send_message(&self, target: String, message: Message) {
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

    async fn send_request(
        &self,
        target: &str,
        content: String,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let request_id = Uuid::new_v4();
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        {
            let mut pending = self.pending_requests.lock().await;
            pending.insert(request_id, response_tx);
        }

        let request = Message {
            id: request_id,
            content,
        };

        self.send_message(target.to_string(), request).await;

        match tokio::time::timeout(Duration::from_secs(5), response_rx).await {
            Ok(Ok(response)) => {
                info!("{} received response for request {}", self.id, request_id);
                Ok(response)
            }
            Ok(Err(_)) => Err("Response channel closed unexpectedly".into()),
            Err(_) => Err("Request timed out".into()),
        }
    }

    async fn connect(&self, peer_id: String, peer: Peer) {
        let mut peers = self.peers.write().await;
        peers.insert(peer_id, peer);
    }

    async fn disconnect(&self, peer_id: &str) {
        let mut peers = self.peers.write().await;
        peers.remove(peer_id);
    }
}

async fn create_actor(id: String) -> Arc<Actor> {
    let actor = Arc::new(Actor::new(id));
    let actor_clone = actor.clone();
    tokio::spawn(async move { actor_clone.run().await });
    actor
}

async fn connect_actors(actor1: &Arc<Actor>, actor2: &Arc<Actor>) {
    let (tx1, rx1) = broadcast::channel(100);
    actor1
        .connect(
            actor2.id.clone(),
            Peer {
                sender: tx1.clone(),
                receiver: rx1.resubscribe(),
            },
        )
        .await;

    let (tx2, rx2) = broadcast::channel(100);
    actor2
        .connect(
            actor1.id.clone(),
            Peer {
                sender: tx2.clone(),
                receiver: rx2.resubscribe(),
            },
        )
        .await;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::new()
        .filter(None, log::LevelFilter::Info)
        .parse_env("RUST_LOG")
        .init();

    let leader = create_actor("Leader".to_string()).await;
    let follower1 = create_actor("Follower1".to_string()).await;
    let follower2 = create_actor("Follower2".to_string()).await;

    connect_actors(&leader, &follower1).await;
    connect_actors(&leader, &follower2).await;
    connect_actors(&follower1, &follower2).await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    match leader.send_request("Follower1", "Hello".to_string()).await {
        Ok(response) => info!("Leader received from Follower1: {}", response),
        Err(e) => error!("{}", e),
    }

    match leader.send_request("Follower2", "Hello".to_string()).await {
        Ok(response) => info!("Leader received from Follower2: {}", response),
        Err(e) => error!("{}", e),
    }

    match follower1.send_request("Leader", "Hello".to_string()).await {
        Ok(response) => info!("Follower1 received from Leader: {}", response),
        Err(e) => error!("{}", e),
    }

    follower1.disconnect("Follower2").await;
    match follower1
        .send_request("Follower2", "This should fail".to_string())
        .await
    {
        Ok(_) => error!("Unexpected success"),
        Err(e) => info!("Expected error: {}", e),
    }

    Ok(())
}
