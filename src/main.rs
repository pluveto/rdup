use log::{error, info};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::time::{timeout, Duration};
use uuid::Uuid;

#[derive(Clone, Serialize, Deserialize, Debug)]
enum MessageType {
    Request,
    Response,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct Message {
    id: Uuid,
    message_type: MessageType,
    sender: String,
    target: String,
    content: String,
}

#[derive(Clone)]
struct Actor {
    id: String,
    sender: mpsc::Sender<Message>,
    receiver: Arc<Mutex<mpsc::Receiver<Message>>>,
    pending_requests: Arc<Mutex<HashMap<Uuid, tokio::sync::oneshot::Sender<String>>>>,
    peers: Arc<Mutex<HashMap<String, mpsc::Sender<Message>>>>,
}

impl Actor {
    async fn new(id: String, peers: Arc<Mutex<HashMap<String, mpsc::Sender<Message>>>>) -> Self {
        let (sender, receiver) = mpsc::channel(100);
        Actor {
            id,
            sender,
            receiver: Arc::new(Mutex::new(receiver)),
            pending_requests: Arc::new(Mutex::new(HashMap::new())),
            peers,
        }
    }

    async fn run(&self) {
        info!("Actor {} started", self.id);
        let mut receiver = self.receiver.lock().await;
        while let Some(msg) = receiver.recv().await {
            match msg.message_type {
                MessageType::Request => {
                    info!("{} received request: {:?}", self.id, msg);
                    let response = self.process_request(msg).await;
                    self.send_message(response).await;
                }
                MessageType::Response => {
                    info!("{} received response: {:?}", self.id, msg);
                    self.handle_response(msg).await;
                }
            }
        }
        info!("Actor {} stopped", self.id);
    }

    async fn process_request(&self, msg: Message) -> Message {
        Message {
            id: msg.id,
            message_type: MessageType::Response,
            sender: self.id.clone(),
            target: msg.sender,
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

    async fn send_message(&self, message: Message) {
        info!("{} sending message: {:?}", self.id, message);
        let peers = self.peers.lock().await;
        let target = message.target.clone();
        if let Some(sender) = peers.get(&target) {
            match sender.send(message).await {
                Ok(_) => info!("{} sent message to {}", self.id, target),
                Err(e) => info!("{} failed to send message to {}: {:?}", self.id, target, e),
            }
        } else {
            error!("{} couldn't find peer {}", self.id, message.target);
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
            message_type: MessageType::Request,
            sender: self.id.clone(),
            target: target.to_string(),
            content,
        };

        self.send_message(request).await;

        tokio::time::sleep(Duration::from_millis(50)).await;

        match timeout(Duration::from_secs(5), response_rx).await {
            Ok(Ok(response)) => {
                info!("{} received response for request {}", self.id, request_id);
                Ok(response)
            }
            Ok(Err(_)) => Err("Response channel closed unexpectedly".into()),
            Err(_) => Err("Request timed out".into()),
        }
    }
}

async fn spawn_actor(
    id: String,
    peers: Arc<Mutex<HashMap<String, mpsc::Sender<Message>>>>,
) -> Actor {
    let actor = Actor::new(id, Arc::clone(&peers)).await;
    let actor_clone = actor.clone();
    tokio::spawn(async move { actor_clone.run().await });
    actor
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::new()
        .filter(None, log::LevelFilter::Info)
        .parse_env("RUST_LOG")
        .init();

    let peers = Arc::new(Mutex::new(HashMap::new()));

    let leader = spawn_actor("Leader".to_string(), Arc::clone(&peers)).await;
    let follower1 = spawn_actor("Follower1".to_string(), Arc::clone(&peers)).await;
    let follower2 = spawn_actor("Follower2".to_string(), Arc::clone(&peers)).await;

    {
        let mut peers = peers.lock().await;
        peers.insert("Leader".to_string(), leader.sender.clone());
        peers.insert("Follower1".to_string(), follower1.sender.clone());
        peers.insert("Follower2".to_string(), follower2.sender.clone());
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    match leader.send_request("Follower1", "Hello".to_string()).await {
        Ok(response) => info!("Leader received: {}", response),
        Err(e) => error!("{}", e),
    }

    match leader.send_request("Follower2", "Hello".to_string()).await {
        Ok(response) => info!("Leader received: {}", response),
        Err(e) => error!("{}", e),
    }

    match follower1.send_request("Leader", "Hello".to_string()).await {
        Ok(response) => info!("Follower1 received: {}", response),
        Err(e) => error!("{}", e),
    }

    Ok(())
}
