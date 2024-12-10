use log::{error, info, trace, warn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, watch, Mutex, Notify, RwLock};
use tokio::task::JoinHandle;
use tokio::time::Duration;
use uuid::Uuid;

#[derive(Debug)]
pub enum ActorError {
    ActorRunning,
    ActorStopped,
    InvalidTarget(String),
    PeerNotConnected(String),
    RequestTimeout,
    ResponseChannelClosed,
}

impl Error for ActorError {}

impl Display for ActorError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ActorError::ActorRunning => write!(f, "Actor already running"),
            ActorError::ActorStopped => write!(f, "Actor already stopped"),
            ActorError::InvalidTarget(target) => write!(f, "Invalid target: {}", target),
            ActorError::PeerNotConnected(peer_id) => write!(f, "Peer not connected: {}", peer_id),
            ActorError::RequestTimeout => write!(f, "Request timeout"),
            ActorError::ResponseChannelClosed => write!(f, "Response channel closed"),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message<T> {
    pub id: Uuid,
    pub data: T,
}

/// Ports for communication between actors
/// NOTE: never use two ends of a channel as peer params to communicate
pub struct Peer<T> {
    /// Sender for messages to other peers
    pub(crate) sender: broadcast::Sender<Message<T>>,
    /// Receiver for messages from other peers
    pub(crate) receiver: broadcast::Receiver<Message<T>>,
}

impl<T> Peer<T>
where
    T: Clone + Debug + Send + Sync + 'static,
{
    pub fn new(
        sender_out: broadcast::Sender<Message<T>>,
        receiver_in: broadcast::Receiver<Message<T>>,
    ) -> Self {
        Self {
            sender: sender_out,
            receiver: receiver_in,
        }
    }
}

struct ActorInner<T> {
    id: String,
    peers: Arc<RwLock<HashMap<String, Peer<T>>>>,
    pending_requests: Arc<Mutex<HashMap<Uuid, tokio::sync::oneshot::Sender<T>>>>,
    request_handler: Arc<dyn Fn(Message<T>, String) -> T + Send + Sync>,
    stop_tx: watch::Sender<()>,
    stopped_notify: Arc<Notify>,
    is_running: AtomicBool,
}

#[derive(Clone)]
pub struct Actor<T> {
    inner: Arc<ActorInner<T>>,
}

pub struct ActorBuilder<T> {
    id: String,
    request_handler: Option<Arc<dyn Fn(Message<T>, String) -> T + Send + Sync>>,
}

impl<T> ActorBuilder<T> {
    pub fn new(id: String) -> Self {
        Self {
            id,
            request_handler: None,
        }
    }

    pub fn with_request_handler<F>(mut self, handler: F) -> Self
    where
        F: Fn(Message<T>, String) -> T + Send + Sync + 'static,
    {
        self.request_handler = Some(Arc::new(handler));
        self
    }

    pub fn build(self) -> Actor<T> {
        Actor {
            inner: Arc::new(ActorInner {
                id: self.id,
                peers: Arc::new(RwLock::new(HashMap::new())),
                pending_requests: Arc::new(Mutex::new(HashMap::new())),
                request_handler: self
                    .request_handler
                    .unwrap_or_else(|| Arc::new(|msg, _| msg.data)),
                stop_tx: watch::channel(()).0,
                stopped_notify: Arc::new(Notify::new()),
                is_running: AtomicBool::new(false),
            }),
        }
    }
}

impl<T> Actor<T>
where
    T: Clone + Debug + Send + Sync + 'static,
{
    pub fn builder(id: String) -> ActorBuilder<T> {
        ActorBuilder::new(id)
    }

    pub async fn start(&self) -> Result<JoinHandle<()>, ActorError> {
        if !self.is_running().await {
            let actor = self.clone();
            Ok(tokio::spawn(async move {
                actor.inner.run().await;
            }))
        } else {
            Err(ActorError::ActorRunning)
        }
    }

    pub async fn is_running(&self) -> bool {
        self.inner.is_running.load(Ordering::SeqCst)
    }

    pub fn id(&self) -> String {
        self.inner.id.clone()
    }

    pub async fn stop(&self) -> bool {
        self.inner.stop().await
    }

    pub async fn send(
        &self,
        target: &str,
        content: T,
        timeout: Option<Duration>,
    ) -> Result<T, ActorError> {
        self.inner.send(target, content, timeout).await
    }

    pub async fn connect(&self, peer_id: String, peer: Peer<T>) -> Result<bool, ActorError> {
        trace!("{}: wire logically connecting to {}", self.id(), peer_id);
        self.inner.connect(peer_id, peer).await
    }

    pub async fn disconnect(&self, peer_id: &str) -> Result<bool, ActorError> {
        trace!(
            "{}: wire logically disconnecting from {}",
            self.id(),
            peer_id
        );
        self.inner.disconnect(peer_id).await
    }

    pub async fn is_connected(&self, peer_id: &str) -> bool {
        let peers = self.inner.peers.read().await;
        peers.contains_key(peer_id)
    }
}

impl<T> ActorInner<T>
where
    T: Clone + Debug + Send + Sync + 'static,
{
    async fn run(&self) {
        let mut stop_rx = self.stop_tx.subscribe();

        info!("{}: actor started", self.id);
        self.is_running.store(true, Ordering::SeqCst);

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
                                "{}: failed to receive message from {}: {}",
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
                            warn!("{}: failed to receive message from any peer", self.id);
                        }
                    }
                }
                _ = stop_rx.changed() => {
                    info!("{}: received stop signal", self.id);
                    break;
                }
            }
        }

        self.stopped_notify.notify_one();
    }

    pub async fn stop(&self) -> bool {
        if self.is_running.load(Ordering::SeqCst) {
            let _ = self.stop_tx.send(());
            self.stopped_notify.notified().await;
            info!("{}: stopped", self.id);
            true
        } else {
            false
        }
    }

    pub(crate) async fn handle_message(&self, peer_id: String, msg: Message<T>) {
        let is_response = self.pending_requests.lock().await.contains_key(&msg.id);
        trace!("{}: is_response: {}", self.id, is_response);
        if is_response {
            info!("{}: received response from {}: {:?}", self.id, peer_id, msg);
            self.handle_response(msg).await;
        } else {
            info!("{}: received request from {}: {:?}", self.id, peer_id, msg);
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
            error!("{}: couldn't find pending request {}", self.id, msg.id);
        }
    }

    async fn send_message(&self, target: String, message: Message<T>) {
        info!("{}: sending message to {}: {:?}", self.id, target, message);
        let peers = self.peers.read().await;
        if let Some(peer) = peers.get(&target) {
            match peer.sender.send(message) {
                Ok(_) => info!("{}: sent message to {}", self.id, target),
                Err(e) => error!("{}: failed to send message to {}: {:?}", self.id, target, e),
            }
        } else {
            error!("{}: couldn't find peer {}", self.id, target);
        }
    }

    pub(crate) async fn send(
        &self,
        target: &str,
        content: T,
        timeout: Option<Duration>,
    ) -> Result<T, ActorError> {
        if target.is_empty() {
            return Err(ActorError::InvalidTarget(target.to_string()));
        }
        {
            let peers = self.peers.read().await;
            if !peers.contains_key(target) {
                // debug all peers
                for (peer_id, _) in peers.iter() {
                    println!("{}: has peer: {}", self.id, peer_id);
                }
                return Err(ActorError::PeerNotConnected(target.to_string()));
            }
        }

        let request_id = Uuid::new_v4();
        info!("{}: sending request to {}: {}", self.id, target, request_id);
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
                    info!("{}: received response for request {}", self.id, request_id);
                    Ok(response)
                }
                Ok(Err(_)) => Err(ActorError::ResponseChannelClosed),
                Err(_) => Err(ActorError::RequestTimeout),
            }
        } else {
            match response_rx.await {
                Ok(response) => {
                    info!("{}: received response for request {}", self.id, request_id);
                    Ok(response)
                }
                Err(_) => Err(ActorError::ResponseChannelClosed),
            }
        }
    }

    /// Connect to another actor logically (no need to be running)
    pub(crate) async fn connect(&self, peer_id: String, peer: Peer<T>) -> Result<bool, ActorError> {
        let mut peers = self.peers.write().await;
        if peers.contains_key(&peer_id) {
            trace!("{}: already connected to peer {}", self.id, peer_id);
            return Ok(false);
        }
        peers.insert(peer_id.clone(), peer);
        trace!("{}: connected to peer {}", self.id, peer_id);
        Ok(true)
    }

    /// Disconnect from another actor logically (no need to be running)
    pub(crate) async fn disconnect(&self, peer_id: &str) -> Result<bool, ActorError> {
        let mut peers = self.peers.write().await;
        if !peers.contains_key(peer_id) {
            trace!("{}: not connected to peer {}", self.id, peer_id);
            return Ok(false);
        }
        peers.remove(peer_id);
        trace!("{}: disconnected from peer {}", self.id, peer_id);
        Ok(true)
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
        actor.start().await.unwrap();
        actor
    }

    async fn connect_actors(actor1: &Arc<Actor<String>>, actor2: &Arc<Actor<String>>) {
        let (tx1, rx1) = broadcast::channel(100);
        let (tx2, rx2) = broadcast::channel(100);
        actor1
            .connect(
                actor2.id(),
                Peer {
                    sender: tx1.clone(),
                    receiver: rx2.resubscribe(),
                },
            )
            .await
            .unwrap();

        actor2
            .connect(
                actor1.id(),
                Peer {
                    sender: tx2.clone(),
                    receiver: rx1.resubscribe(),
                },
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_actor() {
        let _ = env_logger::Builder::new().parse_env("RUST_LOG").try_init();

        let actor1 = create_actor("actor1".to_string()).await;
        let actor2 = create_actor("actor2".to_string()).await;

        connect_actors(&actor1, &actor2).await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        let response = actor1.send("actor2", "Hello".to_string(), None).await;
        assert_eq!(response.unwrap(), "Response from actor2: Processed Hello");
    }
}
