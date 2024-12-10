use futures_util::{SinkExt, StreamExt};
use log::{info, trace, warn};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{oneshot, Mutex, Notify};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{connect_async, WebSocketStream};

use crate::actor::{self, Actor, ActorBuilder};

#[derive(Clone)]
pub struct LeaderActor<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + Debug + Clone + 'static,
{
    actor: Arc<Actor<T>>,
}

impl<T> LeaderActor<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + Debug + Clone + 'static,
{
    pub async fn new(
        request_handler: impl Fn(actor::Message<T>, String) -> T + Send + Sync + 'static,
    ) -> Self {
        LeaderActor {
            actor: Arc::new(
                ActorBuilder::new("leader".to_string())
                    .with_request_handler(request_handler)
                    .build(),
            ),
        }
    }

    pub async fn start(&self, addr: &str) {
        let listener = TcpListener::bind(addr).await.unwrap();

        self.actor.start().await.expect("Failed to start actor");
        let self_clone = self.clone();
        tokio::spawn(async move { self_clone.run_listener(listener).await });
        info!("{}: started", self.actor.id());
    }

    async fn run_listener(self, listener: TcpListener) {
        let id = self.actor.id();
        info!(
            "{}: listening on: {}",
            self.actor.id(),
            listener.local_addr().unwrap()
        );
        while let Ok((stream, _)) = listener.accept().await {
            let peer_addr = stream.peer_addr().unwrap().to_string();
            trace!("{}: new connection from: {}", id, peer_addr);
            let ws_stream = tokio_tungstenite::accept_async(stream).await.unwrap();
            let actor_clone = self.actor.clone();

            let id_cloned = id.clone();
            tokio::spawn(async move {
                let (disconnect_tx, disconnect_rx) = tokio::sync::oneshot::channel();

                FollowerActor::on_connected(
                    actor_clone,
                    ws_stream,
                    peer_addr.clone(),
                    None,
                    disconnect_rx,
                )
                .await;

                warn!("{}: Follower disconnected: {}", id_cloned, peer_addr);
                disconnect_tx
                    .send(())
                    .expect("Failed to send disconnect signal");
            });
        }
    }

    pub async fn stop(&self) {
        self.actor.stop().await;
    }

    pub async fn peer_ids(&self) -> Vec<String> {
        self.actor.peer_ids().await
    }
}

#[derive(Clone)]
pub struct FollowerActor<T> {
    actor: Arc<Actor<T>>,
    busy: Arc<AtomicBool>,
    disconnect_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

impl<T> FollowerActor<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + Debug + Clone + 'static,
{
    pub async fn new(
        request_handler: impl Fn(actor::Message<T>, String) -> T + Send + Sync + 'static,
    ) -> Self {
        FollowerActor {
            actor: Arc::new(
                ActorBuilder::new("follower".to_string())
                    .with_request_handler(request_handler)
                    .build(),
            ),
            busy: Arc::new(AtomicBool::new(false)),
            disconnect_tx: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn is_connected(&self) -> bool {
        self.actor.is_connected("leader").await
    }

    pub async fn follow(&self, url: &str) -> Result<bool, anyhow::Error> {
        if self.busy.load(Ordering::SeqCst) {
            return Err(anyhow::anyhow!("Actor is busy"));
        }
        self.busy.store(true, Ordering::SeqCst);
        {
            if self.is_connected().await {
                return Ok(false);
            }

            trace!("{}: connecting to leader: {}", self.actor.id(), url);
            let request = url.into_client_request().unwrap();
            let (ws_stream, _) = connect_async(request).await.expect("Failed to connect");
            let (disconnect_tx, disconnect_rx) = oneshot::channel();
            assert!(self.disconnect_tx.lock().await.is_none());
            self.disconnect_tx.lock().await.replace(disconnect_tx);

            let actor = self.actor.clone();
            let connected_notify = Arc::new(Notify::new());
            let connected_notify_cloned = connected_notify.clone();

            trace!("{}: waiting ws stream", self.actor.id());
            tokio::spawn(async move {
                Self::on_connected(
                    actor,
                    ws_stream,
                    "leader".to_string(),
                    Some(connected_notify_cloned),
                    disconnect_rx,
                )
                .await;
            });
            connected_notify.notified().await;
        }
        self.busy.store(false, Ordering::SeqCst);
        Ok(true)
    }

    pub async fn unfollow(&self) -> Result<bool, anyhow::Error> {
        if self.busy.load(Ordering::SeqCst) {
            return Err(anyhow::anyhow!("Actor is busy"));
        }
        self.busy.store(true, Ordering::SeqCst);
        {
            if !self.is_connected().await {
                return Ok(false);
            }

            if let Some(disconnect_tx) = self.disconnect_tx.lock().await.take() {
                let _ = disconnect_tx.send(());
            }

            self.actor.disconnect("leader").await.unwrap();
        }
        self.busy.store(false, Ordering::SeqCst);
        Ok(true)
    }

    async fn on_connected<S>(
        actor: Arc<Actor<T>>,
        ws_stream: WebSocketStream<S>,
        peer_id: String,
        connected_notify: Option<Arc<Notify>>,
        mut disconnect_rx: oneshot::Receiver<()>,
    ) where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        trace!("{}: on_connected", actor.id());
        let (mut ws_sink, mut ws_source) = ws_stream.split();

        let (peer_source_tx, peer_source_rx) = tokio::sync::broadcast::channel(100);
        let (peer_sink_tx, mut peer_sink_rx) = tokio::sync::broadcast::channel(100);

        actor
            .connect(
                peer_id.clone(),
                actor::Peer {
                    sender: peer_sink_tx.clone(),
                    receiver: peer_source_rx.resubscribe(),
                },
            )
            .await
            .unwrap();

        if let Some(connected_notify) = connected_notify {
            connected_notify.notify_one();
        }

        trace!("{}: stream connected to leader: {}", actor.id(), peer_id);
        loop {
            tokio::select! {
                msg = ws_source.next() => {
                    if let Some(Ok(msg)) = msg {
                        if let Some(bin_msg) = Self::extract_binary_message(msg) {
                            let msg: actor::Message<T> = serde_json::from_slice(&bin_msg).unwrap();
                            peer_source_tx.send(msg).unwrap();
                        }
                    } else {
                        trace!("{}: websocket connection closed from {}", actor.id(), peer_id);
                        break;
                    }
                }
                Ok(msg) = peer_sink_rx.recv() => {
                    let bin_msg = serde_json::to_vec(&msg).unwrap();
                    let ret = ws_sink.send(WsMessage::Binary(bin_msg)).await;
                    if ret.is_err() {
                        trace!("{}: failed to send message to {}", actor.id(), peer_id);
                        break;
                    }
                }
                _ = &mut disconnect_rx => {
                    trace!("{}: disconnect signal received", actor.id());
                    break;
                }
            }
        }

        // Close the WebSocket connection
        let _ = ws_sink.close().await;

        actor.disconnect(&peer_id).await.unwrap();
    }

    fn extract_binary_message(msg: WsMessage) -> Option<Vec<u8>> {
        match msg {
            WsMessage::Binary(bin) => Some(bin),
            WsMessage::Text(text) => Some(text.into_bytes()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use log::debug;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use super::*;

    async fn find_free_port() -> u16 {
        let addr = SocketAddr::from((IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0));
        let listener = std::net::TcpListener::bind(addr).unwrap();

        listener.local_addr().unwrap().port()
    }

    #[tokio::test]
    async fn test_one_leader_one_follower() {
        let _ = env_logger::Builder::new().parse_env("RUST_LOG").try_init();
        let port = find_free_port().await;

        debug!("Starting test_leader");
        let leader = LeaderActor::new(|msg, _| format!("Leader processed: {}", msg.data)).await;
        leader.start(&format!("127.0.0.1:{port}")).await;

        let regular =
            FollowerActor::new(|msg, _| format!("Follower processed: {}", msg.data)).await;
        regular.actor.start().await.unwrap();

        regular
            .follow(&format!("ws://127.0.0.1:{port}"))
            .await
            .unwrap();

        trace!("Sending message to leader");
        let reply = regular
            .actor
            .send("leader", "Hello".to_string(), None)
            .await
            .unwrap();

        assert_eq!("Leader processed: Hello", reply);
        leader.stop().await;
    }

    #[tokio::test]
    async fn test_multiple_followers() {
        let _ = env_logger::Builder::new().parse_env("RUST_LOG").try_init();
        let port = find_free_port().await;

        debug!("Starting test_multiple_followers");
        let leader = LeaderActor::new(|msg, _| format!("Leader processed: {}", msg.data)).await;
        leader.start(&format!("127.0.0.1:{port}")).await;

        let follower_count = 2;
        let mut followers = Vec::new();

        for i in 0..follower_count {
            let follower =
                FollowerActor::new(move |msg, _| format!("Follower {} processed: {}", i, msg.data))
                    .await;
            follower.actor.start().await.unwrap();

            follower
                .follow(&format!("ws://127.0.0.1:{port}"))
                .await
                .unwrap();

            followers.push(follower);
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

        trace!("Sending messages to leader from followers");
        for (i, f) in followers.iter().enumerate().take(follower_count) {
            let reply = f
                .actor
                .send("leader", format!("Hello from follower {}", i), None)
                .await
                .unwrap();

            assert_eq!(
                format!("Leader processed: Hello from follower {}", i),
                reply
            );
        }

        leader.stop().await;
    }

    #[tokio::test]
    async fn test_leader_sends_to_follower() {
        let _ = env_logger::Builder::new().parse_env("RUST_LOG").try_init();
        let port = find_free_port().await;

        debug!("Starting test_leader_sends_to_follower");
        let leader = LeaderActor::new(|msg, _| format!("Leader processed: {}", msg.data)).await;
        leader.start(&format!("127.0.0.1:{port}")).await;

        let follower =
            FollowerActor::new(|msg, _| format!("Follower processed: {}", msg.data)).await;
        follower.actor.start().await.unwrap();

        follower
            .follow(&format!("ws://127.0.0.1:{port}"))
            .await
            .unwrap();

        trace!("Leader sending message to follower");
        let reply = leader
            .actor
            .send(
                &leader.peer_ids().await[0].clone(),
                "Hello from leader".to_string(),
                None,
            )
            .await
            .unwrap();

        assert_eq!("Follower processed: Hello from leader", reply);
        leader.stop().await;
    }

    use std::sync::Arc;
    use tokio::sync::Semaphore;
    use tokio::task;

    #[tokio::test]
    async fn fuzz_test_leader_follower_concurrency() {
        let _ = env_logger::Builder::new().parse_env("RUST_LOG").try_init();
        let port = find_free_port().await;

        debug!("Starting fuzz_test_leader_follower_concurrency");

        let leader = LeaderActor::new(|msg, _| format!("Leader processed: {}", msg.data)).await;
        leader.start(&format!("127.0.0.1:{port}")).await;

        let follower =
            FollowerActor::new(|msg, _| format!("Follower processed: {}", msg.data)).await;
        follower.actor.start().await.unwrap();

        follower
            .follow(&format!("ws://127.0.0.1:{port}"))
            .await
            .unwrap();

        let concurrency = 100; // Number of concurrent tasks
        let semaphore = Arc::new(Semaphore::new(concurrency));

        let mut handles = vec![];

        for i in 0..concurrency {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let leader = leader.clone();
            let handle = task::spawn(async move {
                let reply = leader
                    .actor
                    .send(
                        &leader.peer_ids().await[0].clone(),
                        format!("Hello from leader {}", i),
                        None,
                    )
                    .await
                    .unwrap();

                assert_eq!(
                    format!("Follower processed: Hello from leader {}", i),
                    reply
                );
                drop(permit); // Release the permit
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        leader.stop().await;
    }
}
