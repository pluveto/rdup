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

    pub async fn start(&self, addr: &str, ready_notify: Arc<Notify>) {
        let listener = TcpListener::bind(addr).await.unwrap();
        info!("Leader listening on: {}", addr);

        let actor_clone = self.actor.clone();
        tokio::spawn(async move {
            actor_clone.start().await;
        });

        ready_notify.notify_one();
        info!("Leader ready");

        while let Ok((stream, _)) = listener.accept().await {
            let peer_addr = stream.peer_addr().unwrap().to_string();
            trace!("New connection from: {}", peer_addr);
            let ws_stream = tokio_tungstenite::accept_async(stream).await.unwrap();
            let actor_clone = self.actor.clone();

            tokio::spawn(async move {
                let (disconnect_tx, disconnect_rx) = tokio::sync::oneshot::channel();
                let stop_signal = Arc::new(std::sync::atomic::AtomicBool::new(false));

                FollowerActor::on_connected(
                    actor_clone,
                    ws_stream,
                    peer_addr.clone(),
                    None,
                    stop_signal,
                    disconnect_rx,
                )
                .await;

                warn!("Follower disconnected: {}", peer_addr);
                disconnect_tx
                    .send(())
                    .expect("Failed to send disconnect signal");
            });
        }
    }

    pub async fn stop(&self) {
        self.actor.stop().await;
    }
}

#[derive(Clone)]
pub struct FollowerActor<T> {
    actor: Arc<Actor<T>>,
    stop_signal: Arc<AtomicBool>,
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

            stop_signal: Arc::new(AtomicBool::new(false)),
            disconnect_tx: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn follow(&self, url: &str, connected_notify: Arc<Notify>) {
        println!("Connecting to leader: {}", url);
        let request = url.into_client_request().unwrap();
        let (ws_stream, _) = connect_async(request).await.expect("Failed to connect");
        let (disconnect_tx, disconnect_rx) = oneshot::channel();
        self.disconnect_tx.lock().await.replace(disconnect_tx);

        Self::on_connected(
            self.actor.clone(),
            ws_stream,
            "leader".to_string(),
            Some(connected_notify),
            self.stop_signal.clone(),
            disconnect_rx,
        )
        .await;
    }
    pub async fn unfollow(&self) {
        self.stop_signal.store(true, Ordering::SeqCst);

        if let Some(disconnect_tx) = self.disconnect_tx.lock().await.take() {
            let _ = disconnect_tx.send(());
        }

        self.actor.disconnect("leader").await;
    }

    async fn on_connected<S>(
        actor: Arc<Actor<T>>,
        ws_stream: WebSocketStream<S>,
        peer_id: String,
        connected_notify: Option<Arc<Notify>>,
        stop_signal: Arc<AtomicBool>,
        mut disconnect_rx: oneshot::Receiver<()>,
    ) where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
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
            .await;

        if let Some(connected_notify) = connected_notify {
            connected_notify.notify_one();
        }

        loop {
            tokio::select! {
                msg = ws_source.next() => {
                    if let Some(Ok(msg)) = msg {
                        if let Some(bin_msg) = Self::extract_binary_message(msg) {
                            let msg: actor::Message<T> = serde_json::from_slice(&bin_msg).unwrap();
                            peer_source_tx.send(msg).unwrap();
                        }
                    } else {
                        trace!("Websocket connection closed: {:?}", msg);
                        break;
                    }
                }
                Ok(msg) = peer_sink_rx.recv() => {
                    let bin_msg = serde_json::to_vec(&msg).unwrap();
                    let ret = ws_sink.send(WsMessage::Binary(bin_msg)).await;
                    if ret.is_err() {
                        trace!("Failed to send message to peer: {}", ret.unwrap_err());
                        break;
                    }
                }
                _ = &mut disconnect_rx => {
                    trace!("Disconnect signal received");
                    break;
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                    if stop_signal.load(Ordering::SeqCst) {
                        trace!("Stop signal received");
                        break;
                    }
                }
            }
        }

        // Close the WebSocket connection
        let _ = ws_sink.close().await;

        actor.disconnect(&peer_id).await;
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

    use super::*;

    #[tokio::test]
    async fn test_leader() {
        let _ = env_logger::Builder::new().parse_env("RUST_LOG").try_init();

        debug!("Starting test_leader");
        let leader = LeaderActor::new(|msg, _| format!("Leader processed: {}", msg.data)).await;
        {
            let leader_ready = Arc::new(Notify::new());
            let leader_ready_cloned = leader_ready.clone();
            let leader_cloned = leader.clone();
            let _ = tokio::spawn(async move {
                leader_cloned.start("127.0.0.1:8080", leader_ready).await;
            });

            debug!("Waiting for leader to be ready");
            leader_ready_cloned.notified().await;
            debug!("Leader is ready");
        }
        let regular =
            FollowerActor::new(|msg, _| format!("Follower processed: {}", msg.data)).await;
        {
            let regular_cloned = regular.clone();
            let _ = tokio::spawn(async move {
                regular_cloned.actor.start().await;
            });
            let regular_cloned = regular.clone();
            let connected_notify = Arc::new(Notify::new());
            let connected_notify_cloned = connected_notify.clone();
            let _ = tokio::spawn(async move {
                regular_cloned
                    .follow("ws://127.0.0.1:8080", connected_notify)
                    .await;
            });

            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

            connected_notify_cloned.notified().await;
        }

        trace!("Sending message to leader");
        let reply = regular
            .actor
            .send("leader", "Hello".to_string(), None)
            .await
            .unwrap();

        assert_eq!("Leader processed: Hello", reply);

        leader.stop().await;
    }
}
