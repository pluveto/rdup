use futures_util::{SinkExt, StreamExt};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

use crate::actor::{self, Actor, ActorBuilder};

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

    pub async fn run(&self, addr: &str) {
        let listener = TcpListener::bind(addr).await.unwrap();
        println!("Leader listening on: {}", addr);

        let actor_clone = self.actor.clone();
        tokio::spawn(async move {
            actor_clone.run().await;
        });

        while let Ok((stream, _)) = listener.accept().await {
            let peer_addr = stream.peer_addr().unwrap().to_string();
            let ws_stream = tokio_tungstenite::accept_async(stream).await.unwrap();
            FollowerActor::handle_connection(self.actor.clone(), ws_stream, peer_addr).await;
        }
    }
}

pub struct FollowerActor<T> {
    actor: Arc<Actor<T>>,
}

impl<T> FollowerActor<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + Debug + Clone + 'static
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
        }
    }

    pub async fn connect_to_leader(&self, url: &str) {
        println!("Connecting to leader: {}", url);
        let request = url.into_client_request().unwrap();
        let (ws_stream, _) = connect_async(request).await.expect("Failed to connect");
        Self::handle_connection(self.actor.clone(), ws_stream, "leader".to_string()).await;
    }

    async fn handle_connection<S>(
        actor: Arc<Actor<T>>,
        ws_stream: WebSocketStream<S>,
        peer_id: String,
    ) where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let (mut ws_sender, mut ws_receiver) = ws_stream.split();
        let (peer_sender, mut peer_receiver) = tokio::sync::broadcast::channel(100);

        actor
            .connect(
                peer_id.clone(),
                actor::Peer {
                    sender: peer_sender.clone(),
                    receiver: peer_receiver.resubscribe(),
                },
            )
            .await;

        loop {
            tokio::select! {
                msg = ws_receiver.next() => {
                    if let Some(Ok(msg)) = msg {
                        if let Some(bin_msg) = Self::extract_binary_message(msg) {
                            let msg: actor::Message<T> = serde_json::from_slice(&bin_msg).unwrap();
                            peer_sender.send(msg).unwrap();
                        }
                    } else {
                        break;
                    }
                }
                Ok(msg) = peer_receiver.recv() => {
                    let bin_msg = serde_json::to_vec(&msg).unwrap();
                    if ws_sender.send(WsMessage::Binary(bin_msg)).await.is_err() {
                        break;
                    }
                }
            }
        }

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
    use super::*;

    #[tokio::test]
    async fn test_leader() {
        env_logger::Builder::new()
            .filter(None, log::LevelFilter::Trace)
            .parse_env("RUST_LOG")
            .init();

        let leader = LeaderActor::new(|msg, _| format!("Leader processed: {}", msg.data)).await;
        let leader_task = tokio::spawn(async move {
            leader.run("127.0.0.1:8080").await;
        });

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let regular =
            FollowerActor::new(|msg, _| format!("Follower processed: {}", msg.data)).await;
        let regular_task = tokio::spawn(async move {
            regular.connect_to_leader("ws://127.0.0.1:8080").await;
        });

        let _ = tokio::join!(leader_task, regular_task);
    }
}
