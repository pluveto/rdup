use futures_util::{SinkExt, StreamExt};
use log::info;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{connect_async, WebSocketStream};
use url::Url;

use crate::actor::{self, Actor};

pub struct LeaderActor {
    actor: Arc<Actor>,
}

impl LeaderActor {
    pub async fn new() -> Self {
        LeaderActor {
            actor: Arc::new(Actor::new("leader".to_string())),
        }
    }

    pub async fn run(&self, addr: &str) {
        let listener = TcpListener::bind(addr).await.unwrap();
        println!("Leader listening on: {}", addr);

        while let Ok((stream, _)) = listener.accept().await {
            let ws_stream = tokio_tungstenite::accept_async(stream).await.unwrap();
            self.handle_connection(ws_stream).await;
        }
    }

    async fn handle_connection(&self, mut ws_stream: WebSocketStream<TcpStream>) {
        let peer_id = ws_stream.get_ref().peer_addr().unwrap();
        let (mut ws_sender, mut ws_receiver) = ws_stream.split();
        let (peer_sender, mut peer_receiver) = tokio::sync::broadcast::channel(100);

        self.actor
            .connect(
                peer_id.to_string(),
                actor::Peer {
                    sender: peer_sender.clone(),
                    receiver: peer_receiver.resubscribe(),
                },
            )
            .await;

        loop {
            tokio::select! {
                msg = ws_receiver.next() => {
                    match msg {
                        Some(Ok(msg)) => {
                            let bin_msg = match msg {
                                WsMessage::Binary(bin) => bin,
                                WsMessage::Text(text) => text.as_bytes().to_vec(),
                                _ => continue,
                            };
                            let msg: actor::Message = serde_json::from_slice(&bin_msg).unwrap();
                            peer_sender.send(msg).unwrap();
                        }
                        Some(Err(e)) => {
                            println!("Error receiving message: {:?}", e);
                            break;
                        }
                        None => break,
                    }
                }
                Ok(msg) = peer_receiver.recv() => {
                    let bin_msg = serde_json::to_vec(&msg).unwrap();
                    if let Err(e) = ws_sender.send(WsMessage::Binary(bin_msg)).await {
                        println!("Error sending message: {:?}", e);
                        break;
                    }
                }
            }
        }

        self.actor.disconnect(&peer_id.to_string()).await;
    }
}

pub struct FollowerActor {
    actor: Arc<Actor>,
}

impl FollowerActor {
    pub async fn new() -> Self {
        FollowerActor {
            actor: Arc::new(Actor::new("follower".to_string())),
        }
    }

    pub async fn connect_to_leader(&self, url: &str) {
        println!("Connecting to leader: {}", url);
        let request = url.into_client_request().unwrap();
        let (ws_stream, _) = connect_async(request).await.expect("Failed to connect");

        let (mut ws_sender, mut ws_receiver) = ws_stream.split();
        let (peer_sender, mut peer_receiver) = tokio::sync::broadcast::channel(100);
        self.actor
            .connect(
                "leader".to_string(),
                actor::Peer {
                    sender: peer_sender.clone(),
                    receiver: peer_receiver.resubscribe(),
                },
            )
            .await;

        loop {
            tokio::select! {
                msg = ws_receiver.next() => {
                    match msg {
                        Some(Ok(msg)) => {
                            let bin_msg = match msg {
                                WsMessage::Binary(bin) => bin,
                                WsMessage::Text(text) => text.as_bytes().to_vec(),
                                _ => continue,
                            };
                            let msg: actor::Message = serde_json::from_slice(&bin_msg).unwrap();
                            peer_sender.send(msg).unwrap();
                        }
                        Some(Err(e)) => {
                            println!("Error receiving message: {:?}", e);
                            break;
                        }
                        None => break,
                    }
                }
                Ok(msg) = peer_receiver.recv() => {
                    let bin_msg = serde_json::to_vec(&msg).unwrap();
                    if let Err(e) = ws_sender.send(WsMessage::Binary(bin_msg)).await {
                        println!("Error sending message: {:?}", e);
                        break;
                    }
                }
            }
        }
        self.actor.disconnect("leader").await;
    }
}
// test
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_leader() {
        env_logger::Builder::new()
            .filter(None, log::LevelFilter::Info)
            .parse_env("RUST_LOG")
            .init();

        // Run as leader
        let leader = LeaderActor::new().await;
        let leader_task = tokio::spawn(async move {
            leader.run("127.0.0.1:8080").await;
        });

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        // Or run as regular actor
        let regular = FollowerActor::new().await;
        let regular_task = tokio::spawn(async move {
            regular.connect_to_leader("ws://127.0.0.1:8080").await;
        });

        let _ = tokio::join!(leader_task, regular_task);
    }
}
