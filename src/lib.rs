pub mod actor;
pub mod transport;
/*
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

*/
