use std::sync::Arc;
use fred::clients::RedisClient;
use fred::cmd;
use fred::interfaces::SetsInterface;
use fred::prelude::{Blocking, ClientLike, ConnectionConfig, KeysInterface, PerformanceConfig, ReconnectPolicy, RedisConfig, Server};
use fred::prelude::ServerConfig::Centralized;
use fred::types::RespVersion;
use log::{debug, LevelFilter};
use sui_indexer::events::IndexerData;

#[tokio::test]
async fn migrate_test(){
    env_logger::builder().filter_level(LevelFilter::Debug).init();
    let client = Arc::new(RedisClient::new(RedisConfig{
        fail_fast: false,
        blocking: Blocking::Interrupt,
        username: None,
        password: None,
        server: Centralized{
            server: Server {
                host: "localhost".parse().unwrap(),
                port: 6379,
            }
        },
        version: RespVersion::RESP2,
        database: Some(0),
    }, Some(PerformanceConfig::default()), Some(ConnectionConfig::default()), Some(ReconnectPolicy::default())));
    // let _ = runtime.block_on(async {
    //     let _ = client.connect();
    //     client.wait_for_connect().await
    // });
    let _ = client.connect();
    client.wait_for_connect().await;
    let keys: Vec<String> = client.custom(cmd!("KEYS"), vec!["*"]).await.unwrap();
    debug!("got: {}", keys.len());
    // let with_id = vec!["BorrowEvent", "BorrowEventV2"];
    for key in keys.iter() {
        let value: String = client.get(key).await.unwrap();
        let indexer_data: IndexerData = serde_json::from_str(&*value).unwrap();
        let digest = indexer_data.digest;
        let data = indexer_data.clone();
            let event = data.parse_event();
            let result = serde_json::to_string(&data).unwrap();
            // more events can have same digest ... with index is unique
            let mut digest_modified = format!("{}::{}::{}", data.checkpoint, digest, data.index);
            if event.is_some() {
                let (_, event_name, obligation_id) = event.unwrap();
                // digest_modified.push_str("::");
                // digest_modified.push_str(&event_name);
                if obligation_id.is_some() {
                    let id = obligation_id.unwrap();
                    client.sadd(id.clone(), result).await;
                    client.sadd("ids", id).await;
                    let mut events_set = "events_".to_string();
                    events_set.push_str(&*data.type_);
                    client.sadd(events_set, digest_modified.clone()).await;
                    debug!("inserting obligations");
                    continue;
                    // digest_modified.push_str("::");
                    // digest_modified.push_str(&id);
                }
            }
            // store keys in set
            let mut events_set = "events_".to_string();
            events_set.push_str(&*data.type_);
            client.sadd(events_set, digest_modified.clone()).await;
            client.set::<String, String, String>(digest_modified, result, None, None, false).await;
            debug!("inserting data: {}", digest);
        client.del(key).await;
    }
}