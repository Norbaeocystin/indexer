use std::sync::Arc;
use fred::clients::RedisClient;
use fred::cmd;
use fred::interfaces::SetsInterface;
use fred::prelude::{Blocking, ClientLike, ConnectionConfig, KeysInterface, PerformanceConfig, ReconnectPolicy, RedisConfig, Server};
use fred::prelude::ServerConfig::Centralized;
use fred::types::RespVersion;
use log::{debug, LevelFilter};
use sui_indexer::events::IndexerData;

#[tokio::main]
async fn main(){
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
    for (idx, key) in keys.iter().enumerate() {
        let value: String = client.get(key).await.unwrap();
        let indexer_data: IndexerData = serde_json::from_str(&*value).unwrap();
        let digest = indexer_data.digest.clone();
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
                let mut id_set = "id_".to_string();
                id_set.push_str(&*id);
                // stores digest modified key in id_{obligation_id}
                client.sadd::<String,String,String>(id_set, result).await;
                // stores obligation_id in ids set ...
                client.sadd::<String,String,String>("ids".to_string(), id).await;
                let mut events_set = "events".to_string();
                events_set.push_str(&*data.type_);
                client.sadd::<String,String,String>(events_set, digest_modified.clone()).await;
                debug!("inserting obligations");
                client.del::<String, String>(key.clone()).await;
                continue;
            }
        }
        // store keys in set
        let mut events_set = "events_".to_string();
        events_set.push_str(&*data.type_);
        // stores digest modified key in events_{event type} query
        client.sadd::<String,String,String>(events_set, digest_modified.clone()).await;
        // stores event data as value with modified digest as key
        client.set::<String, String, String>(digest_modified, result, None, None, false).await;
        debug!("inserting data: {} {}", digest, idx);
        client.del::<String, String>(key.clone()).await;
    }
}