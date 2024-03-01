use std::path::PathBuf;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};
use async_trait::async_trait;
use tokio::sync::oneshot;
use sui_data_ingestion_core::{DataIngestionMetrics, ProgressStore, IndexerExecutor, Worker, WorkerPool};
use sui_types::full_checkpoint_content::CheckpointData;
use anyhow::Result;
use sui_types::base_types::ObjectID;
use prometheus::{Registry};
use sui_types::messages_checkpoint::CheckpointSequenceNumber;
use mongodb::{bson::doc, Client, Cursor};
use mongodb::options::{InsertOneOptions};
use serde::{Deserialize, Serialize};
use bcs;
use clap::Parser;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use libc;
use fred::prelude::*;
use fred::prelude::ServerConfig::Centralized;
use fred::types::RespVersion;

pub mod events;

#[derive(Parser)]
struct Cli {
    #[arg(short,long, default_value = "mongodb://localhost:27017")]
    mongo_uri: String,
    #[arg(short,long, default_value = "indexer")]
    db_name: String

}

struct CustomWorker {
    // TODO add names to objects
    ids: Vec<ObjectID>,
    mongodb_client: Client,
}

struct DummyProgressStore{
    redis_client: RedisClient,
}

#[derive(Serialize)]
struct Progress {
    checkpoint: i64,
    _id: i64,
    name: String,
}

#[async_trait]
impl ProgressStore for DummyProgressStore {
    async fn load(&mut self, task_name: String) -> Result<CheckpointSequenceNumber> {
        let result = self.redis_client.get(0_u64).await;
        return Ok(result.unwrap_or(0_u64));
        // let mut file = OpenOptions::new().read(true).open("/tmp/checkpoint").await;
        // if file.is_err() {
        //     return Ok(0);
        // } else {
        //     let mut buffer = [0_u8; 8];
        //     file.unwrap().read(&mut buffer).await;
        //     return Ok(u64::from_be_bytes(buffer))
        // }
        // return Ok(0);
        // let mut cursor: Cursor<Progress> = self.mongodb_client.database("indexer").collection("progress").find(None, None).await.unwrap();
        // while cursor.advance().await.unwrap_or(false) {
        //     return Ok(cursor.current().get("checkpoint").unwrap().unwrap().as_i64().unwrap() as u64)
        // }
        return Ok(0);
    }

    async fn save(
        &mut self,
        task_name: String,
        checkpoint_number: CheckpointSequenceNumber,
    ) -> Result<()> {
        self.redis_client.set::<u64, u64, u64>(0_u64, checkpoint_number, None, None, false).await;
        return Ok(());
        // let mut file = OpenOptions::new().write(true).custom_flags(libc::O_CREAT).custom_flags(libc::O_EXCL).open("/tmp/checkpoint").await.unwrap();
        // file.write_all(&checkpoint_number.to_be_bytes()).await;
        // return Ok(())
        // TODO too slow
        // let _opts = InsertOneOptions::builder()
        //     .bypass_document_validation(true)
        //     .build();
        // let data = Progress{ checkpoint: checkpoint_number as i64, _id: 1, name: task_name };
        // self.mongodb_client.database("indexer").collection::<Progress>("progress").delete_one(doc! {"_id":1}, None).await;
        // self.mongodb_client.database("indexer").collection::<Progress>("progress").insert_one(data, _opts ).await;
        // Ok(())
    }
}

#[derive(Serialize)]
struct Point {
    data: Vec<u8>,
    digest: String,
    timestamp: u64,
    index: usize
}

#[async_trait]
impl Worker for CustomWorker {
    async fn process_checkpoint(&self, checkpoint: CheckpointData) -> Result<()>{
        // custom processing logic
        for item in checkpoint.transactions {
            // println!("{}", item.transaction.digest());
            let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
            for events in item.events.iter() {
                for (idx, event) in events.data.iter().enumerate() {
                    unsafe {
                        if self.ids.contains(&event.package_id){
                            let data = Point{
                                data: event.clone().contents,
                                digest: item.transaction.digest().to_string(),
                                timestamp: t,
                                index: idx,
                            };
                            // TODO _id derive from data ...
                            self.mongodb_client.database("indexer").collection("events").insert_one(data, None).await;
                        }
                    }
                }
            }
        };
            Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()>{
    let cli = Cli::parse();
    let (exit_sender, exit_receiver) = oneshot::channel();
    let metrics = DataIngestionMetrics::new(&Registry::new());
    // let progress_store = DummyProgressStore{mongodb_client: Client::with_uri_str(cli.mongo_uri.clone()).await.unwrap()}; // FileProgressStore::new(PathBuf::from("/tmp/checkpoint"));
    let client = RedisClient::new(RedisConfig{
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
    }, Some(PerformanceConfig::default()), Some(ConnectionConfig::default()), Some(ReconnectPolicy::default()));
    client.connect();
    client.wait_for_connect().await;
    // client.init().await;
    let progress_store = DummyProgressStore{ redis_client: client};
    // let progress_store = DummyProgressStore;
    let custom_worker = CustomWorker{ ids: vec![ObjectID::from_str("0xefe8b36d5b2e43728cc323298626b83177803521d195cfb11e15b910e892fddf").unwrap(),
    ObjectID::from_str("0xc38f849e81cfe46d4e4320f508ea7dda42934a329d5a6571bb4c3cb6ea63f5da").unwrap(),
    ], mongodb_client: Client::with_uri_str(cli.mongo_uri).await.unwrap() };
    let mut executor = IndexerExecutor::new(progress_store, 1 /* number of workflow types */, metrics);
    let worker_pool = WorkerPool::new(custom_worker, "custom worker".to_string(), 100);
    executor.register(worker_pool).await?;
    executor.run(
        PathBuf::from("/mnt/sui/ingestion"), // path to a local directory
        Some("https://checkpoints.mainnet.sui.io".to_string()),
        vec![], // optional remote store access options
        1,
        exit_receiver,
    ).await?;
    Ok(())
}
