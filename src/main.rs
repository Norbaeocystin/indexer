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
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use libc;
use fred::prelude::*;
use fred::prelude::ServerConfig::Centralized;
use fred::types::RespVersion;
use log::{info, LevelFilter};
use reader::CheckpointReader;
use clap::Parser;
use clap::ArgAction;

pub mod events;
pub mod reader;

#[derive(Parser)]
struct Cli {
    #[arg(short,long, default_value = "/mnt/sui/ingestion")]
    path: String,
    #[arg(short, long, action)]
    debug: bool,

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

fn main(){
    let cli = Cli::parse();
    if cli.debug {
        env_logger::builder().filter_level(LevelFilter::Debug).init();
    } else {
        env_logger::builder().filter_level(LevelFilter::Info).init();
    }
    let reader = CheckpointReader{ path: "/mnt/sui/ingestion".parse().unwrap(), current_checkpoint_number: 0 };
    let files = reader.read_local_files(10_000).unwrap();
    // info!()
}
