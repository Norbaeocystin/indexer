use std::str::FromStr;
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration};
use sui_types::base_types::ObjectID;
use serde::{Deserialize, Serialize};
use bcs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use fred::prelude::*;
use fred::prelude::ServerConfig::Centralized;
use fred::types::RespVersion;
use log::{debug, info, LevelFilter};
use reader::CheckpointReader;
use clap::Parser;
use tokio::runtime::Builder;
use crate::events::process_txn;

pub mod events;
pub mod reader;

#[derive(Parser)]
struct Cli {
    #[arg(short,long, default_value = "/mnt/sui/ingestion")]
    path: String,
    #[arg(short, long, action)]
    debug: bool,
    #[arg(short,long, default_value_t=0)]
    start: u64,
}

fn main(){
    let filter = vec![ObjectID::from_str("0xefe8b36d5b2e43728cc323298626b83177803521d195cfb11e15b910e892fddf").unwrap(),
                      ObjectID::from_str("0xc38f849e81cfe46d4e4320f508ea7dda42934a329d5a6571bb4c3cb6ea63f5da").unwrap(),
    ];
    let cli = Cli::parse();
    if cli.debug {
        env_logger::builder().filter_level(LevelFilter::Debug).init();
    } else {
        env_logger::builder().filter_level(LevelFilter::Info).init();
    }
    let runtime = Builder::new_multi_thread().enable_all().worker_threads(8).build().unwrap();
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
    let _ = client.connect();
    let _ = runtime.block_on(async {
        client.wait_for_connect().await
    });
    let mut reader = CheckpointReader{ path: "/mnt/sui/ingestion".parse().unwrap(), current_checkpoint_number: cli.start };
    loop {
        let files = reader.read_all_files();
        if files.len() > 0 {
            sleep(Duration::from_millis(100));
            debug!("No files to process ...");
            continue
        }
        for (number, path) in files.iter() {
            // TODO process
            let checkpoint_data = reader.read_checkpoint(path).unwrap();
            let result = process_txn(&checkpoint_data, &filter);
            if result.len() > 0 {
                // TODO send to redis or mongodb via crossbeam channel
                runtime.spawn({
                    let client = client.clone();
                    async move {
                        for (digest, data) in result {
                            let result = serde_json::to_string(&data).unwrap();
                            client.set::<String, String, String>(digest.clone(), result, None, None, false).await;
                            debug!("inserting data: {}", digest);
                        }
                        // return client.connect().await.unwrap();
                    }});
            }
            // TODO progressor
            runtime.spawn({
            let client = client.clone();
                let checkpoint_number = number.clone();
            async move {
                info!("conecting redis client");
                client.set::<u64, u64, u64>(0_u64, checkpoint_number, None, None, false).await;
                // return client.connect().await.unwrap();
            }});
            reader.gc_processed_files(reader.current_checkpoint_number);
            debug!("checkpoint processed: {}", reader.current_checkpoint_number);
            reader.current_checkpoint_number = number.clone();
        }
    }
}
