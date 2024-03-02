use std::str::FromStr;
use std::sync::Arc;
use std::fs::remove_file;
use std::thread;
use std::thread::sleep;
use std::time::{Duration};
use sui_types::base_types::ObjectID;
use serde::{Deserialize, Serialize};
use bcs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use fred::prelude::*;
use fred::prelude::ServerConfig::Centralized;
use fred::types::RespVersion;
use log::{debug, info, LevelFilter, warn};
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
    #[arg(short,long, default_value_t=0)]
    db: u8,
}

fn main(){
    info!("starting indexer");
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
        database: Some(cli.db),
    }, Some(PerformanceConfig::default()), Some(ConnectionConfig::default()), Some(ReconnectPolicy::default())));
    let _ = runtime.block_on(async {
        let _ = client.connect();
        client.wait_for_connect().await
    });
    info!("preparing redis done");
    let mut reader = CheckpointReader{ path: cli.path.parse().unwrap(), current_checkpoint_number: cli.start };
    loop {
        debug!("fetching file");
        // race condition?
        let file_response = reader.read_local_files();
        if file_response.is_err() {
            sleep(Duration::from_millis(100));
            warn!("something bad happened");
            continue;
        }
        let checkpoints = file_response.unwrap();
        if checkpoints.len() == 0 {
            sleep(Duration::from_millis(100));
            debug!("No files to process ...");
            continue
        }
        for checkpoint_data in checkpoints.iter() {
            // TODO process
            let number = checkpoint_data.checkpoint_summary.sequence_number.clone();
            // let checkpoint_data = reader.read_checkpoint(path).unwrap();
            let result = process_txn(&checkpoint_data, &filter);
            if result.len() > 0 {
                // TODO send to redis or mongodb via crossbeam channel
                runtime.spawn({
                    let client = client.clone();
                    async move {
                        for (digest, data) in result {
                            let event = data.parse_event();
                            let result = serde_json::to_string(&data).unwrap();
                            // more events can have same digest ... with index is unique
                            let mut digest_modified = format!("{}::{}::{}", data.checkpoint, digest, data.index);
                            if event.is_some() {
                                let (_, event_name, obligation_id) = event.unwrap();
                                digest_modified.push_str("::");
                                digest_modified.push_str(&event_name);
                                if obligation_id.is_some() {
                                    let id = obligation_id.unwrap();
                                    digest_modified.push_str("::");
                                    digest_modified.push_str(&id);
                                }
                            }
                            client.set::<String, String, String>(digest_modified, result, None, None, false).await;
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
                debug!("conecting redis client");
                client.set::<u64, u64, u64>(0_u64, checkpoint_number, None, None, false).await;
                // return client.connect().await.unwrap();
            }});
            println!("checkpoint {}", number);
            let file_path = format!("{}/{}.chk", cli.path, reader.current_checkpoint_number );
            remove_file(file_path);
            reader.current_checkpoint_number = number;
        }
        let number = reader.current_checkpoint_number.clone();
        let path = reader.path.clone();
        thread::spawn( move ||
            {
            CheckpointReader::gc_processed_files(number, path);
        });
    }
}
