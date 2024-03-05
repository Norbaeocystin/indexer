use std::str::FromStr;
use std::sync::Arc;
use std::fs::remove_file;
use std::time::{Duration};
use sui_types::base_types::ObjectID;
use bcs;
use bcs::from_bytes;
use fred::prelude::*;
use fred::prelude::ServerConfig::Centralized;
use fred::types::RespVersion;
use log::{debug, info, LevelFilter, warn};
use sui_indexer::reader::CheckpointReader;
use clap::Parser;
use reqwest::header::ACCEPT;
use sui_types::full_checkpoint_content::CheckpointData;
use tokio::time::sleep;
use sui_indexer::events::process_txn;

#[derive(Parser)]
struct Cli {
    #[arg(short,long, default_value = "/mnt/sui/ingestion")]
    path: String,
    #[arg(short, long, action)]
    debug: bool,
    #[arg(short,long, default_value_t=0)]
    start: u64,
    #[arg(long, default_value_t=0)]
    db: u8,
    #[arg(short,long, default_value_t=0)]
    batch: u64,
    #[arg(short, long, action)]
    exit: bool,
    #[arg(long, action, help="optional URL to experimental api, needs to be allowed on rpc node, eg. http://localhost:9000/rest")]
    experimental: Option<String>,
    // #[arg(short, long, action, requires(rpc_experimental))]
    // experimental: bool,
}

#[tokio::main]
async fn main(){
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
    // let runtime = Builder::new_multi_thread().enable_all().worker_threads(8).build().unwrap();
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
    // let _ = runtime.block_on(async {
    //     let _ = client.connect();
    //     client.wait_for_connect().await
    // });
    let _ = client.connect();
    client.wait_for_connect().await;
    info!("preparing redis done");
    let mut reader = CheckpointReader{ path: cli.path.parse().unwrap(), current_checkpoint_number: cli.start };
    let reqwest_client = reqwest::Client::new();
    loop {
        if cli.experimental.is_some() {
            let checkpoint = client.get::<u64, u64>(0).await.unwrap_or(0);
            // let health_url = format!("{}/health", cli.experimental.clone().unwrap());
            // let response = reqwest_client.get(health_url).await;
            // response.headers().get("x-sui-checkpoint-height").unwrap().to_str().unwrap().parse().unwrap();
            let url = format!("{}/checkpoints/{}/full", cli.experimental.clone().unwrap(), checkpoint + 1);
            let result = reqwest_client
                .get(url)
                .header(ACCEPT, "application/bcs")
                .send().await;
           if  result.is_ok() {
               let response = result.unwrap();
               let checkpoint_height: u64 = response.headers().get("x-sui-checkpoint-height").unwrap().to_str().unwrap().parse().unwrap();
               if checkpoint > checkpoint_height {
                   sleep(Duration::from_millis(250)).await;
                   debug!("looping again, checkpoint is not yet stored ...");
                   continue;
               }
               let status_code = response.status().as_u16();
               // TODO add check ...
               match status_code {
                   200 => {
                       // TODO process normally
                       let checkpoint_data = from_bytes::<CheckpointData>(&response.bytes().await.unwrap()).unwrap();
                       let number = checkpoint_data.checkpoint_summary.sequence_number.clone();
                       // let checkpoint_data = reader.read_checkpoint(path).unwrap();
                       let result = process_txn(&checkpoint_data, &filter);
                       if result.len() > 0 {
                           // TODO send to redis or mongodb via crossbeam channel
                           // runtime.spawn({
                           //     let client = client.clone();
                           //     async move {
                           for (digest, data) in result {
                               info!("digest: {}", digest);
                               let event = data.parse_event();
                               let result = serde_json::to_string(&data).unwrap();
                               // more events can have same digest ... with index is unique
                               let mut digest_modified = format!("{}::{}::{}", data.checkpoint, digest, data.index);
                               if event.is_some() {
                                   let (_, event_name, obligation_id) = event.unwrap();
                                   if obligation_id.is_some() {
                                       let id = obligation_id.unwrap();
                                       let mut id_set = "id_".to_string();
                                       id_set.push_str(&*id);
                                       // stores indexer data in id_{obligation_id}
                                       client.sadd::<String,String,String>(id_set, result).await;
                                       // stores obligation_id in ids set ...
                                       client.sadd::<String,String,String>("ids".to_string(), id).await;
                                       let mut events_set = "events_".to_string();
                                       events_set.push_str(&*data.type_);
                                       // storing digest but this digest does not have value ...
                                       client.sadd::<String,String,String>(events_set, digest_modified.clone()).await;
                                       debug!("inserting obligations");
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
                           }
                           // return client.connect().await.unwrap();
                           // }});
                       }
                       client.set::<u64, u64, u64>(0_u64, number.clone(), None, None, false).await;
                   }
                   404 => {
                       // 2 cases - checkpoint is too low or do not exists in the moment
                       debug!("not found");
                       sleep(Duration::from_millis(250)).await;
                   }
                   // rpc return 500 - not found ...
                   _ => {
                     // TODO retry
                       debug!("problem: {} {:?}", status_code, response);
                       sleep(Duration::from_millis(1000)).await;
                   }
               }
            }
            continue
        }
        debug!("fetching file");
        // race condition?
        let file_response =  if cli.batch > 0 {reader.read_random_batch_of_files(cli.batch.clone() as usize)} else {reader.read_local_files()};
        if file_response.is_err() {
            warn!("something bad happened {:?}", file_response.err());
            if cli.exit {
                std::process::exit(1);
            }
            continue;
        }
        let checkpoints = file_response.unwrap();
        if checkpoints.len() == 0 {
            sleep(Duration::from_millis(100)).await;
            debug!("No files to process ...");
            if cli.exit {
                std::process::exit(0);
            }
            continue
        }
        for checkpoint_data in checkpoints.iter() {
            // TODO process
            let number = checkpoint_data.checkpoint_summary.sequence_number.clone();
            // let checkpoint_data = reader.read_checkpoint(path).unwrap();
            let result = process_txn(&checkpoint_data, &filter);
            if result.len() > 0 {
                // TODO send to redis or mongodb via crossbeam channel
                // runtime.spawn({
                //     let client = client.clone();
                //     async move {
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
                    // }});
            }
            client.set::<u64, u64, u64>(0_u64, number.clone(), None, None, false).await;
            // TODO progressor
            // runtime.spawn({
            // let client = client.clone();
            //     let checkpoint_number = number.clone();
            // async move {
            //     debug!("conecting redis client");
            //     client.set::<u64, u64, u64>(0_u64, checkpoint_number, None, None, false).await;
            //     // return client.connect().await.unwrap();
            // }});
            println!("checkpoint {}", number);
            let file_path = format!("{}/{}.chk", cli.path, reader.current_checkpoint_number );
            remove_file(file_path);
            reader.current_checkpoint_number = number;
        }
    }
}
