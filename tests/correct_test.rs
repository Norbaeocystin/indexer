use std::fmt::format;
use std::fs;
use std::sync::Arc;
use reqwest;
use std::io::Cursor;
use std::str::FromStr;
use std::time::{Duration, SystemTime};
use sui_storage::blob::Blob;
use sui_types::base_types::ObjectID;
use sui_types::full_checkpoint_content::CheckpointData;
use tokio::try_join;
use sui_indexer::events::{process_txn, ScallopEvent};
use sui_indexer::events::ScallopEvent::ObligationCreatedEvent;
use futures;
use futures::future::join_all;
use reqwest::Response;
use reqwest::Result;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::time::sleep;
use sui_indexer::reader::CheckpointReader;

// type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
#[tokio::test]
async fn fetch_checkpoint(){
    // let url = "https://checkpoints.mainnet.sui.io/27668100.chk";
    // let response = reqwest::get(url).await;
    let start = SystemTime::now();
    let url = format!("https://checkpoints.mainnet.sui.io/10964321.chk");
    let response = reqwest::get(url).await;
    let filter = vec![ObjectID::from_str("0xefe8b36d5b2e43728cc323298626b83177803521d195cfb11e15b910e892fddf").unwrap(),
                      ObjectID::from_str("0xc38f849e81cfe46d4e4320f508ea7dda42934a329d5a6571bb4c3cb6ea63f5da").unwrap(),
    ];
    let checkpoint = Blob::from_bytes::<CheckpointData>(&response.unwrap().bytes().await.unwrap()).unwrap();
    let result = process_txn(&checkpoint, &filter);
    println!("result: {:?}", result);
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
        println!("digest: {}",  digest_modified)
    }
}