use std::fmt::format;
use std::fs;
use reqwest;
use std::io::Cursor;
use std::str::FromStr;
use std::time::SystemTime;
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

// type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
#[tokio::test]
async fn fetch_checkpoint(){
    // let url = "https://checkpoints.mainnet.sui.io/27668100.chk";
    // let response = reqwest::get(url).await;
    let start = SystemTime::now();
    let mut fts = vec![];
    let mut idx = 200;
    while idx > 0 {
        let checkpoint_file = format!("{}.chk", 27668100 + idx);
        let url = format!("https://checkpoints.mainnet.sui.io/{checkpoint_file}");
        let response = reqwest::get(url);
        fts.push(response);
        idx -= 1;
    }
    println!("{}", fts.len());
    let mut dm:Vec<Result<Response>> = join_all(fts).await;
    let response1 = dm.pop().unwrap().unwrap();
    println!("download of checkpoint took: {} ms", start.elapsed().unwrap().as_millis());
    println!("{}", dm.len());
    let mut i = 0;
    let start = SystemTime::now();
    while dm.len() > 0 {
        let response = dm.pop().unwrap().unwrap();
        let checkpoint_file = format!("/tmp/{}.chk", 27668100 + i);
        let mut file = std::fs::File::create(checkpoint_file).unwrap();
        let iter = response.bytes().await.unwrap();
        let bytes = iter.iter();
        let mut content =  Cursor::new(bytes);
        std::io::copy(&mut content, &mut file);
        i += 1;
    }
    println!("writing to files took: {} ms", start.elapsed().unwrap().as_millis());
    let checkpoint_data = Blob::from_bytes::<CheckpointData>(&response1.bytes().await.unwrap()).unwrap();
    let filter = vec![ObjectID::from_str("0xefe8b36d5b2e43728cc323298626b83177803521d195cfb11e15b910e892fddf").unwrap(),
                      ObjectID::from_str("0xc38f849e81cfe46d4e4320f508ea7dda42934a329d5a6571bb4c3cb6ea63f5da").unwrap(),
    ];
    let result = process_txn(&checkpoint_data, &filter);
    println!("{:?}", result);
    for (digest, indexer_data) in result {
        let start = SystemTime::now();
        let event = indexer_data.parse_event();
        let mut digest_modified = format!("{}::{}::{}",  indexer_data.checkpoint, digest, indexer_data.index);
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
        println!("{} key: {}", start.elapsed().unwrap().as_millis(), digest_modified);
    }
}
