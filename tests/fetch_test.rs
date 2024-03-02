use std::fs;
use reqwest;
use std::io::Cursor;
use std::str::FromStr;
use std::time::SystemTime;
use sui_storage::blob::Blob;
use sui_types::base_types::ObjectID;
use sui_types::full_checkpoint_content::CheckpointData;
use sui_indexer::events::{process_txn, ScallopEvent};
use sui_indexer::events::ScallopEvent::ObligationCreatedEvent;

// type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
#[tokio::test]
async fn fetch_checkpoint(){
    let url = "https://checkpoints.mainnet.sui.io/27668100.chk";
    let start = SystemTime::now();
    let response = reqwest::get(url).await.unwrap();
    println!("download of checkpoint took: {} ms", start.elapsed().unwrap().as_millis());
    // let start = SystemTime::now();
    // let mut file = std::fs::File::create("27618100.chk").unwrap();
    // let iter = response.bytes().await.unwrap();
    // let bytes = iter.iter();
    // let mut content =  Cursor::new(bytes);
    // std::io::copy(&mut content, &mut file);
    // println!("writing to file took: {} ms", start.elapsed().unwrap().as_millis());
    let checkpoint_data = Blob::from_bytes::<CheckpointData>(&response.bytes().await.unwrap()).unwrap();
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