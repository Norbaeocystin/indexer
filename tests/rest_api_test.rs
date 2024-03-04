use std::time::SystemTime;
use log::{info, LevelFilter};
use reqwest::header::ACCEPT;
use sui_types::base_types::ObjectID;
use sui_indexer::reader::CheckpointReader;
use bcs::from_bytes;
use sui_types::full_checkpoint_content::CheckpointData;

#[tokio::test]
async fn speed_test() {
    let start = SystemTime::now();
    env_logger::builder().filter_level(LevelFilter::Debug).init();
    let client = reqwest::Client::new();
    // latest checkpoint
    let result = client.get("http://localhost:6000/rest/checkpoints").header(ACCEPT, "application/bcs").send().await;
    info!("{:?} {}", result, start.elapsed().unwrap().as_millis());
    let result = client.get("http://localhost:6000/rest/checkpoints/27837398/full").header(ACCEPT, "application/bcs").send().await;
    let r = from_bytes::<CheckpointData>(&result.unwrap().bytes().await.unwrap()).unwrap();
    info!("{:?} {}", r.checkpoint_summary.epoch, start.elapsed().unwrap().as_millis());
}