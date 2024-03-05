use futures::SinkExt;
use reqwest::header::{ACCEPT, CONTENT_TYPE};
use serde_json::Value;

#[tokio::test]
async fn sync_test(){
    let reqwest_client = reqwest::Client::new();
    let json = &serde_json::json!({
                                      "jsonrpc": "2.0",
                                      "id": 1,
                                      "method": "sui_getLatestCheckpointSequenceNumber",
                                      "params": []
                                    });
    let latest_synced_checkpoint_request = reqwest_client.post(
        "http://localhost:9000"
    ).json(json)
        .header(ACCEPT, "application/json")
        .header(CONTENT_TYPE, "application/json").build().unwrap();
    let response = reqwest_client.execute(latest_synced_checkpoint_request).await.unwrap();
    // println!("{:?}", response.text().await);
    let latest_sync_checkpoint: u64 = response.json::<Value>().await.unwrap().get("result").unwrap().to_string().replace("\"","").parse().unwrap();
    println!("{}", latest_sync_checkpoint)
}