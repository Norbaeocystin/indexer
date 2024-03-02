use reqwest;
use std::io::Cursor;
use std::time::SystemTime;

// type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
#[tokio::test]
async fn fetch_checkpoint(){
    let url = "https://checkpoints.mainnet.sui.io/27000000.chk";
    let start = SystemTime::now();
    let response = reqwest::get(url).await.unwrap();
    println!("download of checkpoint took: {} ms", start.elapsed().unwrap().as_millis());
    let start = SystemTime::now();
    let mut file = std::fs::File::create("27000000.chk").unwrap();
    let iter = response.bytes().await.unwrap();
    let bytes = iter.iter();
    let mut content =  Cursor::new(bytes);
    std::io::copy(&mut content, &mut file);
    println!("writing to file took: {} ms", start.elapsed().unwrap().as_millis());
}