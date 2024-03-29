use std::time::SystemTime;
use log::{info, LevelFilter};
use sui_types::base_types::ObjectID;
use sui_indexer::reader::CheckpointReader;

#[test]
fn speed_test(){
    env_logger::builder().filter_level(LevelFilter::Debug).init();
    let start = SystemTime::now();
    let mut reader = CheckpointReader{ path: "/mnt/sui/ingestion".parse().unwrap(), current_checkpoint_number: 0 };
    let files = reader.read_local_files().unwrap();
    info!("first: {}, last: {}, length: {} took: {} ms",
        files.first().unwrap().checkpoint_summary.sequence_number,
        files.last().unwrap().checkpoint_summary.sequence_number,
        files.len(),
        start.elapsed().unwrap().as_millis(),
    );
    let random = ObjectID::random();
    let start = SystemTime::now();
    let checkpoint = files.first().unwrap();
    for data in checkpoint.transactions.iter() {
        for events in data.events.iter() {
            for event in events.data.iter() {
                if event.package_id == random {
                    info!("same");
                }
            }
        }
    }
    info!("iteration took: {} ms", start.elapsed().unwrap().as_millis());
    let start = SystemTime::now();
    let removed = reader.gc_processed_files(files.last().unwrap().checkpoint_summary.sequence_number).unwrap();
    info!("removing took: {} ms removed: {}", start.elapsed().unwrap().as_millis(), removed);
    let checkpoints_and_paths = reader.read_all_files();
    let start = SystemTime::now();
    let batch_size:usize = 1000;
    for (checkpoint_number, path) in checkpoints_and_paths[..batch_size].iter() {
        let checkpoint = reader.read_checkpoint(path).unwrap();
        for data in checkpoint.transactions.iter() {
            for events in data.events.iter() {
                for event in events.data.iter() {
                    if event.package_id == random {
                        info!("same");
                    }
                }
            }
        }
    }
    info!("iteration took: {} ms checkpoints: {}", start.elapsed().unwrap().as_millis(), batch_size);
}
