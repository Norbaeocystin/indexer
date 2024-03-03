use anyhow::Result;
use std::ffi::OsString;
use std::fs;
use std::io::{Cursor};
use std::path::PathBuf;
use std::time::SystemTime;
use sui_storage::blob::Blob;
use sui_types::full_checkpoint_content::CheckpointData;
use sui_types::messages_checkpoint::CheckpointSequenceNumber;
use log::{debug, info, warn};
use bytes::Bytes;
use tokio::sync::mpsc;

pub(crate) const ENV_VAR_LOCAL_READ_TIMEOUT_MS: &str = "LOCAL_READ_TIMEOUT_MS";

/// Implements a checkpoint reader that monitors a local directory.
/// Designed for setups where the indexer daemon is colocated with FN.
/// This implementation is push-based and utilizes the inotify API.
pub struct CheckpointReader {
    pub path: PathBuf,
    // remote_store: Option<Box<dyn ObjectStore>>,
    // remote_read_batch_size: usize,
    pub current_checkpoint_number: CheckpointSequenceNumber,
    // last_pruned_watermark: CheckpointSequenceNumber,
    // checkpoint_sender: mpsc::Sender<CheckpointData>,
    // processed_receiver: mpsc::Receiver<CheckpointSequenceNumber>,
    // exit_receiver: oneshot::Receiver<()>,
}

const MAX_CHECKPOINTS_IN_PROGRESS: usize = 10000;

impl CheckpointReader {

    pub fn read_batch_of_files(&self, batch_size: usize) -> Result<Vec<CheckpointData>>{
        let start = SystemTime::now();
        let mut files = vec![];
        for entry in fs::read_dir(self.path.clone()).unwrap() {
            if files.len() >= batch_size {
                break
            }
            let entry = entry.unwrap();
            let filename = entry.file_name();
            if let Some(sequence_number) = Self::checkpoint_number_from_file_path(&filename) {
                    files.push((sequence_number, entry.path()));
            }
        }
        let mut checkpoints = vec![];
        for (_, filename) in files.iter().take(MAX_CHECKPOINTS_IN_PROGRESS) {
            let checkpoint = Blob::from_bytes::<CheckpointData>(&fs::read(filename)?)?;
            checkpoints.push(checkpoint);
        }
        debug!("files: {} took: {} ms", files.len(), start.elapsed().unwrap().as_millis());
        return Ok(checkpoints);
    }
    // return all files
    pub fn read_all_files(&self) -> Vec<(CheckpointSequenceNumber, PathBuf)>{
        let start = SystemTime::now();
        let mut files = vec![];
        for entry in fs::read_dir(self.path.clone()).unwrap() {
            let entry = entry.unwrap();
            let filename = entry.file_name();
            if let Some(sequence_number) = Self::checkpoint_number_from_file_path(&filename) {
                if sequence_number >= self.current_checkpoint_number {
                    files.push((sequence_number, entry.path()));
                }
            }
        }
        files.sort();
        debug!("all checkpoint files: {} took: {} ms", files.len(), start.elapsed().unwrap().as_millis());
        return files;
    }

    pub fn read_checkpoint(&self, filename: &PathBuf) -> Result<CheckpointData> {
        let checkpoint = Blob::from_bytes::<CheckpointData>(&fs::read(filename)?)?;
        return Ok(checkpoint)
    }
    /// Represents a single iteration of the reader.
    /// Reads files in a local directory, validates them, and forwards `CheckpointData` to the executor.
    pub fn read_local_files(&self) -> Result<Vec<CheckpointData>> {
        let mut files = vec![];
        for entry in fs::read_dir(self.path.clone())? {
            let entry = entry?;
            let filename = entry.file_name();
            if let Some(sequence_number) = Self::checkpoint_number_from_file_path(&filename) {
                if sequence_number >= self.current_checkpoint_number {
                    files.push((sequence_number, entry.path()));
                }
            }
        }
        // debug!("unprocessed local files {:?}", files);
        files.sort();
        let mut checkpoints = vec![];
        for (_, filename) in files.iter().take(MAX_CHECKPOINTS_IN_PROGRESS) {
            let checkpoint = Blob::from_bytes::<CheckpointData>(&fs::read(filename)?)?;
            checkpoints.push(checkpoint);
        }
        Ok(checkpoints)
    }

    /// Cleans the local directory by removing all processed checkpoint files.
    pub fn gc_processed_files(watermark: CheckpointSequenceNumber, path_buf: PathBuf) -> Result<u64> {
        debug!("cleaning processed files, watermark is {}", watermark);
        let mut removed: u64 = 0;
        for entry in fs::read_dir(path_buf)? {
            let entry = entry?;
            let filename = entry.file_name();
            if let Some(sequence_number) = Self::checkpoint_number_from_file_path(&filename) {
                if sequence_number < watermark {
                    removed += 1;
                    fs::remove_file(entry.path())?;
                }
            }
        }
        Ok(removed)
    }

    pub fn checkpoint_number_from_file_path(file_name: &OsString) -> Option<CheckpointSequenceNumber> {
        file_name
            .to_str()
            .and_then(|s| s.rfind('.').map(|pos| &s[..pos]))
            .and_then(|s| s.parse().ok())
    }

    pub async fn fetch_from_external_interval(start_checkpoint: u64, end_checkpoint: u64) -> Vec<(Option<Bytes>, u64)>{
        let (tx, mut rx) = mpsc::unbounded_channel();
        for i in start_checkpoint..end_checkpoint {
            let txc = tx.clone();
            let i = i.clone();
            tokio::spawn(async move {
                // println!("receiver dropped");
                let checkpoint_file = format!("{}.chk", i);
                let url = format!("https://checkpoints.mainnet.sui.io/{checkpoint_file}");
                let response = reqwest::get(url).await;
                println!("sending");
                txc.send((response, i));
            });
            }
            let mut bytes_and_checkpoint_number = vec![];
            for i in start_checkpoint..end_checkpoint {
                let result =  rx.recv().await;
                if result.is_some() {
                    let (response, checkpoint_number) = result.unwrap();
                    if response.is_ok() {
                        let res = response.unwrap();
                        let bytes = res.bytes().await.unwrap();
                        bytes_and_checkpoint_number.push((Some(bytes), checkpoint_number));
                    } else {
                        warn!("{:?}", response.err());
                        bytes_and_checkpoint_number.push((None, checkpoint_number))
                    }
                };
            }
            return bytes_and_checkpoint_number;
        }

}