use apibara_core::starknet::v1alpha2::Block;
use rocksdb::{
    DBIteratorWithThreadMode, DBWithThreadMode, IteratorMode, MultiThreaded, ReadOptions,
};
use serializer::{deserialize_from_bytes, serialize_to_bytes};
use thiserror::Error;

use std::{array::TryFromSliceError, sync::Arc};

use crate::db::get_ro_db_connection;

pub mod db;
pub mod monitoring;
pub mod serializer;
pub mod transaction_receipt;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error(transparent)]
    ConnectionError(#[from] rocksdb::Error),
    #[error("missing env var 'STORAGE_PATH'")]
    MissingEnvVar(#[from] std::env::VarError),
    #[error("failed to increase tx count for {0} with message {1}")]
    IncreaseCountError(String, String),
    #[error(transparent)]
    TryFromSlice(#[from] TryFromSliceError),
    #[error("failed to serialize starknet block")]
    CannotSerializeBlock(serde_json::Error),
    #[error("block header is 'None'")]
    InvalidBlock,
}

/// Increase address transaction count.
/// Add tx to timeseries {TIMESTAMP}#CONTRACT#{ADDRESS}
/// Add tx to contract timeseries CONTRACT#{ADDRESS}#{TIMESERIES}
/// Add tx to timeseries CONTRACT#{ADDRESS}
/// Add tx to protocol STARKNET_PROTOCOL#{ADDRESS}
/// * `to` - [&str] - Account address
pub async fn increase_transaction_count(
    db: Arc<DBWithThreadMode<MultiThreaded>>,
    tx_hash: &str,
    account_address: &str,
    timestamp: i64,
) -> Result<(), StorageError> {
    let tx_key = format!("ACCOUNT#{account_address}#{timestamp}");
    let timestamp_key = format!("{timestamp}#ACCOUNT#{account_address}");

    if let Some(txs) = db.get(&tx_key)? {
        let mut value = deserialize_from_bytes(&txs);
        if !value.contains(&tx_hash) {
            value.push(tx_hash);
            db.put(&tx_key, serialize_to_bytes(value.as_slice()))?;
        }
    } else {
        db.put(&tx_key, serialize_to_bytes(&[tx_hash]))?;
    }

    if let Some(txs) = db.get(&timestamp_key)? {
        let mut value = deserialize_from_bytes(&txs);
        if !value.contains(&tx_hash) {
            value.push(tx_hash);
            db.put(&timestamp_key, serialize_to_bytes(value.as_slice()))?;
        }
    } else {
        db.put(&timestamp_key, serialize_to_bytes(&[tx_hash]))?;
    }

    Ok(())
}

/// Store Starknet block as is into db. This is required to not replay stream every single time.
/// * `db` - [Arc<DBWithThreadMode<MultiThreaded>>] - RocksDB instance.
/// * `block` - [&Block] - Starknet block
pub async fn store_block(
    db: Arc<DBWithThreadMode<MultiThreaded>>,
    block: &Block,
) -> Result<(), StorageError> {
    if let Some(header) = &block.header {
        let block_key = format!("BLOCK#{}", header.block_number);
        // not the efficientest way to store data... but it's a poc so nvm
        // TODO: Custom bytes serialization with proper trait is required there.
        let str_block =
            serde_json::to_string(&block).map_err(StorageError::CannotSerializeBlock)?;
        db.put("LAST_HANDLED_BLOCK", header.block_number.to_be_bytes())?;
        db.put(block_key, str_block.as_bytes())?;
        return Ok(());
    }

    Err(StorageError::InvalidBlock)
}

/// Get starknet block iterator
/// * `db` - [&DBWithThreadMode<MultiThreaded>] - RocksDB instance. get_ro_db_connection_with_prefix_extractor(6)
pub async fn get_block_iterator(
    db: &DBWithThreadMode<MultiThreaded>,
) -> Result<DBIteratorWithThreadMode<DBWithThreadMode<MultiThreaded>>, StorageError> {
    let prefix_key = "BLOCK#";

    let mut readopts = ReadOptions::default();
    readopts.set_prefix_same_as_start(true);
    readopts.set_iterate_lower_bound(prefix_key.as_bytes().to_vec());
    readopts.set_pin_data(true);

    Ok(db.iterator_opt(IteratorMode::Start, readopts))
}

pub async fn get_last_handled_block() -> Result<u64, StorageError> {
    let db = get_ro_db_connection(None).await?;
    if let Some(block_number) = db.get("LAST_HANDLED_BLOCK")? {
        let last_block = u64::from_be_bytes(block_number[..].try_into()?);
        // if block was cut in half, replay current block (existing data will be overidden)
        return Ok(last_block - 1);
    }
    Ok(0)
}
