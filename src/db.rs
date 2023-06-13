use rocksdb::{DBWithThreadMode, MultiThreaded, Options, SliceTransform};

use crate::StorageError;

/// Get connection to rocksdb
pub async fn get_db_connection(
    opts: Option<rocksdb::Options>,
) -> Result<DBWithThreadMode<MultiThreaded>, StorageError> {
    let storage_path = std::env::var("STORAGE_PATH")?;
    let mut options = opts.unwrap_or(Options::default());
    options.create_if_missing(true);
    let db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::<MultiThreaded>::open(&options, storage_path)?;
    Ok(db)
}

/// Get connection to rocksdb
pub async fn get_ro_db_connection(
    opts: Option<rocksdb::Options>,
) -> Result<DBWithThreadMode<MultiThreaded>, StorageError> {
    let storage_path = std::env::var("STORAGE_PATH")?;
    let options = opts.unwrap_or(Options::default());

    let db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::<MultiThreaded>::open_for_read_only(&options, storage_path, false)?;
    Ok(db)
}

/// Get connection to rocksdb
pub async fn get_ro_db_connection_with_prefix_extractor(
    extractor: SliceTransform,
) -> Result<DBWithThreadMode<MultiThreaded>, StorageError> {
    let mut opts = Options::default();
    opts.set_prefix_extractor(extractor);

    get_ro_db_connection(Some(opts)).await
}
