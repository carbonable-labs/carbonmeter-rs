use anyhow::Context;
use apibara_core::{
    node::v1alpha2::DataFinality,
    starknet::v1alpha2::{
        transaction::Transaction, Block, BlockHeader, FieldElement, Filter, HeaderFilter,
        InvokeTransactionV0, InvokeTransactionV1, TransactionMeta, TransactionWithReceipt,
    },
};
use apibara_sdk::{ClientBuilder, Configuration, DataMessage};
use carbonmeter_rs::{
    db::get_db_connection, get_last_handled_block, increase_transaction_count, store_block,
    transaction_receipt::TransactionReceiptError, StorageError,
};
use log::{debug, error, info};
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::sync::Arc;
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let dna_uri = std::env::var("DNA_URI")?;
    let dna_token = std::env::var("DNA_TOKEN")?;

    let uri = dna_uri.parse()?;
    let (mut stream, configuration_handle) = ClientBuilder::<Filter, Block>::default()
        .with_bearer_token(dna_token)
        .connect(uri)
        .await?;

    info!("🔌 Connected to stream");

    let db = Arc::new(get_db_connection(None).await?);
    let block_number = get_last_handled_block().await?;

    let config = Configuration::<Filter>::default()
        .with_finality(DataFinality::DataStatusPending)
        .with_starting_block(block_number)
        .with_filter(|mut filter| {
            filter
                .with_header(HeaderFilter { weak: false })
                .add_transaction(|transaction_filter| transaction_filter)
                // .add_event(|event_filter| event_filter)
                .build()
        });

    info!("🏁 Configuration ready...");

    configuration_handle.send(config).await?;

    info!("🚀 Starting stream...");

    loop {
        match stream.try_next().await {
            Ok(Some(response)) => match response {
                DataMessage::Data {
                    cursor: _,
                    end_cursor: _,
                    finality: _,
                    batch,
                } => {
                    for block in batch {
                        if let Some(header) = &block.header {
                            store_block(db.clone(), &block).await?;
                            info!("Received block: {:#?}", header.block_number);
                            let mut handles = vec![];
                            for transaction in block.transactions {
                                let db = db.clone();
                                let block_header = header.clone();
                                let handle = tokio::task::spawn(async move {
                                    store_single_transaction(
                                        db,
                                        block_header.clone(),
                                        transaction.clone(),
                                    )
                                    .await
                                });
                                handles.push(flatten(handle));
                            }

                            let _ = futures::future::try_join_all(handles).await?;
                        }
                    }
                }
                DataMessage::Invalidate { cursor } => match cursor {
                    Some(c) => {
                        error!("Received an invalidate request data at {}", &c.order_key)
                    }
                    None => error!("Invalidate request without cursor provided"),
                },
                DataMessage::Heartbeat => {
                    debug!("Heartbeat received");
                }
            },
            Ok(None) => continue,
            Err(e) => error!("{:#?}", e),
        }
    }
}

/// Dispatch single transaction into relevant handler.
/// Each transaction have to be handled in a specific way.
///
/// * tx - [`TransactionWithReceipt`] - Apibara starknet transaction wrapper type.
///
/// # Errors
///
async fn store_single_transaction(
    db: Arc<DBWithThreadMode<MultiThreaded>>,
    header: BlockHeader,
    tx: TransactionWithReceipt,
) -> anyhow::Result<()> {
    if let Some(t) = tx.transaction {
        if let Some(tx_meta) = t.meta {
            if let Some(t) = t.transaction {
                match t {
                    Transaction::InvokeV0(invoke_v0) => handle_invoke_v0(
                        db.clone(),
                        &header,
                        &Transaction::InvokeV0(invoke_v0),
                        &tx_meta,
                    )
                    .await
                    .context("invokev0 tx")?,
                    Transaction::InvokeV1(invoke_v1) => handle_invoke_v1(
                        db.clone(),
                        &header,
                        &Transaction::InvokeV1(invoke_v1),
                        &tx_meta,
                    )
                    .await
                    .context("invokev1 tx")?,
                    Transaction::Deploy(deploy) => {
                        handle_deploy(db.clone(), &header, &Transaction::Deploy(deploy), &tx_meta)
                            .await
                            .context("deploy tx")?
                    }
                    Transaction::Declare(declare) => handle_declare(
                        db.clone(),
                        &header,
                        &Transaction::Declare(declare),
                        &tx_meta,
                    )
                    .await
                    .context("declare tx")?,
                    Transaction::L1Handler(l1_handler) => handle_l1_handler(
                        db.clone(),
                        &header,
                        &Transaction::L1Handler(l1_handler),
                        &tx_meta,
                    )
                    .await
                    .context("handle_l1 tx")?,
                    Transaction::DeployAccount(deploy_account) => handle_deploy_account(
                        db.clone(),
                        &header,
                        &Transaction::DeployAccount(deploy_account),
                        &tx_meta,
                    )
                    .await
                    .context("deploy account tx")?,
                }
            }
        }
    }

    Ok(())
}

/// Flatten inner spawned task result to get proper result error handling from parent function.
///
/// * handle - [`JoinHandle`] - over some Result<T, E> function.
/// # Errors
///
/// This function will return an error if any of the inner spawned task returns an error.
pub async fn flatten<T, E: Into<anyhow::Error> + std::convert::From<tokio::task::JoinError>>(
    handle: JoinHandle<Result<T, E>>,
) -> Result<T, E> {
    match handle.await {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(e)) => Err(e),
        Err(err) => Err(err.into()),
    }
}

#[derive(Error, Debug)]
pub(crate) enum TransactionError {
    #[error("invalid transaction provided")]
    Invalid,
    #[error(transparent)]
    TransactionReceiptError(#[from] TransactionReceiptError),
    #[error(transparent)]
    StorageError(#[from] StorageError),
}

/// [`Transaction::InvokeV0`] handler
///
/// # Errors
///
/// This function will return an error if .
async fn handle_invoke_v0(
    db: Arc<DBWithThreadMode<MultiThreaded>>,
    header: &BlockHeader,
    tx: &Transaction,
    meta: &TransactionMeta,
) -> Result<(), TransactionError> {
    if let Transaction::InvokeV0(t) = tx {
        if let Some(hash) = &meta.hash {
            debug!("InvokeV0: {:?}", hash.to_hex());
            // Use this when you want to retrieve execution_resources
            // let tx_resources = get_transaction_execution_resources(hash.to_hex().as_str()).await?;
            // println!("Execution Resources : {:#?}", tx_resources);
            let extracted = extract_v0_tx_to(t).await?;
            for to_address in extracted {
                increase_transaction_count(
                    db.clone(),
                    hash.to_hex().as_str(),
                    &to_address.to_hex(),
                    header.timestamp.clone().unwrap().seconds,
                )
                .await?;
            }

            return Ok(increase_transaction_count(
                db,
                hash.to_hex().as_str(),
                &t.contract_address.clone().unwrap().to_hex(),
                header.timestamp.clone().unwrap().seconds,
            )
            .await?);
            // println!("InvokeV0: {:?}", t);
            // println!("Meta: {:#?}", hash.to_hex());
            // println!("{:#?}", t.calldata);
        }
    }
    Err(TransactionError::Invalid)
}

/// [`Transaction::InvokeV1`] handler
///
/// # Errors
///
/// This function will return an error if .
async fn handle_invoke_v1(
    db: Arc<DBWithThreadMode<MultiThreaded>>,
    header: &BlockHeader,
    tx: &Transaction,
    meta: &TransactionMeta,
) -> Result<(), TransactionError> {
    if let Transaction::InvokeV1(t) = tx {
        if let Some(hash) = &meta.hash {
            debug!("InvokeV1: {:?}", hash.to_hex());
            // Use this when you want to retrieve execution_resources
            // let tx_resources = get_transaction_execution_resources(hash.to_hex().as_str()).await?;
            // println!("Execution Resources : {:#?}", tx_resources);
            let extracted = extract_v1_tx_to(t).await?;
            for to_address in extracted {
                increase_transaction_count(
                    db.clone(),
                    hash.to_hex().as_str(),
                    &to_address.to_hex(),
                    header.timestamp.clone().unwrap().seconds,
                )
                .await?;
            }

            return Ok(increase_transaction_count(
                db,
                hash.to_hex().as_str(),
                &t.sender_address.clone().unwrap().to_hex(),
                header.timestamp.clone().unwrap().seconds,
            )
            .await?);
            // println!("InvokeV1: {:?}", t);
            // println!("Meta: {:#?}", hash.to_hex());
            // println!("{:#?}", t.calldata);
        }
    }
    Err(TransactionError::Invalid)
}

/// Extract [`to`] from [`InvokeTransactionV1`]
async fn extract_v1_tx_to(
    transaction: &InvokeTransactionV1,
) -> Result<Vec<&FieldElement>, TransactionError> {
    let mut extracted = Vec::new();
    if let Some(len) = transaction.calldata.first() {
        transaction
            .calldata
            .iter()
            .skip(1)
            .collect::<Vec<&FieldElement>>()
            .as_slice()
            .chunks(4)
            .take(len.hi_hi.try_into().unwrap())
            .for_each(|c| extracted.push(c[0]));
    }

    Ok(extracted)
}

/// Extract [`to`] from [`InvokeTransactionV0`]
async fn extract_v0_tx_to(
    transaction: &InvokeTransactionV0,
) -> Result<Vec<&FieldElement>, TransactionError> {
    Ok(vec![&transaction.calldata[0]])
}

/// [`Transaction::Deploy`] handler
///
/// # Errors
///
/// This function will return an error if .
async fn handle_deploy(
    db: Arc<DBWithThreadMode<MultiThreaded>>,
    header: &BlockHeader,
    tx: &Transaction,
    meta: &TransactionMeta,
) -> Result<(), TransactionError> {
    if let Transaction::Deploy(t) = tx {
        if let Some(hash) = &meta.hash {
            debug!("Deploy: {:?}", hash.to_hex());
            // Use this when you want to retrieve execution_resources
            // let tx_resources = get_transaction_execution_resources(hash.to_hex().as_str()).await?;
            // println!("Execution Resources : {:#?}", tx_resources);

            // there's no way to get the deploy contract address there...
            // NOTE: get contract address from starknet directly
            return Ok(increase_transaction_count(
                db,
                hash.to_hex().as_str(),
                &t.class_hash.clone().unwrap().to_hex(),
                header.timestamp.clone().unwrap().seconds,
            )
            .await?);
            // println!("Declare: {:?}", t);
            // println!("Meta: {:#?}", hash.to_hex());
        }
    }
    Err(TransactionError::Invalid)
}

/// [`Transaction::Declare`] handler
///
/// # Errors
///
/// This function will return an error if .
async fn handle_declare(
    db: Arc<DBWithThreadMode<MultiThreaded>>,
    header: &BlockHeader,
    tx: &Transaction,
    meta: &TransactionMeta,
) -> Result<(), TransactionError> {
    if let Transaction::Declare(t) = tx {
        if let Some(hash) = &meta.hash {
            debug!("Declare: {:?}", hash.to_hex());
            // Use this when you want to retrieve execution_resources
            // let tx_resources = get_transaction_execution_resources(hash.to_hex().as_str()).await?;
            // println!("Execution Resources : {:#?}", tx_resources);

            return Ok(increase_transaction_count(
                db,
                hash.to_hex().as_str(),
                &t.sender_address.clone().unwrap().to_hex(),
                header.timestamp.clone().unwrap().seconds,
            )
            .await?);
            // println!("Declare: {:?}", t);
            // println!("Meta: {:#?}", hash.to_hex());
        }
    }
    Err(TransactionError::Invalid)
}

/// [`Transaction::L1Handler`] handler
///
/// # Errors
///
/// This function will return an error if .
async fn handle_l1_handler(
    db: Arc<DBWithThreadMode<MultiThreaded>>,
    header: &BlockHeader,
    tx: &Transaction,
    meta: &TransactionMeta,
) -> Result<(), TransactionError> {
    if let Transaction::L1Handler(t) = tx {
        if let Some(hash) = &meta.hash {
            debug!("L1Handler: {:?}", hash.to_hex());
            // Use this when you want to retrieve execution_resources
            // let tx_resources = get_transaction_execution_resources(hash.to_hex().as_str()).await?;
            // println!("Execution Resources : {:#?}", tx_resources);

            return Ok(increase_transaction_count(
                db,
                hash.to_hex().as_str(),
                &t.contract_address.clone().unwrap().to_hex(),
                header.timestamp.clone().unwrap().seconds,
            )
            .await?);
            // println!("Declare: {:?}", t);
            // println!("Meta: {:#?}", hash.to_hex());
        }
    }
    Err(TransactionError::Invalid)
}

/// [`Transaction::DeployAccount`] handler
///
/// # Errors
///
/// This function will return an error if .
async fn handle_deploy_account(
    db: Arc<DBWithThreadMode<MultiThreaded>>,
    header: &BlockHeader,
    tx: &Transaction,
    meta: &TransactionMeta,
) -> Result<(), TransactionError> {
    if let Transaction::DeployAccount(t) = tx {
        if let Some(hash) = &meta.hash {
            debug!("DeployAccount: {:?}", hash.to_hex());
            // Use this when you want to retrieve execution_resources
            // let tx_resources = get_transaction_execution_resources(hash.to_hex().as_str()).await?;
            // println!("Execution Resources : {:#?}", tx_resources);

            return Ok(increase_transaction_count(
                db,
                hash.to_hex().as_str(),
                &t.class_hash.clone().unwrap().to_hex(),
                header.timestamp.clone().unwrap().seconds,
            )
            .await?);
            // println!("Declare: {:?}", t);
            // println!("Meta: {:#?}", hash.to_hex());
        }
    }
    Err(TransactionError::Invalid)
}

// on transaction increment transaction count by one for each `from_address`.
// on transaction increment transaction count by one for each `to` in calldata. for each
// transaction type you have to parse out "to" based on calldata order.
// eg. InvokeV0 - calldata[0] = to
// eg. InvokeV1 - calldata[1] = CallArray (find to in callarray)
//
#[cfg(test)]
mod tests {
    use apibara_core::starknet::v1alpha2::{FieldElement, InvokeTransactionV1};

    use crate::extract_v1_tx_to;

    #[tokio::test]
    async fn test_extract_to_transaction() {
        let calldata = [
            FieldElement {
                lo_lo: 0,
                lo_hi: 0,
                hi_lo: 0,
                hi_hi: 1,
            },
            FieldElement {
                lo_lo: 528642610080664005,
                lo_hi: 2276061702884689271,
                hi_lo: 11153158363279249689,
                hi_hi: 8056614741232289178,
            },
            FieldElement {
                lo_lo: 184998192538858248,
                lo_hi: 14131786057843337807,
                hi_lo: 12306571739644741482,
                hi_hi: 7726687080764924166,
            },
            FieldElement {
                lo_lo: 0,
                lo_hi: 0,
                hi_lo: 0,
                hi_hi: 0,
            },
            FieldElement {
                lo_lo: 0,
                lo_hi: 0,
                hi_lo: 0,
                hi_hi: 14,
            },
        ];
        let tx = InvokeTransactionV1 {
            sender_address: Some(FieldElement {
                lo_lo: 431078386762221800,
                lo_hi: 4723068717221267708,
                hi_lo: 13141689637839447599,
                hi_hi: 5182790949074909961,
            }),
            calldata: calldata.to_vec(),
        };

        let extracted = extract_v1_tx_to(&tx).await.unwrap();
        assert!(extracted.len() == 1);
        assert_eq!(
            extracted[0].to_hex().as_str(),
            "0x07561da72afe49c51f96320f475005779ac7fa707eefd1196fced86bdf53859a"
        );

        let calldata = [
            FieldElement {
                lo_lo: 0,
                lo_hi: 0,
                hi_lo: 0,
                hi_hi: 2,
            },
            // to
            FieldElement {
                lo_lo: 528642610080664005,
                lo_hi: 2276061702884689271,
                hi_lo: 11153158363279249689,
                hi_hi: 8056614741232289178,
            },
            // selector
            FieldElement {
                lo_lo: 184998192538858248,
                lo_hi: 14131786057843337807,
                hi_lo: 12306571739644741482,
                hi_hi: 7726687080764924166,
            },
            // data_offset
            FieldElement {
                lo_lo: 0,
                lo_hi: 0,
                hi_lo: 0,
                hi_hi: 0,
            },
            // data_len
            FieldElement {
                lo_lo: 0,
                lo_hi: 0,
                hi_lo: 0,
                hi_hi: 14,
            },
            // to
            FieldElement {
                lo_lo: 184998192538858248,
                lo_hi: 14131786057843337807,
                hi_lo: 12306571739644741482,
                hi_hi: 7726687080764924166,
            },
            // selector
            FieldElement {
                lo_lo: 528642610080664005,
                lo_hi: 2276061702884689271,
                hi_lo: 11153158363279249689,
                hi_hi: 8056614741232289178,
            },
            // data_offset
            FieldElement {
                lo_lo: 0,
                lo_hi: 0,
                hi_lo: 0,
                hi_hi: 0,
            },
            // data_len
            FieldElement {
                lo_lo: 0,
                lo_hi: 0,
                hi_lo: 0,
                hi_hi: 14,
            },
        ];
        let tx = InvokeTransactionV1 {
            sender_address: Some(FieldElement {
                lo_lo: 431078386762221800,
                lo_hi: 4723068717221267708,
                hi_lo: 13141689637839447599,
                hi_hi: 5182790949074909961,
            }),
            calldata: calldata.to_vec(),
        };

        // inverse selector and to for testing easyness
        let extracted = extract_v1_tx_to(&tx).await.unwrap();
        assert!(extracted.len() == 2);
        assert_eq!(
            extracted[0].to_hex().as_str(),
            "0x07561da72afe49c51f96320f475005779ac7fa707eefd1196fced86bdf53859a"
        );
        assert_eq!(
            extracted[1].to_hex().as_str(),
            "0x02913ee03e5e3308c41e308bd391ea4faac9b9cb5062c76a6b3ab4f65397e106"
        );
    }
}
