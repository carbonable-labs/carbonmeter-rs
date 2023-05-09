use apibara_core::{
    node::v1alpha2::DataFinality,
    starknet::v1alpha2::{
        transaction::Transaction, Block, BlockHeader, Filter, HeaderFilter, TransactionMeta,
        TransactionWithReceipt,
    },
};
use apibara_sdk::{ClientBuilder, Configuration, DataMessage, Uri};
use carbonmeter_rs::transaction_receipt::{
    get_transaction_execution_resources, TransactionReceiptError,
};
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let (mut stream, configuration_handle) = ClientBuilder::<Filter, Block>::default()
        .connect(Uri::from_static("https://mainnet.starknet.a5a.ch:443"))
        .await
        .unwrap();

    let config = Configuration::<Filter>::default()
        .with_finality(DataFinality::DataStatusPending)
        .with_filter(|mut filter| {
            filter
                .with_header(HeaderFilter { weak: false })
                .add_transaction(|transaction_filter| transaction_filter)
                // .add_event(|event_filter| event_filter)
                .build()
        });

    configuration_handle.send(config).await?;

    while let Ok(Some(message)) = stream.try_next().await {
        match message {
            DataMessage::Data {
                cursor: _,
                end_cursor: _,
                finality: _,
                batch,
            } => {
                for block in batch {
                    if let Some(header) = block.header {
                        let mut handles = vec![];
                        for transaction in block.transactions {
                            let block_header = header.clone();
                            let handle = tokio::task::spawn(async move {
                                store_single_transaction(block_header.clone(), transaction.clone())
                                    .await
                            });
                            handles.push(flatten(handle));
                        }

                        let _res = match futures::future::try_join_all(handles).await {
                            Ok(res) => Ok(res),
                            Err(e) => Err(e),
                        };
                    }
                }
            }
            DataMessage::Invalidate { cursor } => {
                println!("Chain reorganization detected: {cursor:?}");
            }
        }
    }

    Ok(())
}

/// Dispatch single transaction into relevant handler.
/// Each transaction have to be handled in a specific way.
///
/// * tx - [`TransactionWithReceipt`] - Apibara starknet transaction wrapper type.
///
/// # Errors
///
async fn store_single_transaction(
    header: BlockHeader,
    tx: TransactionWithReceipt,
) -> anyhow::Result<()> {
    if let Some(t) = tx.transaction {
        if let Some(tx_meta) = t.meta {
            if let Some(t) = t.transaction {
                match t {
                    Transaction::InvokeV0(invoke_v0) => {
                        handle_invoke_v0(&header, &Transaction::InvokeV0(invoke_v0), &tx_meta)
                            .await?
                    }
                    Transaction::InvokeV1(_) => (),
                    Transaction::Deploy(_) => (),
                    Transaction::Declare(_) => (),
                    Transaction::L1Handler(_) => (),
                    Transaction::DeployAccount(_) => (),
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
}

/// [`Transaction::InvokeV0`] handler
///
/// # Errors
///
/// This function will return an error if .
async fn handle_invoke_v0(
    header: &BlockHeader,
    tx: &Transaction,
    meta: &TransactionMeta,
) -> Result<(), TransactionError> {
    if let Transaction::InvokeV0(t) = tx {
        if let Some(hash) = &meta.hash {
            // Use this when you want to retrieve execution_resources
            // let tx_resources = get_transaction_execution_resources(hash.to_hex().as_str()).await?;
            // println!("Execution Resources : {:#?}", tx_resources);

            println!("InvokeV0: {:?}", t);
            println!("Header: {:#?}", header);
            println!("Meta: {:#?}", meta);
        }
    }
    Err(TransactionError::Invalid)
}
