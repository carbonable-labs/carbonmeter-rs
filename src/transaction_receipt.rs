use crypto_bigint::U256;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::error;

const SEQUENCER_URI: &str = "https://alpha4.starknet.io";

#[derive(Debug, Serialize, Deserialize)]
pub struct ExecutionBuiltin {
    pub(crate) pedersen: u64,
    pub(crate) range_check: u64,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct ExecutionResources {
    pub(crate) steps: u64,
    pub(crate) builtin: ExecutionBuiltin,
    pub(crate) memory: u64,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct TxResources {
    pub(crate) execution_resources: ExecutionResources,
    pub(crate) actual_fee: U256,
}

#[derive(Error, Debug)]
pub enum TransactionReceiptError {
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
}

/// Get transaction receipt from starknet sequencer.
/// Sequencer might be rate limiting us.
/// We retry consequently all minutes.
///
/// # Errors
///
/// This function will return an error if .
pub async fn get_transaction_execution_resources(
    tx_hash: &str,
) -> Result<TxResources, TransactionReceiptError> {
    let path = format!(
        "{}/feeder_gateway/get_transaction_receipt?transactionHash={}",
        SEQUENCER_URI, tx_hash
    );
    let client = Client::new();

    let res: TxResources = match client.get(path).send().await {
        Ok(r) => r.json().await?,
        Err(e) => {
            error!("{:#?}", e);
            return Err(TransactionReceiptError::Reqwest(e));
        }
    };
    println!("{:#?}", res);
    Ok(res)
}
