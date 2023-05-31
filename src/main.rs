use std::any;

use apibara_core::{
    node::v1alpha2::DataFinality,
    starknet::v1alpha2::{Block, Filter, HeaderFilter, EventFilter, EventWithTransaction, TransactionFilter, FieldElement},
};
use apibara_sdk::{ClientBuilder, Configuration, DataMessage, Uri};
use chrono::{DateTime, Utc};
use tokio_stream::StreamExt;
use tracing::{error, info};
use apibara_core::starknet::v1alpha2::transaction::Transaction as ApiTransaction;


#[derive(Clone)]
struct TransactionData {
    addr: String,
    tx_type: String,
    count: usize,
    //gas: FieldElement,
}



fn update_transaction_count(tx_type: &str, from_addr: &str, contract_transaction_counts: &mut Vec<TransactionData>) {
    if let Some(index) = contract_transaction_counts
        .iter()
        .position(|data| data.addr == from_addr && data.tx_type == tx_type)
    {
        // Increment the transaction count for the contract_address and transaction type.
        contract_transaction_counts[index].count += 1;
        println!(
            "Update:  on {} {}s: {}; ",
            
            from_addr,
            tx_type,
            contract_transaction_counts[index].count,
        );
    } else {
        // Insert the contract_address with a transaction count of 1 and the gas value for the transaction type.
        contract_transaction_counts.push(TransactionData {
            addr: from_addr.to_string(),
            tx_type: tx_type.to_string(),
            count: 1,
        });
        println!(
            "Added:  on {} {}s: 1; ",
             from_addr, tx_type
        );
    }
}



#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let (mut stream, configuration_handle) = ClientBuilder::<Filter, Block>::default()
        // .with_bearer_token("dna_ZoZMlBLvG81yBCQ5cL8R".to_string())
        .connect(Uri::from_static("https://mainnet.starknet.a5a.ch:443"))
        .await
        .unwrap();


    let config = Configuration::<Filter>::default()
    .with_finality(DataFinality::DataStatusPending)
    .with_starting_block(54037)
    .with_filter(|mut filter| {
        filter
            .with_header(HeaderFilter { weak: false })
            .add_transaction(|transaction_filter| transaction_filter)
            .build()
    });


    configuration_handle.send(config).await?;

    // Initialize a Vec to store contract addresses and their transaction counts
    let mut contract_transaction_counts: Vec<(String, usize, (u64, u64, u64, u64))> = vec![];

    let mut contract_transaction_counts: Vec<TransactionData> = vec![];

    while let Some(message) = stream.try_next().await.unwrap() {
        // messages can be either data or invalidate
        // - data: new data produced
        // - invalidate: a chain reorganization happened and some previously sent data is not valid
        // anymore
        match message {
            DataMessage::Data {
                cursor, 
                end_cursor,
                finality,
                batch,
            } => {
                // cursor that generated the batch. if cursor = `None`, then it's the start of the
                // chain (includes genesis block).
                let start_block = cursor.map(|c| c.order_key).unwrap_or_default();
                // cursor that will be used to generate the next batch
                let end_block = end_cursor.order_key;

                // println!("Received data from block {start_block} to {end_block} with finality {finality:?}");

                // go through all blocks in the batch
                

                for block in batch {
                    // get block header and timestamp
                    let header = block.header.clone().unwrap_or_default();
                    // let timestamp: DateTime<Utc> =
                    //     header.timestamp.unwrap_or_default().try_into()?;
                    // println!("  Block {:>6} ({})", header.block_number, timestamp);
                    // // let events: Vec<EventWithTransaction> = block.events.clone();
                    // let transactions = block.transactions.clone();
                    

                    // println!("  Block {:>6} ({}) with {} tx ", header.block_number, timestamp, transactions.len());
                    // go through all events in the block

                    // //only loop for those transaction 0x036cda58864369aed48efcf81673615cf3f83423c9de83aecccbb3d79f28f5a3 block 54038
                    // if header.block_number == 54038 {
                    // // ...

                    for tx_with_receipt in block.transactions {
                        if let Some(tx) = tx_with_receipt.transaction {
                            match tx.transaction {
                                Some(ApiTransaction::InvokeV0(invoke_v0)) => {
                                    let from_addr = invoke_v0.contract_address.clone().unwrap().to_hex();
                                    update_transaction_count("InvokeV0", &from_addr, &mut contract_transaction_counts);
                                    //print calldata it's a Vec<u8>
                                    invoke_v0.calldata.iter().for_each(|x| println!("{:?}", x.to_string()));  
                                    println!("braa {:?} ", tx.meta.as_ref().and_then(|meta| meta.hash.as_ref()).map(|hash| hash.to_hex()).unwrap_or_else(|| String::from("No hash")));
                                    // pint tx hash
                                    // println!("  InvokeV0: tx_hash {:?}", tx_with_receipt.receipt.tx_hash.to_hex());
                                }
                                Some(ApiTransaction::InvokeV1(invoke_v1)) => {
                                    let from_addr = invoke_v1.sender_address.clone().unwrap().to_hex();
                                    update_transaction_count("InvokeV1", &from_addr, &mut contract_transaction_counts);
                            
                                    let mut iter = invoke_v1.calldata.iter().peekable();

                                    // Get the first line and convert it to an integer
                                    let first_line = iter.next().expect("Calldata should not be empty").to_string();
                                    let count = usize::from_str_radix(&first_line[2..], 16).expect("Could not parse count");

                                    for _ in 0..count {
                                        let line = iter.next().expect("Expected a contract address").to_string();
                                        println!("Contract Address: \"{}\"", line);
                                        // Skip the next 3 lines
                                        for _ in 0..3 {
                                            if iter.peek().is_some() {
                                                iter.next();
                                            }
                                        }
                                    }

                                    println!("braa {:?} ", tx.meta.as_ref().and_then(|meta| meta.hash.as_ref()).map(|hash| hash.to_hex()).unwrap_or_else(|| String::from("No hash")));
                                }
                                Some(ApiTransaction::Deploy(deploy)) => {
                                    let from_addr = deploy.class_hash.clone().unwrap().to_hex();
                                    update_transaction_count("Deploy", &from_addr, &mut contract_transaction_counts);
                                }
                                Some(ApiTransaction::Declare(declare)) => {
                                    let from_addr = declare.sender_address.clone().unwrap().to_hex();
                                    update_transaction_count("Declare", &from_addr, &mut contract_transaction_counts);
                                }
                                Some(ApiTransaction::L1Handler(l1_handler)) => {
                                    let from_addr = l1_handler.contract_address.clone().unwrap().to_hex();
                                    update_transaction_count("L1Handler", &from_addr, &mut contract_transaction_counts);
                                }
                                Some(ApiTransaction::DeployAccount(deploy_account)) => {
                                    let from_addr = deploy_account.class_hash.clone().unwrap().to_hex();
                                    update_transaction_count("DeployAccount", &from_addr, &mut contract_transaction_counts);
                                }
                                None => {
                                    println!("No transaction data");
                                }
                            }

                        
                        }
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