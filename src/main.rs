use std::any;

use apibara_core::{
    node::v1alpha2::DataFinality,
    starknet::v1alpha2::{Block, Filter, HeaderFilter, EventFilter, EventWithTransaction, TransactionFilter},
};
use apibara_sdk::{ClientBuilder, Configuration, DataMessage, Uri};
use chrono::{DateTime, Utc};
use tokio_stream::StreamExt;
use tracing::{error, info};


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let (mut stream, configuration_handle) = ClientBuilder::<Filter, Block>::default()
        // .with_bearer_token("dna_ZoZMlBLvG81yBCQ5cL8R".to_string())
        .connect(Uri::from_static("https://mainnet.starknet.a5a.ch:443"))
        .await
        .unwrap();

    // let config = Configuration::<Filter>::default()
    //     .with_finality(DataFinality::DataStatusPending)
    //     .with_filter(|mut filter| filter.with_header(HeaderFilter { weak: false }).add_transaction(TransactionFilter { filter: None }).build());

    let config = Configuration::<Filter>::default()
    .with_finality(DataFinality::DataStatusPending)
    .with_filter(|mut filter| {
        filter
            .with_header(HeaderFilter { weak: false })
            .add_transaction(|transaction_filter| transaction_filter)
            .add_event(|event_filter| event_filter)
            .build()
    });

    configuration_handle.send(config).await?;

    // Initialize a Vec to store contract addresses and their transaction counts
    let mut contract_transaction_counts: Vec<(String, usize, (u64, u64, u64, u64))> = vec![];



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
                    let timestamp: DateTime<Utc> =
                        header.timestamp.unwrap_or_default().try_into()?;
                    //println!("  Block {:>6} ({})", header.block_number, timestamp);
                    // let events: Vec<EventWithTransaction> = block.events.clone();
                    // let transactions = block.transactions.clone();
                    
                    //only if events.len() is >0 then print else not print
                    //
                    // if transactions.len() > 0 {
                    //     println!("  Block {:>6} ({}) with {} transactions", header.block_number, timestamp, transactions.len());
                    // }
                    // else {
                    //     println!("  Block {:>6} ({}) with {} events", header.block_number, timestamp, events.len());
                    // }

                    // println!("  Block {:>6} ({}) with {} events", header.block_number, timestamp, events.len());
                    // go through all events in the block
                    for event_with_tx in block.events {

                        // event includes the tx that triggered the event emission
                        // it also include the receipt in `event_with_tx.receipt`, but
                        // it's not used in this example
                        let event = event_with_tx.event.unwrap_or_default();
                        let tx = event_with_tx.transaction.unwrap_or_default();
                        let tx_hash = tx.clone()
                            .meta
                            .unwrap_or_default()
                            .hash
                            .unwrap_or_default()
                            .to_hex();

                        // print tx_hash
                        let gas = (
                            tx.meta.clone().unwrap_or_default().max_fee.unwrap_or_default().lo_lo,
                            tx.meta.clone().unwrap_or_default().max_fee.unwrap_or_default().lo_hi,
                            tx.meta.clone().unwrap_or_default().max_fee.unwrap_or_default().hi_lo,
                            tx.meta.clone().unwrap_or_default().max_fee.unwrap_or_default().hi_hi,
                        );
                        // println!("tx_hash: {} ; gas: {:?}", tx_hash, tx.meta.clone().unwrap_or_default().max_fee.unwrap_or_default());
                        
                        //print events length
                        // println!("Block {:>6} ({}) with {} events", header.block_number, timestamp, events.len());

                        // if enven.data.len() <=1 then print error
                        // ts_hash not empty then print error without this error " a local variable with a similar name exists: `tx_hash`"
                        if event.data.len() <= 1 {
                            error!("Event data is empty");
                        }
                        else {
                            let from_addr = event.data[0].to_hex();
                            let to_addr = event.data[1].to_hex();

                            // println!(
                            //     "    {} => {} ({})",
                            //     &from_addr[..8],
                            //     &to_addr[..8],
                            //     &tx_hash[..8]
                            // );

                            if let Some(index) = contract_transaction_counts.iter().position(|(addr, _, _)| addr == &from_addr) {
                                // Increment the transaction count for the contract_address.
                                contract_transaction_counts[index].1 += 1;
                                // Add gas for the contract_address.
                                contract_transaction_counts[index].2.0 += gas.0;
                                contract_transaction_counts[index].2.1 += gas.1;
                                contract_transaction_counts[index].2.2 += gas.2;
                                contract_transaction_counts[index].2.3 += gas.3;
                                println!("Update: {} on {} events: {}; gas: {:?}", timestamp , from_addr, contract_transaction_counts[index].1, contract_transaction_counts[index].2);
                            } else {
                                // Insert the contract_address with a transaction count of 1 and the gas value.
                                contract_transaction_counts.push((from_addr.clone(), 1, gas));
                                println!("Added: {} on {} events: 1; gas: {:?}", timestamp , from_addr, gas);
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