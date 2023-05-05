use apibara_core::{
    node::v1alpha2::DataFinality,
    starknet::v1alpha2::{Block, Filter, HeaderFilter},
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
        .connect(Uri::from_static("https://goerli.starknet.a5a.ch"))
        .await
        .unwrap();

    let config = Configuration::<Filter>::default()
        .with_finality(DataFinality::DataStatusPending)
        .with_filter(|mut filter| filter.with_header(HeaderFilter { weak: false }).build());

    configuration_handle.send(config).await?;

    // Initialize a Vec to store contract addresses and their transaction counts
    let mut contract_transaction_counts: Vec<(String, u32)> = Vec::new();



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
                    let header = block.header.unwrap_or_default();
                    let timestamp: DateTime<Utc> =
                        header.timestamp.unwrap_or_default().try_into()?;
                    //println!("  Block {:>6} ({})", header.block_number, timestamp);

                    // go through all events in the block
                    for event_with_tx in block.events {
                        // event includes the tx that triggered the event emission
                        // it also include the receipt in `event_with_tx.receipt`, but
                        // it's not used in this example
                        let event = event_with_tx.event.unwrap_or_default();
                        let tx = event_with_tx.transaction.unwrap_or_default();
                        let tx_hash = tx
                            .meta
                            .unwrap_or_default()
                            .hash
                            .unwrap_or_default()
                            .to_hex();

                        let from_addr = event.data[0].to_hex();
                        let to_addr = event.data[1].to_hex();

                        println!(
                            "    {} => {} ({})",
                            &from_addr[..8],
                            &to_addr[..8],
                            &tx_hash[..8]
                        );

                         // Update the transaction count for the contract address
                            // if let Some((_address, count)) = contract_transaction_counts
                            // .iter_mut()
                            // .find(|(address, _count)| address == &to_addr)
                            //     {
                            //         *count += 1;
                            //         println!("Updated: {}: {}", _address, count);
                            //     } else {
                            //         contract_transaction_counts.push((to_addr.clone(), 1));
                            //         println!("Added: {}: 1", to_addr);
                            //     }

                                // Find the index of the contract_address (from_addr) in the Vec.
                            if let Some(index) = contract_transaction_counts.iter().position(|(addr, _)| addr == &from_addr) {
                                // Increment the transaction count for the contract_address.
                                contract_transaction_counts[index].1 += 1;
                                println!("Updated: {}: {}", from_addr, contract_transaction_counts[index].1);
                            } else {
                                // Insert the contract_address with a transaction count of 1.
                                contract_transaction_counts.push((from_addr.clone(), 1));
                                println!("Added: {}: 1", from_addr);
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