use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use anyhow::Result;
use carbonmeter_rs::{
    db::get_ro_db_connection_with_prefix_extractor,
    get_last_handled_block,
    monitoring::{MonitoringData, MonitoringItem},
    serializer::deserialize_from_bytes,
};
use chrono::{Local, TimeZone};
use log::debug;
use rocksdb::{DBWithThreadMode, IteratorMode, MultiThreaded, ReadOptions};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;

pub struct AppDependencies {
    pub db: Arc<DBWithThreadMode<MultiThreaded>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TimerangeQuery {
    pub start: String,
    pub end: String,
}

async fn get_last_block_ingested() -> impl Responder {
    debug!("/monitoring/last_block");
    match get_last_handled_block().await {
        Ok(block) => HttpResponse::Ok().json(json!({ "last_block": block.to_string() })),
        Err(e) => HttpResponse::InternalServerError().json(json!({ "error": e.to_string() })),
    }
}

async fn get_global_protocol_txs(q: web::Query<TimerangeQuery>) -> impl Responder {
    debug!("/monitoring/data/global?start={}&end={}", q.start, q.end);

    let prefix_extractor = rocksdb::SliceTransform::create_fixed_prefix(11);
    let db = get_ro_db_connection_with_prefix_extractor(prefix_extractor)
        .await
        .unwrap();

    let lower_prefix_key = format!("{}#", q.start);
    let upper_prefix_key = format!("{}#", q.end);

    let mut readopts = ReadOptions::default();
    readopts.set_iterate_lower_bound(lower_prefix_key.as_bytes().to_vec());
    readopts.set_iterate_upper_bound(upper_prefix_key.as_bytes().to_vec());
    readopts.set_pin_data(true);

    let iter = db.iterator_opt(IteratorMode::Start, readopts);

    let mut data: MonitoringData<MonitoringItem> = MonitoringData::default();

    for item in iter {
        let (key, value) = item.unwrap();
        let key_str = std::str::from_utf8(&key).expect("should be valid utf-8");
        let value_vec = value.to_vec();
        let txs = deserialize_from_bytes(&value_vec);

        let timestamp = key_str.split('#').next().unwrap();
        let dt = Local
            .timestamp_opt(timestamp.parse::<i64>().unwrap(), 0)
            .unwrap();

        let txs: Vec<String> = txs.into_iter().map(String::from).collect();
        let monitoring_item = MonitoringItem::new(dt, txs);
        data.insert_monitoring_line(monitoring_item);
    }

    HttpResponse::Ok().json(data)
}

async fn get_protocol_txs(path: web::Path<String>) -> impl Responder {
    let wallet = path.into_inner();
    debug!("/monitoring/data/{wallet}");

    let prefix_extractor = rocksdb::SliceTransform::create_fixed_prefix(74);
    let db = get_ro_db_connection_with_prefix_extractor(prefix_extractor)
        .await
        .unwrap();

    let prefix_key = format!("ACCOUNT#{wallet}");

    let mut readopts = ReadOptions::default();
    readopts.set_prefix_same_as_start(true);
    readopts.set_iterate_lower_bound(prefix_key.as_bytes().to_vec());
    readopts.set_pin_data(true);

    let iter = db.iterator_opt(IteratorMode::Start, readopts);

    let mut data: MonitoringData<MonitoringItem> = MonitoringData::default();

    for item in iter {
        let (key, value) = item.unwrap();
        let key_str = std::str::from_utf8(&key).expect("should be valid utf-8");
        let value_vec = value.to_vec();
        let txs = deserialize_from_bytes(&value_vec);

        let timestamp = key_str.split('#').last().unwrap();
        let dt = Local
            .timestamp_opt(timestamp.parse::<i64>().unwrap(), 0)
            .unwrap();

        let txs: Vec<String> = txs.into_iter().map(String::from).collect();
        let monitoring_item = MonitoringItem::new(dt, txs);
        data.insert_monitoring_line(monitoring_item);
    }

    HttpResponse::Ok().json(data)
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    Ok(HttpServer::new(move || {
        App::new().service(
            web::scope("/monitoring/data")
                .route("/global", web::get().to(get_global_protocol_txs))
                .route("/last_block", web::get().to(get_last_block_ingested))
                .route("/{contract}", web::get().to(get_protocol_txs)),
        )
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await?)
}
