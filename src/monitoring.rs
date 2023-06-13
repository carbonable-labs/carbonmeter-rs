use chrono::{DateTime, Local};
use serde::Serialize;

#[derive(Debug, Default, Serialize)]
pub struct MonitoringData<T> {
    pub txs_count: usize,
    pub data: Vec<T>,
}
impl MonitoringData<MonitoringItem> {
    pub fn insert_monitoring_line(&mut self, item: MonitoringItem) {
        self.txs_count += item.txs.len();
        self.data.push(item);
    }
}

#[derive(Debug, Default, Serialize)]
pub struct MonitoringItem {
    pub time: DateTime<Local>,
    pub txs: Vec<String>,
}

impl MonitoringItem {
    pub fn new(time: DateTime<Local>, txs: Vec<String>) -> MonitoringItem {
        MonitoringItem { time, txs }
    }
}
