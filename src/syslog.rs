use anyhow::{Error, Result};
use capnp::serialize;
use chrono::{DateTime, TimeZone, Utc};
use std::net::IpAddr;
use std::str::FromStr;

pub mod schema_capnp {
    include!(concat!(env!("OUT_DIR"), "/schema/syslog_capnp.rs"));
}
use schema_capnp::syslog_message;

#[derive(Debug, Clone)]
pub struct SyslogMessageData {
    #[allow(dead_code)]
    pub timestamp: DateTime<chrono::Utc>, // comes in a UNIX EPOCH time and should stay that way in this application and db
    pub source: IpAddr,
    #[allow(dead_code)]
    pub facility: i32,
    pub severity: i32,
    pub raw_message: String,
}

impl SyslogMessageData {
    fn from_syslog_capnp(msg: syslog_message::Reader) -> Result<Self> {
        Ok(SyslogMessageData {
            timestamp: Utc
                .timestamp_millis_opt(msg.get_timestamp() as i64)
                .single()
                .ok_or_else(|| Error::msg("Invalid timestamp"))?,
            source: IpAddr::from_str(msg.get_source()?.to_str()?)?,
            facility: msg.get_facility() as i32,
            severity: msg.get_severity() as i32,
            raw_message: msg.get_raw_message()?.to_str()?.to_owned(),
        })
    }
}

pub fn deserialize_message(bytes: &[u8]) -> Result<SyslogMessageData> {
    let mut slice = bytes;
    let message_reader =
        serialize::read_message_from_flat_slice(&mut slice, capnp::message::ReaderOptions::new())?;
    let msg = message_reader.get_root::<syslog_message::Reader>()?;
    SyslogMessageData::from_syslog_capnp(msg)
}
