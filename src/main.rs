use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Error, Result};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio_tungstenite::{accept_async, tungstenite::Message};
use zmq::{Context, SUB};

pub mod schema_capnp {
    include!(concat!(env!("OUT_DIR"), "/schema/syslog_capnp.rs"));
}

mod syslog;
use syslog::{SyslogMessageData, deserialize_message};

const ZMQ_SUB_ADDRESS: &str = "tcp://localhost:18050";
const WEBSOCKET_SERVER_ADDRESS: &str = "0.0.0.0:18056";
const MAX_MESSAGES: usize = 250;
const DEBUG_MODE: bool = true;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WebSocketMessage {
    timestamp_raw: i64,
    source: String,
    facility: i32,
    severity: i32,
    raw_message: String,
}

impl From<&SyslogMessageData> for WebSocketMessage {
    fn from(msg: &SyslogMessageData) -> Self {
        WebSocketMessage {
            timestamp_raw: msg.timestamp.timestamp_millis(),
            source: msg.source.to_string(),
            facility: msg.facility,
            severity: msg.severity,
            raw_message: msg.raw_message.clone(),
        }
    }
}

fn _severity_to_name(severity: i32) -> String {
    match severity {
        0 => "EMERGENCY",
        1 => "ALERT",
        2 => "CRITICAL",
        3 => "ERROR",
        4 => "WARNING",
        5 => "NOTICE",
        6 => "INFO",
        7 => "DEBUG",
        _ => "UNKNOWN",
    }
    .to_string()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting syslog to WebSocket relay service");

    let message_buffer = Arc::new(Mutex::new(VecDeque::with_capacity(MAX_MESSAGES)));

    let (tx, _) = broadcast::channel::<WebSocketMessage>(100);
    let tx_clone = tx.clone();

    let buffer_clone = Arc::clone(&message_buffer);
    tokio::spawn(async move {
        loop {
            match run_zmq_subscriber(buffer_clone.clone(), tx_clone.clone()).await {
                Ok(_) => {
                    eprintln!("ZMQ subscriber unexpectedly completed. Restarting...");
                }
                Err(e) => {
                    eprintln!("ZMQ subscriber error: {}", e);
                    eprintln!("Attempting to reconnect in 5 seconds...");
                }
            }

            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });

    let listener = TcpListener::bind(WEBSOCKET_SERVER_ADDRESS).await?;
    println!("WebSocket server listening on {}", WEBSOCKET_SERVER_ADDRESS);

    while let Ok((stream, addr)) = listener.accept().await {
        println!("New WebSocket connection from: {}", addr);
        let tx = tx.clone();
        let buffer_clone = Arc::clone(&message_buffer);

        tokio::spawn(async move {
            if let Err(e) = handle_websocket_connection(stream, tx, buffer_clone).await {
                eprintln!("WebSocket error with {}: {}", addr, e);
            }
        });
    }

    Ok(())
}

async fn run_zmq_subscriber(
    message_buffer: Arc<Mutex<VecDeque<WebSocketMessage>>>,
    tx: broadcast::Sender<WebSocketMessage>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let context = Context::new();
    let socket = context.socket(SUB)?;
    socket.connect(ZMQ_SUB_ADDRESS)?;
    socket.set_subscribe(b"syslog")?;

    println!("Connected to ZMQ publisher at {}", ZMQ_SUB_ADDRESS);

    let (zmq_tx, mut zmq_rx) = tokio::sync::mpsc::channel::<Vec<Vec<u8>>>(100);

    std::thread::spawn(move || {
        loop {
            let mut parts = Vec::new();
            let mut msg = zmq::Message::new();

            if let Ok(_) = socket.recv(&mut msg, 0) {
                parts.push(msg.to_vec());

                loop {
                    let more = socket.get_rcvmore().unwrap_or(false);
                    if !more {
                        break;
                    }

                    let mut part = zmq::Message::new();
                    if let Ok(_) = socket.recv(&mut part, 0) {
                        parts.push(part.to_vec());
                    } else {
                        break;
                    }
                }

                let _ = zmq_tx.blocking_send(parts);
            }

            std::thread::sleep(Duration::from_millis(1));
        }
    });

    while let Some(parts) = zmq_rx.recv().await {
        if parts.is_empty() {
            continue;
        }

        if DEBUG_MODE {
            println!("Received ZMQ message with {} parts", parts.len());
        }

        // Process the message based on parts
        // In a ZMQ SUB socket with a filter:
        // - Part 1 is often the filter/topic (e.g., "syslog")
        // - Part 2 (or the remainder of part 1 after the filter) is the payload

        if parts.len() == 1 {
            let bytes = &parts[0];
            if bytes.starts_with(b"syslog") {
                let payload = extract_payload_after_topic(bytes, b"syslog");
                process_message(&payload, &message_buffer, &tx).await;
            } else {
                process_message(bytes, &message_buffer, &tx).await;
            }
        } else if parts.len() >= 2 {
            let bytes = &parts[1];
            process_message(bytes, &message_buffer, &tx).await;
        }
    }

    Ok(())
}

fn extract_payload_after_topic(bytes: &[u8], topic: &[u8]) -> Vec<u8> {
    if bytes.starts_with(topic) {
        let mut pos = topic.len();

        while pos < bytes.len() && (bytes[pos] == b' ' || bytes[pos] == 0) {
            pos += 1;
        }

        bytes[pos..].to_vec()
    } else {
        bytes.to_vec()
    }
}

async fn process_message(
    bytes: &[u8],
    message_buffer: &Arc<Mutex<VecDeque<WebSocketMessage>>>,
    tx: &broadcast::Sender<WebSocketMessage>,
) {
    if DEBUG_MODE && !bytes.is_empty() {
        println!("Processing message of length: {} bytes", bytes.len());

        let preview_len = std::cmp::min(bytes.len(), 32);
        let hex_dump = bytes[..preview_len]
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<Vec<String>>()
            .join(" ");
        println!("Message begins with: {}", hex_dump);
    }

    match process_capnp_message(bytes) {
        Ok(processed_msg) => {
            if DEBUG_MODE {
                println!(
                    "Successfully processed message: ({}) - {}",
                    processed_msg.source,
                    processed_msg
                        .raw_message
                        .chars()
                        .take(50)
                        .collect::<String>()
                );
            }

            {
                let mut buffer = message_buffer.lock().unwrap();
                if buffer.len() >= MAX_MESSAGES {
                    buffer.pop_front();
                }
                buffer.push_back(processed_msg.clone());
            }

            let _ = tx.send(processed_msg);
        }
        Err(e) => {
            eprintln!("Error processing Cap'n Proto message: {}", e);

            if DEBUG_MODE && !bytes.is_empty() {
                let dump_len = std::cmp::min(bytes.len(), 128);
                let hex_dump = bytes[..dump_len]
                    .iter()
                    .map(|b| format!("{:02x}", b))
                    .collect::<Vec<String>>()
                    .join(" ");
                eprintln!("Failed message bytes (first {}): {}", dump_len, hex_dump);
            }
        }
    }
}

fn process_capnp_message(bytes: &[u8]) -> Result<WebSocketMessage> {
    if bytes.is_empty() {
        return Err(anyhow::Error::msg("Empty message received"));
    }

    match deserialize_message(bytes) {
        Ok(syslog_msg) => Ok(WebSocketMessage::from(&syslog_msg)),
        Err(e) => Err(anyhow::Error::msg(format!(
            "Deserialization error: {}. Message length: {}",
            e,
            bytes.len()
        ))),
    }
}

async fn handle_websocket_connection(
    stream: TcpStream,
    tx: broadcast::Sender<WebSocketMessage>,
    message_buffer: Arc<Mutex<VecDeque<WebSocketMessage>>>,
) -> Result<()> {
    let ws_stream = accept_async(stream).await?;
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();

    let initial_messages = {
        let buffer = message_buffer.lock().unwrap();
        buffer.iter().cloned().collect::<Vec<WebSocketMessage>>()
    };

    if !initial_messages.is_empty() {
        let json = serde_json::to_string(&initial_messages)?;
        ws_sender.send(Message::Text(json.into())).await?;
    }

    let mut rx = tx.subscribe();

    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = ws_receiver.next().await {
            if let Message::Text(text) = msg {
                if DEBUG_MODE {
                    println!("Received message from client: {}", text);
                }
            }
        }
    });

    let mut send_task = tokio::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            let json = serde_json::to_string(&msg)?;
            if let Err(e) = ws_sender.send(Message::Text(json.into())).await {
                eprintln!("Error sending to WebSocket: {}", e);
                break;
            }
        }

        Ok::<_, Error>(())
    });

    tokio::select! {
        _ = &mut recv_task => {},
        _ = &mut send_task => {},
    }

    recv_task.abort();
    send_task.abort();

    Ok(())
}
