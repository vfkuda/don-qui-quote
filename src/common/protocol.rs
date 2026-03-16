use std::net::SocketAddr;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const DEFAULT_TICK_INTERVAL: Duration = Duration::from_secs(1);
pub const PING_INTERVAL: Duration = Duration::from_secs(2);
pub const PING_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StockQuote {
    pub ticker: String,
    pub price: f64,
    pub volume: u32,
    pub timestamp: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuotesBatch {
    pub quotes: Vec<StockQuote>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamRequest {
    pub udp_addr: SocketAddr,
    pub tickers: Vec<String>,
}

#[derive(Debug, Error, PartialEq)]
pub enum ProtocolError {
    #[error("invalid command")]
    InvalidCommand,
    #[error("invalid udp url")]
    InvalidUdpUrl,
    #[error("invalid udp address")]
    InvalidUdpAddress,
    #[error("empty tickers list")]
    EmptyTickers,
    #[error("json error: {0}")]
    Json(String),
}

pub fn parse_stream_command(line: &str) -> Result<StreamRequest, ProtocolError> {
    let trimmed = line.trim();
    let mut parts = trimmed.split_whitespace();

    let command = parts.next().ok_or(ProtocolError::InvalidCommand)?;
    if !command.eq_ignore_ascii_case("STREAM") {
        return Err(ProtocolError::InvalidCommand);
    }

    let udp_url = parts.next().ok_or(ProtocolError::InvalidCommand)?;
    if !udp_url.starts_with("udp://") {
        return Err(ProtocolError::InvalidUdpUrl);
    }

    let addr: SocketAddr = udp_url
        .trim_start_matches("udp://")
        .parse()
        .map_err(|_| ProtocolError::InvalidUdpAddress)?;

    let tickers_part = parts.next().ok_or(ProtocolError::EmptyTickers)?;

    if parts.next().is_some() {
        return Err(ProtocolError::InvalidCommand);
    }

    let tickers: Vec<String> = tickers_part
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();

    if tickers.is_empty() {
        return Err(ProtocolError::EmptyTickers);
    }

    Ok(StreamRequest {
        udp_addr: addr,
        tickers,
    })
}

pub fn serialize_quotes_batch(batch: &QuotesBatch) -> Result<String, ProtocolError> {
    serde_json::to_string(batch).map_err(|e| ProtocolError::Json(e.to_string()))
}

pub fn parse_quotes_batch(payload: &str) -> Result<QuotesBatch, ProtocolError> {
    serde_json::from_str(payload).map_err(|e| ProtocolError::Json(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_stream_command() {
        let parsed = parse_stream_command("STREAM udp://127.0.0.1:34254 AAPL,TSLA").unwrap();

        assert_eq!(parsed.udp_addr, "127.0.0.1:34254".parse().unwrap());
        assert_eq!(parsed.tickers, vec!["AAPL", "TSLA"]);
    }

    #[test]
    fn rejects_empty_tickers() {
        let err = parse_stream_command("STREAM udp://127.0.0.1:34254 ,").unwrap_err();
        assert_eq!(err, ProtocolError::EmptyTickers);
    }

    #[test]
    fn json_roundtrip_quotes_batch() {
        let batch = QuotesBatch {
            quotes: vec![
                StockQuote {
                    ticker: "AAPL".to_string(),
                    price: 123.45,
                    volume: 100,
                    timestamp: 1,
                },
                StockQuote {
                    ticker: "TSLA".to_string(),
                    price: 300.5,
                    volume: 200,
                    timestamp: 2,
                },
            ],
        };

        let payload = serialize_quotes_batch(&batch).unwrap();
        let parsed = parse_quotes_batch(&payload).unwrap();

        assert_eq!(parsed, batch);
    }
}
