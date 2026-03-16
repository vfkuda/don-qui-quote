use std::io::{self, BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum TcpSessionError {
    #[error("empty tickers list")]
    EmptyTickers,
    #[error("tcp connect failed: {0}")]
    Connect(#[source] io::Error),
    #[error("tcp io failed: {0}")]
    Io(#[source] io::Error),
    #[error("server error: {0}")]
    ServerErr(String),
    #[error("unexpected server response: {0}")]
    UnexpectedResponse(String),
}

pub fn build_stream_command(
    udp_addr: SocketAddr,
    tickers: &[String],
) -> Result<String, TcpSessionError> {
    if tickers.is_empty() {
        return Err(TcpSessionError::EmptyTickers);
    }

    let payload = tickers.join(",");
    Ok(format!("STREAM udp://{udp_addr} {payload}\n"))
}

pub fn parse_stream_response(response: &str) -> Result<(), TcpSessionError> {
    let line = response.trim();
    if line == "OK" {
        return Ok(());
    }

    if let Some(err) = line.strip_prefix("ERR ") {
        return Err(TcpSessionError::ServerErr(err.to_string()));
    }

    Err(TcpSessionError::UnexpectedResponse(line.to_string()))
}

pub fn send_stream_command(
    server_addr: SocketAddr,
    udp_addr: SocketAddr,
    tickers: &[String],
    timeout: Option<Duration>,
) -> Result<(), TcpSessionError> {
    let mut stream = TcpStream::connect(server_addr).map_err(TcpSessionError::Connect)?;
    if let Some(timeout) = timeout {
        stream
            .set_read_timeout(Some(timeout))
            .map_err(TcpSessionError::Io)?;
        stream
            .set_write_timeout(Some(timeout))
            .map_err(TcpSessionError::Io)?;
    }

    let command = build_stream_command(udp_addr, tickers)?;
    stream
        .write_all(command.as_bytes())
        .map_err(TcpSessionError::Io)?;
    stream.flush().map_err(TcpSessionError::Io)?;

    let mut reader = BufReader::new(stream);
    let mut response = String::new();
    reader
        .read_line(&mut response)
        .map_err(TcpSessionError::Io)?;

    parse_stream_response(&response)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_stream_command() {
        let udp_addr: SocketAddr = "127.0.0.1:34254".parse().unwrap();
        let command = build_stream_command(udp_addr, &["AAPL".into(), "TSLA".into()]).unwrap();
        assert_eq!(command, "STREAM udp://127.0.0.1:34254 AAPL,TSLA\n");
    }

    #[test]
    fn parse_response_ok() {
        assert!(parse_stream_response("OK\n").is_ok());
    }

    #[test]
    fn parse_response_err() {
        let err = parse_stream_response("ERR already streaming\n").unwrap_err();
        assert!(matches!(err, TcpSessionError::ServerErr(_)));
    }
}
