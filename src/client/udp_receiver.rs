use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use log::{debug, info, warn};

use crate::common::protocol::{ProtocolError, QuotesBatch, parse_quotes_batch};

pub fn decode_quotes_batch(payload: &[u8]) -> Result<QuotesBatch, ProtocolError> {
    let text = String::from_utf8_lossy(payload);
    parse_quotes_batch(&text)
}

pub fn recv_quotes_batch(
    socket: &UdpSocket,
    buffer: &mut [u8],
) -> io::Result<(QuotesBatch, SocketAddr)> {
    let (size, src) = socket.recv_from(buffer)?;
    let batch = decode_quotes_batch(&buffer[..size])
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
    Ok((batch, src))
}

pub fn spawn_udp_receiver(
    bind_addr: &str,
    stop: Arc<AtomicBool>,
) -> io::Result<(JoinHandle<()>, Receiver<QuotesBatch>)> {
    let socket = UdpSocket::bind(bind_addr)?;
    socket.set_nonblocking(true)?;

    let (tx, rx) = mpsc::channel();
    info!("udp receiver started on {bind_addr}");

    let handle = thread::spawn(move || {
        let mut buf = [0_u8; 65_536];
        while !stop.load(Ordering::Relaxed) {
            match recv_quotes_batch(&socket, &mut buf) {
                Ok((batch, src)) => {
                    debug!(
                        "received udp quotes batch from {src}, quotes_count={}",
                        batch.quotes.len()
                    );
                    if tx.send(batch).is_err() {
                        warn!("udp receiver output channel closed, stopping receiver");
                        break;
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(25));
                }
                Err(err) if err.kind() == io::ErrorKind::InvalidData => {
                    warn!("udp receiver skipped invalid payload: {err}");
                }
                Err(err) => {
                    warn!("udp receiver stopped due to socket error: {err}");
                    break;
                }
            }
        }
        info!("udp receiver stopped");
    });

    Ok((handle, rx))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_valid_quotes_batch_payload() {
        let payload =
            br#"{"quotes":[{"ticker":"AAPL","price":123.4,"volume":1000,"timestamp":1}]}"#;
        let batch = decode_quotes_batch(payload).unwrap();
        assert_eq!(batch.quotes.len(), 1);
        assert_eq!(batch.quotes[0].ticker, "AAPL");
    }

    #[test]
    fn fails_on_invalid_payload() {
        let payload = b"not-json";
        assert!(decode_quotes_batch(payload).is_err());
    }
}
