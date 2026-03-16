use std::io::{self, BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use log::{debug, error, info, warn};

use crate::common::protocol::{StreamRequest, parse_stream_command};

use super::subscriptions::{RegistryError, SubscriptionRegistry};

pub fn process_command(
    command: &str,
    registry: &SubscriptionRegistry,
    stream_events_tx: Option<&Sender<StreamRequest>>,
) -> String {
    match parse_stream_command(command) {
        Ok(req) => {
            let registration = req.clone();
            match registry.register(registration.udp_addr, registration.tickers) {
                Ok(_) => {
                    if let Some(tx) = stream_events_tx {
                        if tx.send(req.clone()).is_err() {
                            registry.remove(&req.udp_addr);
                            error!("failed to enqueue stream event for client={}", req.udp_addr);
                            return "ERR internal server error\n".to_string();
                        }
                    }
                    info!("stream accepted for client={}", req.udp_addr);
                    "OK\n".to_string()
                }
                Err(RegistryError::AlreadyStreaming) => {
                    warn!(
                        "duplicating stream rejected for already active client={}",
                        req.udp_addr
                    );
                    "ERR already streaming\n".to_string()
                }
            }
        }
        Err(err) => {
            warn!("stream command parse failed: {err}");
            format!("ERR {err}\n")
        }
    }
}

pub fn handle_tcp_connection(
    stream: TcpStream,
    registry: SubscriptionRegistry,
    stream_events_tx: Option<Sender<StreamRequest>>,
) -> io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut command = String::new();
    reader.read_line(&mut command)?;
    debug!("received tcp command: {}", command.trim());

    let response = process_command(&command, &registry, stream_events_tx.as_ref());

    let mut writer = stream;
    writer.write_all(response.as_bytes())?;
    writer.flush()?;

    Ok(())
}

pub fn spawn_tcp_listener(
    bind_addr: &str,
    registry: SubscriptionRegistry,
    stop: Arc<AtomicBool>,
    stream_events_tx: Sender<StreamRequest>,
) -> io::Result<JoinHandle<()>> {
    let listener = TcpListener::bind(bind_addr)?;
    listener.set_nonblocking(true)?;
    info!("tcp listener started on {bind_addr}");

    Ok(thread::spawn(move || {
        while !stop.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((stream, _addr)) => {
                    debug!("accepted tcp connection");
                    let registry = registry.clone();
                    let stream_events_tx = stream_events_tx.clone();
                    thread::spawn(move || {
                        if let Err(err) =
                            handle_tcp_connection(stream, registry, Some(stream_events_tx))
                        {
                            error!("tcp connection handler failed: {err}");
                        }
                    });
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(25));
                }
                Err(err) => {
                    error!("tcp listener stopped due to unexpected accept error {err}");
                    break;
                }
            }
        }
        info!("tcp listener stopped");
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_ok_for_valid_stream_command() {
        let registry = SubscriptionRegistry::new();
        let response = process_command("STREAM udp://127.0.0.1:43210 AAPL,TSLA", &registry, None);

        assert_eq!(response, "OK\n");
    }

    #[test]
    fn returns_err_for_invalid_command() {
        let registry = SubscriptionRegistry::new();
        let response = process_command("HELLO", &registry, None);
        assert!(response.starts_with("ERR"));
    }

    #[test]
    fn returns_err_for_duplicate_subscription() {
        let registry = SubscriptionRegistry::new();
        let _ = process_command("STREAM udp://127.0.0.1:43210 AAPL,TSLA", &registry, None);
        let response = process_command("STREAM udp://127.0.0.1:43210 AAPL,TSLA", &registry, None);
        assert_eq!(response, "ERR already streaming\n");
    }
}
