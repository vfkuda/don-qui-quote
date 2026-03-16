use std::collections::{HashMap, HashSet};
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use log::{debug, info, warn};

use crate::common::protocol::{PING_TIMEOUT, QuotesBatch, StockQuote, serialize_quotes_batch};

use super::subscriptions::{ClientSubscription, SubscriptionRegistry};

pub type WorkerRegistry = Arc<Mutex<HashMap<SocketAddr, Sender<Vec<StockQuote>>>>>;

pub fn new_worker_registry() -> WorkerRegistry {
    Arc::new(Mutex::new(HashMap::new()))
}

pub fn build_client_batch(subscription: &ClientSubscription, quotes: &[StockQuote]) -> QuotesBatch {
    let allowed: HashSet<&str> = subscription.tickers.iter().map(String::as_str).collect();
    let filtered = quotes
        .iter()
        .filter(|q| allowed.contains(q.ticker.as_str()))
        .cloned()
        .collect();

    QuotesBatch { quotes: filtered }
}

pub fn spawn_quotes_broadcaster(
    quotes_rx: Receiver<Vec<StockQuote>>,
    workers: WorkerRegistry,
    stop: Arc<AtomicBool>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        info!("quotes broadcaster started");
        while !stop.load(Ordering::Relaxed) {
            match quotes_rx.recv_timeout(Duration::from_millis(100)) {
                Ok(quotes) => {
                    let targets: Vec<(SocketAddr, Sender<Vec<StockQuote>>)> = {
                        lock_workers(&workers)
                            .iter()
                            .map(|(addr, tx)| (*addr, tx.clone()))
                            .collect()
                    };

                    let mut disconnected = Vec::new();
                    for (client_addr, tx) in targets {
                        if tx.send(quotes.clone()).is_err() {
                            disconnected.push(client_addr);
                        }
                    }

                    if !disconnected.is_empty() {
                        let mut guard = lock_workers(&workers);
                        for client_addr in disconnected {
                            guard.remove(&client_addr);
                            warn!("removed disconnected stream worker for client={client_addr}");
                        }
                    }
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => {
                    warn!("quotes broadcaster stopped: generator channel disconnected");
                    break;
                }
            }
        }
        info!("quotes broadcaster stopped");
    })
}

pub fn spawn_client_stream_worker(
    socket: Arc<UdpSocket>,
    registry: SubscriptionRegistry,
    workers: WorkerRegistry,
    client_addr: SocketAddr,
    quotes_rx: Receiver<Vec<StockQuote>>,
    stop: Arc<AtomicBool>,
    done_tx: Sender<String>,
    ping_timeout: Option<Duration>,
) -> JoinHandle<()> {
    let timeout = ping_timeout.unwrap_or(PING_TIMEOUT);
    thread::spawn(move || {
        info!("client stream worker started for client={client_addr}");
        while !stop.load(Ordering::Relaxed) {
            if !registry.is_ping_fresh(&client_addr, timeout) {
                warn!("stopped streaming due to ping timeout for client={client_addr}");
                break;
            }

            match quotes_rx.recv_timeout(Duration::from_millis(100)) {
                Ok(quotes) => {
                    let Some(subscription) = registry
                        .snapshot()
                        .into_iter()
                        .find(|sub| sub.client_addr == client_addr)
                    else {
                        break;
                    };

                    let batch = build_client_batch(&subscription, &quotes);
                    if batch.quotes.is_empty() {
                        continue;
                    }

                    if let Err(err) = send_batch(&socket, client_addr, &batch) {
                        warn!("stream send failed for client={client_addr}: {err}");
                        break;
                    }
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => {
                    warn!(
                        "client stream worker stopped: broadcast channel closed for client={client_addr}"
                    );
                    break;
                }
            }
        }

        registry.remove(&client_addr);
        lock_workers(&workers).remove(&client_addr);
        let _ = done_tx.send(client_addr.to_string());
        info!("client stream worker stopped for client={client_addr}");
    })
}

pub fn register_stream_worker(
    workers: &WorkerRegistry,
    client_addr: SocketAddr,
) -> Receiver<Vec<StockQuote>> {
    let (tx, rx) = mpsc::channel();
    lock_workers(workers).insert(client_addr, tx);
    rx
}

fn send_batch(socket: &UdpSocket, client_addr: SocketAddr, batch: &QuotesBatch) -> io::Result<()> {
    let payload = serialize_quotes_batch(batch)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    socket.send_to(payload.as_bytes(), client_addr)?;
    debug!(
        "sent quotes batch to client={}, quotes_count={}",
        client_addr,
        batch.quotes.len()
    );
    Ok(())
}

fn lock_workers(
    workers: &WorkerRegistry,
) -> MutexGuard<'_, HashMap<SocketAddr, Sender<Vec<StockQuote>>>> {
    match workers.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_batch_for_requested_tickers_only() {
        let subscription = ClientSubscription {
            client_addr: "127.0.0.1:5000".parse().unwrap(),
            tickers: vec!["AAPL".into(), "TSLA".into()],
            last_seen_ping: std::time::Instant::now(),
        };

        let quotes = vec![
            StockQuote {
                ticker: "AAPL".into(),
                price: 1.0,
                volume: 1,
                timestamp: 1,
            },
            StockQuote {
                ticker: "GOOGL".into(),
                price: 2.0,
                volume: 2,
                timestamp: 2,
            },
        ];

        let batch = build_client_batch(&subscription, &quotes);

        assert_eq!(batch.quotes.len(), 1);
        assert_eq!(batch.quotes[0].ticker, "AAPL");
    }

    #[test]
    fn register_stream_worker_adds_sender_for_client() {
        let workers = new_worker_registry();
        let client_addr: SocketAddr = "127.0.0.1:5555".parse().unwrap();

        let _rx = register_stream_worker(&workers, client_addr);

        assert!(lock_workers(&workers).contains_key(&client_addr));
    }

    #[test]
    fn stale_client_worker_condition_is_detected_via_registry() {
        let registry = SubscriptionRegistry::new();
        let client_addr: SocketAddr = "127.0.0.1:5556".parse().unwrap();

        registry.register(client_addr, vec!["AAPL".into()]).unwrap();
        std::thread::sleep(Duration::from_millis(25));

        assert!(!registry.is_ping_fresh(&client_addr, Duration::from_millis(5)));
    }
}
