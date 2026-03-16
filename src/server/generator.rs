use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc::Sender};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use log::{error, info};
use rand::Rng;

use crate::common::protocol::{DEFAULT_TICK_INTERVAL, StockQuote};

pub struct QuoteGenerator {
    prices: HashMap<String, f64>,
}

impl QuoteGenerator {
    pub fn new(tickers: &[String]) -> Self {
        let mut prices = HashMap::new();
        for ticker in tickers {
            prices.insert(ticker.clone(), 100.0);
        }
        Self { prices }
    }

    pub fn generate_batch(&mut self, tickers: &[String]) -> Vec<StockQuote> {
        tickers
            .iter()
            .filter_map(|ticker| self.generate_quote(ticker))
            .collect()
    }

    fn generate_quote(&mut self, ticker: &str) -> Option<StockQuote> {
        let last_price = self.prices.get_mut(ticker)?;

        let mut rng = rand::thread_rng();
        let delta: f64 = rng.gen_range(-2.0..=2.0);
        *last_price = (*last_price + delta).max(0.0);

        let volume = match ticker {
            "AAPL" | "AMZN" | "NVDA" => 1000 + (rng.gen_range(0.0..5000.0) as u32),
            _ => 100 + (rng.gen_range(0.0..1000.0) as u32),
        };

        Some(StockQuote {
            ticker: ticker.to_string(),
            price: *last_price,
            volume,
            timestamp: current_timestamp_millis(),
        })
    }
}

pub fn spawn_generator(
    tickers: Vec<String>,
    tx: Sender<Vec<StockQuote>>,
    stop: Arc<AtomicBool>,
    interval: Option<Duration>,
) -> JoinHandle<()> {
    let interval = interval.unwrap_or(DEFAULT_TICK_INTERVAL);
    thread::spawn(move || {
        let mut generator = QuoteGenerator::new(&tickers);
        info!(
            "quote generator started with {} tickers, interval_ms={}",
            tickers.len(),
            interval.as_millis()
        );
        while !stop.load(Ordering::Relaxed) {
            let batch = generator.generate_batch(&tickers);
            if tx.send(batch).is_err() {
                error!("quote generator channel closed, stopping generator");
                break;
            }
            thread::sleep(interval);
        }
        info!("quote generator stopped");
    })
}

fn current_timestamp_millis() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis() as u64,
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    use super::*;

    #[test]
    fn generated_prices_are_non_negative() {
        let tickers = vec!["AAPL".to_string(), "TSLA".to_string()];
        let mut generator = QuoteGenerator::new(&tickers);

        for _ in 0..500 {
            let batch = generator.generate_batch(&tickers);
            assert!(!batch.is_empty());
            assert!(batch.iter().all(|q| q.price >= 0.0));
        }
    }

    #[test]
    fn emits_batches_periodically() {
        let tickers = vec!["AAPL".to_string()];
        let (tx, rx) = mpsc::channel();
        let stop = Arc::new(AtomicBool::new(false));

        let handle = spawn_generator(
            tickers,
            tx,
            Arc::clone(&stop),
            Some(Duration::from_millis(50)),
        );

        let start = Instant::now();
        let _first = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let _second = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let elapsed = start.elapsed();

        stop.store(true, Ordering::Relaxed);
        handle.join().unwrap();

        assert!(elapsed >= Duration::from_millis(40));
    }
}
