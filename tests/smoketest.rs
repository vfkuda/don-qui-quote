use std::net::SocketAddr;
use std::thread;
use std::time::Duration;

use don_qui_quote::common::protocol::StockQuote;
use don_qui_quote::server::streaming::build_client_batch;
use don_qui_quote::server::subscriptions::SubscriptionRegistry;
use don_qui_quote::server::tcp_listener::process_command;

#[test]
fn smoke_duplicate_subscription_returns_err_already_streaming() {
    let registry = SubscriptionRegistry::new();

    let first = process_command("STREAM udp://127.0.0.1:34001 AAPL,TSLA", &registry, None);
    let second = process_command("STREAM udp://127.0.0.1:34001 AAPL,TSLA", &registry, None);

    assert_eq!(first, "OK\n");
    assert_eq!(second, "ERR already streaming\n");
}

#[test]
fn smoke_timeout_without_ping_removes_client_subscription() {
    let registry = SubscriptionRegistry::new();
    let client_addr: SocketAddr = "127.0.0.1:34002".parse().unwrap();

    registry
        .register(client_addr, vec!["AAPL".to_string()])
        .unwrap();

    thread::sleep(Duration::from_millis(35));
    let removed = registry.remove_stale(Duration::from_millis(10));

    assert_eq!(removed, vec![client_addr]);
    assert_eq!(registry.len(), 0);
}

#[test]
fn smoke_multi_client_batch_filtering_independent_per_subscription() {
    let registry = SubscriptionRegistry::new();
    let client_a: SocketAddr = "127.0.0.1:34010".parse().unwrap();
    let client_b: SocketAddr = "127.0.0.1:34011".parse().unwrap();

    registry
        .register(client_a, vec!["AAPL".to_string(), "TSLA".to_string()])
        .unwrap();
    registry
        .register(client_b, vec!["GOOGL".to_string()])
        .unwrap();

    let quotes = vec![
        StockQuote {
            ticker: "AAPL".to_string(),
            price: 101.0,
            volume: 1000,
            timestamp: 1,
        },
        StockQuote {
            ticker: "GOOGL".to_string(),
            price: 202.0,
            volume: 2000,
            timestamp: 1,
        },
        StockQuote {
            ticker: "TSLA".to_string(),
            price: 303.0,
            volume: 3000,
            timestamp: 1,
        },
    ];

    let snapshot = registry.snapshot();

    let batch_a = snapshot
        .iter()
        .find(|s| s.client_addr == client_a)
        .map(|s| build_client_batch(s, &quotes))
        .unwrap();

    let batch_b = snapshot
        .iter()
        .find(|s| s.client_addr == client_b)
        .map(|s| build_client_batch(s, &quotes))
        .unwrap();

    let tickers_a: Vec<&str> = batch_a.quotes.iter().map(|q| q.ticker.as_str()).collect();
    let tickers_b: Vec<&str> = batch_b.quotes.iter().map(|q| q.ticker.as_str()).collect();

    assert_eq!(tickers_a, vec!["AAPL", "TSLA"]);
    assert_eq!(tickers_b, vec!["GOOGL"]);
}
