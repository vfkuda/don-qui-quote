use std::error::Error;
use std::io;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

use clap::Parser;
use log::{error, info, warn};

use don_qui_quote::common::logging::init_logging;
use don_qui_quote::common::protocol::{StockQuote, StreamRequest};
use don_qui_quote::common::shutdown::{AppRuntime, install_ctrlc_handler};
use don_qui_quote::server::generator::spawn_generator;
use don_qui_quote::server::streaming::{
    new_worker_registry, register_stream_worker, spawn_client_stream_worker,
    spawn_quotes_broadcaster,
};
use don_qui_quote::server::subscriptions::SubscriptionRegistry;
use don_qui_quote::server::tcp_listener::spawn_tcp_listener;

#[derive(Debug, Parser)]
#[command(name = "quote_server")]
struct ServerArgs {
    #[arg(long = "tcp-addr", default_value = "127.0.0.1:7000")]
    tcp_addr: String,
    #[arg(long = "udp-addr", default_value = "127.0.0.1:7001")]
    udp_addr: String,
    #[arg(
        long = "tickers",
        default_value = "AAPL,MSFT,GOOGL,AMZN,NVDA,META,TSLA,NFLX,ADBE,INTC,CSCO,NKE"
    )]
    tickers: String,
}

fn main() -> Result<(), Box<dyn Error>> {
    // init and pass cli args
    init_logging("info");
    let args = ServerArgs::parse();

    let generator_tickers = parse_tickers(&args.tickers);
    if generator_tickers.is_empty() {
        error!("--tickers cannot be empty");
        return Err("abort".into());
    }

    // create runtime
    let stop = Arc::new(AtomicBool::new(false));
    install_ctrlc_handler(Arc::clone(&stop))?;
    let runtime = AppRuntime::new(stop);

    // clients registry
    let registry = SubscriptionRegistry::new();
    let workers = new_worker_registry();

    // spawn thread : for generator and consumption channel
    let (quotes_tx, quotes_rx) = mpsc::channel::<Vec<StockQuote>>();
    let generator_handle = spawn_generator(generator_tickers, quotes_tx, runtime.stop_flag(), None);
    runtime.register_handle(generator_handle);

    let (stream_events_tx, stream_events_rx) = mpsc::channel::<StreamRequest>();
    // spawn thread :  for tcp listerner which spawns new thread for every unique client accepted
    let tcp_handle = spawn_tcp_listener(
        &args.tcp_addr,
        registry.clone(),
        runtime.stop_flag(),
        stream_events_tx,
    )?;
    runtime.register_handle(tcp_handle);

    // listen udp socket
    let udp_socket = Arc::new(UdpSocket::bind(&args.udp_addr)?);
    udp_socket.set_nonblocking(true)?;

    // spawn thread : for heartbeats
    let keepalive_socket = Arc::clone(&udp_socket);
    let keepalive_registry = registry.clone();
    let keepalive_stop = runtime.stop_flag();
    let keepalive_handle = thread::spawn(move || {
        const PINGPONG_BUFFER_SIZE: usize = 64;
        let mut buf = [0_u8; PINGPONG_BUFFER_SIZE];
        while !keepalive_stop.load(Ordering::Relaxed) {
            match keepalive_socket.recv_from(&mut buf) {
                Ok((size, src)) => {
                    if &buf[..size] == b"PING" {
                        if keepalive_registry.update_ping(&src) {
                            let _ = keepalive_socket.send_to(b"PONG", src);
                        }
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(25));
                }
                Err(err) => {
                    warn!("keepalive listener stopped due to socket error: {err}");
                    break;
                }
            }
        }
        info!("keepalive listener stopped");
    });
    runtime.register_handle(keepalive_handle);

    let broadcaster_handle =
        spawn_quotes_broadcaster(quotes_rx, workers.clone(), runtime.stop_flag());
    runtime.register_handle(broadcaster_handle);

    info!(
        "quote_server started tcp_addr={} udp_addr={} tickers={}",
        args.tcp_addr, args.udp_addr, args.tickers
    );

    // main process loop
    while !runtime.stop_flag().load(Ordering::Relaxed) {
        let cleaned = runtime.cleanup_finished_dynamic();
        if cleaned > 0 {
            info!("cleaned up {} finished dynamic thread(s)", cleaned);
        }

        match stream_events_rx.recv_timeout(Duration::from_millis(100)) {
            Ok(req) => {
                let worker_key = req.udp_addr.to_string();
                let worker_rx = register_stream_worker(&workers, req.udp_addr);
                let worker_handle = spawn_client_stream_worker(
                    Arc::clone(&udp_socket),
                    registry.clone(),
                    workers.clone(),
                    req.udp_addr,
                    worker_rx,
                    runtime.stop_flag(),
                    runtime.dynamic_done_notifier(),
                    None,
                );
                runtime.register_dynamic_handle(worker_key, worker_handle);
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                warn!("stream events channel disconnected");
                runtime.trigger_shutdown();
                break;
            }
        }
    }
    // waiting all threads shuted down
    runtime.wait();
    Ok(())
}

fn parse_tickers(csv: &str) -> Vec<String> {
    csv.split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}
