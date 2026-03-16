use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use log::{debug, info, warn};

use don_qui_quote::client::config::ClientConfig;
use don_qui_quote::client::keepalive::{
    ConnectionHealth, ConnectionSnapshot, ConnectionState, RECONNECT_INTERVAL,
    assess_connection_health,
};
use don_qui_quote::client::tcp_session::send_stream_command;
use don_qui_quote::client::udp_receiver::decode_quotes_batch;
use don_qui_quote::common::logging::init_logging;
use don_qui_quote::common::protocol::PING_INTERVAL;
use don_qui_quote::common::shutdown::{AppRuntime, install_ctrlc_handler};

#[derive(Debug, Clone, Copy)]
struct ClientConnectionState {
    server_udp_addr: Option<SocketAddr>,
    last_quote_at: Option<Instant>,
    last_pong_at: Option<Instant>,
    last_subscribe_at: Option<Instant>,
    state: ConnectionState,
}

impl ClientConnectionState {
    fn new() -> Self {
        Self {
            server_udp_addr: None,
            last_quote_at: None,
            last_pong_at: None,
            last_subscribe_at: None,
            state: ConnectionState::Reconnecting,
        }
    }

    fn snapshot(&self) -> ConnectionSnapshot {
        ConnectionSnapshot {
            server_udp_addr: self.server_udp_addr,
            last_quote_at: self.last_quote_at,
            last_pong_at: self.last_pong_at,
            last_subscribe_at: self.last_subscribe_at,
            state: self.state,
        }
    }
}

fn spawn_udp_reader(
    socket_for_recv_data: UdpSocket,
    recv_stop: Arc<AtomicBool>,
    recv_state: Arc<Mutex<ClientConnectionState>>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut buf = [0_u8; 65_536];

        while !recv_stop.load(Ordering::Relaxed) {
            match socket_for_recv_data.recv_from(&mut buf) {
                Ok((size, src)) => {
                    if &buf[..size] == b"PONG" {
                        let mut guard = lock_or_recover(&recv_state);
                        guard.last_pong_at = Some(Instant::now());
                        debug!("received PONG from server={src}");
                        continue;
                    }

                    {
                        let mut guard = lock_or_recover(&recv_state);
                        guard.server_udp_addr = Some(src);
                        guard.last_quote_at = Some(Instant::now());
                        guard.state = ConnectionState::Connected;
                    }

                    match decode_quotes_batch(&buf[..size]) {
                        Ok(batch) => {
                            for quote in batch.quotes {
                                println!(
                                    "received from server: {} price={:.2} volume={} ts={}",
                                    quote.ticker, quote.price, quote.volume, quote.timestamp
                                );
                            }
                        }
                        Err(err) => warn!("invalid udp payload from {src}: {err}"),
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(25));
                }
                Err(err) => {
                    warn!("udp receiver stopped due to socket error: {err}");
                    break;
                }
            }
        }
        info!("udp receiver stopped");
    })
}
fn main() -> Result<(), Box<dyn Error>> {
    init_logging("info");

    let config = ClientConfig::parse()?;
    let local_bind_addr = format!("0.0.0.0:{}", config.udp_port);
    let udp_socket = UdpSocket::bind(&local_bind_addr)?;
    udp_socket.set_nonblocking(true)?;

    let advertised_udp_addr = resolve_advertised_udp_addr(config.server_addr, config.udp_port)?;

    let stop_flag = Arc::new(AtomicBool::new(false));
    install_ctrlc_handler(Arc::clone(&stop_flag))?;
    let runtime = AppRuntime::new(stop_flag);

    let socket_for_recv_data = udp_socket.try_clone()?;
    let socket_for_send_ping = udp_socket.try_clone()?;

    let connection_state = Arc::new(Mutex::new(ClientConnectionState::new()));

    // udp reader thread
    let recv_handle = spawn_udp_reader(
        socket_for_recv_data,
        runtime.stop_flag(),
        Arc::clone(&connection_state),
    );
    runtime.register_handle(recv_handle);

    // keepalive sender thread
    let ping_state = Arc::clone(&connection_state);
    let ping_stop = runtime.stop_flag();
    let ping_handle = thread::spawn(move || {
        while !ping_stop.load(Ordering::Relaxed) {
            let server_addr = {
                let guard = lock_or_recover(&ping_state);
                guard.server_udp_addr
            };

            if let Some(server_addr) = server_addr {
                if let Err(err) = socket_for_send_ping.send_to(b"PING", server_addr) {
                    warn!("failed to send PING to {server_addr}: {err}");
                }
            } else {
                debug!("waiting first quote packet to discover server udp address");
            }

            thread::sleep(PING_INTERVAL);
        }
        info!("keepalive sender stopped");
    });
    runtime.register_handle(ping_handle);

    info!(
        "quote_client started server_addr={} local_udp_bind={} advertised_udp_addr={} tickers={}",
        config.server_addr,
        local_bind_addr,
        advertised_udp_addr,
        config.tickers.join(",")
    );

    let mut last_reconnect_attempt = None;

    // main looop
    while !runtime.stop_flag().load(Ordering::Relaxed) {
        let now = Instant::now();
        let snapshot = {
            let guard = lock_or_recover(&connection_state);
            guard.snapshot()
        };

        match assess_connection_health(snapshot, now) {
            ConnectionHealth::Healthy | ConnectionHealth::AwaitingTraffic => {}
            ConnectionHealth::NeedsReconnect(reason) => {
                let should_attempt = last_reconnect_attempt
                    .is_none_or(|last| now.duration_since(last) >= RECONNECT_INTERVAL);

                if should_attempt {
                    {
                        let mut guard = lock_or_recover(&connection_state);
                        if guard.state != ConnectionState::Reconnecting {
                            warn!("connection degraded, entering reconnecting state: {reason}");
                        }
                        guard.state = ConnectionState::Reconnecting;
                        guard.last_subscribe_at = Some(now);
                        guard.last_quote_at = None;
                        guard.last_pong_at = None;
                    }

                    last_reconnect_attempt = Some(now);

                    match send_stream_command(
                        config.server_addr,
                        advertised_udp_addr,
                        &config.tickers,
                        Some(Duration::from_secs(3)),
                    ) {
                        Ok(()) => {
                            let mut guard = lock_or_recover(&connection_state);
                            guard.state = ConnectionState::Degraded;
                            guard.last_subscribe_at = Some(Instant::now());
                            info!("client resubscribed to server, awaiting fresh traffic");
                        }
                        Err(err) => {
                            warn!("client reconnect attempt failed: {err}");
                        }
                    }
                }
            }
        }

        thread::sleep(Duration::from_millis(100));
    }

    // shutting down runtime (all threads)
    runtime.wait();
    Ok(())
}

fn resolve_advertised_udp_addr(
    server_addr: SocketAddr,
    udp_port: u16,
) -> Result<SocketAddr, std::io::Error> {
    let probe_bind = match server_addr {
        SocketAddr::V4(_) => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        SocketAddr::V6(_) => SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0),
    };

    let probe = UdpSocket::bind(probe_bind)?;
    probe.connect(server_addr)?;
    let local_ip = probe.local_addr()?.ip();

    Ok(SocketAddr::new(local_ip, udp_port))
}

fn lock_or_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}
