use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use log::{debug, warn};

use crate::common::protocol::{PING_INTERVAL, PING_TIMEOUT};

pub const QUOTE_STREAM_TIMEOUT: Duration = Duration::from_secs(6);
pub const RECONNECT_INTERVAL: Duration = Duration::from_secs(2);
pub const SUBSCRIBE_GRACE_PERIOD: Duration = Duration::from_secs(3);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Connected,
    Degraded,
    Reconnecting,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionHealth {
    Healthy,
    AwaitingTraffic,
    NeedsReconnect(&'static str),
}

#[derive(Debug, Clone, Copy)]
pub struct ConnectionSnapshot {
    pub server_udp_addr: Option<SocketAddr>,
    pub last_quote_at: Option<Instant>,
    pub last_pong_at: Option<Instant>,
    pub last_subscribe_at: Option<Instant>,
    pub state: ConnectionState,
}

pub fn assess_connection_health(snapshot: ConnectionSnapshot, now: Instant) -> ConnectionHealth {
    if snapshot.state == ConnectionState::Reconnecting {
        return ConnectionHealth::NeedsReconnect("state is reconnecting");
    }

    if let Some(last_subscribe_at) = snapshot.last_subscribe_at {
        if snapshot.last_quote_at.is_none()
            && now.duration_since(last_subscribe_at) > SUBSCRIBE_GRACE_PERIOD
        {
            return ConnectionHealth::NeedsReconnect("no quotes after subscribe grace period");
        }
    }

    if let Some(last_quote_at) = snapshot.last_quote_at {
        if now.duration_since(last_quote_at) > QUOTE_STREAM_TIMEOUT {
            return ConnectionHealth::NeedsReconnect("quotes stream timeout");
        }
    } else {
        return ConnectionHealth::AwaitingTraffic;
    }

    if snapshot.server_udp_addr.is_some() {
        if let Some(last_pong_at) = snapshot.last_pong_at {
            if now.duration_since(last_pong_at) > PING_TIMEOUT {
                return ConnectionHealth::NeedsReconnect("pong timeout");
            }
        } else {
            return ConnectionHealth::NeedsReconnect("server udp addr known but no pong received");
        }
    }

    ConnectionHealth::Healthy
}

pub fn is_valid_pong(payload: &[u8], src: SocketAddr, expected_server: SocketAddr) -> bool {
    payload == b"PONG" && src == expected_server
}

pub fn ping_once(
    socket: &UdpSocket,
    server_addr: SocketAddr,
    read_timeout: Duration,
) -> io::Result<bool> {
    socket.send_to(b"PING", server_addr)?;
    socket.set_read_timeout(Some(read_timeout))?;

    let mut buf = [0_u8; 16];
    match socket.recv_from(&mut buf) {
        Ok((size, src)) => Ok(is_valid_pong(&buf[..size], src, server_addr)),
        Err(err)
            if matches!(
                err.kind(),
                io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
            ) =>
        {
            Ok(false)
        }
        Err(err) => Err(err),
    }
}

pub fn spawn_keepalive_sender(
    local_bind_addr: &str,
    server_addr: SocketAddr,
    stop: Arc<AtomicBool>,
    interval: Option<Duration>,
) -> io::Result<JoinHandle<()>> {
    let socket = UdpSocket::bind(local_bind_addr)?;
    let interval = interval.unwrap_or(PING_INTERVAL);

    Ok(thread::spawn(move || {
        while !stop.load(Ordering::Relaxed) {
            match ping_once(&socket, server_addr, Duration::from_millis(600)) {
                Ok(true) => debug!("keepalive pong received from server={server_addr}"),
                Ok(false) => {
                    warn!("keepalive pong timeout or invalid reply from server={server_addr}")
                }
                Err(err) => warn!("keepalive send/recv failed for server={server_addr}: {err}"),
            }
            thread::sleep(interval);
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn validates_only_expected_pong() {
        let server_addr: SocketAddr = "127.0.0.1:5000".parse().unwrap();
        let other_addr: SocketAddr = "127.0.0.1:5001".parse().unwrap();

        assert!(is_valid_pong(b"PONG", server_addr, server_addr));
        assert!(!is_valid_pong(b"PING", server_addr, server_addr));
        assert!(!is_valid_pong(b"PONG", other_addr, server_addr));
    }

    #[test]
    fn reconnects_when_quotes_timeout() {
        let now = Instant::now();
        let snapshot = ConnectionSnapshot {
            server_udp_addr: Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5000)),
            last_quote_at: Some(now - QUOTE_STREAM_TIMEOUT - Duration::from_secs(1)),
            last_pong_at: Some(now),
            last_subscribe_at: Some(now),
            state: ConnectionState::Connected,
        };

        assert_eq!(
            assess_connection_health(snapshot, now),
            ConnectionHealth::NeedsReconnect("quotes stream timeout")
        );
    }

    #[test]
    fn reconnects_when_subscribe_grace_elapsed_without_quotes() {
        let now = Instant::now();
        let snapshot = ConnectionSnapshot {
            server_udp_addr: None,
            last_quote_at: None,
            last_pong_at: None,
            last_subscribe_at: Some(now - SUBSCRIBE_GRACE_PERIOD - Duration::from_secs(1)),
            state: ConnectionState::Degraded,
        };

        assert_eq!(
            assess_connection_health(snapshot, now),
            ConnectionHealth::NeedsReconnect("no quotes after subscribe grace period")
        );
    }
}
