use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use log::{debug, error, info, warn};

use super::subscriptions::SubscriptionRegistry;

pub fn handle_keepalive_packet(
    socket: &UdpSocket,
    registry: &SubscriptionRegistry,
    src: SocketAddr,
    payload: &[u8],
) -> io::Result<()> {
    if should_ack_ping(registry, src, payload) {
        debug!("keepalive ping accepted from client={src}");
        socket.send_to(b"PONG", src)?;
    } else {
        warn!("keepalive packet ignored from client={src}");
    }
    Ok(())
}

pub fn should_ack_ping(registry: &SubscriptionRegistry, src: SocketAddr, payload: &[u8]) -> bool {
    payload == b"PING" && registry.update_ping(&src)
}

pub fn spawn_keepalive_listener(
    bind_addr: &str,
    registry: SubscriptionRegistry,
    stop: Arc<AtomicBool>,
) -> io::Result<JoinHandle<()>> {
    let socket = UdpSocket::bind(bind_addr)?;
    socket.set_nonblocking(true)?;
    info!("keepalive udp listener started on {bind_addr}");

    Ok(thread::spawn(move || {
        let mut buf = [0_u8; 1024];
        while !stop.load(Ordering::Relaxed) {
            match socket.recv_from(&mut buf) {
                Ok((size, src)) => {
                    if let Err(err) = handle_keepalive_packet(&socket, &registry, src, &buf[..size])
                    {
                        error!("failed to handle keepalive packet from {src}: {err}");
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(25));
                }
                Err(_) => {
                    error!("keepalive listener stopped due to receive error");
                    break;
                }
            }
        }
        info!("keepalive udp listener stopped");
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_ping_is_acknowledged() {
        let registry = SubscriptionRegistry::new();
        let client_addr: SocketAddr = "127.0.0.1:5001".parse().unwrap();

        registry.register(client_addr, vec!["AAPL".into()]).unwrap();
        let ack = should_ack_ping(&registry, client_addr, b"PING");

        assert!(ack);
        assert!(registry.last_seen(&client_addr).is_some());
    }

    #[test]
    fn unknown_source_is_not_acknowledged() {
        let registry = SubscriptionRegistry::new();
        let unknown_addr: SocketAddr = "127.0.0.1:6001".parse().unwrap();

        assert!(!should_ack_ping(&registry, unknown_addr, b"PING"));
        assert!(!should_ack_ping(&registry, unknown_addr, b"PANG"));
    }
}
