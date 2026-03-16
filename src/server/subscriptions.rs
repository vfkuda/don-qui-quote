use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use log::{debug, info, warn};
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct ClientSubscription {
    pub client_addr: SocketAddr,
    pub tickers: Vec<String>,
    pub last_seen_ping: Instant,
}

#[derive(Debug, Clone, Default)]
pub struct SubscriptionRegistry {
    inner: Arc<Mutex<HashMap<SocketAddr, ClientSubscription>>>,
}

#[derive(Debug, Error, PartialEq)]
pub enum RegistryError {
    #[error("already streaming")]
    AlreadyStreaming,
}

impl SubscriptionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        &self,
        client_addr: SocketAddr,
        tickers: Vec<String>,
    ) -> Result<(), RegistryError> {
        let mut guard = self.lock_map();

        if guard.contains_key(&client_addr) {
            warn!("duplicate subscription rejected for client={client_addr}");
            return Err(RegistryError::AlreadyStreaming);
        }

        guard.insert(
            client_addr,
            ClientSubscription {
                client_addr,
                tickers,
                last_seen_ping: Instant::now(),
            },
        );
        info!("registered subscription for client={client_addr}");

        Ok(())
    }

    pub fn remove(&self, client_addr: &SocketAddr) -> Option<ClientSubscription> {
        let removed = self.lock_map().remove(client_addr);
        if removed.is_some() {
            info!("removed subscription for client={client_addr}");
        }
        removed
    }

    pub fn update_ping(&self, client_addr: &SocketAddr) -> bool {
        let mut guard = self.lock_map();
        if let Some(subscription) = guard.get_mut(client_addr) {
            subscription.last_seen_ping = Instant::now();
            debug!("updated heartbeat for client={client_addr}");
            return true;
        }
        false
    }

    pub fn is_ping_fresh(&self, client_addr: &SocketAddr, timeout: Duration) -> bool {
        self.lock_map()
            .get(client_addr)
            .is_some_and(|sub| sub.last_seen_ping.elapsed() <= timeout)
    }

    pub fn last_seen(&self, client_addr: &SocketAddr) -> Option<Instant> {
        self.lock_map().get(client_addr).map(|s| s.last_seen_ping)
    }

    pub fn len(&self) -> usize {
        self.lock_map().len()
    }

    pub fn snapshot(&self) -> Vec<ClientSubscription> {
        self.lock_map().values().cloned().collect()
    }

    pub fn remove_stale(&self, timeout: Duration) -> Vec<SocketAddr> {
        let mut guard = self.lock_map();
        let stale_clients: Vec<SocketAddr> = guard
            .iter()
            .filter_map(|(addr, sub)| {
                if sub.last_seen_ping.elapsed() > timeout {
                    Some(*addr)
                } else {
                    None
                }
            })
            .collect();

        for client_addr in &stale_clients {
            guard.remove(client_addr);
            info!("removed stale subscription for client={client_addr}");
        }

        stale_clients
    }

    fn lock_map(&self) -> MutexGuard<'_, HashMap<SocketAddr, ClientSubscription>> {
        match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    #[test]
    fn rejects_duplicate_subscription() {
        let registry = SubscriptionRegistry::new();
        let addr: SocketAddr = "127.0.0.1:5000".parse().unwrap();

        registry.register(addr, vec!["AAPL".into()]).unwrap();
        let err = registry.register(addr, vec!["TSLA".into()]).unwrap_err();

        assert_eq!(err, RegistryError::AlreadyStreaming);
    }

    #[test]
    fn supports_concurrent_registration() {
        let registry = SubscriptionRegistry::new();

        let handles: Vec<_> = (0..20)
            .map(|i| {
                let registry = registry.clone();
                thread::spawn(move || {
                    let addr: SocketAddr = format!("127.0.0.1:{}", 6000 + i).parse().unwrap();
                    registry.register(addr, vec!["AAPL".into()]).unwrap();
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(registry.len(), 20);
    }

    #[test]
    fn removes_stale_clients() {
        let registry = SubscriptionRegistry::new();
        let addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        registry.register(addr, vec!["AAPL".into()]).unwrap();

        std::thread::sleep(Duration::from_millis(30));
        let stale = registry.remove_stale(Duration::from_millis(5));

        assert_eq!(stale, vec![addr]);
        assert_eq!(registry.len(), 0);
    }
}
