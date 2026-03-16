use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::{Mutex, MutexGuard};
use std::thread::JoinHandle;

use log::{debug, info};

pub fn install_ctrlc_handler(stop: Arc<AtomicBool>) -> Result<(), ctrlc::Error> {
    ctrlc::set_handler(move || {
        stop.store(true, Ordering::Relaxed);
        info!("shutdown requested via Ctrl+C");
    })
}

// Coordinates shutdown for both static infrastructure threads and dynamic client workers.
pub struct AppRuntime {
    stop: Arc<AtomicBool>,
    static_threads: Arc<Mutex<Vec<JoinHandle<()>>>>,
    dynamic_threads: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
    dynamic_done_tx: Sender<String>,
    dynamic_done_rx: Arc<Mutex<Receiver<String>>>,
}

impl AppRuntime {
    pub fn new(stop: Arc<AtomicBool>) -> Self {
        let (dynamic_done_tx, dynamic_done_rx) = mpsc::channel();

        Self {
            stop,
            static_threads: Arc::new(Mutex::new(Vec::new())),
            dynamic_threads: Arc::new(Mutex::new(HashMap::new())),
            dynamic_done_tx,
            dynamic_done_rx: Arc::new(Mutex::new(dynamic_done_rx)),
        }
    }

    pub fn register_handle(&self, handle: JoinHandle<()>) {
        self.lock_static_threads().push(handle);
    }

    pub fn register_dynamic_handle(&self, key: String, handle: JoinHandle<()>) {
        let old_handle = self.lock_dynamic_threads().insert(key.into(), handle);
        if let Some(old_handle) = old_handle {
            let _ = old_handle.join();
            debug!("replaced existing dynamic worker handle for same client key");
        }
    }

    pub fn dynamic_done_notifier(&self) -> Sender<String> {
        self.dynamic_done_tx.clone()
    }

    pub fn cleanup_finished_dynamic(&self) -> usize {
        let mut cleaned = 0;

        loop {
            let done_key = match self.lock_dynamic_done_rx().try_recv() {
                Ok(key) => key,
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => break,
            };

            if let Some(handle) = self.lock_dynamic_threads().remove(&done_key) {
                let _ = handle.join();
                cleaned += 1;
            }
        }

        cleaned
    }

    pub fn trigger_shutdown(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }

    pub fn stop_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.stop)
    }

    pub fn wait(self) {
        let mut static_threads = self.lock_static_threads();
        for handle in static_threads.drain(..) {
            let _ = handle.join();
        }

        let mut dynamic_threads = self.lock_dynamic_threads();
        for (_key, handle) in dynamic_threads.drain() {
            let _ = handle.join();
        }
    }

    fn lock_static_threads(&self) -> MutexGuard<'_, Vec<JoinHandle<()>>> {
        match self.static_threads.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn lock_dynamic_threads(&self) -> MutexGuard<'_, HashMap<String, JoinHandle<()>>> {
        match self.dynamic_threads.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn lock_dynamic_done_rx(&self) -> MutexGuard<'_, Receiver<String>> {
        match self.dynamic_done_rx.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::thread;
    use std::time::Duration;

    #[test]
    fn trigger_shutdown_sets_stop_flag() {
        let stop = Arc::new(AtomicBool::new(false));
        let runtime = AppRuntime::new(Arc::clone(&stop));

        runtime.trigger_shutdown();

        assert!(stop.load(Ordering::Relaxed));
    }

    #[test]
    fn cleanup_finished_dynamic_removes_completed_thread() {
        let stop = Arc::new(AtomicBool::new(false));
        let runtime = AppRuntime::new(stop);
        let done_tx = runtime.dynamic_done_notifier();

        let handle = thread::spawn(move || {
            done_tx.send("client-1".to_string()).unwrap();
        });

        runtime.register_dynamic_handle(String::from("client-1"), handle);
        thread::sleep(Duration::from_millis(20));

        assert_eq!(runtime.cleanup_finished_dynamic(), 1);
        assert_eq!(runtime.cleanup_finished_dynamic(), 0);
    }

    #[test]
    fn wait_joins_remaining_dynamic_threads() {
        let stop = Arc::new(AtomicBool::new(false));
        let runtime = AppRuntime::new(stop);

        let handle = thread::spawn(|| {});
        runtime.register_dynamic_handle(String::from("client-2"), handle);

        runtime.wait();
    }
}
