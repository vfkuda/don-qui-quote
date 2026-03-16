pub mod client {
    // client's config related structure and code
    pub mod config;

    pub mod keepalive;
    pub mod tcp_session;
    pub mod udp_receiver;
}
pub mod common {
    // logging init routines
    pub mod logging;
    // shared data structures and contracts
    pub mod protocol;
    // stop-flag and spawned threads handles
    pub mod shutdown;
}
pub mod server {
    // quotes generator @dedicated thread
    pub mod generator;

    pub mod keepalive;

    pub mod streaming;
    // clients register
    pub mod subscriptions;
    // tcp listener and  @dedicated thread
    pub mod tcp_listener;
}
