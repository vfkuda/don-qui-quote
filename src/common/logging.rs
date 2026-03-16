use std::sync::Once;

use env_logger::{Builder, Env};

static INIT_LOGGER: Once = Once::new();

pub fn init_logging(default_level: &str) {
    INIT_LOGGER.call_once(|| {
        let env = Env::default().default_filter_or(default_level);
        let mut builder = Builder::from_env(env);
        builder.format_timestamp_millis();
        let _ = builder.try_init();
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_is_idempotent() {
        init_logging("debug");
        init_logging("info");
    }
}
