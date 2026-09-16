use std::sync::atomic::AtomicBool;

pub struct Config {
    pub name: String,
    pub delay_ms: usize,
    pub fail_requests: AtomicBool,
}
