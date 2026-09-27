use std::sync::atomic::AtomicBool;

use serde::Serialize;

#[derive(Debug)]
pub struct Config {
    pub name: String,
    pub delay_ms: usize,
    pub fail_requests: AtomicBool,
}

#[derive(Debug, Serialize)]
pub struct SerializedConfig {
    pub name: String,
    pub delay_ms: usize,
    pub fail_requests: bool,
}
