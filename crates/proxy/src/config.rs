use std::net::SocketAddr;

use serde::Deserialize;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum Algorithm {
    RoundRobin,
    Weighted,
    LeastConnections,
}
#[derive(Deserialize, Debug)]
pub struct BackendConfig {
    pub addr: String,
    pub weight: Option<u8>,
}
pub struct ParsedBackendConfig {
    pub addr: SocketAddr,
    pub weight: u8,
}

#[derive(Deserialize)]
pub struct ProxyConfig {
    pub algorithm: Algorithm,
    pub backends: Vec<BackendConfig>,
}
