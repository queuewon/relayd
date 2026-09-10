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
    #[serde(default)]
    pub circuit: CircuitConfig,
}

#[derive(Deserialize, Clone, Copy)]
pub struct CircuitConfig {
    pub failure_threshold: isize, // 임계값
    pub initial_open_secs: u64,   // 첫 Open 시간(첫 차단 시간)
    pub backoff_multiplier: u32,  // 시험 실패 시, 차단 시간 증가 배수
    pub max_open_secs: u64,       // 최대 차단 시간
}
impl Default for CircuitConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            initial_open_secs: 10,
            backoff_multiplier: 2,
            max_open_secs: 60,
        }
    }
}
