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
#[derive(Deserialize, Debug)]
pub struct HealthConfig {
    pub interval_milli_secs: u64,
    pub health_threshold: isize,
    pub unhealth_threshold: isize,
}
impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            interval_milli_secs: 1000,
            health_threshold: 3,
            unhealth_threshold: 3,
        }
    }
}
// TODO: config 가져오기
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
#[derive(Deserialize)]
pub struct ProxyConfig {
    pub algorithm: Algorithm,
    pub backends: Vec<BackendConfig>,
    #[serde(default)]
    pub health: HealthConfig,
    #[serde(default)]
    pub circuit: CircuitConfig,
}
