use std::net::SocketAddr;

use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Algorithm {
    RoundRobin,
    Weighted,
    LeastConnections,
}
#[derive(Deserialize, Debug, Clone)]
pub struct BackendConfig {
    pub addr: String,
    pub weight: Option<u8>,
}
#[derive(Debug, Clone)]
pub struct ParsedBackendConfig {
    pub addr: SocketAddr,
    pub weight: u8,
}
#[derive(Deserialize, Debug, Clone)]
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

#[derive(Deserialize, Clone, Copy, Debug)]
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
#[derive(Deserialize, Debug, Clone)]
pub struct ConnectionPoolConfig {
    /// max_idle < 대상 백엔드 keep-alive. 넘으면 백엔드가 이미 닫은 소켓을 꺼내 써서 EOF가 나버림.
    /// 기본값 4초는 Actix 기본 keep-alive 5초 기준이고 15단계 run1 실측에서 백엔드는 put() 후 약 4.85~4.9초에 소켓을 닫았기에 4초로설정함.
    pub max_idle_secs: u64,
    pub cleanup_interval_secs: u64,
}
impl Default for ConnectionPoolConfig {
    fn default() -> Self {
        Self {
            max_idle_secs: 4,
            cleanup_interval_secs: 4,
        }
    }
}
#[derive(Deserialize, Debug, Clone)]
pub struct ProxyConfig {
    pub algorithm: Algorithm,
    pub backends: Vec<BackendConfig>,
    #[serde(default)]
    pub health: HealthConfig,
    #[serde(default)]
    pub circuit: CircuitConfig,
    #[serde(default)]
    pub connection_pool: ConnectionPoolConfig,
}
