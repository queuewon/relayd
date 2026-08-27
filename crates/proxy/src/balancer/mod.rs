use std::{
    net::SocketAddr,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use crate::{
    backend::Backend,
    balancer::{
        least_connections::{LeastConnectionsBackend, LeastConnectionsBalancer},
        round_robin::RoundRobinBalancer,
        weight_round_robin::{WeightRoundRobinBackend, WeightRoundRobinBalancer},
    },
    config::{self, ParsedBackendConfig, ProxyConfig},
};

pub mod least_connections;
pub mod round_robin;
pub mod weight_round_robin;

struct DecrementGuard {
    counter: Arc<AtomicUsize>,
}

impl Drop for DecrementGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

// 밸런서 별 백엔드 구조체에 DecrementGuard를 추가하려 했으나, 백엔드 서버가 실제로 죽어야만 drop 되므로 Selection
pub struct Selection {
    pub addr: SocketAddr,
    _guard: Option<DecrementGuard>,
}

impl Selection {
    // RR/WRR처럼 감소시킬 게 없는 경우
    fn without_guard(addr: SocketAddr) -> Self {
        Self { addr, _guard: None }
    }

    // LeastConnections처럼 감소가 필요한 경우
    fn with_guard(addr: SocketAddr, guard: DecrementGuard) -> Self {
        Self {
            addr,
            _guard: Some(guard),
        }
    }
}

#[derive(Debug)]
pub enum BalancerError {
    NoBackendAvailable,
}

pub enum Balancer {
    RoundRobin(RoundRobinBalancer),
    Weighted(WeightRoundRobinBalancer),
    LeastConnections(LeastConnectionsBalancer),
}
impl Balancer {
    pub fn next_backend(&self) -> Result<Selection, BalancerError> {
        match self {
            Balancer::RoundRobin(balancer) => {
                let backend = balancer.next_backend()?;
                Ok(backend)
            }
            Balancer::Weighted(balancer) => {
                let backend = balancer.next_backend()?;
                Ok(backend)
            }
            Balancer::LeastConnections(balancer) => {
                let backend = balancer.next_backend()?;
                Ok(backend)
            }
        }
    }

    pub fn backend_count(&self) -> usize {
        match self {
            Balancer::RoundRobin(balancer) => balancer.backend_count(),
            Balancer::Weighted(balancer) => balancer.backend_count(),
            Balancer::LeastConnections(balancer) => balancer.backend_count(),
        }
    }
    pub fn healthy_targets(&self) -> Vec<Backend> {
        match self {
            Balancer::RoundRobin(balancer) => balancer.healthy_targets(),
            Balancer::Weighted(balancer) => balancer.healthy_targets(),
            Balancer::LeastConnections(balancer) => balancer.healthy_targets(),
        }
    }

    pub fn from_config(cfg: ProxyConfig) -> Self {
        let backend_configs: Vec<ParsedBackendConfig> = cfg
            .backends
            .iter()
            .map(|b| {
                let addr = SocketAddr::from_str(&b.addr).expect(
                    "프록시 설정파일 중 아이피 주소 형식이 올바르지 않은 상태이므로 확인필요",
                );
                let weight = b.weight.unwrap_or(1);

                ParsedBackendConfig { addr, weight }
            })
            .collect();

        let balancer = match cfg.algorithm {
            config::Algorithm::RoundRobin => {
                let backends: Vec<Backend> = backend_configs
                    .iter()
                    .map(|b| Backend::new(b.addr))
                    .collect();
                Balancer::RoundRobin(RoundRobinBalancer::new(backends))
            }
            config::Algorithm::Weighted => {
                let backends: Vec<WeightRoundRobinBackend> = backend_configs
                    .iter()
                    .map(|b| WeightRoundRobinBackend::new(b.addr, b.weight))
                    .collect();
                Balancer::Weighted(WeightRoundRobinBalancer::new(backends))
            }
            config::Algorithm::LeastConnections => {
                let backends: Vec<LeastConnectionsBackend> = backend_configs
                    .iter()
                    .map(|b| LeastConnectionsBackend::new(b.addr))
                    .collect();
                Balancer::LeastConnections(LeastConnectionsBalancer::new(backends))
            }
        };

        println!(
            "{:#?} 전략 실행 | {:#?} 설정된 백엔드 주소",
            cfg.algorithm, cfg.backends
        );

        balancer
    }
}
