use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use crate::balancer::{
    least_connections::LeastConnectionsBalancer, round_robin::RoundRobinBalancer,
    weight_round_robin::WeightRoundRobinBalancer,
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
}
