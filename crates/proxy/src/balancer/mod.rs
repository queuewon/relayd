use std::net::SocketAddr;

use crate::balancer::{
    round_robin::RoundRobinBalancer, weight_round_robin::WeightRoundRobinBalancer,
};

pub mod round_robin;
pub mod weight_round_robin;

#[derive(Debug)]
pub enum BalancerError {
    NoBackendAvailable,
}

pub enum Balancer {
    RoundRobin(RoundRobinBalancer),
    Weighted(WeightRoundRobinBalancer),
}
impl Balancer {
    pub fn next_backend(&self) -> Result<SocketAddr, BalancerError> {
        match self {
            Balancer::RoundRobin(balancer) => {
                let backend = balancer.next_backend()?;
                Ok(backend)
            }
            Balancer::Weighted(balancer) => {
                let backend = balancer.next_backend()?;
                Ok(backend)
            }
        }
    }

    pub fn backend_count(&self) -> usize {
        match self {
            Balancer::RoundRobin(balancer) => balancer.backend_count(),
            Balancer::Weighted(balancer) => balancer.backend_count(),
        }
    }
}
