use std::{
    net::SocketAddr,
    sync::atomic::{AtomicUsize, Ordering},
};

use crate::balancer::BalancerError;

pub struct RoundRobinBalancer {
    backends: Vec<SocketAddr>,
    counter: AtomicUsize,
}

impl RoundRobinBalancer {
    pub fn new(backends: Vec<SocketAddr>) -> Self {
        Self {
            backends,
            counter: AtomicUsize::new(0),
        }
    }

    pub fn next_backend(&self) -> Result<SocketAddr, BalancerError> {
        if self.backends.is_empty() {
            return Err(BalancerError::NoBackendAvailable);
        }

        // 중간에 다른 스레드가 끼어들 수 있으므로 읽음+증가 동시에 해줘야 함
        // fetch_add는 값이 타입의 최댓값을 넘어서면 wrapping(감싸돌기) 방식으로 동작하므로 별도 usize 최대값 처리 불필요
        let old_counter = self.counter.fetch_add(1, Ordering::Relaxed);
        let index = old_counter % self.backends.len();

        let target = self.backends[index];

        Ok(target)
    }

    pub fn backend_count(&self) -> usize {
        self.backends.len()
    }
}
