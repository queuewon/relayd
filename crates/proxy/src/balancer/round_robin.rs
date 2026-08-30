use std::{
    collections::HashSet,
    net::SocketAddr,
    sync::atomic::{AtomicUsize, Ordering},
};

use crate::{
    backend::Backend,
    balancer::{BalancerError, Selection},
};

pub struct RoundRobinBalancer {
    backends: Vec<Backend>,
    counter: AtomicUsize,
}

impl RoundRobinBalancer {
    pub fn new(backends: Vec<Backend>) -> Self {
        Self {
            backends,
            counter: AtomicUsize::new(0),
        }
    }

    pub fn next_backend(
        &self,
        failed_backends: &HashSet<SocketAddr>,
    ) -> Result<Selection, BalancerError> {
        if self.backends.is_empty() {
            return Err(BalancerError::NoBackendAvailable);
        }

        let available_backends: Vec<&Backend> = self
            .backends
            .iter()
            .filter(|b| !failed_backends.contains(&b.addr))
            .filter(|b| b.is_routable())
            .collect();
        if available_backends.is_empty() {
            return Err(BalancerError::NoBackendAvailable);
        }

        // 중간에 다른 스레드가 끼어들 수 있으므로 읽음+증가 동시에 해줘야 함
        // fetch_add는 값이 타입의 최댓값을 넘어서면 wrapping(감싸돌기) 방식으로 동작하므로 별도 usize 최대값 처리 불필요
        let old_counter = self.counter.fetch_add(1, Ordering::Relaxed);
        let index = old_counter % available_backends.len();

        let target = available_backends[index];

        Ok(Selection::without_guard(target.clone()))
    }

    pub fn backend_count(&self) -> usize {
        self.backends.len()
    }

    pub fn all_backends(&self) -> Vec<Backend> {
        self.backends.clone()
    }
}
