use std::{
    collections::HashSet,
    net::SocketAddr,
    sync::atomic::{AtomicUsize, Ordering},
};

use crate::{
    backend::{Admitted, Backend},
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

        for _ in 0..self.backend_count() {
            let available_backends: Vec<&Backend> = self
                .backends
                .iter()
                .filter(|b| !failed_backends.contains(&b.addr))
                .filter(|b| b.is_routable())
                .collect();
            if available_backends.is_empty() {
                return Err(BalancerError::NoBackendAvailable);
            }

            // 1. 중간에 다른 스레드가 끼어들 수 있으므로 읽음+증가 동시에 해줘야 함. fetch_add는 값이 타입의 최댓값을 넘어서면 wrapping(감싸돌기) 방식으로 동작하므로 별도 usize 최대값 처리 불필요

            // 2. Rejected로 재시도해도 이 fetch_add는 되돌리지 않음. 되돌리려면 load 후 성공 시점에 증가시켜야 하는데, 그 사이 원자성이 깨져 평상시 분배가 느슨해짐.
            // Rejected는 HalfOpen 동시요청일 때만 드물게 나므로, 헛증가로 인한 분배 왜곡이 원자성 완화보다 작다고 보고 즉시 증가를 유지.
            let old_counter = self.counter.fetch_add(1, Ordering::Relaxed);
            let index = old_counter % available_backends.len();

            let target = available_backends[index];
            let admitted = target.try_admit();

            match admitted {
                Admitted::Closed => return Ok(Selection::new(target.clone(), None, None)),
                Admitted::Probe(half_open_guard) => {
                    return Ok(Selection::new(target.clone(), None, Some(half_open_guard)));
                }
                Admitted::Rejected => {
                    continue;
                }
            }
        }

        Err(BalancerError::NoBackendAvailable)
    }

    pub fn backend_count(&self) -> usize {
        self.backends.len()
    }

    pub fn all_backends(&self) -> Vec<Backend> {
        self.backends.clone()
    }
}
