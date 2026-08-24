use std::{net::SocketAddr, sync::Mutex};

use crate::balancer::BalancerError;

pub struct Backend {
    addr: SocketAddr,
    current_weight: i32,
    weight: u8,
}
impl Backend {
    pub fn new(addr: SocketAddr, weight: u8) -> Self {
        Self {
            addr,
            current_weight: 0,
            weight,
        }
    }
}

pub struct WeightRoundRobinBalancer {
    backends: Mutex<Vec<Backend>>,
}

impl WeightRoundRobinBalancer {
    pub fn new(backends: Vec<Backend>) -> Self {
        Self {
            backends: Mutex::new(backends),
        }
    }

    pub fn next_backend(&self) -> Result<SocketAddr, BalancerError> {
        let mut guard = self.backends.lock().unwrap();

        if guard.is_empty() {
            return Err(BalancerError::NoBackendAvailable);
        }

        let mut total_weight: i32 = 0;

        for backend in guard.iter_mut() {
            let weight = backend.weight as i32;
            backend.current_weight += weight;

            total_weight += backend.weight as i32;
        }

        let found_max_current_weight_item = guard
            .iter_mut()
            .max_by_key(|backend| backend.current_weight);

        let max_current_weight_item = match found_max_current_weight_item {
            Some(backend) => backend,
            None => {
                unreachable!()
            }
        };

        max_current_weight_item.current_weight -= total_weight as i32;

        Ok(max_current_weight_item.addr)
    }

    pub fn backend_count(&self) -> usize {
        let guard = self.backends.lock().unwrap();
        guard.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smooth_wrr_selects_in_expected_order() {
        let backends = vec![
            Backend {
                addr: "127.0.0.1:8081".parse().unwrap(),
                current_weight: 0,
                weight: 3,
            },
            Backend {
                addr: "127.0.0.1:8082".parse().unwrap(),
                current_weight: 0,
                weight: 1,
            },
        ];

        let balancer = WeightRoundRobinBalancer::new(backends);

        let addr_a: SocketAddr = "127.0.0.1:8081".parse().unwrap();
        let addr_b: SocketAddr = "127.0.0.1:8082".parse().unwrap();

        let mut selected = Vec::new();
        for _ in 0..4 {
            let addr = balancer.next_backend().unwrap();
            selected.push(addr);
        }

        assert_eq!(selected, vec![addr_a, addr_b, addr_a, addr_a]);
    }

    #[test]
    fn ratio_holds_over_many_calls() {
        let backends = vec![
            Backend {
                addr: "127.0.0.1:8081".parse().unwrap(),
                current_weight: 0,
                weight: 3,
            },
            Backend {
                addr: "127.0.0.1:8082".parse().unwrap(),
                current_weight: 0,
                weight: 1,
            },
        ];

        let balancer = WeightRoundRobinBalancer::new(backends);
        let addr_a: SocketAddr = "127.0.0.1:8081".parse().unwrap();

        let mut count_a = 0;
        for _ in 0..400 {
            if balancer.next_backend().unwrap() == addr_a {
                count_a += 1;
            }
        }

        assert_eq!(count_a, 300);
    }

    #[test]
    fn empty_backend_list_returns_error() {
        let balancer = WeightRoundRobinBalancer::new(vec![]);
        let result = balancer.next_backend();
        assert!(matches!(result, Err(BalancerError::NoBackendAvailable)));
    }

    #[test]
    fn single_backend_always_selected() {
        let backends = vec![Backend {
            addr: "127.0.0.1:8081".parse().unwrap(),
            current_weight: 0,
            weight: 5,
        }];

        let balancer = WeightRoundRobinBalancer::new(backends);
        let addr: SocketAddr = "127.0.0.1:8081".parse().unwrap();

        for _ in 0..10 {
            assert_eq!(balancer.next_backend().unwrap(), addr);
        }
    }
}
