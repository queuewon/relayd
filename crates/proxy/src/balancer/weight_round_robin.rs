use std::{collections::HashSet, net::SocketAddr, sync::Mutex};

use crate::{
    backend::{Admitted, HealthPolicy},
    balancer::{Backend, BalancerError, Selection},
    circuit_breaker::CircuitBreaker,
};

pub struct WeightRoundRobinBackend {
    base: Backend,
    current_weight: i32,
    weight: u8,
}
impl WeightRoundRobinBackend {
    pub fn new(
        addr: SocketAddr,
        weight: u8,
        health_policy: HealthPolicy,
        circuit: CircuitBreaker,
    ) -> Self {
        Self {
            base: Backend::new(addr, health_policy, circuit),
            current_weight: 0,
            weight,
        }
    }
}

pub struct WeightRoundRobinBalancer {
    backends: Mutex<Vec<WeightRoundRobinBackend>>,
}

impl WeightRoundRobinBalancer {
    pub fn new(backends: Vec<WeightRoundRobinBackend>) -> Self {
        Self {
            backends: Mutex::new(backends),
        }
    }

    pub fn next_backend(
        &self,
        failed_backends: &HashSet<SocketAddr>,
    ) -> Result<Selection, BalancerError> {
        let mut guard = self.backends.lock().unwrap();

        if guard.is_empty() {
            return Err(BalancerError::NoBackendAvailable);
        }

        // is_routable / failed_backends 통과한 후보의 '인덱스'만 모음 (참조 대신 인덱스 -> borrow 회피)
        let available_indices: Vec<usize> = guard
            .iter()
            .enumerate()
            .filter(|(_, b)| !failed_backends.contains(&b.base.addr))
            .filter(|(_, b)| b.base.is_routable())
            .map(|(i, _)| i)
            .collect();
        if available_indices.is_empty() {
            return Err(BalancerError::NoBackendAvailable);
        }

        // 후보 전체 current_weight 증가 (1회) + total_weight 계산
        let mut total_weight: i32 = 0;
        for &i in &available_indices {
            let w = guard[i].weight as i32;
            guard[i].current_weight += w;
            total_weight += w;
        }

        // Rejected로 제외된 인덱스 추적
        let mut rejected: HashSet<usize> = HashSet::new();

        loop {
            // 아직 제외 안 된 후보 중 current_weight 최대 인덱스
            let found_max = available_indices
                .iter()
                .filter(|i| !rejected.contains(i))
                .max_by_key(|&&i| guard[i].current_weight)
                .copied();

            let max_idx = match found_max {
                Some(i) => i,
                None => return Err(BalancerError::NoBackendAvailable), // 후보 다 소진
            };

            match guard[max_idx].base.try_admit() {
                Admitted::Closed => {
                    guard[max_idx].current_weight -= total_weight;
                    return Ok(Selection::new(guard[max_idx].base.clone(), None, None));
                }
                Admitted::Probe(half_open_guard) => {
                    guard[max_idx].current_weight -= total_weight;
                    return Ok(Selection::new(
                        guard[max_idx].base.clone(),
                        None,
                        Some(half_open_guard),
                    ));
                }
                Admitted::Rejected => {
                    rejected.insert(max_idx);
                    continue;
                }
            }
        }
    }

    pub fn backend_count(&self) -> usize {
        let guard = self.backends.lock().unwrap();
        guard.len()
    }

    pub fn all_backends(&self) -> Vec<Backend> {
        let lock = self.backends.lock().unwrap();
        lock.iter().map(|b| b.base.clone()).collect()
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn smooth_wrr_selects_in_expected_order() {
//         let backends = vec![
//             Backend {
//                 addr: "127.0.0.1:8081".parse().unwrap(),
//                 current_weight: 0,
//                 weight: 3,
//             },
//             Backend {
//                 addr: "127.0.0.1:8082".parse().unwrap(),
//                 current_weight: 0,
//                 weight: 1,
//             },
//         ];

//         let balancer = WeightRoundRobinBalancer::new(backends);

//         let addr_a: SocketAddr = "127.0.0.1:8081".parse().unwrap();
//         let addr_b: SocketAddr = "127.0.0.1:8082".parse().unwrap();

//         let mut selected = Vec::new();
//         for _ in 0..4 {
//             let addr = balancer.next_backend().unwrap();
//             selected.push(addr);
//         }

//         assert_eq!(selected, vec![addr_a, addr_b, addr_a, addr_a]);
//     }

//     #[test]
//     fn ratio_holds_over_many_calls() {
//         let backends = vec![
//             Backend {
//                 addr: "127.0.0.1:8081".parse().unwrap(),
//                 current_weight: 0,
//                 weight: 3,
//             },
//             Backend {
//                 addr: "127.0.0.1:8082".parse().unwrap(),
//                 current_weight: 0,
//                 weight: 1,
//             },
//         ];

//         let balancer = WeightRoundRobinBalancer::new(backends);
//         let addr_a: SocketAddr = "127.0.0.1:8081".parse().unwrap();

//         let mut count_a = 0;
//         for _ in 0..400 {
//             if balancer.next_backend().unwrap() == addr_a {
//                 count_a += 1;
//             }
//         }

//         assert_eq!(count_a, 300);
//     }

//     #[test]
//     fn empty_backend_list_returns_error() {
//         let balancer = WeightRoundRobinBalancer::new(vec![]);
//         let result = balancer.next_backend();
//         assert!(matches!(result, Err(BalancerError::NoBackendAvailable)));
//     }

//     #[test]
//     fn single_backend_always_selected() {
//         let backends = vec![Backend {
//             addr: "127.0.0.1:8081".parse().unwrap(),
//             current_weight: 0,
//             weight: 5,
//         }];

//         let balancer = WeightRoundRobinBalancer::new(backends);
//         let addr: SocketAddr = "127.0.0.1:8081".parse().unwrap();

//         for _ in 0..10 {
//             assert_eq!(balancer.next_backend().unwrap(), addr);
//         }
//     }
// }
