use std::{
    collections::HashSet,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use crate::balancer::{Backend, BalancerError, DecrementGuard, Selection};

pub struct LeastConnectionsBackend {
    base: Backend,
    active_connections: Arc<AtomicUsize>,
}

impl LeastConnectionsBackend {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            base: Backend::new(addr),
            active_connections: Arc::new(AtomicUsize::new(0)),
        }
    }
}
// 여러 커넥션이 동시에 들어오면 같은 순간에 같은 백엔드를 최적이라고 판단해 둘 다 거기로 몰릴 수 있음
pub struct LeastConnectionsBalancer {
    backends: Vec<LeastConnectionsBackend>,
}

impl LeastConnectionsBalancer {
    pub fn new(backends: Vec<LeastConnectionsBackend>) -> Self {
        Self { backends }
    }

    pub fn next_backend(
        &self,
        failed_backends: &HashSet<SocketAddr>,
    ) -> Result<Selection, BalancerError> {
        if self.backends.is_empty() {
            return Err(BalancerError::NoBackendAvailable);
        }

        let available_backends: Vec<&LeastConnectionsBackend> = self
            .backends
            .iter()
            .filter(|b| !failed_backends.contains(&b.base.addr))
            .filter(|b| b.base.healthy.load(Ordering::Relaxed))
            .collect();

        if available_backends.is_empty() {
            return Err(BalancerError::NoBackendAvailable);
        }

        let found_min_count_item = available_backends
            .iter()
            .min_by_key(|b| b.active_connections.load(Ordering::Relaxed));

        let backend = match found_min_count_item {
            Some(backend) => backend,
            None => {
                unreachable!()
            }
        };

        backend.active_connections.fetch_add(1, Ordering::Relaxed);

        let guard = DecrementGuard {
            counter: backend.active_connections.clone(),
        };

        Ok(Selection::with_guard(backend.base.addr, guard))
    }

    pub fn backend_count(&self) -> usize {
        self.backends.len()
    }

    pub fn healthy_targets(&self) -> Vec<Backend> {
        self.backends.iter().map(|b| b.base.clone()).collect()
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use std::sync::atomic::Ordering;

//     fn addr(port: u16) -> SocketAddr {
//         format!("127.0.0.1:{port}").parse().unwrap()
//     }

//     #[test]
//     fn selecting_increments_chosen_backend_counter() {
//         let backends = vec![
//             LeastConnectionsBackend::new(addr(8081)),
//             LeastConnectionsBackend::new(addr(8082)),
//         ];
//         let balancer = LeastConnectionsBalancer::new(backends);

//         let selection = balancer.next_backend(vec![]).unwrap();

//         let chosen = balancer
//             .backends
//             .iter()
//             .find(|b| b.base.addr == selection.addr)
//             .unwrap();

//         assert_eq!(chosen.active_connections.load(Ordering::Relaxed), 1);
//     }

//     #[test]
//     fn dropping_selection_decrements_counter() {
//         let backends = vec![LeastConnectionsBackend::new(addr(8081))];
//         let balancer = LeastConnectionsBalancer::new(backends);

//         let selection = balancer.next_backend(vec![]).unwrap();
//         assert_eq!(
//             balancer.backends[0]
//                 .active_connections
//                 .load(Ordering::Relaxed),
//             1
//         );

//         drop(selection); // handle_connection이 정상 종료될 때와 동일한 동작

//         assert_eq!(
//             balancer.backends[0]
//                 .active_connections
//                 .load(Ordering::Relaxed),
//             0
//         );
//     }

//     #[test]
//     fn early_return_still_decrements_counter() {
//         // handle_connection 안에서 ?로 조기 반환되는 상황을 흉내냄
//         fn simulate_early_error(balancer: &LeastConnectionsBalancer) -> Result<(), ()> {
//             let _selection = balancer.next_backend(vec![]).unwrap();
//             return Err(()); // 여기서 _selection이 스코프를 벗어나며 drop
//         }

//         let backends = vec![LeastConnectionsBackend::new(addr(8081))];
//         let balancer = LeastConnectionsBalancer::new(backends);

//         let result = simulate_early_error(&balancer);

//         assert!(result.is_err());
//         assert_eq!(
//             balancer.backends[0]
//                 .active_connections
//                 .load(Ordering::Relaxed),
//             0 // 에러로 일찍 끝났어도 카운터는 정상적으로 원복돼야 함
//         );
//     }

//     #[test]
//     fn selects_backend_with_fewest_connections() {
//         let backends = vec![
//             LeastConnectionsBackend::new(addr(8081)),
//             LeastConnectionsBackend::new(addr(8082)),
//         ];
//         let balancer = LeastConnectionsBalancer::new(backends);

//         // 8081을 두 번 선택해서 인위적으로 부하를 올림 (drop 안 시키고 계속 들고 있음)
//         let _s1 = balancer.next_backend(vec![]).unwrap();
//         let _s2 = balancer.next_backend(vec![]).unwrap();

//         // 이제 두 backend 다 카운터 1씩이므로, 다음 선택은 둘 중 하나
//         // 8081을 하나 더 선택해서 2로 만들면, 그 다음은 반드시 8082가 선택돼야 함
//         let addr_8081 = addr(8081);
//         if _s1.addr == addr_8081 && _s2.addr == addr_8081 {
//             // 우연히 둘 다 같은 backend를 골랐을 경우를 대비한 재확인
//             let next = balancer.next_backend(vec![]).unwrap();
//             assert_ne!(next.addr, addr_8081);
//         }
//     }

//     #[test]
//     fn empty_backend_list_returns_error() {
//         let balancer = LeastConnectionsBalancer::new(vec![]);
//         let result = balancer.next_backend(vec![]);
//         assert!(matches!(result, Err(BalancerError::NoBackendAvailable)));
//     }
// }
