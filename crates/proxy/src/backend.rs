use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use serde::Deserialize;

use crate::circuit_breaker::{CircuitBreaker, CircuitDecision, CircuitState};

pub struct HalfOpenGuard {
    circuit: Arc<Mutex<CircuitBreaker>>,
}

// probing: 시험 슬롯 잡는 부분 초기화 용도. 오류 발생 시 drop 하도록 - 수동 상태변경을 수행하지 않는 부분을 위한
// 경우 1. 클라이언트 write 실패 - 현재 백엔드 응답 관련 읽기/쓰기 실패에만 기록 중
// 경우 2. permit 획득 실패
// 경우 3. future 취소
impl Drop for HalfOpenGuard {
    fn drop(&mut self) {
        let mut guard = self.circuit.lock().unwrap();
        if matches!(guard.state, CircuitState::HalfOpen { .. }) {
            guard.state = CircuitState::HalfOpen { probing: false }
        }
    }
}

#[derive(Debug)]
pub enum StreakTransition {
    NoChange,
    Unhealthy,
    Healthy,
}
pub enum Admitted {
    Closed,               // 통과, 가드 없음
    Probe(HalfOpenGuard), // 통과, 가드 있음
    Rejected,             // 거절
}

// 동기화 정책과 상태 표현을 분리. - StreakState는 상태표현만을 나타내므로 이 구조체가 동기화 정책에 대해 관심사를 두는것은 옳지가 않음. 목적이 그것이 아니기 때문에
// 순수 갱신로직 - 이미 락이 잡힌 상태(&mut self)에서 streak 계산. threshold 비교,healthy 갱신만 담당. 락 관심사가 전혀 없는 순수 함수
pub struct StreakState {
    pub streak: isize, // 상태 확인 횟수(양수 - 성공, 음수 - 실패)
    pub healthy: bool, // 정상, 비정상 상태
}
impl StreakState {
    pub fn record(&mut self, success: bool, threshold: &Threshold) -> StreakTransition {
        if !success {
            if self.streak >= 0 {
                self.streak = -1;
            } else {
                self.streak -= 1;
            }

            if self.healthy && (self.streak <= -threshold.unhealth) {
                self.healthy = false;

                return StreakTransition::Unhealthy;
            }

            StreakTransition::NoChange
        } else {
            if self.streak <= 0 {
                self.streak = 1;
            } else {
                self.streak += 1;
            }

            if !self.healthy && (self.streak >= threshold.health) {
                self.healthy = true;

                return StreakTransition::Healthy;
            }

            StreakTransition::NoChange
        }
    }
}

#[derive(Clone, Copy, Deserialize)]
pub struct Threshold {
    pub health: isize,
    pub unhealth: isize,
}
impl Threshold {
    pub fn new(health: isize, unhealth: isize) -> Self {
        Self { health, unhealth }
    }
}
#[derive(Clone, Copy)]
pub struct HealthPolicy {
    pub probe: Threshold,
}
// note_probe_result는 probe_health 락을 잡고 note_traffic_result는 circuit 락을 잡음.
// 각 상태의 락을 여닫는 지점을 Backend 메서드 안으로 한정해, 락을 쪼개 잡는 코드가 밖에서 끼어드지 못하도록 함.
#[derive(Clone)]
pub struct Backend {
    pub addr: SocketAddr,
    probe_health: Arc<Mutex<StreakState>>,

    probe_threshold: Threshold, // health: 3, unhealth: 3

    // 서킷
    pub circuit: Arc<Mutex<CircuitBreaker>>,
}
impl Backend {
    pub fn new(addr: SocketAddr, health_policy: HealthPolicy, circuit: CircuitBreaker) -> Self {
        let probe_health = Arc::new(Mutex::new(StreakState {
            streak: 0,
            healthy: true,
        }));
        let circuit = Arc::new(Mutex::new(circuit));
        Self {
            addr,
            probe_health,
            probe_threshold: health_policy.probe,
            circuit,
        }
    }

    pub fn note_probe_result(&self, success: bool) {
        // lock을 빨리 풀기 위해 스코프 지정
        let result = {
            let mut state = self.probe_health.lock().unwrap();
            state.record(success, &self.probe_threshold)
        };

        if !matches!(result, StreakTransition::NoChange) {
            println!(
                "probe 헬스 체크 | 백엔드 {} {:#?}로 상태변환",
                self.addr, result
            );
        }
    }

    pub fn note_traffic_result(&self, success: bool) {
        let mut cb = self.circuit.lock().unwrap();
        cb.record(success)
    }

    // 라우팅 대상으로 선택 가능한지 판단. - probe 헬스체크와 서킷 둘 다 통과할 때만 true
    pub fn is_routable(&self) -> bool {
        // lock을 변수로 바인딩하면 함수 끝까지 락이 유지돼 두 락을 동시에 들게 되는 문제가 있음.
        // MutexGuard를 변수에 바인딩하지 않고 필드 값만 꺼내어 한꺼번에 비교까지 처리. 가드는 이 문장 끝에서 drop되고 락도 즉시 풀리게 처리.
        // probe가 false면 && short-circuit으로 can_admit()의 서킷 락은 잡지않음.
        self.probe_health.lock().unwrap().healthy && self.can_admit()
    }

    // 백엔드가 지금 요청을 받을 수 있는 상태인지 확인
    pub fn can_admit(&self) -> bool {
        self.circuit.lock().unwrap().can_admit()
    }

    // 현재 백엔드로 요청을 들여보내며 서킷 상태 갱신. 회복 확인용 시험 요청(Probe)일 경우 요청이 끝나면 시험슬롯을 되돌릴 가드를 함께 반환 (Selection에 담아야 하므로)
    pub fn try_admit(&self) -> Admitted {
        let admission = {
            let mut cb = self.circuit.lock().unwrap();
            cb.try_admit()
        };

        match admission {
            CircuitDecision::Closed => Admitted::Closed,
            CircuitDecision::Probe => Admitted::Probe(HalfOpenGuard {
                circuit: self.circuit.clone(),
            }),
            CircuitDecision::Rejected => Admitted::Rejected,
        }
    }
}
