use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
};

#[derive(Debug)]
pub enum StreakTransition {
    NoChange,
    Unhealthy,
    Healthy,
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

#[derive(Clone, Copy)]
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
    pub traffic: Threshold,
}
// probe, traffic. 여기서 락을 열고 StreakState의 메서드를 호출하고 닫음. 이러면 "락 하나로 갱신+판정을 묶는다"는 4번 결정이 구조적으로 강제됨.
// 락을 여닫는 지점이 이 두 메서드 안 한 곳뿐이니, 나중에 실수로 락을 쪼개서 잡는 코드가 들어올 여지가 없음
#[derive(Clone)]
pub struct Backend {
    pub addr: SocketAddr,
    probe_health: Arc<Mutex<StreakState>>,
    traffic_health: Arc<Mutex<StreakState>>,
    probe_threshold: Threshold,   // health: 3, unhealth: 3
    traffic_threshold: Threshold, // health: 2, unhealth: 5
}
impl Backend {
    pub fn new(addr: SocketAddr, health_policy: HealthPolicy) -> Self {
        let probe_health = Arc::new(Mutex::new(StreakState {
            streak: 0,
            healthy: true,
        }));
        let traffic_health = Arc::new(Mutex::new(StreakState {
            streak: 0,
            healthy: true,
        }));
        Self {
            addr,
            probe_health,
            traffic_health,
            probe_threshold: health_policy.probe,
            traffic_threshold: health_policy.traffic,
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
        let result = {
            let mut state = self.traffic_health.lock().unwrap();
            state.record(success, &self.traffic_threshold)
        };

        if !matches!(result, StreakTransition::NoChange) {
            println!(
                "traffic 헬스 체크 | 백엔드 {} {:#?}로 상태변환",
                self.addr, result
            );
        }
    }

    // 라우팅 대상으로 선택 가능한지 판단. - probe, traffic 두 개 모두가 정상일 때만 true
    pub fn is_routable(&self) -> bool {
        // lock을 변수로 바인딩하면 함수 끝까지 락이 유지돼 두 락을 동시에 들게 되는 문제가 있음.
        // MutexGuard를 변수에 바인딩하지 않고 필드 값만 꺼내어 한꺼번에 비교까지 처리. 가드는 이 문장 끝에서 drop되고 락도 즉시 풀리게 처리.
        // probe가 false면 && short-circuit으로 traffic 락은 아예 잡지않음.
        self.probe_health.lock().unwrap().healthy && self.traffic_health.lock().unwrap().healthy
    }
}
