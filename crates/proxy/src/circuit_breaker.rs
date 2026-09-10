use std::time::{Duration, Instant};

use crate::config::CircuitConfig;

// 가드 통과 구분용
pub enum CircuitDecision {
    Rejected,
    Closed,
    Probe, // 격리된 백엔드 회복목적 테스트 신호 발송
}

#[derive(Clone, Copy, PartialEq)]
pub enum CircuitState {
    Closed,
    Open { until: Instant },
    HalfOpen { probing: bool }, // 시험 요청 중인지 구분
}
#[derive(Clone)]
pub struct CircuitBreaker {
    pub streak: isize, // 연속 성공/실패
    pub state: CircuitState,
    pub backoff_count: u64, // Open 상태 진입 횟수(Closed 가 되면 0으로)
    pub config: CircuitConfig,
}
impl CircuitBreaker {
    fn open_until(&self) -> Instant {
        let exponent = self.backoff_count as u32;

        // 1. 초기 대기시간 × 백오프 배수^지수를 계산, 오버플로 시 최댓값에서 멈추기.
        let wait_secs = self
            .config
            .initial_open_secs
            .saturating_mul((self.config.backoff_multiplier as u64).saturating_pow(exponent))
            // 2. 계산된 대기시간이 최대 대기시간을 넘지 않도록 제한하기
            .min(self.config.max_open_secs);

        Instant::now() + Duration::from_secs(wait_secs)
    }

    pub fn record(&mut self, success: bool) {
        // 1. streak 계산
        if !success {
            if self.streak >= 0 {
                self.streak = -1;
            } else {
                self.streak -= 1;
            }
        } else {
            if self.streak <= 0 {
                self.streak = 1;
            } else {
                self.streak += 1;
            }
        }

        // 2. 서킷 상태 조정
        match self.state {
            CircuitState::Closed => {
                if !success && self.streak <= -self.config.failure_threshold {
                    let until = self.open_until();
                    self.state = CircuitState::Open { until };
                    self.backoff_count += 1;
                    println!(
                        "[circuit] Closed→Open | streak={} backoff={} until={:?}",
                        self.streak, self.backoff_count, until
                    );
                }
            }

            CircuitState::Open { .. } => {}

            CircuitState::HalfOpen { .. } => {
                if success {
                    self.state = CircuitState::Closed;
                    self.backoff_count = 0;
                    println!("[circuit] HalfOpen→Closed | 시험 성공");
                } else {
                    let until = self.open_until();
                    self.backoff_count += 1;
                    self.state = CircuitState::Open { until };

                    println!(
                        "[circuit] HalfOpen→Open | 시험 실패 backoff={} until={:?}",
                        self.backoff_count, until
                    );
                }
            }
        }
    }

    // 이 백엔드가 지금 요청을 받을 수 있는 상태인지 확인
    pub fn can_admit(&self) -> bool {
        match self.state {
            CircuitState::Closed => return true,
            CircuitState::Open { until } => {
                let now = Instant::now();

                // 차단 시간이 끝남 - 회복 여부를 볼 첫 시험 요청을 이 요청으로 보냄
                if now >= until {
                    return true;
                }

                return false;
            }
            CircuitState::HalfOpen { probing } => {
                if !probing {
                    return true;
                }
                return false;
            }
        };
    }

    // 이 백엔드로 요청을 들여보내며 서킷 상태를 갱신
    pub fn try_admit(&mut self) -> CircuitDecision {
        match self.state {
            CircuitState::Closed => CircuitDecision::Closed,
            CircuitState::Open { until } => {
                let now = Instant::now();

                // 차단 시간이 끝남 - 회복 여부를 볼 첫 시험 요청을 이 요청으로 보냄
                if now >= until {
                    self.state = CircuitState::HalfOpen { probing: true };
                    println!("[circuit] Open→HalfOpen | 시험 요청 시작");
                    return CircuitDecision::Probe;
                }
                CircuitDecision::Rejected
            }
            CircuitState::HalfOpen { probing } => {
                // 시험 슬롯이 비어 있음  이 요청이 슬롯을 잡아 시험을 수행
                if !probing {
                    self.state = CircuitState::HalfOpen { probing: true };
                    return CircuitDecision::Probe;
                }

                CircuitDecision::Rejected
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    // 테스트용 고정 config
    fn test_config() -> CircuitConfig {
        CircuitConfig {
            failure_threshold: 5,
            initial_open_secs: 10,
            backoff_multiplier: 2,
            max_open_secs: 60,
        }
    }

    fn breaker(streak: isize, state: CircuitState, backoff_count: u64) -> CircuitBreaker {
        CircuitBreaker {
            streak,
            state,
            backoff_count,
            config: test_config(),
        }
    }

    // 1. streak 배타성 — 실패 쌓이다 성공 1회 오면 양수로 리셋 (3회 터졌던 버그)
    #[test]
    fn success_after_failures_resets_streak_to_positive() {
        let mut cb = breaker(0, CircuitState::Closed, 0);

        cb.record(false);
        cb.record(false);
        cb.record(false);
        assert_eq!(cb.streak, -3);

        cb.record(true);
        assert_eq!(cb.streak, 1); // -2가 아니라 1이어야 함
    }

    // 1-b. 반대 방향 — 성공 쌓이다 실패 1회 오면 음수로 리셋
    #[test]
    fn failure_after_successes_resets_streak_to_negative() {
        let mut cb = breaker(0, CircuitState::Closed, 0);

        cb.record(true);
        cb.record(true);
        assert_eq!(cb.streak, 2);

        cb.record(false);
        assert_eq!(cb.streak, -1); // 1이 아니라 -1이어야 함
    }

    // 2. Closed→Open 경계 — threshold 미달이면 Closed 유지
    #[test]
    fn stays_closed_below_threshold() {
        let mut cb = breaker(-3, CircuitState::Closed, 0);

        cb.record(false); // streak -4, threshold 5 미달
        assert_eq!(cb.streak, -4);
        assert!(matches!(cb.state, CircuitState::Closed));
    }

    // 2-b. Closed→Open 경계 — 정확히 threshold에서 전이
    #[test]
    fn opens_exactly_at_threshold() {
        let mut cb = breaker(-4, CircuitState::Closed, 0);

        cb.record(false); // streak -5, threshold 도달
        assert_eq!(cb.streak, -5);
        assert!(matches!(cb.state, CircuitState::Open { .. }));
        assert_eq!(cb.backoff_count, 1); // Open 진입으로 증가
    }

    // 3. HalfOpen→Closed — 성공 시 Closed + backoff_count 리셋
    #[test]
    fn halfopen_success_closes_and_resets_backoff() {
        let mut cb = breaker(-5, CircuitState::HalfOpen { probing: true }, 2);

        cb.record(true);
        assert!(matches!(cb.state, CircuitState::Closed));
        assert_eq!(cb.backoff_count, 0); // 리셋 확인
        assert_eq!(cb.streak, 1); // 성공이니 양수로
    }

    // 4. HalfOpen→Open — 실패 시 Open + backoff_count 증가
    #[test]
    fn halfopen_failure_reopens_and_increments_backoff() {
        let mut cb = breaker(-5, CircuitState::HalfOpen { probing: true }, 1);

        cb.record(false);
        assert!(matches!(cb.state, CircuitState::Open { .. }));
        assert_eq!(cb.backoff_count, 2); // 증가 확인
    }

    // 5. Closed 상태에서 성공 누적은 Open 안 됨 (streak 양수 유지)
    #[test]
    fn closed_stays_closed_on_success() {
        let mut cb = breaker(0, CircuitState::Closed, 0);

        for _ in 0..10 {
            cb.record(true);
        }
        assert_eq!(cb.streak, 10);
        assert!(matches!(cb.state, CircuitState::Closed));
    }
}
