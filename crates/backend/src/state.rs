use std::{sync::atomic::AtomicU64, time::Instant};

use serde::Serialize;

#[derive(Debug)]
pub struct State {
    pub started_at: Instant,             // 서버 시작 시간
    pub burst_end_elapsed_ms: AtomicU64, // 버스트 종료시간
    pub bursts_started: AtomicU64,       // 버스트 몇번 시작했는지
    pub injected_failures: AtomicU64,    // 반환한 503 응답 수
}

#[derive(Serialize)]
pub struct SerializedState {
    pub uptime_ms: u64,            // 서버 시작 시간
    pub burst_end_elapsed_ms: u64, // 버스트 종료시간
    pub bursts_started: u64,       // 버스트 몇번 시작했는지
    pub injected_failures: u64,    // 반환한 503 응답 수
}
