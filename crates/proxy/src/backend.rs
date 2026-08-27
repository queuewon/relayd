use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicIsize},
    },
};

#[derive(Clone)]
pub struct Backend {
    pub addr: SocketAddr,
    pub healthy: Arc<AtomicBool>,       // 정상, 비정상 상태
    pub probe_streak: Arc<AtomicIsize>, // 상태 확인 횟수(양수 - 성공, 음수 - 실패)
}
impl Backend {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            healthy: Arc::new(AtomicBool::new(true)),
            probe_streak: Arc::new(AtomicIsize::new(0)),
        }
    }
}
