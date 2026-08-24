use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use tokio::{
    net::TcpStream,
    sync::{OwnedSemaphorePermit, Semaphore},
};

use crate::http::error::PoolError;

pub struct PooledConnection {
    pub stream: TcpStream,

    backend_addr: SocketAddr,

    // TcpStream 인스턴스가 존재하는 동안 계속 쥐고 있어야 하는 permit. '이 구조체(=이 TcpStream)가 drop될 때 같이 drop되며 자동으로 세마포어에 반납됨'.
    // take, put에서는 건드리지 않음 (각각 풀에 있는 커넥션 획득, 풀에 반납이기 때문. 커넥션을 폐기/생성하지 않음).
    #[allow(dead_code)]
    permit: OwnedSemaphorePermit,

    // 이 커넥션이 '풀 안에서 얼마나 오래 아무도 안 쓰고 방치돼 있는지를' 재기 위한 시각. "커넥션이 태어난 이후 총 얼마나 됐는지"를 재는 게 아님
    // put()에서 반납되는 순간 값이 채워지고,
    // cleanup_idle()이 주기적으로 이 값을 기준으로 유휴시간을 체크해 오래된 것을 제거함.
    // TODO: 추후 별도 분리 - 반납 됐을때 필요한 필드인데 생성자 때 의미없이 생성하게 됨
    returned_at: Instant,
}
impl PooledConnection {
    pub fn new(stream: TcpStream, permit: OwnedSemaphorePermit, backend_addr: SocketAddr) -> Self {
        Self {
            stream,
            backend_addr,
            permit,
            returned_at: Instant::now(),
        }
    }
}

// HTTP/1.1은 기본적으로 한 TCP 커넥션 위에서 "요청 보냄 → 응답 받음 → 다음 요청 보냄 → 응답 받음" 식으로 순서대로(sequential) 주고받는 구조
// 한 커넥션으로는 한 번에 한 요청밖에 못 다루기 때문에 한 클라이언트가 여러 요청을 보냈을 때에 대응을 위해 Vec<PooledConnection> 타입 유지
type Pool = Mutex<HashMap<SocketAddr, Vec<PooledConnection>>>;

pub struct ConnectionPool {
    pool: Pool,

    // Memo
    // 1."총 N개까지"라는 하나의 상한선만 갖고 있음. TcpStream 개별 정보는 전혀 안 가짐.
    // 2. 그러다보니 '특정 TcpStream 하나가 다 쓰이고 죽어야 하는 상황이 온 경우' -> 이 TcpStream이 permit을 갖고 있었으니, permiit 반납을 해줘야 세마포어 총량이 정확하게 유지됨. 안그러면 세마포어 양 즉, 커넥션 풀의 유휴 커넥션 양이 정확히 일치하지 않음. 반납 신호가 없었으니
    // 4. 근데 반납하려면 "반납할 permit 값" 자체가 있어야 함
    // 5. semaphore.add_permits(1) 같은 걸 부르거나, 아니면 permit 값을 drop시켜야 반납이 됨.
    // 6. 제거할 때가 된 TcpStream이 받았던 permit 값이 어디 있는지를 코드가 모르면, 반납을 시도할 수조차 없음
    // 7. 그로므로 TcpStream 별 permit을 관리하여 실제로 폐기될 때 해당 permite을 사용해 sempaphore 값을 증가 시켜줘야 함(폐기되면 커넥션 총량이 늘어나는 것이니 증가)
    semaphore: Arc<Semaphore>, // 커넥션 총량 강제. 사용·유휴 관계없이 이 풀이 소유한 커넥션 양.

    reuse_count: AtomicUsize, // take가 Some 반환 횟수
    new_count: AtomicUsize,   // take가 None 반환 횟수
}

impl ConnectionPool {
    pub fn new(max_size: usize) -> Self {
        Self {
            pool: Mutex::new(HashMap::new()),
            semaphore: Arc::new(Semaphore::new(max_size)),
            reuse_count: AtomicUsize::new(0),
            new_count: AtomicUsize::new(0),
        }
    }

    // 풀에서 재사용 가능한 커넥션 꺼내기
    // 1. semaphore 필드는 TcpStream 인스턴스 총량을 세는 필드임.
    // 2. permit은 "TcpStream 인스턴스가 메모리상 존재하냐 안 하냐"를 확인하는 용도 이므로 take()에서는 풀에 있던 것을 활용하는 것이기 때문에 permit의 값에 변경을 줄 필요가 없음. 왜냐하면 살아있되 쉬고 있던걸 일을 다시 시키는 것이기 때문
    pub async fn take(&self, addr: SocketAddr) -> Option<PooledConnection> {
        // TODO: Mutex poison 발생 시 복구 전략 미정. 현재는 lock 구간에 .await/panic 가능 로직을 두지 않는 설계로
        let mut guard = self.pool.lock().unwrap();
        let found_conn = guard.get_mut(&addr).and_then(|p| p.pop());

        if found_conn.is_some() {
            self.reuse_count.fetch_add(1, Ordering::Relaxed);
        } else {
            self.new_count.fetch_add(1, Ordering::Relaxed);
        }

        found_conn
    }

    // 풀에 커넥션 반납
    pub async fn put(&self, mut conn: PooledConnection) {
        // TODO: Mutex poison 발생 시 복구 전략 미정. 현재는 lock 구간에 .await/panic 가능 로직을 두지 않는 설계로
        let mut guard = self.pool.lock().unwrap();
        conn.returned_at = Instant::now();

        guard
            .entry(conn.backend_addr)
            .or_insert_with(Vec::new)
            .push(conn);
    }

    // 새로운 커넥션 생성을 위해 세마포어로부터 permit을 하나 새로 확보
    pub async fn acquire_permit(
        &self,
        timeout: Duration,
    ) -> Result<OwnedSemaphorePermit, PoolError> {
        let future = self.semaphore.clone().acquire_owned();
        let timeout_result = tokio::time::timeout(timeout, future).await;

        let acquire_result = match timeout_result {
            Ok(ac_result) => ac_result,
            Err(_) => return Err(PoolError::AcquireTimeout),
        };
        let permit = match acquire_result {
            Ok(permit) => permit,
            Err(_) => {
                unreachable!()
            }
        };

        Ok(permit)
    }

    // TODO: 주기적인 청소 시간마다 pools 점유로 태스크의 take()/put()이 잠깐 막히게 됨. 개선방안 생각 필요
    // 풀에서 유휴 타임아웃 시간(returned_at)이 지난 커넥션 제거
    pub async fn cleanup_idle(&self, max_idle: Duration) {
        // lock()을 쓰면 take, put이 몰리는 시기와 겹쳤을 때, cleanup_idle 그 경합에 끼어들어서 실제 요청 처리(take/put)를 아주 살짝 지연시킬 수 있으니 try_lock으로 회차 건너뛰는 식의 유동적 처리로 결정
        let lock = self.pool.try_lock();

        if let Ok(mut guard) = lock {
            for conns in guard.values_mut() {
                conns.retain(|conn| conn.returned_at.elapsed() > max_idle);
            }
        }
    }

    // 백그라운드 유휴 커넥션 정리
    pub fn spawn_cleanup_task(self: &Arc<Self>, max_idle: Duration, interval: Duration) {
        let pool = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                pool.cleanup_idle(max_idle).await;
            }
        });
    }

    // 지금까지의 재사용률(%) 조회
    pub fn reuse_rate(&self) -> f64 {
        let reuse = self.reuse_count.load(Ordering::Relaxed) as f64;
        let new = self.new_count.load(Ordering::Relaxed) as f64;
        let total = reuse + new;
        if total == 0.0 {
            0.0
        } else {
            reuse / total * 100.0
        }
    }
}
