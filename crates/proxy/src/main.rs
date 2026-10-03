use std::{sync::Arc, time::Duration};

use tokio::{io, net::TcpListener};

use crate::{balancer::Balancer, config::ProxyConfig, pool::connection_pool::ConnectionPool};

pub mod backend;
pub mod balancer;
pub mod circuit_breaker;
pub mod config;
pub mod connection;
pub mod health_check;
pub mod http;
pub mod pool;

#[tokio::main]
async fn main() -> io::Result<()> {
    let path = std::env::args()
        .nth(1)
        .expect("config 파일 설정 경로를 인자로 지정하는 작업이 필요");
    let content = std::fs::read_to_string(path).expect("프록시 설정파일 불러오기 실패");
    let proxy_config: ProxyConfig = toml::from_str(&content).expect("프록시 설정파일 적용 실패");

    if proxy_config.connection_pool.max_idle_secs < 1 {
        panic!("connection_pool.max_idle_secs 값은 1 이상이어야 합니다.");
    }
    if proxy_config.connection_pool.cleanup_interval_secs < 1 {
        panic!("connection_pool.cleanup_interval_secs 값은 1 이상이어야 합니다.");
    }

    println!("\n프록시 설정파일 적용 성공: {:#?}\n", proxy_config);

    let health_check_interval = Duration::from_millis(proxy_config.health.interval_milli_secs);

    let balancer = Balancer::from_config(proxy_config.clone());

    health_check::start_health_checks(&balancer, health_check_interval);

    let listener = TcpListener::bind("127.0.0.1:8080").await?;

    let arc_balancer = Arc::new(balancer);

    let max_idle = Duration::new(proxy_config.connection_pool.max_idle_secs, 0);

    let conn_pool = Arc::new(ConnectionPool::new(20, max_idle));

    let cleanup_interval = Duration::new(proxy_config.connection_pool.cleanup_interval_secs, 0);

    println!(
        "[init] 프록시 서버 시작: max_idle={:?}, cleanup_interval={:?}",
        max_idle, cleanup_interval
    );

    conn_pool.spawn_cleanup_task(cleanup_interval);

    loop {
        // 여러 .await를 동시에 감시하다가 먼저 끝나는 쪽을 처리하는 도구
        tokio::select! {
            accept_result = listener.accept() => {
                let (client_stream, client_addr) = match accept_result {
                    Ok((stream, addr)) => (stream, addr),
                    Err(e) => {
                        eprintln!("[error] 클라이언트 연결 수락 실패: {}", e);
                        continue;
                    }
                };

                let balancer_clone = arc_balancer.clone();
                let conn_pool_clone = conn_pool.clone();

                tokio::spawn(async move {
                    if let Err(e) = connection::handle_connection(
                        client_stream,
                        client_addr,
                        &balancer_clone,
                        &conn_pool_clone,
                    )
                    .await
                    {
                        eprintln!("[error] 연결 처리 중 에러: {:?}", e);
                    }
                });
            }
            _ = tokio::signal::ctrl_c() => {
                eprintln!("[shutdown] 종료 신호 수신, 커넥션 풀 재사용률: {:.2}%", conn_pool.reuse_rate());
                break;
            }
        }
    }

    Ok(())
}
