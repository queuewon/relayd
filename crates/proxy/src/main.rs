use std::{sync::Arc, time::Duration};

use tokio::{io, net::TcpListener};

use crate::{balancer::Balancer, config::ProxyConfig, pool::connection_pool::ConnectionPool};

pub mod backend;
pub mod balancer;
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
    let config: ProxyConfig = toml::from_str(&content).expect("프록시 설정파일 적용 실패");

    let balancer = Balancer::from_config(config);

    health_check::start_health_checks(&balancer);

    let listener = TcpListener::bind("127.0.0.1:8080").await?;

    let arc_balancer = Arc::new(balancer);

    let conn_pool = Arc::new(ConnectionPool::new(20));

    let max_idle = Duration::new(5, 0);
    let interval = Duration::new(10, 0);
    conn_pool.spawn_cleanup_task(max_idle, interval);

    loop {
        // 여러 .await를 동시에 감시하다가 먼저 끝나는 쪽을 처리하는 도구
        tokio::select! {
            accept_result = listener.accept() => {
                let (client_stream, client_addr) = match accept_result {
                    Ok((stream, addr)) => (stream, addr),
                    Err(e) => {
                        eprintln!("클라이언트 연결 수락 실패: {}", e);
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
                        eprintln!("연결 처리 중 에러: {:?}", e);
                    }
                });
            }
            _ = tokio::signal::ctrl_c() => {
                eprintln!("종료 신호 수신, 커넥션 풀 재사용률: {:.2}%", conn_pool.reuse_rate());
                break;
            }
        }
    }

    Ok(())
}
