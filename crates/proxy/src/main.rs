use std::{net::SocketAddr, sync::Arc, time::Duration};

use tokio::{io, net::TcpListener};

use crate::{balancer::round_robin::RoundRobinBalancer, pool::connection_pool::ConnectionPool};

pub mod balancer;
pub mod connection;
pub mod http;
pub mod pool;

#[tokio::main]
async fn main() -> io::Result<()> {
    // bind()를 loop 안에 넣으면 loop 돌 때마다 같은 포트(8080)에 또 서버를 열려고 시도하는 꼴
    let listener = TcpListener::bind("127.0.0.1:8080").await?;

    let backends = vec![
        "127.0.0.1:8081"
            .parse::<SocketAddr>()
            .map_err(io::Error::other)?,
        "127.0.0.1:8082"
            .parse::<SocketAddr>()
            .map_err(io::Error::other)?,
    ];

    let rr_balancer = Arc::new(RoundRobinBalancer::new(backends));
    let conn_pool = Arc::new(ConnectionPool::new(10));

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

                let rr_balancer_clone = rr_balancer.clone();
                let conn_pool_clone = conn_pool.clone();

                tokio::spawn(async move {
                    if let Err(e) = connection::handle_connection(
                        client_stream,
                        client_addr,
                        &rr_balancer_clone,
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
