use std::{net::SocketAddr, sync::Arc};

use tokio::{io, net::TcpListener};

use crate::balancer::round_robin::RoundRobinBalancer;

pub mod balancer;
pub mod connection;
pub mod http;

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

    loop {
        // 클라이언트 연결 수락
        let (client_stream, client_addr) = match listener.accept().await {
            Ok((stream, addr)) => (stream, addr),
            Err(e) => {
                eprintln!("클라이언트 연결 수락 실패: {}", e);
                continue;
            }
        };

        let rr_balancer_clone = Arc::clone(&rr_balancer);

        // await 해버리면 다음 accept을 못함
        tokio::spawn(async move {
            if let Err(e) =
                connection::handle_connection(client_stream, client_addr, &rr_balancer_clone).await
            {
                eprintln!("연결 처리 중 에러: {:?}", e);
            }
        });
    }
}
