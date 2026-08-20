use std::{net::SocketAddr, sync::Arc};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::{
    balancer::{BalancerError, round_robin::RoundRobinBalancer},
    http::{
        error::{BodyKind, ConnectionError},
        request, response,
        types::{HeaderParseResult, ParsedRequest},
    },
};

pub async fn handle_connection(
    mut stream: TcpStream,
    addr: SocketAddr,
    balancer: &Arc<RoundRobinBalancer>,
) -> Result<(), ConnectionError> {
    let mut client_buf: Vec<u8> = Vec::new();

    client_buf.clear();

    let mut parsed_req = ParsedRequest {
        header: HeaderParseResult {
            method: String::new(),
            headers: Vec::new(),
            path: String::new(),
            version: "1.1".to_string(),
            header_end: 0,
        },
        body: Vec::new(),
    };

    // 1. TODO: 무한 partial 우려: httpparse는 공백으로 헤더 종결을 구분하는데 악의적 요청, 클라이언트의 요청이 지속적으로 부분적으로만 오는 경우 (떠오르는 방안: 타임아웃) -> 이건 추후 고려
    // 2. TODO: Content-Length가 있으면 그만큼 읽고 청크 인코딩이면 청크 단위로 읽기
    // 3. TODO: X-forwarded-for 의 경우 이전 프록시에서 거쳐왔을 수도 있으나, 현재는 일단 추가만 하는 방식
    if let Err(e) =
        request::parse_client_header(&mut stream, &mut parsed_req, &mut client_buf).await
    {
        match e {
            ConnectionError::Io(ioe) => {
                eprintln!("클라이언트 스트림 앍기 오류: {}", ioe);
                return Err(ConnectionError::Io(ioe));
            }
            ConnectionError::ClientClosed => {
                eprintln!("요청 완료 전 클라이언트 측 연결 종료");
                return Err(ConnectionError::ClientClosed);
            }
            ConnectionError::MalformedRequest(mre) => {
                eprintln!("잘못된 요청: {:?}", mre);
                let res = response::BAD_REQUEST;
                if let Err(write_err) = stream.write_all(res).await {
                    eprintln!("400 응답 작성 실패: {}", write_err);
                }
                let _ = stream.shutdown().await;
                return Err(ConnectionError::MalformedRequest(mre));
            }
            ConnectionError::AllBackendsUnreachable | ConnectionError::NoBackendAvailable => {
                unreachable!("parse_client_header는 이 에러를 반환하지 않음");
            }
        }
    };

    // 백엔드 서버 연결 TODO: 별도 분리
    let mut backend_stream = match connect_with_retry(&balancer).await {
        Ok(stream) => stream,
        Err(ConnectionError::AllBackendsUnreachable) => {
            eprintln!("백엔드 연결 실패");
            let res = response::ALL_BACKENDS_UNAVAILABLE;
            if let Err(write_err) = stream.write_all(res).await {
                eprintln!("502 응답 작성 실패: {}", write_err);
            }
            let _ = stream.shutdown().await;
            return Err(ConnectionError::AllBackendsUnreachable);
        }
        Err(ConnectionError::NoBackendAvailable) => {
            eprintln!("가용 백엔드 없음");
            let res = response::SERVICE_UNAVAILABLE;
            if let Err(write_err) = stream.write_all(res).await {
                eprintln!("503 응답 작성 실패: {}", write_err);
            }
            let _ = stream.shutdown().await;
            return Err(ConnectionError::NoBackendAvailable);
        }
        Err(e) => return Err(e), // connect_with_retry가 다른 variant는 반환 안 하지만 방어적으로
    };

    // X-Forwarded-For 추가
    let xff_name = "X-Forwarded-For".to_string();
    let xff_value = addr.ip().to_string().as_bytes().to_vec();

    // Keep-Alive로 인한 성능 테스트 이슈로 임시방편 추가 추후 제거
    let connection_name = "Connection".to_string();
    let connection_value = "close".to_string().as_bytes().to_vec();

    parsed_req.header.headers.push((xff_name, xff_value));
    parsed_req
        .header
        .headers
        .push((connection_name, connection_value));

    // HTTP 요청 헤더 직렬화
    let mut ser_req_buf = request::serialize_headers(&parsed_req.header);

    let body_read_result = request::read_body(&mut stream, &parsed_req, &mut client_buf).await;
    let body_kind = match body_read_result {
        Ok(kind) => kind,
        Err(e) => {
            match e {
                ConnectionError::Io(ioe) => {
                    eprintln!("클라이언트 스트림 앍기 오류: {}", ioe);
                    return Err(ConnectionError::Io(ioe));
                }
                ConnectionError::ClientClosed => {
                    eprintln!("요청 완료 전 클라이언트 측 연결 종료");
                    return Err(ConnectionError::ClientClosed);
                }
                ConnectionError::MalformedRequest(mre) => {
                    eprintln!("잘못된 요청: {:?}", mre);
                    let res = response::BAD_REQUEST;
                    if let Err(write_err) = stream.write_all(res).await {
                        eprintln!("400 응답 작성 실패: {}", write_err);
                    }
                    let _ = stream.shutdown().await;
                    return Err(ConnectionError::MalformedRequest(mre));
                }
                ConnectionError::AllBackendsUnreachable | ConnectionError::NoBackendAvailable => {
                    unreachable!("read_body는 이 에러를 반환하지 않음");
                }
            };
        }
    };

    // 클라이언트 -> 백엔드 데이터 전송
    match body_kind {
        BodyKind::None => {
            backend_stream.write_all(&ser_req_buf).await.unwrap();
        }
        BodyKind::ContentLength(content_len) => {
            let header_idx = parsed_req.header.header_end;
            let body_idx = header_idx + content_len;

            let body = &client_buf[header_idx..body_idx];

            ser_req_buf.extend_from_slice(body);
            backend_stream.write_all(&ser_req_buf).await.unwrap();
        }
        BodyKind::Chunked => todo!(),
    };

    // 백엔드 -> 클라이언트 데이터 전송
    loop {
        let mut chunk = [0u8; 4096];
        // '비어있음, 읽을 값 없음' 이라는 상태 자체가 0을 리턴하는 조건이 아님
        // 경우 1: 커널이 "이 소켓에 바이트가 도착해 있다"를 확인함 → 그 바이트 수만큼 n을 리턴 (n > 0)
        // 경우 2: 커널이 "상대방이 이 연결을 끊었다(TCP FIN 수신)"를 확인함 → n = 0을 리턴
        let n = backend_stream.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        stream.write_all(&chunk[..n]).await?;
    }

    return Ok(());
}

pub async fn connect_with_retry(
    balancer: &Arc<RoundRobinBalancer>,
) -> Result<TcpStream, ConnectionError> {
    if balancer.backend_count() == 0 {
        return Err(ConnectionError::NoBackendAvailable);
    }

    for _ in 0..balancer.backend_count() {
        let found_next_backend = balancer.next_backend();
        let addr = match found_next_backend {
            Ok(addr) => addr,
            Err(e) => match e {
                BalancerError::NoBackendAvailable => {
                    return Err(ConnectionError::NoBackendAvailable);
                }
            },
        };
        match TcpStream::connect(addr).await {
            Ok(stream) => return Ok(stream),
            Err(e) => {
                eprintln!("백엔드 {} 연결 실패: {}", addr, e);
                continue;
            }
        }
    }

    Err(ConnectionError::AllBackendsUnreachable)
}
