use std::{net::SocketAddr, sync::Arc, time::Duration};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::{
    balancer::{Balancer, BalancerError, Selection},
    http::{
        error::{
            BodyKind,
            ConnectionError::{self},
        },
        request,
        response::{self, parse_backend_header},
        types::{ParsedRequest, RequestHeaderParseResult},
    },
    pool::connection_pool::{ConnectionPool, PooledConnection},
};

pub async fn handle_connection(
    mut client_stream: TcpStream,
    client_addr: SocketAddr,
    balancer: &Arc<Balancer>,
    conn_pool: &Arc<ConnectionPool>,
) -> Result<(), ConnectionError> {
    let mut client_buf: Vec<u8> = Vec::new();

    client_buf.clear();

    let mut parsed_req = ParsedRequest {
        header: RequestHeaderParseResult {
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
        request::parse_client_header(&mut client_stream, &mut parsed_req, &mut client_buf).await
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
                if let Err(write_err) = client_stream.write_all(res).await {
                    eprintln!("400 응답 작성 실패: {}", write_err);
                }
                let _ = client_stream.shutdown().await;
                return Err(ConnectionError::MalformedRequest(mre));
            }
            ConnectionError::AllBackendsUnreachable
            | ConnectionError::NoBackendAvailable
            | ConnectionError::PoolAcquireTimeout
            | ConnectionError::MalformedResponse(_)
            | ConnectionError::BackendClose => {
                unreachable!("parse_client_header는 이 에러를 반환하지 않음");
            }
        }
    };

    // 백엔드 서버 연결 TODO: 별도 분리
    let (mut backend_conn, _selection) = match connect_with_retry(&balancer, conn_pool).await {
        Ok(conn) => conn,
        Err(ConnectionError::AllBackendsUnreachable) => {
            eprintln!("백엔드 연결 실패");
            let res = response::ALL_BACKENDS_UNAVAILABLE;
            if let Err(write_err) = client_stream.write_all(res).await {
                eprintln!("502 응답 작성 실패: {}", write_err);
            }
            let _ = client_stream.shutdown().await;
            return Err(ConnectionError::AllBackendsUnreachable);
        }
        Err(ConnectionError::NoBackendAvailable) => {
            eprintln!("가용 백엔드 없음");
            let res = response::SERVICE_UNAVAILABLE;
            if let Err(write_err) = client_stream.write_all(res).await {
                eprintln!("503 응답 작성 실패: {}", write_err);
            }
            let _ = client_stream.shutdown().await;
            return Err(ConnectionError::NoBackendAvailable);
        }
        Err(e) => return Err(e), // connect_with_retry가 다른 variant는 반환 안 하지만 방어적으로
    };

    // X-Forwarded-For 추가
    let xff_name = "X-Forwarded-For".to_string();
    let xff_value = client_addr.ip().to_string().as_bytes().to_vec();

    parsed_req.header.headers.push((xff_name, xff_value));

    // HTTP 요청 헤더 직렬화
    let mut ser_req_buf = request::serialize_headers(&parsed_req.header);

    let body_read_result =
        request::read_body(&mut backend_conn.stream, &parsed_req, &mut client_buf).await;
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
                    if let Err(write_err) = client_stream.write_all(res).await {
                        eprintln!("400 응답 작성 실패: {}", write_err);
                    }
                    let _ = client_stream.shutdown().await;
                    return Err(ConnectionError::MalformedRequest(mre));
                }
                ConnectionError::AllBackendsUnreachable
                | ConnectionError::NoBackendAvailable
                | ConnectionError::PoolAcquireTimeout
                | ConnectionError::MalformedResponse(_)
                | ConnectionError::BackendClose => {
                    unreachable!("read_body는 이 에러를 반환하지 않음");
                }
            };
        }
    };

    // 클라이언트 -> 백엔드 데이터 전송
    match body_kind {
        BodyKind::None => {
            backend_conn.stream.write_all(&ser_req_buf).await.unwrap();
        }
        BodyKind::ContentLength(content_len) => {
            let header_idx = parsed_req.header.header_end;
            let body_idx = header_idx + content_len;

            let body = &client_buf[header_idx..body_idx];

            ser_req_buf.extend_from_slice(body);
            backend_conn.stream.write_all(&ser_req_buf).await.unwrap();
        }
        BodyKind::Chunked => todo!(),
    };

    // 백엔드 -> 클라이언트 데이터 전송
    let mut backend_buf: Vec<u8> = Vec::new(); // 응답 데이터 파싱 전용 buf
    let parsed_res = parse_backend_header(&mut backend_conn.stream, &mut backend_buf).await?;

    // Content-Length 찾기
    let content_length: usize = parsed_res
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("Content-Length"))
        .and_then(|(_, value)| std::str::from_utf8(value).ok())
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0);

    // 헤더 블록이 끝난 지점 + 바디 길이 = 응답 전체가 끝나는 바이트 위치
    let target = parsed_res.header_end + content_length;

    // 헤더 + 이미 도착해 있던 바디 일부를 먼저 클라이언트에게 전달
    client_stream.write_all(&backend_buf).await?;
    let mut total_received = backend_buf.len();

    // 남은 바디를 목표치까지 스트리밍
    while total_received < target {
        let mut chunk = [0u8; 4096];
        // TODO: 에러 발생시 backend_conn put() 수행 필요
        let n = backend_conn.stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(ConnectionError::BackendClose);
        }
        client_stream.write_all(&chunk[..n]).await?;
        total_received += n;
    }

    conn_pool.put(backend_conn).await;

    return Ok(());
}

pub async fn connect_with_retry(
    balancer: &Arc<Balancer>,
    conn_pool: &Arc<ConnectionPool>,
) -> Result<(PooledConnection, Selection), ConnectionError> {
    if balancer.backend_count() == 0 {
        return Err(ConnectionError::NoBackendAvailable);
    }

    for _ in 0..balancer.backend_count() {
        let found_next_backend = balancer.next_backend();
        let selection = match found_next_backend {
            Ok(addr) => addr,
            Err(e) => match e {
                BalancerError::NoBackendAvailable => {
                    return Err(ConnectionError::NoBackendAvailable);
                }
            },
        };

        let found_conn = conn_pool.take(selection.addr).await;
        match found_conn {
            Some(conn) => return Ok((conn, selection)),
            None => {
                let stream = match TcpStream::connect(selection.addr).await {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("백엔드 {} 연결 실패: {}", selection.addr, e);
                        continue;
                    }
                };

                let timeout = Duration::new(5, 0);
                // TODO: 다른 백엔드로의 재시도
                let permit = match conn_pool.acquire_permit(timeout).await {
                    Ok(p) => p,
                    Err(e) => {
                        eprintln!(
                            "백엔드 {:#?}에 연결은 성공했으나 permit 획득 실패, 연결 폐기: {e:#?}",
                            selection.addr
                        );
                        return Err(e.into());
                    }
                };

                return Ok((
                    PooledConnection::new(stream, permit, selection.addr),
                    selection,
                ));
            }
        }
    }

    Err(ConnectionError::AllBackendsUnreachable)
}
