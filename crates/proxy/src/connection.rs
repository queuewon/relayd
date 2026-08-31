use std::{collections::HashSet, net::SocketAddr, sync::Arc, time::Duration};

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
            TimeoutKind,
        },
        request,
        response::{self, parse_backend_header},
        types::{ParsedRequest, RequestHeaderParseResult, ResponseHeaderParseResult},
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

    // TODO 1.: 무한 partial 우려: httpparse는 공백으로 헤더 종결을 구분하는데 악의적 요청, 클라이언트의 요청이 지속적으로 부분적으로만 오는 경우 (떠오르는 방안: 타임아웃) -> 이건 추후 고려
    // TODO 2.: Content-Length가 있으면 그만큼 읽고 청크 인코딩이면 청크 단위로 읽기
    // TODO 3.: X-forwarded-for 의 경우 이전 프록시에서 거쳐왔을 수도 있으나, 현재는 일단 추가만 하는 방식
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
            _ => {
                unreachable!("parse_client_header는 이 에러를 반환하지 않음");
            }
        }
    };

    // 백엔드 서버 연결 TODO: 별도 분리
    let (mut backend_conn, selection) = match connect_with_retry(balancer, conn_pool).await {
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
        request::read_body(&mut client_stream, &parsed_req, &mut client_buf).await;
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
                _ => {
                    unreachable!("read_body는 이 에러를 반환하지 않음");
                }
            };
        }
    };

    match body_kind {
        BodyKind::None => {}
        BodyKind::ContentLength(content_len) => {
            let header_idx = parsed_req.header.header_end;
            let body_idx = header_idx + content_len;
            ser_req_buf.extend_from_slice(&client_buf[header_idx..body_idx]);
        }
        BodyKind::Chunked => todo!(),
    };

    // 클라이언트 <-> 백엔드 간 데이터 스트리밍
    let future = send_and_relay_response(
        &mut client_stream,
        &mut backend_conn.stream,
        &ser_req_buf,
        &selection,
    );

    let relay_duration = Duration::from_secs(30);
    let relay_timeout_result = tokio::time::timeout(relay_duration, future).await;
    let relay_result = match relay_timeout_result {
        Ok(res) => res,
        Err(e) => {
            eprintln!(
                "타임아웃 | 백엔드 {}, 백엔드 요청 ~ 클라이언트 응답 작업 실패, 경과시간: {}",
                selection.backend.addr, e
            );

            // 실패 기록은 원칙적으로 send_and_relay_response 내부에서 하지만,
            // 타임아웃은 future가 취소되어 내부 코드가 실행될 수 없으므로 여기서 기록함

            selection.backend.note_traffic_result(false);

            let _ = client_stream.shutdown().await;
            return Err(ConnectionError::BackendTimeout(TimeoutKind::Overall));
        }
    };

    // 이 구간 이후 즉, Overall 타임아웃은 항상 응답 바디 전송 중에 발동한다고 전제하고 504 대체 응답 없이 연결만 끊기. (헤더 타임아웃 < 전체 타임아웃 관계에 의존)
    // send_and_relay_response()에선 헤더 타임아웃(5s)이 먼저 자르는 구간이 존재하기 때문에, 30s를 소모할 수 있는 구간은 바디 스트리밍뿐
    // 그 시점엔 이미 클라이언트에 200 상태를 write한 뒤라(헤더 라인을 이미 써버림.) 504로 정정할 방법이 없음(HTTP/1.1은 응답당 상태 라인이 하나).
    let parsed_res = match relay_result {
        Ok(res) => res,
        Err(e) => {
            eprintln!("백엔드 {} 릴레이 실패: {:#?}", selection.backend.addr, e);

            // 클라이언트 스트림 write 실패는 백엔드 책임이 아니므로 traffic 기록 없이 반환
            return Err(e);
        }
    };

    // HTTP 레벨에서는 정상적인 응답이므로 백엔드 커넥션을 풀에 반납하도록 함.
    let backend_ok = !matches!(parsed_res.code, 502 | 503 | 504);
    if !backend_ok {
        eprintln!(
            "from : {} 응답 오류, status code: {}",
            selection.backend.addr, parsed_res.code
        );
    }
    selection.backend.note_traffic_result(backend_ok);

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

    let mut failed_backends: HashSet<SocketAddr> = HashSet::new();

    for _ in 0..balancer.backend_count() {
        let found_next_backend = balancer.next_backend(&failed_backends);
        let selection = match found_next_backend {
            Ok(addr) => addr,
            Err(e) => match e {
                BalancerError::NoBackendAvailable => {
                    return Err(ConnectionError::NoBackendAvailable);
                }
            },
        };
        println!("백엔드 {} 선택", selection.backend.addr);

        let found_conn = conn_pool.take(selection.backend.addr).await;
        match found_conn {
            Some(mut conn) => {
                conn.reused = true;
                return Ok((conn, selection));
            }
            None => {
                let connect_timeout = Duration::from_secs(1);
                let future = TcpStream::connect(selection.backend.addr);
                let timeout_result = tokio::time::timeout(connect_timeout, future).await;

                let connect_result = match timeout_result {
                    Ok(t) => t,
                    Err(e) => {
                        selection.backend.note_traffic_result(false);
                        failed_backends.insert(selection.backend.addr);
                        eprintln!(
                            "타임아웃 | 백엔드 {} 연결 실패: {}",
                            selection.backend.addr, e
                        );
                        continue;
                    }
                };
                let stream = match connect_result {
                    Ok(stream) => stream,
                    Err(e) => {
                        selection.backend.note_traffic_result(false);
                        failed_backends.insert(selection.backend.addr);
                        eprintln!("백엔드 {} 연결 실패: {}", selection.backend.addr, e);
                        continue;
                    }
                };

                let permit_timeout = Duration::new(5, 0);

                let permit = match conn_pool.acquire_permit(permit_timeout).await {
                    Ok(p) => p,
                    Err(e) => {
                        // 프록시 측 자원 고갈 문제이기에 수동 헬스체크 미수행
                        failed_backends.insert(selection.backend.addr);
                        eprintln!(
                            "백엔드 {:#?}에 연결은 성공했으나 permit 획득 실패, 연결 폐기: {e:#?}",
                            selection.backend.addr
                        );
                        continue;
                    }
                };

                // handle_connection에서 백엔드로 데이터를 보내고 나서야 비로소 올바른 헬스체크라 판단하여 해당 메소드에서는 success 수동 헬스체크 미수행

                return Ok((
                    PooledConnection::new(stream, permit, selection.backend.addr),
                    selection,
                ));
            }
        }
    }

    Err(ConnectionError::AllBackendsUnreachable)
}

async fn send_and_relay_response(
    client_stream: &mut TcpStream,
    backend_stream: &mut TcpStream,
    req_bytes: &[u8],
    selection: &Selection,
) -> Result<ResponseHeaderParseResult, ConnectionError> {
    if let Err(e) = backend_stream.write_all(req_bytes).await {
        selection.backend.note_traffic_result(false);
        eprintln!("클라이언트 -> 백엔드 데이터 전송 실패: {}", e);
        return Err(ConnectionError::Io(e));
    }

    let mut backend_buf: Vec<u8> = Vec::new(); // 응답 데이터 파싱 전용 buf
    let response_header_timeout = Duration::from_secs(5);

    let future = parse_backend_header(backend_stream, &mut backend_buf);
    let timeout_result = tokio::time::timeout(response_header_timeout, future).await;
    let parsed_backend_header_result = match timeout_result {
        Ok(result) => result,
        Err(e) => {
            eprintln!("타임아웃 | 백엔드 헤더 파싱 실패, 경과 시간: {}", e);

            selection.backend.note_traffic_result(false);

            let res = response::GATEWAY_TIMEOUT;

            // handle_connection()의 호출부에서는 client stream write 전 구간이므로 504 에러 반환
            if let Err(write_err) = client_stream.write_all(res).await {
                eprintln!("504 응답 작성 실패: {}", write_err);
            }

            let _ = client_stream.shutdown().await;

            return Err(ConnectionError::BackendTimeout(TimeoutKind::ResponseHeader));
        }
    };

    let parsed_res = match parsed_backend_header_result {
        Ok(res) => res,
        Err(e) => {
            eprintln!("백엔드 헤더 파싱 실패: {:#?}", e);
            selection.backend.note_traffic_result(false);
            return Err(e);
        }
    };

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
        let read_backend_body_result = backend_stream.read(&mut chunk).await;
        let n = match read_backend_body_result {
            Ok(n) => n,
            Err(e) => {
                selection.backend.note_traffic_result(false);
                eprintln!("백엔드 응답 body read 실패: {}", e);
                return Err(e.into());
            }
        };
        // 백엔드가 응답을 전부 하지 못하고 스트림을 끊었으니 어디까지 왔고 어디서 끝나는지가 불확실한 상태이기에 해당 커넥션은 drop
        if n == 0 {
            selection.backend.note_traffic_result(false);
            return Err(ConnectionError::BackendClosedMidResponse);
        }

        client_stream.write_all(&chunk[..n]).await?;
        total_received += n;
    }

    Ok(parsed_res)
}
