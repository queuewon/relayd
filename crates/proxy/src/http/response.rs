use tokio::{io::AsyncReadExt, net::TcpStream};

use crate::http::{error::ConnectionError, types::ResponseHeaderParseResult, version_to_string};

// 400
pub const BAD_REQUEST: &[u8] =
    b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
pub const REQUEST_ENTITY_TOO_LARGE: &[u8] =
    b"HTTP/1.1 413 Request Entity Too Large\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

// 500
pub const ALL_BACKENDS_UNAVAILABLE: &[u8] =
    b"HTTP/1.1 502 All Backends Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
pub const SERVICE_UNAVAILABLE: &[u8] =
    b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
pub const GATEWAY_TIMEOUT: &[u8] =
    b"HTTP/1.1 504 Gateway Timeout\r\nContent-Length: 0\r\nConnection: Close\r\n\r\n";

pub async fn parse_backend_header(
    backend_stream: &mut TcpStream,
    buf: &mut Vec<u8>,
) -> Result<ResponseHeaderParseResult, ConnectionError> {
    loop {
        // OS 소켓 버퍼에 있던 바이트들은 backend_buf(유저 공간 메모리)로 옮겨짐
        let n: usize = match backend_stream.read_buf(buf).await {
            Ok(n) => n,
            Err(e) => {
                eprintln!("Failed to read from backend stream: {}", e);
                return Err(ConnectionError::Io(e));
            }
        };
        // 백엔드가 연결을 맺었다가 완전히 닫아버린 경우(정상 종료든, 갑자기 끊기든, 악의적 공격이든) 처리. 0이면 연결이 끊긴 것임.
        if n == 0 {
            eprintln!("Backend closed the connection");
            return Err(ConnectionError::BackendClosedBeforeResponse);
        }

        // 헤더 버퍼 초기화
        let mut headers = [httparse::EMPTY_HEADER; 64];
        let mut res = httparse::Response::new(&mut headers);

        // 헤더 파싱
        let parsing_status = match res.parse(buf) {
            Ok(s) => s,
            Err(e) => return Err(ConnectionError::MalformedResponse(e.to_string())),
        };

        match parsing_status {
            httparse::Status::Complete(s) => {
                let received_version = res.version.unwrap_or_default();
                let version = version_to_string(received_version);
                let code = res.code.unwrap_or_default();
                let headers = res
                    .headers
                    .iter()
                    .map(|h| (h.name.to_string(), h.value.to_vec()))
                    .collect();

                return Ok(ResponseHeaderParseResult {
                    version,
                    code,
                    headers,
                    header_end: s,
                });
            }
            httparse::Status::Partial => {
                continue;
            }
        };
    }
}
