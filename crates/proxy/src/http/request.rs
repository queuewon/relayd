use tokio::{io::AsyncReadExt, net::TcpStream};

use crate::http::{
    error::{BodyKind, ConnectionError},
    request,
    types::{HeaderParseResult, ParsedRequest},
};

pub async fn parse_client_header(
    stream: &mut TcpStream,
    parsed_req: &mut ParsedRequest,
    buf: &mut Vec<u8>,
) -> Result<(), ConnectionError> {
    loop {
        // OS 소켓 버퍼에 있던 바이트들은 client_buf(유저 공간 메모리)로 옮겨짐
        let n: usize = match stream.read_buf(buf).await {
            Ok(n) => n,
            Err(e) => {
                eprintln!("Failed to read from client stream: {}", e);
                return Err(ConnectionError::Io(e));
            }
        };
        // 클라이언트가 연결을 맺었다가 완전히 닫아버린 경우(정상 종료든, 갑자기 끊기든, 악의적 공격이든) 처리. 0이면 연결이 끊긴 것임.
        if n == 0 {
            eprintln!("Client closed the connection");
            return Err(ConnectionError::ClientClosed);
        }

        // 헤더 버퍼 초기화
        let mut headers = [httparse::EMPTY_HEADER; 64];
        let mut req = httparse::Request::new(&mut headers);

        // 헤더 파싱
        let client_buf_parsing_status = match req.parse(buf) {
            Ok(s) => s,
            Err(e) => return Err(ConnectionError::MalformedRequest(e.to_string())),
        };

        match client_buf_parsing_status {
            httparse::Status::Complete(s) => {
                parsed_req.header.method = req.method.unwrap_or_default().to_string();
                parsed_req.header.path = req.path.unwrap_or_default().to_string();

                let received_version = req.version.unwrap_or_default();
                let version = request::version_to_string(received_version);

                parsed_req.header.version = version;
                parsed_req.header.headers = req
                    .headers
                    .iter()
                    .map(|h| (h.name.to_string(), h.value.to_vec()))
                    .collect();
                parsed_req.header.header_end = s;
                return Ok(());
            }
            httparse::Status::Partial => {
                continue;
            }
        };
    }
}

pub fn serialize_headers(req: &HeaderParseResult) -> Vec<u8> {
    let mut ser_req: Vec<u8> = Vec::new();
    ser_req.extend_from_slice(
        format!("{} {} HTTP/{}\r\n", req.method, req.path, req.version).as_bytes(),
    );

    // 헤더
    for header in &req.headers {
        ser_req.extend_from_slice(header.0.as_bytes());
        ser_req.extend_from_slice(b": ");
        ser_req.extend_from_slice(&header.1);
        ser_req.extend_from_slice(b"\r\n");
    }
    ser_req.extend_from_slice(b"\r\n\r\n");

    ser_req
}

pub async fn read_body(
    stream: &mut TcpStream,
    req: &ParsedRequest,
    buf: &mut Vec<u8>,
) -> Result<BodyKind, ConnectionError> {
    // TODO: 추후 헤더 명 조회 시 입력 값을 하드 코딩으로 하는 부분 수정 고려
    // TODO: 추후 청크 인코딩의 경우도 구현

    let found_content_length = req.header.headers.iter().find(|h| h.0 == "Content-Length");
    let content_length = match found_content_length {
        Some(h) => h,
        None => return Ok(BodyKind::None),
    };
    let content_length_str = std::str::from_utf8(&content_length.1)?;
    let content_length: usize = content_length_str.parse()?;

    // 바디 길이가 content-length와 동일 해질때까지
    while (buf.len() - req.header.header_end) < content_length {
        let n = stream.read_buf(buf).await?;

        if n == 0 {
            eprintln!("Client closed the connection");
            return Err(ConnectionError::ClientClosed);
        }
    }

    Ok(BodyKind::ContentLength(content_length))
}

pub fn version_to_string(version: u8) -> String {
    match version {
        0 => return "1.0".to_string(),
        1 => return "1.1".to_string(),
        2 => return "2".to_string(),
        3 => return "3".to_string(),
        _ => return "1.1".to_string(),
    }
}
