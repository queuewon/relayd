use std::{io, num::ParseIntError, str::Utf8Error};

#[derive(Debug)]
pub enum TimeoutKind {
    ResponseHeader, // 요청 전송 후 응답 헤더가 온전할 때까지
    Overall,        // 요청에서 응답까지
}

#[derive(Debug)]
pub enum PoolError {
    AcquireTimeout, // 풀에서 permit을 timeout 안에 못 얻음ㄴ
}

#[derive(Debug)]
pub enum ConnectionError {
    Io(io::Error),            // read 자체가 실패한 경우, 소켓이 죽음
    ClientClosed,             // 파싱 완료 전 EOF
    MalformedRequest(String), // 파싱 실패 — 400 대상
    AllBackendsUnreachable,   // 연결 실패 - 모든 백엔드 순회 후 실패
    NoBackendAvailable,       // 백엔드 목록 없음
    PoolAcquireTimeout,       // 풀에서 permit을 timeout 안에 못 얻음

    MalformedResponse(String),
    BackendClosedBeforeResponse, // 응답 헤더 받기 전 백엔드 연결이 끊어짐.
    BackendClosedMidResponse,    // 헤더, 바디 일부를 클라이언트에 write 후 백엔드가 연결이 끊어짐.

    BackendTimeout(TimeoutKind), // 백엔드로부터 제때 응답을 받지 못해 시간이 초과됨.
}
impl From<io::Error> for ConnectionError {
    fn from(e: io::Error) -> Self {
        ConnectionError::Io(e)
    }
}
impl From<std::num::ParseIntError> for ConnectionError {
    fn from(e: ParseIntError) -> Self {
        ConnectionError::MalformedRequest(e.to_string())
    }
}
impl From<std::str::Utf8Error> for ConnectionError {
    fn from(e: Utf8Error) -> Self {
        ConnectionError::MalformedRequest(e.to_string())
    }
}
impl From<PoolError> for ConnectionError {
    fn from(e: PoolError) -> Self {
        match e {
            PoolError::AcquireTimeout => ConnectionError::PoolAcquireTimeout,
        }
    }
}

#[derive(Eq, PartialEq)]
pub enum BodyKind {
    None,                 // Content-Length도 chunked도 없음 — 바디 없음
    ContentLength(usize), // 정해진 길이만큼 읽기
    Chunked,              // 청크 단위로 읽기 (아직은 미구현)
}
