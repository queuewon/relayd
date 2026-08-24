// TODO: 추후 문자열 슬라이스로 바꾸는 것 고려 필요.
pub struct RequestHeaderParseResult {
    pub method: String,
    pub headers: Vec<(String, Vec<u8>)>,
    pub path: String,
    pub version: String,
    // 파싱한 buf 헤더 블록이 끝나는 지점(바이트 인덱스). buf[..header_end] 까지가 헤더 블록, buf[header_end..] -> 그 시점까지 도착한 바디
    // header_end + Content-Length = 응답 전체가 끝나는 목표 바이트 위치
    pub header_end: usize,
}

pub struct ParsedRequest {
    pub header: RequestHeaderParseResult,
    pub body: Vec<u8>,
}

pub struct ResponseHeaderParseResult {
    pub version: String,
    pub code: u16,
    pub headers: Vec<(String, Vec<u8>)>,
    // 파싱한 buf 헤더 블록이 끝나는 지점(바이트 인덱스). buf[..header_end] 까지가 헤더, buf[header_end..] -> 그 시점까지 도착한 바디
    // header_end + Content-Length = 응답 전체가 끝나는 목표 바이트 위치
    pub header_end: usize,
}
