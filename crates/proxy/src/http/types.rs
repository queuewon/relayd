// TODO: 추후 문자열 슬라이스로 바꾸는 것 고려 필요.
pub struct HeaderParseResult {
    pub method: String,
    pub headers: Vec<(String, Vec<u8>)>,
    pub path: String,
    pub version: String,
    pub header_end: usize,
}

pub struct ParsedRequest {
    pub header: HeaderParseResult,
    pub body: Vec<u8>,
}
