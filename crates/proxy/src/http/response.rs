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
