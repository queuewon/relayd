pub mod error;
pub mod request;
pub mod response;
pub mod types;

pub fn version_to_string(version: u8) -> String {
    match version {
        0 => return "1.0".to_string(),
        1 => return "1.1".to_string(),
        2 => return "2".to_string(),
        3 => return "3".to_string(),
        _ => return "1.1".to_string(),
    }
}
