pub mod round_robin;

#[derive(Debug)]
pub enum BalancerError {
    NoBackendAvailable,
}
