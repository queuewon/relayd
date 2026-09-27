use clap::Parser;
use serde::Serialize;

#[derive(Parser, Clone, Debug, Serialize)]
pub struct CliArgs {
    #[arg(short, long)]
    pub name: String,

    #[arg(short, long)]
    pub port: u16,

    #[arg(short = 'd', long, default_value_t = 0)]
    pub delay_ms: usize,

    #[arg(short = 'f', long, default_value_t = 0.0, value_parser = probability_in_range)]
    pub fail_prob: f64, // 독립 실패 확률

    #[arg(short = 'b', long, default_value_t = 0.0, value_parser = probability_in_range)]
    pub burst_prob: f64, // 버스트 확률

    #[arg(short = 'B', long, default_value_t = 0)]
    pub burst_ms: u64, // 버스트 지속 시간
}

fn probability_in_range(p: &str) -> Result<f64, String> {
    let value: f64 = p
        .parse()
        .map_err(|_| "확률은 f64 값이어야 합니다.".to_string())?;

    if !(0.0..=1.0).contains(&value) {
        return Err("확률은 0.0 ~ 1.0 사이여야 합니다.".to_string());
    }

    Ok(value)
}
