use clap::Parser;

#[derive(Parser)]
pub struct CliArgs {
    #[arg(short, long)]
    pub name: String,

    #[arg(short, long)]
    pub port: u16,

    #[arg(short = 'd', long, default_value_t = 0)]
    pub delay_ms: usize,

    #[arg(short = 'f', long, default_value_t = 0.0)]
    pub fail_prob: f64,

    #[arg(short = 'b', long, default_value_t = 0.0)]
    pub burst_prob: f64,

    #[arg(short = 'B', long, default_value_t = 0.0)]
    pub burst_ms: f64,
}
