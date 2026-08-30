use std::time::Duration;

use reqwest::{Client, StatusCode};

use crate::{backend::Backend, balancer::Balancer};

pub struct HealthProbe {
    client: Client,
    backend: Backend,
}

// 현재는 reqwest 사용
pub async fn health_check_loop(health_probe: HealthProbe) {
    loop {
        let url = format!("http://{}/healthz", health_probe.backend.addr);
        let health = health_probe.client.get(&url).send().await;

        let resp = match health {
            Ok(r) => r,
            Err(e) => {
                health_probe.backend.note_probe_result(false);

                println!(
                    "{} 주소 헬스체크 오류 | {e:#?}",
                    health_probe.backend.addr.to_string()
                );

                let sleep = Duration::from_secs(1);
                tokio::time::sleep(sleep).await;

                continue;
            }
        };

        match resp.status() {
            StatusCode::OK => {
                health_probe.backend.note_probe_result(true);
            }
            _ => {
                health_probe.backend.note_probe_result(false);
            }
        }

        let sleep = Duration::from_secs(1);
        tokio::time::sleep(sleep).await;
    }
}

// 성공 시: counter가 0 이하였으면 1로 설정, 0보다 컸으면 +1. 그 후 healthy가 false이고 counter가 M 이상이면 healthy를 true로.
// 실패 시: counter가 0보다 컸으면 -1로 설정, 0 이하였으면 -1. 그 후 healthy가 true이고 counter가 -N 이하이면 healthy를 false로.

pub fn start_health_checks(balancer: &Balancer) {
    let backends = balancer.all_backends();

    for backend in backends.into_iter() {
        tokio::spawn(async move {
            let timeout = Duration::from_secs(5);
            let client = reqwest::Client::builder().timeout(timeout).build().unwrap();

            let health_probe = HealthProbe { client, backend };

            health_check_loop(health_probe).await;
        });
    }
}
