use std::{sync::atomic::Ordering, time::Duration};

use reqwest::{Client, StatusCode};

use crate::{backend::Backend, balancer::Balancer};

pub struct HealthProbe {
    client: Client,
    backend: Backend,
    unhealthy_threshold: isize,
    healthy_threshold: isize,
}

pub fn start_health_checks(balancer: &Balancer) {
    let backends = balancer.healthy_targets();

    for backend in backends.into_iter() {
        tokio::spawn(async move {
            let timeout = Duration::from_secs(5);
            let client = reqwest::Client::builder().timeout(timeout).build().unwrap();

            let health_probe = HealthProbe {
                client,
                backend,
                unhealthy_threshold: 3,
                healthy_threshold: 3,
            };

            health_check_loop(health_probe).await;
        });
    }
}

// 현재는 reqwest 사용
pub async fn health_check_loop(health_probe: HealthProbe) {
    loop {
        let url = format!("http://{}/healthz", health_probe.backend.addr);
        let health = health_probe.client.get(&url).send().await;

        let resp = match health {
            Ok(r) => r,
            Err(e) => {
                let probe_streak = health_probe.backend.probe_streak.load(Ordering::Relaxed);

                if probe_streak >= 0 {
                    health_probe
                        .backend
                        .probe_streak
                        .store(-1, Ordering::Relaxed);
                } else {
                    health_probe
                        .backend
                        .probe_streak
                        .fetch_add(-1, Ordering::Relaxed);

                    let health = health_probe.backend.healthy.load(Ordering::Relaxed);
                    let fetched_probe_streak =
                        health_probe.backend.probe_streak.load(Ordering::Relaxed);

                    if health && (fetched_probe_streak <= -health_probe.unhealthy_threshold) {
                        health_probe.backend.healthy.store(false, Ordering::Relaxed);
                    }
                }
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
                let probe_streak = health_probe.backend.probe_streak.load(Ordering::Relaxed);

                if probe_streak <= 0 {
                    health_probe
                        .backend
                        .probe_streak
                        .store(1, Ordering::Relaxed);
                } else {
                    health_probe
                        .backend
                        .probe_streak
                        .fetch_add(1, Ordering::Relaxed);

                    let health = health_probe.backend.healthy.load(Ordering::Relaxed);
                    let fetched_probe_streak =
                        health_probe.backend.probe_streak.load(Ordering::Relaxed);

                    if !health && (fetched_probe_streak >= health_probe.healthy_threshold) {
                        health_probe.backend.healthy.store(true, Ordering::Relaxed);
                    }
                }
            }
            _ => {
                let probe_streak = health_probe.backend.probe_streak.load(Ordering::Relaxed);

                if probe_streak >= 0 {
                    health_probe
                        .backend
                        .probe_streak
                        .store(-1, Ordering::Relaxed);
                } else {
                    health_probe
                        .backend
                        .probe_streak
                        .fetch_add(-1, Ordering::Relaxed);

                    let health = health_probe.backend.healthy.load(Ordering::Relaxed);
                    let fetched_probe_streak =
                        health_probe.backend.probe_streak.load(Ordering::Relaxed);

                    if health && (fetched_probe_streak <= -health_probe.unhealthy_threshold) {
                        health_probe.backend.healthy.store(false, Ordering::Relaxed);
                    }
                }
            }
        }

        let sleep = Duration::from_secs(1);
        tokio::time::sleep(sleep).await;
    }
}

// 성공 시: counter가 0 이하였으면 1로 설정, 0보다 컸으면 +1. 그 후 healthy가 false이고 counter가 M 이상이면 healthy를 true로.

// 실패 시: counter가 0보다 컸으면 -1로 설정, 0 이하였으면 -1. 그 후 healthy가 true이고 counter가 -N 이하이면 healthy를 false로.
