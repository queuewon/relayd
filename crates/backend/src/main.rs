use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, get, web};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::{
    sync::atomic::{
        AtomicBool, AtomicU64,
        Ordering::{self},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::{
    cli_args::CliArgs,
    config::{Config, SerializedConfig},
    state::{SerializedState, State},
};

mod cli_args;
mod config;
mod state;

#[derive(Deserialize)]
struct SlowBodyReqQuery {
    duration: usize,
}

#[derive(Serialize)]
struct StatsResBody {
    pub cfg: SerializedConfig,
    pub state: SerializedState,
    pub args: CliArgs,
}

#[get("/")]
async fn root(
    cfg: web::Data<Config>,
    state: web::Data<State>,
    args: web::Data<CliArgs>,
    _req: HttpRequest,
) -> impl Responder {
    // 1. 실패 요청 확인
    let fail_request = cfg.fail_requests.load(Ordering::SeqCst);
    if fail_request {
        // 503
        return HttpResponse::ServiceUnavailable().body(format!("from {} reason=manual", cfg.name));
    }

    // 2. 버스트 진행 중인지 체크
    // 서버 시작 후 경과 시간
    let elapsed_ms = state.started_at.elapsed().as_millis() as u64;
    let burst_end_elapsed_ms = state.burst_end_elapsed_ms.load(Ordering::SeqCst);

    // 2.1. 버스트 중
    if elapsed_ms < burst_end_elapsed_ms {
        state.injected_failures.fetch_add(1, Ordering::SeqCst);
        return HttpResponse::ServiceUnavailable().body(format!("from {} reason=burst", cfg.name));
    }

    // 2.2. 새 버스트 시작
    // 2.2.1. 확률적으로 버스트 시작
    let start = rand::random_bool(args.burst_prob);
    if start {
        if state
            .burst_end_elapsed_ms
            .compare_exchange(
                burst_end_elapsed_ms,
                elapsed_ms + args.burst_ms,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_err()
        {
            state.injected_failures.fetch_add(1, Ordering::SeqCst);
            return HttpResponse::ServiceUnavailable()
                .body(format!("from {} reason=burst", cfg.name));
        }

        // 2.2.2. 로그 출력
        let end_elapsed_ms = elapsed_ms + args.burst_ms;
        let epoch_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before UNIX_EPOCH")
            .as_millis();
        let seq = state.bursts_started.fetch_add(1, Ordering::SeqCst) + 1;

        println!(
            "[inject] burst_start backend={} epoch_ms={} elapsed_ms={} duration_ms={} end_elapsed_ms={} seq={}",
            cfg.name, epoch_ms, elapsed_ms, args.burst_ms, end_elapsed_ms, seq
        );

        // 2.2.3. 실패 응답 횟수 증가
        state.injected_failures.fetch_add(1, Ordering::SeqCst);

        return HttpResponse::ServiceUnavailable()
            .body(format!("from {} reason=burst_start", cfg.name));
    }

    // 3. 독립 실패
    let isolated_start = rand::random_bool(args.fail_prob);
    if isolated_start {
        state.injected_failures.fetch_add(1, Ordering::SeqCst);
        return HttpResponse::ServiceUnavailable().body(format!("from {} reason=prob", cfg.name));
    }

    if cfg.delay_ms > 0 {
        let duration = Duration::from_millis(cfg.delay_ms as u64);
        tokio::time::sleep(duration).await;
    }

    HttpResponse::Ok().body(format!("hello from {}", cfg.name))
}

#[get("/healthz")]
async fn healthz() -> impl Responder {
    HttpResponse::Ok().body("ok")
}

#[get("/fail-on")]
async fn fail_on(cfg: web::Data<Config>) -> impl Responder {
    cfg.fail_requests.store(true, Ordering::Relaxed);
    let epoch_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX_EPOCH")
        .as_millis();
    println!(
        "[inject] fail_on backend={} epoch_ms={}",
        cfg.name, epoch_ms
    );
    HttpResponse::Ok().body(format!("fail on: {}", cfg.name))
}

#[get("/fail-off")]
async fn fail_off(cfg: web::Data<Config>) -> impl Responder {
    cfg.fail_requests.store(false, Ordering::Relaxed);
    HttpResponse::Ok().body(format!("fail off {}", cfg.name))
}

#[get("/delay")]
async fn slow(cfg: web::Data<Config>) -> impl Responder {
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    HttpResponse::Ok().body(format!("slow from {}", cfg.name))
}

#[get("/error")]
async fn error() -> impl Responder {
    HttpResponse::InternalServerError().body("error")
}

#[get("/slow-body")]
async fn slow_body(cfg: web::Data<Config>, query: web::Query<SlowBodyReqQuery>) -> impl Responder {
    let name = cfg.name.clone();
    let per_chunk = Duration::from_millis(query.duration as u64 * 1000 / 10);

    let s = futures::stream::unfold(0usize, move |i| {
        let name = name.clone();
        async move {
            if i >= 10 {
                return None;
            }
            tokio::time::sleep(per_chunk).await;
            Some((
                Ok::<_, actix_web::Error>(web::Bytes::from(format!("chunk{i} from {name}\n"))),
                i + 1,
            ))
        }
    });

    HttpResponse::Ok().streaming(s)
}

#[get("/stats")]
async fn stats(
    cfg: web::Data<Config>,
    state: web::Data<State>,
    args: web::Data<CliArgs>,
) -> impl Responder {
    // 1. Config 필요 필드 변환
    let fail_requests = cfg.fail_requests.load(Ordering::Relaxed);

    // 2. State  필요 필드 변환
    let uptime_ms = state.started_at.elapsed().as_millis() as u64;
    let burst_end_elapsed_ms = state.burst_end_elapsed_ms.load(Ordering::Relaxed);
    let bursts_started = state.bursts_started.load(Ordering::Relaxed);
    let injected_failures = state.injected_failures.load(Ordering::Relaxed);

    // 4. 응답 값 직렬화
    let res = StatsResBody {
        cfg: SerializedConfig {
            name: cfg.name.clone(),
            delay_ms: cfg.delay_ms,
            fail_requests,
        },
        state: SerializedState {
            uptime_ms,
            burst_end_elapsed_ms,
            bursts_started,
            injected_failures,
        },
        args: args.get_ref().clone(),
    };

    HttpResponse::Ok().json(res)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args = CliArgs::parse();
    // 버스트가 발동한 경우 지속시간 0이 되는 경우 방지
    if args.burst_prob > 0.0 && args.burst_ms == 0 {
        panic!("burst_prob가 양수로 지정된 경우, burst_ms는 0 이상이어야 합니다.");
    }

    let config = Config {
        name: args.name.clone(),
        delay_ms: args.delay_ms.clone(),
        fail_requests: AtomicBool::new(false),
    };

    let state = State {
        started_at: Instant::now(),
        burst_end_elapsed_ms: AtomicU64::new(0),
        bursts_started: AtomicU64::new(0),
        injected_failures: AtomicU64::new(0),
    };

    println!("backend {} listening on {}", config.name, args.port);

    let config_data = web::Data::new(config);
    let state_data = web::Data::new(state);
    let args_data = web::Data::new(args.clone());

    HttpServer::new(move || {
        App::new()
            .app_data(config_data.clone())
            .app_data(state_data.clone())
            .app_data(args_data.clone())
            .service(root)
            .service(healthz)
            .service(fail_on)
            .service(fail_off)
            .service(slow)
            .service(slow_body)
            .service(error)
            .service(stats)
    })
    .bind(("127.0.0.1", args.port))?
    .run()
    .await
}
