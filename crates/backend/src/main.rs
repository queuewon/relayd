use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, get, web};
use serde::Deserialize;
use std::{
    env,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

struct Config {
    name: String,
    delay_ms: usize,
    fail_requests: AtomicBool,
}

#[derive(Deserialize)]
struct SlowBodyReqQuery {
    duration: usize,
}

#[get("/")]
async fn root(cfg: web::Data<Config>, _req: HttpRequest) -> impl Responder {
    if cfg.delay_ms > 0 {
        let duration = Duration::from_millis(cfg.delay_ms as u64);
        tokio::time::sleep(duration).await;
    }
    let fail_request = cfg.fail_requests.load(Ordering::Relaxed);
    if fail_request {
        // 503
        return HttpResponse::ServiceUnavailable().body(format!("from {}", cfg.name));
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
#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();

    let name = args.get(1).cloned().unwrap_or_else(|| "A".into());
    let port: u16 = args.get(2).and_then(|p| p.parse().ok()).unwrap_or(8081);
    let delay_ms = args.get(3).and_then(|p| p.parse().ok()).unwrap_or(0);

    let config = Config {
        name,
        delay_ms,
        fail_requests: AtomicBool::new(false),
    };

    println!("backend {} listening on {}", config.name, port);

    let data = web::Data::new(config);

    HttpServer::new(move || {
        App::new()
            .app_data(data.clone())
            .service(root)
            .service(healthz)
            .service(fail_on)
            .service(fail_off)
            .service(slow)
            .service(slow_body)
            .service(error)
    })
    .bind(("127.0.0.1", port))?
    .run()
    .await
}
