use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, get, web};
use std::env;

struct Config {
    name: String,
}

#[get("/")]
async fn root(cfg: web::Data<Config>, req: HttpRequest) -> impl Responder {
    HttpResponse::Ok().body(format!("hello from {}", cfg.name))
}

#[get("/healthz")]
async fn healthz() -> impl Responder {
    HttpResponse::Ok().body("ok")
}

#[get("/delay")]
async fn slow(cfg: web::Data<Config>) -> impl Responder {
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    HttpResponse::Ok().body(format!("slow from {}", cfg.name))
}

#[get("/error")]
async fn error() -> impl Responder {
    HttpResponse::InternalServerError().body("boom")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    let name = args.get(1).cloned().unwrap_or_else(|| "A".into());
    let port: u16 = args.get(2).and_then(|p| p.parse().ok()).unwrap_or(8081);

    println!("backend {} listening on {}", name, port);

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(Config { name: name.clone() }))
            .service(root)
            .service(healthz)
            .service(slow)
            .service(error)
    })
    .bind(("127.0.0.1", port))?
    .run()
    .await
}
