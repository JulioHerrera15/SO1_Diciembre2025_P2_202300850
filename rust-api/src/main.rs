use actix_web::{post, web, App, HttpResponse, HttpServer, Responder};
use serde::{Deserialize, Serialize};
use reqwest::Client;

// Estructura de entrada (Locust)
#[derive(Deserialize, Serialize, Clone)]
struct VentaInput {
    categoria: i32,
    producto_id: String,
    precio: f64,
    cantidad_vendida: i32,
}

// Estructura para compartir el cliente HTTP
struct AppState {
    client: Client,
    go_url: String,
}

#[post("/venta")]
async fn registrar_venta(
    info: web::Json<VentaInput>,
    data: web::Data<AppState>,
) -> impl Responder {
    println!("📦 Rust recibió: {}", info.producto_id);

    // Enviar a Go (Deployment 1)
    // Usamos la URL configurada en el estado (localhost:3000 por ahora)
    let url = format!("{}/procesar", data.go_url);

    let res = data.client.post(&url)
        .json(&info.into_inner())
        .send()
        .await;

    match res {
        Ok(response) => {
            if response.status().is_success() {
                HttpResponse::Ok().body("Procesado por Go correctamente")
            } else {
                HttpResponse::InternalServerError().body("Go devolvió error")
            }
        }
        Err(e) => {
            println!("❌ Error contactando a Go: {}", e);
            HttpResponse::InternalServerError().body("Error de comunicación")
        }
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("🚀 API Rust corriendo en puerto 8080");

    // Configuración: URL del servicio de Go
    // En local es localhost:3000. En K8s será una variable de entorno.
    let go_service_url = std::env::var("GO_SERVICE_URL")
        .unwrap_or_else(|_| "http://localhost:3000".to_string());

    let state = web::Data::new(AppState {
        client: Client::new(),
        go_url: go_service_url,
    });

    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .service(registrar_venta)
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}