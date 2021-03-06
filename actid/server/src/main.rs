use serde::{Deserialize, Serialize};

use std::collections::HashMap;
use std::io;

use actix_web::{
    client::Client,
    error::ErrorBadRequest,
    web::{self, BytesMut},
    App, Error, HttpResponse, HttpServer,
};
use futures::StreamExt;
use validator::Validate;
use validator_derive::Validate;

#[derive(Debug, Validate, Deserialize, Serialize)]
struct SomeData {
    #[validate(range(min = 0, max = 70000000))]
    uid: u32,
    #[validate(length(min = 1, max = 100))]
    name: String,
}

#[derive(Debug, Validate, Deserialize, Serialize)]
struct MxRequest {
    #[validate(range(min = 0, max = 700000000))]
    uid: u32
}

#[derive(Debug, Deserialize)]
struct HttpBinResponse {
    args: HashMap<String, String>,
    data: String,
    files: HashMap<String, String>,
    form: HashMap<String, String>,
    headers: HashMap<String, String>,
    json: SomeData,
    origin: String,
    url: String,
}

/// validate data, post json to httpbin, get it back in the response body, return deserialized
async fn step_x(data: SomeData, client: &Client) -> Result<SomeData, Error> {
    // validate data
    data.validate().map_err(ErrorBadRequest)?;

    let mut res = client
        .post("https://httpbin.org/post")
        .send_json(&data)
        .await
        .map_err(Error::from)?; // <- convert SendRequestError to an Error

    let mut body = BytesMut::new();
    while let Some(chunk) = res.next().await {
        body.extend_from_slice(&chunk?);
    }

    let body: HttpBinResponse = serde_json::from_slice(&body).unwrap();
    Ok(body.json)
}

async fn create_something(
    some_data: web::Json<SomeData>,
    client: web::Data<Client>,
) -> Result<HttpResponse, Error> {
    println!("{:?}", some_data);
    let some_data_2 = step_x(some_data.into_inner(), &client).await?;
    let some_data_3 = step_x(some_data_2, &client).await?;
    let d = step_x(some_data_3, &client).await?;

    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(serde_json::to_string(&d).unwrap()))
}

async fn do_mx(
    data: String,
    client: web::Data<Client>,
) -> Result<HttpResponse, Error> {

    let res = serde_json::from_str::<MxRequest>(data.as_str());
    match res {
        Err(e) => {
            println!("{}", e);
            Ok(HttpResponse::Ok()
                .content_type("application/json")
                .body(format!("{{\"error\":\"{}\"}}","invalid request")))
        }
        Ok(req) => {
            Ok(HttpResponse::Ok()
                .content_type("application/json")
                .body(format!("{{\"uid\":{},\"phone\":\"{}\"}}",req.uid, req.uid)))
        }
    }
}

#[actix_rt::main]
async fn main() -> io::Result<()> {
    std::env::set_var("RUST_LOG", "actix_web=trace");
    env_logger::init();
    let endpoint = "127.0.0.1:8080";

    println!("Starting server at: {:?}", endpoint);
    HttpServer::new(|| {
        App::new()
            .data(Client::default())
            .service(web::resource("/something").route(web::post().to(create_something)))
            .service(web::resource("/mx").route(web::post().to(do_mx)))
    })
    .bind(endpoint)?
    .run()
    .await
}