//! A blocking HTTP/1.1 client over hyper's connection API: one connection per
//! request, on one shared runtime. Redirects are never followed; status, headers and
//! bytes come back as received.

use std::collections::BTreeMap;
use std::sync::OnceLock;
use std::time::Duration;

use http_body_util::{BodyExt as _, Full};
use hyper::body::Bytes;
use hyper_util::rt::TokioIo;
use tokio::runtime::Runtime;

/// A response as received: header names lower-cased, a repeated header's values
/// joined with `, `.
#[derive(Debug)]
pub struct Response {
    pub status: u16,
    pub headers: BTreeMap<String, String>,
    pub content: Vec<u8>,
}

fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("the client runtime")
    })
}

/// Send one request to `127.0.0.1:port`. `target` is the path and query, already
/// encoded.
pub fn request(
    port: u16,
    method: &str,
    target: &str,
    headers: &[(&str, String)],
    body: Option<Vec<u8>>,
    timeout: Duration,
) -> Result<Response, String> {
    let mut builder = hyper::Request::builder()
        .method(method)
        .uri(target)
        .header(hyper::header::HOST, format!("127.0.0.1:{port}"));
    for (name, value) in headers {
        builder = builder.header(*name, value);
    }
    let request = builder
        .body(Full::new(Bytes::from(body.unwrap_or_default())))
        .map_err(|e| format!("{method} {target}: {e}"))?;
    let exchange = async move {
        let stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .map_err(|e| e.to_string())?;
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .map_err(|e| e.to_string())?;
        tokio::spawn(connection);
        let response = sender
            .send_request(request)
            .await
            .map_err(|e| e.to_string())?;
        let (parts, body) = response.into_parts();
        let content = body.collect().await.map_err(|e| e.to_string())?.to_bytes();
        let mut headers = BTreeMap::<String, String>::new();
        for (name, value) in &parts.headers {
            let value = String::from_utf8_lossy(value.as_bytes()).into_owned();
            headers
                .entry(name.as_str().to_owned())
                .and_modify(|joined| *joined = format!("{joined}, {value}"))
                .or_insert(value);
        }
        Ok(Response {
            status: parts.status.as_u16(),
            headers,
            content: content.to_vec(),
        })
    };
    runtime()
        .block_on(async { tokio::time::timeout(timeout, exchange).await })
        .map_err(|_| format!("{method} {target}: no response in {timeout:?}"))?
        .map_err(|e: String| format!("{method} {target}: {e}"))
}

/// Send `body` (if any) as JSON with `headers` and read the answer as JSON: the status
/// and the document.
pub fn json(
    port: u16,
    method: &str,
    target: &str,
    headers: &[(&str, String)],
    body: Option<&serde_json::Value>,
) -> Result<(u16, serde_json::Value), String> {
    let mut all = headers.to_vec();
    let body = body.map(|b| {
        all.push(("content-type", "application/json".to_owned()));
        serde_json::to_vec(b).expect("a JSON value serializes")
    });
    let response = request(port, method, target, &all, body, Duration::from_secs(60))?;
    let document = serde_json::from_slice(&response.content).map_err(|e| {
        format!(
            "{method} {target}: {} is not JSON ({e}): {}",
            response.status,
            String::from_utf8_lossy(&response.content)
        )
    })?;
    Ok((response.status, document))
}
