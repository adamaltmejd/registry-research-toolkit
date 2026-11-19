//! `reg-mock fetch` against a stub catalog on a local port (the network is the process
//! boundary). It must wait out the rate limit's 429, follow `next_cursor` to the last
//! page, and replace the answers saved before with exactly the layout `compile` reads
//! (DESIGN.md, "Compile"). A regression that gives up on a 429, saves only a first page,
//! names pages otherwise or leaves a stale page behind fails here.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::Path;
use std::process::Command;

use serde_json::{Value, json};

/// Each call the stub answers: (tool, arguments) → the document, as
/// `tests/fixtures/fetch_catalog.json` states them.
fn serve(answers: Vec<(Value, Value)>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a local port");
    let url = format!("http://{}/mcp", listener.local_addr().expect("bound"));
    std::thread::spawn(move || {
        let mut limited = false;
        for stream in listener.incoming() {
            let mut stream = stream.expect("a connection");
            let mut reader = BufReader::new(stream.try_clone().expect("a stream"));
            let mut length = 0;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).expect("a header");
                if line == "\r\n" {
                    break;
                }
                if let Some((name, value)) = line.split_once(':')
                    && name.eq_ignore_ascii_case("content-length")
                {
                    length = value.trim().parse().expect("a length");
                }
            }
            let mut body = vec![0; length];
            reader.read_exact(&mut body).expect("the body");
            let request: Value = serde_json::from_slice(&body).expect("JSON-RPC");
            let called = json!([request["params"]["name"], request["params"]["arguments"]]);
            let (status, reply) = if limited {
                let document = answers
                    .iter()
                    .find(|(call, _)| *call == called)
                    .map_or_else(
                        || panic!("unexpected call {called}"),
                        |(_, document)| document.clone(),
                    );
                let result = json!({"isError": false, "structuredContent": document});
                (
                    "200 OK",
                    json!({"jsonrpc": "2.0", "id": 1, "result": result}).to_string(),
                )
            } else {
                limited = true;
                ("429 Too Many Requests\r\nretry-after: 0", String::new())
            };
            write!(
                stream,
                "HTTP/1.1 {status}\r\ncontent-type: application/json\r\n\
                 content-length: {}\r\nconnection: close\r\n\r\n{reply}",
                reply.len()
            )
            .expect("a reply");
        }
    });
    url
}

#[test]
fn fetch_saves_every_page_after_a_rate_limit() {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fetch_catalog.json");
    let fixture: Value =
        serde_json::from_slice(&std::fs::read(fixture).expect("fixture")).expect("JSON");
    let mut expected = BTreeMap::new();
    let mut answers = Vec::new();
    for call in fixture["calls"].as_array().expect("calls") {
        let saved_as = call["saved_as"].as_str().expect("a file name");
        expected.insert(saved_as.to_owned(), call["document"].clone());
        answers.push((
            json!([call["tool"], call["arguments"]]),
            call["document"].clone(),
        ));
    }
    let url = serve(answers);

    let out = Path::new(env!("CARGO_TARGET_TMPDIR")).join("reg-mock-fetch");
    let _ = std::fs::remove_dir_all(&out);
    std::fs::create_dir_all(out.join("values")).expect("a directory");
    std::fs::write(out.join("values/stale-0001.json"), "{}").expect("a stale page");
    std::fs::write(out.join("manifest.json"), "{}").expect("a stale order answer");
    let project = out.with_extension("project.json");
    std::fs::write(&project, fixture["project"].to_string()).expect("the project");

    let run = Command::new(env!("CARGO_BIN_EXE_reg-mock"))
        .args(["fetch", "--catalog", &url, "--mode", "order", "--project"])
        .arg(&project)
        .arg("--out")
        .arg(&out)
        .output()
        .expect("runs");
    assert!(
        run.status.success(),
        "{}",
        String::from_utf8_lossy(&run.stderr)
    );

    let mut saved = BTreeMap::new();
    for dir in [out.clone(), out.join("segments"), out.join("values")] {
        for entry in std::fs::read_dir(&dir).expect("written") {
            let path = entry.expect("an entry").path();
            if path.is_file() {
                let name = path.strip_prefix(&out).expect("inside").to_string_lossy();
                let document: Value =
                    serde_json::from_slice(&std::fs::read(&path).expect("readable")).expect("JSON");
                saved.insert(name.replace('\\', "/"), document);
            }
        }
    }
    assert_eq!(saved, expected);
}

/// A steward behind Cloudflare Access: the call carries both Access headers only to the
/// host `CF_ACCESS_HOST` names, and a call that meets the login redirect is refused as
/// `access_required` without printing the token. A token for plain http to a remote host
/// (even one hidden behind a loopback prefix in the user-info) is refused before any
/// request. A regression that sends the token to another host, drops the headers,
/// follows the redirect, trusts the URL prefix or echoes the secret fails here.
#[test]
fn fetch_sends_access_headers_only_to_the_steward() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a local port");
    listener.set_nonblocking(true).expect("nonblocking");
    let url = format!("http://{}/mcp", listener.local_addr().expect("bound"));
    let seen = std::thread::spawn(move || {
        let mut seen = Vec::new();
        for _ in 0..3 {
            // A deadline, so a client that never connects fails the test instead of hanging it.
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(std::time::Instant::now() < deadline, "no connection");
                        std::thread::sleep(std::time::Duration::from_millis(20));
                    }
                    Err(e) => panic!("accept: {e}"),
                }
            };
            stream.set_nonblocking(false).expect("blocking");
            let mut reader = BufReader::new(stream.try_clone().expect("a stream"));
            let mut headers = String::new();
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).expect("a header");
                if line == "\r\n" {
                    break;
                }
                headers.push_str(&line.to_ascii_lowercase());
            }
            seen.push(headers);
            write!(
                stream,
                "HTTP/1.1 302 Found\r\nlocation: https://team.cloudflareaccess.com/login\r\n\
                 content-length: 0\r\nconnection: close\r\n\r\n"
            )
            .expect("a reply");
        }
        seen
    });
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("reg-mock-access");
    std::fs::create_dir_all(&dir).expect("a directory");
    let project = dir.join("project.json");
    std::fs::write(&project, "{}").expect("the project");
    let run = |catalog: &str, token_host: Option<&str>| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_reg-mock"));
        command
            .args([
                "fetch",
                "--catalog",
                catalog,
                "--mode",
                "order",
                "--project",
            ])
            .arg(&project)
            .arg("--out")
            .arg(&dir)
            .env_remove("CF_ACCESS_CLIENT_ID")
            .env_remove("CF_ACCESS_CLIENT_SECRET")
            .env_remove("CF_ACCESS_HOST");
        if let Some(host) = token_host {
            command
                .env("CF_ACCESS_CLIENT_ID", "id-123.access")
                .env("CF_ACCESS_CLIENT_SECRET", "s3cret-value")
                .env("CF_ACCESS_HOST", host);
        }
        command.output().expect("runs")
    };
    let without = run(&url, None);
    let other_host = run(&url, Some("steward.example"));
    let steward = run(&url, Some("127.0.0.1"));
    let userinfo = run("http://127.0.0.1@evil.example/mcp", Some("evil.example"));
    let seen = seen.join().expect("the stub");
    assert!(!seen[0].contains("cf-access-client"));
    assert!(!seen[1].contains("cf-access-client"));
    assert!(seen[2].contains("cf-access-client-id: id-123.access\r\n"));
    assert!(seen[2].contains("cf-access-client-secret: s3cret-value\r\n"));
    for run in [without, other_host, steward] {
        assert_eq!(run.status.code(), Some(2));
        let stderr = String::from_utf8_lossy(&run.stderr);
        assert!(stderr.contains("\"code\":\"access_required\""), "{stderr}");
        assert!(!stderr.contains("s3cret-value"));
    }
    assert_eq!(userinfo.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&userinfo.stderr);
    assert!(
        stderr.contains("\"code\":\"access_credentials\""),
        "{stderr}"
    );
    assert!(!stderr.contains("s3cret-value"));
}
