//! The write limits of `serve`: `/mcp` and every POST operation and download sit
//! behind [`guard`], one token bucket per client address shared by all of them, then
//! the body cap.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::{ConnectInfo, FromRequest, Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use reg_catalog::ops::{Server, body};
use reg_catalog::{Code, Error};
use sha2::{Digest, Sha256};

use crate::Answer;

/// Requests per client address to `/mcp` and the POST routes together: a token bucket
/// of this many tokens, refilled one per second. Sized for agent tool calls; the SPA's
/// debounced validation and its order downloads stay well inside it.
const RATE_PER_MINUTE: u64 = 60;
/// simplify: buckets that have refilled are dropped only once this many addresses are
/// tracked; make it a time-ordered sweep if a hosted burst of addresses shows in RSS
/// or in write latency.
const MAX_TRACKED: usize = 10_000;
/// The secret the edge worker sends as [`EDGE_TOKEN_HEADER`] on every origin request.
/// A request carrying it came through the edge, so its `CF-Connecting-IP` is the
/// client's address; any other request is keyed on its peer address.
const EDGE_TOKEN_ENV: &str = "REG_META_EDGE_TOKEN";
const EDGE_TOKEN_HEADER: &str = "x-edge-token";

pub struct Limits {
    server: Arc<Server>,
    /// The SHA-256 of the edge token; `None` trusts no request as the edge's.
    edge_token: Option<[u8; 32]>,
    buckets: Mutex<HashMap<IpAddr, Bucket>>,
}

struct Bucket {
    tokens: u64,
    refilled: Instant,
}

impl Bucket {
    /// Add a token per whole second since the last refill, up to the capacity.
    fn refill(&mut self, now: Instant) -> &mut Self {
        let seconds = now.duration_since(self.refilled).as_secs();
        self.tokens = (self.tokens + seconds).min(RATE_PER_MINUTE);
        self.refilled += Duration::from_secs(seconds);
        self
    }
}

impl Limits {
    /// The limits of one server, with the edge token from [`EDGE_TOKEN_ENV`].
    pub fn new(server: Arc<Server>) -> Arc<Self> {
        Arc::new(Self {
            server,
            edge_token: std::env::var(EDGE_TOKEN_ENV)
                .ok()
                .filter(|token| !token.is_empty())
                .map(|token| Sha256::digest(token).into()),
            buckets: Mutex::default(),
        })
    }

    /// The address a request is limited by: the edge-supplied `CF-Connecting-IP` when
    /// the request carries the edge token, otherwise its peer; an IPv6 address as its
    /// /64.
    fn client(&self, headers: &HeaderMap, peer: IpAddr) -> IpAddr {
        let header = |name| headers.get(name).and_then(|value| value.to_str().ok());
        let from_edge = self.edge_token.is_some_and(|token| {
            header(EDGE_TOKEN_HEADER).is_some_and(|sent| {
                // Equal digests, compared without an early exit.
                let sent: [u8; 32] = Sha256::digest(sent).into();
                let diff = sent
                    .iter()
                    .zip(token)
                    .fold(0, |diff, (a, b)| diff | (a ^ b));
                diff == 0
            })
        });
        let client = from_edge
            .then(|| header("cf-connecting-ip")?.parse().ok())
            .flatten()
            .unwrap_or(peer);
        // One host holds a whole IPv6 /64, so it is one client: keyed on the full
        // address, it could rotate through fresh buckets.
        match client.to_canonical() {
            IpAddr::V6(v6) => IpAddr::V6((u128::from(v6) & !u128::from(u64::MAX)).into()),
            v4 @ IpAddr::V4(_) => v4,
        }
    }

    /// Take a token from `client`'s bucket; false when it is empty.
    fn allow(&self, client: IpAddr) -> bool {
        let now = Instant::now();
        let mut buckets = self.buckets.lock().expect("rate buckets");
        if buckets.len() >= MAX_TRACKED {
            // A full bucket is the same as none. simplify: while this many addresses
            // are active, every request sweeps them all under the lock; replace the
            // sweep (see MAX_TRACKED) if it shows in write latency.
            buckets.retain(|_, bucket| bucket.refill(now).tokens < RATE_PER_MINUTE);
        }
        let bucket = buckets
            .entry(client)
            .or_insert(Bucket {
                tokens: RATE_PER_MINUTE,
                refilled: now,
            })
            .refill(now);
        let allowed = bucket.tokens > 0;
        bucket.tokens = bucket.tokens.saturating_sub(1);
        allowed
    }

    /// `err` as `{error, meta}` with its status.
    fn refuse(&self, err: Error) -> Response {
        let answer = Answer::new(&self.server, self.server.catalog.default_scope(), Err(err));
        (answer.status, crate::json(answer.body.to_string())).into_response()
    }
}

/// The write limits, answered with the error document: `rate_limited` per client
/// address ([`Limits::client`]), then `payload_too_large` over [`body::MAX_BYTES`]
/// (read through the `DefaultBodyLimit` layered outside this one, which the MCP
/// service itself does not consult).
pub async fn guard(
    State(limits): State<Arc<Limits>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
    next: Next,
) -> Response {
    if !limits.allow(limits.client(request.headers(), peer.ip())) {
        let err = Error::new(
            Code::RateLimited,
            format!("More than {RATE_PER_MINUTE} write requests a minute from this address."),
            vec![1.into()],
        );
        return ([(header::RETRY_AFTER, "1")], limits.refuse(err)).into_response();
    }
    let (parts, body) = request.into_parts();
    match Bytes::from_request(Request::from_parts(parts.clone(), body), &()).await {
        Ok(bytes) => next.run(Request::from_parts(parts, bytes.into())).await,
        Err(rejection) if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE => {
            limits.refuse(body::too_large())
        }
        Err(rejection) => rejection.into_response(),
    }
}
