//! The origin lock and the write limits of `serve`. With the edge token set, every route
//! but `/healthz` sits behind [`edge`], which refuses a request without it. `/mcp` and
//! every POST operation and download then sit behind [`guard`], one token bucket per
//! client address shared by all of them, then the body cap.

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

/// Requests per client address to `/mcp` and the POST routes together: a token bucket
/// of this many tokens, refilled one per second. Sized for agent tool calls; the SPA's
/// debounced validation and its order downloads stay well inside it. `serve
/// --write-limit` raises it for verification runs, which replay many projects from one
/// address.
pub const DEFAULT_PER_MINUTE: u64 = 60;
/// simplify: buckets that have refilled are dropped only once this many addresses are
/// tracked, and while that many are active every request sweeps them all under the
/// lock; make it a time-ordered sweep if a hosted burst of addresses shows in RSS or in
/// write latency.
const MAX_TRACKED: usize = 10_000;
/// The secret the edge worker sends as [`EDGE_TOKEN_HEADER`] on every origin request.
/// When it is set, [`edge`] refuses any request without it, so every request that
/// reaches [`guard`] came through the edge and its `CF-Connecting-IP` is the client's
/// address. When it is unset, every request is keyed on its peer address.
const EDGE_TOKEN_ENV: &str = "REG_META_EDGE_TOKEN";
const EDGE_TOKEN_HEADER: &str = "x-edge-token";

pub struct Limits {
    server: Arc<Server>,
    /// Each bucket's capacity.
    capacity: u64,
    /// The SHA-256 of the edge token; `None` locks nothing and trusts no request as the
    /// edge's.
    edge_token: Option<[u8; 32]>,
    buckets: Mutex<HashMap<IpAddr, Bucket>>,
}

struct Bucket {
    tokens: u64,
    refilled: Instant,
}

impl Bucket {
    /// Add a token per whole second since the last refill, up to `capacity`.
    fn refill(&mut self, now: Instant, capacity: u64) -> &mut Self {
        let seconds = now.duration_since(self.refilled).as_secs();
        self.tokens = self.tokens.saturating_add(seconds).min(capacity);
        self.refilled += Duration::from_secs(seconds);
        self
    }
}

impl Limits {
    /// The limits of one server: buckets of `capacity` tokens, and the edge token from
    /// [`EDGE_TOKEN_ENV`].
    pub fn new(server: Arc<Server>, capacity: u64) -> Arc<Self> {
        Arc::new(Self {
            server,
            capacity,
            edge_token: std::env::var(EDGE_TOKEN_ENV)
                .ok()
                .filter(|token| !token.is_empty())
                .map(|token| Sha256::digest(token).into()),
            buckets: Mutex::default(),
        })
    }

    /// Whether `headers` carry the edge token; true when none is set.
    fn admits(&self, headers: &HeaderMap) -> bool {
        self.edge_token.is_none_or(|token| {
            headers.get(EDGE_TOKEN_HEADER).is_some_and(|sent| {
                // Equal digests, compared without an early exit.
                let sent: [u8; 32] = Sha256::digest(sent.as_bytes()).into();
                let diff = sent
                    .iter()
                    .zip(token)
                    .fold(0, |diff, (a, b)| diff | (a ^ b));
                diff == 0
            })
        })
    }

    /// The address a request is limited by: the edge-supplied `CF-Connecting-IP` when an
    /// edge token is set ([`edge`] admits only requests that carry it), otherwise its
    /// peer; an IPv6 address as its /64.
    fn client(&self, headers: &HeaderMap, peer: IpAddr) -> IpAddr {
        let header = |name| headers.get(name).and_then(|value| value.to_str().ok());
        let client = self
            .edge_token
            .is_some()
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
            // A full bucket is the same as none. The sweep's ceiling: see MAX_TRACKED.
            let capacity = self.capacity;
            buckets.retain(|_, bucket| bucket.refill(now, capacity).tokens < capacity);
        }
        let bucket = buckets
            .entry(client)
            .or_insert(Bucket {
                tokens: self.capacity,
                refilled: now,
            })
            .refill(now, self.capacity);
        let allowed = bucket.tokens > 0;
        bucket.tokens = bucket.tokens.saturating_sub(1);
        allowed
    }
}

/// The origin lock: with the edge token set, `edge_token_required` (403) for a request
/// that does not carry it. Layered over every route but `/healthz`, outside [`guard`],
/// so a refused request never takes a token from a bucket.
pub async fn edge(State(limits): State<Arc<Limits>>, request: Request, next: Next) -> Response {
    if limits.admits(request.headers()) {
        return next.run(request).await;
    }
    let err = Error::new(
        Code::EdgeTokenRequired,
        "This request did not come through the public host.",
        vec![],
    );
    crate::refusal(&limits.server, err)
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
            format!(
                "More than {} write requests a minute from this address.",
                limits.capacity
            ),
            vec![1.into()],
        );
        return (
            [(header::RETRY_AFTER, "1")],
            crate::refusal(&limits.server, err),
        )
            .into_response();
    }
    let (parts, body) = request.into_parts();
    match Bytes::from_request(Request::from_parts(parts.clone(), body), &()).await {
        Ok(bytes) => next.run(Request::from_parts(parts, bytes.into())).await,
        Err(rejection) if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE => {
            crate::refusal(&limits.server, body::too_large())
        }
        Err(rejection) => rejection.into_response(),
    }
}
