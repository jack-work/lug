//! The HTTP transport: the same frames, without the length prefix.
//!
//! `POST /v1/call` carries one JSON `Request` and answers with one JSON
//! `Response`. `GET /v1/stream` is SSE, one `Response` per `data:` line. An
//! SSE body has no uplink, so a stream is steered by calls that name its
//! session in `X-Lug-Session`.

use crate::acl::Acl;
use crate::config::Limits;
use crate::registry::Logs;
use crate::session::Session;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response as HttpResponse};
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use lug_proto::{Code, Id, Mode, Request, Response, VERSION, Version, http};
use serde::Deserialize;
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct Api {
    pub logs: Arc<dyn Logs>,
    pub acl: Arc<Acl>,
    pub limits: Limits,
    pub uid: u32,
    token: Arc<String>,
    sessions: Arc<Mutex<HashMap<String, Arc<Session>>>>,
    next: Arc<AtomicU64>,
}

impl Api {
    pub fn new(
        logs: Arc<dyn Logs>,
        acl: Arc<Acl>,
        limits: Limits,
        uid: u32,
        token: String,
    ) -> Self {
        Self {
            logs,
            acl,
            limits,
            uid,
            token: Arc::new(token),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            next: Arc::new(AtomicU64::new(1)),
        }
    }

    fn authorized(&self, headers: &HeaderMap) -> bool {
        let Some(value) = headers.get(http::AUTH_HEADER).and_then(|v| v.to_str().ok()) else {
            return false;
        };
        let Some(offered) = value.strip_prefix("Bearer ").or_else(|| value.strip_prefix("bearer "))
        else {
            return false;
        };
        constant_time_eq(offered.as_bytes(), self.token.as_bytes())
    }

    fn session(&self, headers: &HeaderMap) -> Option<Arc<Session>> {
        let name = headers.get(http::SESSION_HEADER)?.to_str().ok()?;
        self.table().get(name).cloned()
    }

    fn table(&self) -> std::sync::MutexGuard<'_, HashMap<String, Arc<Session>>> {
        self.sessions.lock().unwrap_or_else(|e| e.into_inner())
    }
}

pub fn router(state: Api) -> Router {
    Router::new()
        .route(http::CALL, post(call))
        .route(http::STREAM, get(stream))
        .route(http::HEALTH, get(health))
        .with_state(state)
}

async fn health(State(state): State<Api>) -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "ok",
        "version": VERSION,
        "logs": state.logs.list().len(),
    }))
}

async fn call(
    State(state): State<Api>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<HttpResponse, HttpResponse> {
    if !state.authorized(&headers) {
        return Err(frame(
            StatusCode::UNAUTHORIZED,
            Response::Error { id: 0, code: Code::Unauthorized, message: "bad token".into() },
        ));
    }
    if body.len() as u32 > lug_proto::MAX_FRAME {
        return Err(frame(
            StatusCode::PAYLOAD_TOO_LARGE,
            Response::Error { id: 0, code: Code::Malformed, message: "body too large".into() },
        ));
    }
    let request: Request = serde_json::from_slice(&body).map_err(|e| {
        frame(
            StatusCode::BAD_REQUEST,
            Response::Error { id: 0, code: Code::Malformed, message: e.to_string() },
        )
    })?;
    let id = request.id();

    // Only Credit and Cancel are steered into an open stream. Everything
    // else is answered inline, session header or not.
    match &request {
        Request::Credit { grant, .. } => {
            let grant = *grant;
            let answer = match state.session(&headers) {
                Some(session) => session.grant(id, grant),
                None => Err(crate::actor::Failure::new(
                    Code::BadId,
                    format!("no session for stream {id}"),
                )),
            };
            return match answer {
                Ok(()) => Ok(frame(StatusCode::OK, Response::Ok { id })),
                Err(e) => Err(frame(
                    StatusCode::BAD_REQUEST,
                    Response::Error { id, code: e.code, message: e.message },
                )),
            };
        }
        Request::Cancel { .. } => {
            let answer = match state.session(&headers) {
                Some(session) => session.cancel(id),
                None => Err(crate::actor::Failure::new(
                    Code::BadId,
                    format!("no session for stream {id}"),
                )),
            };
            return match answer {
                Ok(()) => Ok(frame(StatusCode::OK, Response::End { id })),
                Err(e) => Err(frame(
                    StatusCode::BAD_REQUEST,
                    Response::Error { id, code: e.code, message: e.message },
                )),
            };
        }
        Request::Subscribe { .. } => {
            return Err(frame(
                StatusCode::BAD_REQUEST,
                Response::Error {
                    id,
                    code: Code::Malformed,
                    message: format!("subscribe over {}", http::STREAM),
                },
            ));
        }
        _ => {}
    }

    let (out, mut outbox) = mpsc::channel(state.limits.outbox);
    let session =
        Session::new(state.logs.clone(), out, state.uid, state.acl.clone(), state.limits, None);
    session.dispatch(request).await;
    match outbox.recv().await {
        Some(response) => Ok(frame(StatusCode::OK, response)),
        None => Err(frame(
            StatusCode::INTERNAL_SERVER_ERROR,
            Response::Error { id, code: Code::Internal, message: "no reply".into() },
        )),
    }
}

#[derive(Debug, Deserialize)]
struct Subscribe {
    id: Id,
    log: String,
    #[serde(default)]
    from: Version,
    #[serde(default)]
    mode: Mode,
    #[serde(default)]
    credit: u32,
}

async fn stream(
    State(state): State<Api>,
    headers: HeaderMap,
    Query(query): Query<Subscribe>,
) -> Result<HttpResponse, HttpResponse> {
    if !state.authorized(&headers) {
        return Err(frame(
            StatusCode::UNAUTHORIZED,
            Response::Error { id: query.id, code: Code::Unauthorized, message: "bad token".into() },
        ));
    }

    let name = format!("s{}", state.next.fetch_add(1, Ordering::Relaxed));
    let (out, outbox) = mpsc::channel(state.limits.outbox);
    let session = Session::new(
        state.logs.clone(),
        out,
        state.uid,
        state.acl.clone(),
        state.limits,
        Some(name.clone()),
    );
    state.table().insert(name.clone(), session.clone());

    // The Welcome is the first event so the client learns the session name
    // before it needs to grant credit.
    session
        .reply(Response::Welcome {
            id: query.id,
            version: VERSION,
            max_frame: lug_proto::MAX_FRAME,
            session: Some(name.clone()),
        })
        .await;
    session
        .dispatch(Request::Subscribe {
            id: query.id,
            log: query.log,
            from: query.from,
            mode: query.mode,
            credit: query.credit,
        })
        .await;

    let guard = Registered { name, sessions: state.sessions.clone(), session };
    let events = futures::stream::unfold((outbox, guard), |(mut outbox, guard)| async move {
        let response = outbox.recv().await?;
        let event = Event::default()
            .json_data(&response)
            .unwrap_or_else(|e| Event::default().comment(e.to_string()));
        Some((Ok::<_, Infallible>(event), (outbox, guard)))
    });
    Ok(Sse::new(events)
        .keep_alive(
            KeepAlive::new().interval(Duration::from_secs(http::KEEPALIVE_SECS)).text("lug"),
        )
        .into_response())
}

/// Ties the session's lifetime to the SSE body: when the client goes away,
/// the subscription and its table entry go with it.
struct Registered {
    name: String,
    sessions: Arc<Mutex<HashMap<String, Arc<Session>>>>,
    session: Arc<Session>,
}

impl Drop for Registered {
    fn drop(&mut self) {
        self.sessions.lock().unwrap_or_else(|e| e.into_inner()).remove(&self.name);
        self.session.close();
    }
}

fn frame(status: StatusCode, response: Response) -> HttpResponse {
    (status, Json(response)).into_response()
}

/// Token comparison that does not leak the matching prefix length.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}
