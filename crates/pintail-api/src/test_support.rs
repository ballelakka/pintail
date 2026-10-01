//! A whole control plane in a temporary directory, for tests that need more
//! than one account, workspace or transport.

use std::time::Duration;

use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use http_body_util::BodyExt as _;
use serde_json::Value;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tower::ServiceExt as _;

use crate::{ApiState, auth::issue_token};

pub(crate) struct Node {
    _data: tempfile::TempDir,
    pub(crate) state: ApiState,
    pub(crate) app: axum::Router,
    /// The first administrator's session, in the first workspace.
    pub(crate) admin: String,
    pub(crate) admin_id: String,
    pub(crate) first_workspace: String,
    accounts: std::cell::Cell<u32>,
}

impl Node {
    /// A node whose first-boot setup has run.
    pub(crate) async fn new() -> Self {
        let data = tempfile::tempdir().expect("API data directory");
        let state = ApiState::new(
            data.path(),
            data.path().join("pintail-meta.db"),
            b"test-jwt-secret-with-enough-entropy",
            &"42".repeat(32),
        )
        .expect("configured API state");
        let app = crate::router_with_state(state.clone());
        let mut node = Self {
            _data: data,
            state,
            app,
            admin: String::new(),
            admin_id: String::new(),
            first_workspace: String::new(),
            accounts: std::cell::Cell::new(0),
        };
        let (status, setup) = node
            .call(
                "POST",
                "/api/auth/setup",
                None,
                Some(r#"{"email":"admin@example.com","password":"correct horse battery"}"#),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{setup}");
        node.admin = text(&setup["token"]);
        node.admin_id = text(&setup["user"]["id"]);
        node.first_workspace = text(&setup["user"]["workspace_id"]);
        node
    }

    pub(crate) fn metadata(&self) -> pintail_meta::MetaStore {
        self.state.metadata().expect("metadata")
    }

    pub(crate) async fn call(
        &self,
        method: &str,
        uri: &str,
        bearer: Option<&str>,
        body: Option<&str>,
    ) -> (StatusCode, Value) {
        let mut request = Request::builder().method(method).uri(uri);
        if let Some(bearer) = bearer {
            request = request.header(header::AUTHORIZATION, format!("Bearer {bearer}"));
        }
        let body = match body {
            Some(body) => {
                request = request.header(header::CONTENT_TYPE, "application/json");
                Body::from(body.to_owned())
            }
            None => Body::empty(),
        };
        let response = self
            .app
            .clone()
            .oneshot(request.body(body).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    /// A new account that belongs to one workspace with one role. Returns
    /// its id and a session in that workspace.
    pub(crate) fn member(&self, workspace_id: &str, role: &str) -> (String, String) {
        let number = self.accounts.get() + 1;
        self.accounts.set(number);
        let user_id = format!("usr_member{number}");
        let metadata = self.metadata();
        metadata
            .create_user(
                &user_id,
                &format!("member{number}@example.com"),
                "unused",
                role,
                "now",
            )
            .expect("account");
        metadata
            .add_workspace_member(workspace_id, &user_id, role, "now")
            .expect("membership");
        let token = issue_token(&self.state, &user_id, role, workspace_id).expect("session");
        (user_id, token)
    }

    /// Creates a workspace as `bearer`. Returns its id and the creator's
    /// session in it.
    pub(crate) async fn workspace(&self, bearer: &str, name: &str) -> (String, String) {
        let (status, created) = self
            .call(
                "POST",
                "/api/workspaces",
                Some(bearer),
                Some(&format!(r#"{{"name":"{name}"}}"#)),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{created}");
        (text(&created["workspace"]["id"]), text(&created["token"]))
    }

    /// Creates a database in the workspace `bearer` is in. Returns its id.
    pub(crate) async fn database(&self, bearer: &str, name: &str) -> String {
        let (status, created) = self
            .call(
                "POST",
                "/api/databases",
                Some(bearer),
                Some(&format!(
                    r#"{{"name":"{name}","dsn":"mysql://pintail:secret@db/{name}","mode":"auto"}}"#
                )),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{created}");
        text(&created["id"])
    }

    /// Issues an API key for one database. Returns its id and secret.
    pub(crate) async fn api_key(&self, bearer: &str, database_id: &str) -> (String, String) {
        let (status, created) = self
            .call(
                "POST",
                &format!("/api/databases/{database_id}/api-keys"),
                Some(bearer),
                Some(r#"{"name":"reader","scopes":["read","query"]}"#),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{created}");
        (text(&created["id"]), text(&created["secret"]))
    }
}

fn text(value: &Value) -> String {
    value.as_str().expect("a string").to_owned()
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Transport {
    Sse,
    WebSocket,
}

pub(crate) const TRANSPORTS: [Transport; 2] = [Transport::Sse, Transport::WebSocket];

/// What an open event stream did next.
#[derive(Debug, PartialEq)]
pub(crate) enum Next {
    Event(Value),
    /// The server ended the stream.
    Closed,
    /// Nothing arrived for a while and the stream is still open.
    Quiet,
}

/// How long a stream may say nothing before a test calls it quiet. Several
/// times the trust interval the streams re-read their standing on.
const QUIET: Duration = Duration::from_millis(900);

pub(crate) enum EventStream {
    Sse {
        body: Body,
        pending: String,
    },
    WebSocket {
        socket: tokio::net::TcpStream,
        server: tokio::task::JoinHandle<()>,
    },
}

impl Drop for EventStream {
    fn drop(&mut self) {
        if let Self::WebSocket { server, .. } = self {
            server.abort();
        }
    }
}

impl EventStream {
    /// Opens an event stream, or returns the status that refused it.
    pub(crate) async fn open(
        node: &Node,
        transport: Transport,
        bearer: Option<&str>,
    ) -> Result<Self, StatusCode> {
        match transport {
            Transport::Sse => {
                let mut request = Request::builder().uri("/api/events");
                if let Some(bearer) = bearer {
                    request = request.header(header::AUTHORIZATION, format!("Bearer {bearer}"));
                }
                let response = node
                    .app
                    .clone()
                    .oneshot(request.body(Body::empty()).unwrap())
                    .await
                    .unwrap();
                if response.status() != StatusCode::OK {
                    return Err(response.status());
                }
                Ok(Self::Sse {
                    body: response.into_body(),
                    pending: String::new(),
                })
            }
            Transport::WebSocket => {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                    .await
                    .expect("HTTP listener");
                let address = listener.local_addr().expect("HTTP address");
                let app = node.app.clone();
                let server = tokio::spawn(async move {
                    let _ = axum::serve(listener, app).await;
                });
                let mut socket = tokio::net::TcpStream::connect(address)
                    .await
                    .expect("connect");
                let authorization = bearer
                    .map(|bearer| format!("Authorization: Bearer {bearer}\r\n"))
                    .unwrap_or_default();
                let upgrade = format!(
                    "GET /api/ws HTTP/1.1\r\nHost: {address}\r\nConnection: Upgrade\r\n\
                     Upgrade: websocket\r\nSec-WebSocket-Version: 13\r\n\
                     Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n{authorization}\r\n"
                );
                socket.write_all(upgrade.as_bytes()).await.expect("upgrade");
                // The response head, byte by byte so no frame is swallowed
                // with it.
                let mut head = Vec::new();
                while !head.ends_with(b"\r\n\r\n") {
                    let mut byte = [0_u8; 1];
                    socket.read_exact(&mut byte).await.expect("response head");
                    head.push(byte[0]);
                }
                let status = std::str::from_utf8(&head)
                    .ok()
                    .and_then(|head| head.split(' ').nth(1))
                    .and_then(|code| code.parse::<u16>().ok())
                    .and_then(|code| StatusCode::from_u16(code).ok())
                    .expect("a status line");
                if status != StatusCode::SWITCHING_PROTOCOLS {
                    server.abort();
                    return Err(status);
                }
                Ok(Self::WebSocket { socket, server })
            }
        }
    }

    pub(crate) async fn next(&mut self) -> Next {
        match tokio::time::timeout(QUIET, self.read()).await {
            Ok(next) => next,
            Err(_) => Next::Quiet,
        }
    }

    /// Waits for the stream to end, failing if it delivers anything first.
    pub(crate) async fn expect_closed(&mut self) {
        let outcome = tokio::time::timeout(Duration::from_secs(10), self.read()).await;
        assert_eq!(outcome.ok(), Some(Next::Closed), "the stream should end");
    }

    async fn read(&mut self) -> Next {
        match self {
            Self::Sse { body, pending } => loop {
                if let Some(end) = pending.find("\n\n") {
                    let block: String = pending.drain(..end + 2).collect();
                    // A block with no data line is a keep-alive comment.
                    if let Some(json) = block.lines().find_map(|line| line.strip_prefix("data: ")) {
                        return Next::Event(serde_json::from_str(json).expect("event JSON"));
                    }
                    continue;
                }
                match body.frame().await {
                    Some(Ok(frame)) => {
                        if let Some(data) = frame.data_ref() {
                            pending.push_str(std::str::from_utf8(data).expect("UTF-8 frame"));
                        }
                    }
                    Some(Err(_)) | None => return Next::Closed,
                }
            },
            Self::WebSocket { socket, .. } => {
                let mut header = [0_u8; 2];
                if socket.read_exact(&mut header).await.is_err() {
                    return Next::Closed;
                }
                // Server frames are unmasked; 0x8 is a close, 0x1 is text.
                if header[0] & 0x0f == 0x8 {
                    return Next::Closed;
                }
                assert_eq!(header[0], 0x81, "a final text frame");
                let length = match header[1] {
                    126 => {
                        let mut extended = [0_u8; 2];
                        socket.read_exact(&mut extended).await.expect("length");
                        usize::from(u16::from_be_bytes(extended))
                    }
                    short => {
                        assert!(short < 126, "a frame this client can size");
                        usize::from(short)
                    }
                };
                let mut payload = vec![0_u8; length];
                socket
                    .read_exact(&mut payload)
                    .await
                    .expect("frame payload");
                Next::Event(serde_json::from_slice(&payload).expect("event JSON"))
            }
        }
    }
}
