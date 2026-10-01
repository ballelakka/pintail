use std::{
    collections::HashMap,
    convert::Infallible,
    time::{Duration, Instant},
};

use axum::{
    Extension,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::{
        IntoResponse as _, Response,
        sse::{Event, KeepAlive, Sse},
    },
};
use chrono::Utc;
use futures_util::{Stream, stream};
use serde::Serialize;

use crate::{ApiState, auth::AuthPrincipal, error::ApiError};

/// One live control-plane event.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct ApiEvent {
    pub(crate) kind: String,
    pub(crate) database_id: Option<String>,
    pub(crate) table: Option<String>,
    pub(crate) message: String,
    pub(crate) rows: Option<u64>,
    pub(crate) bytes: Option<u64>,
    pub(crate) eta_seconds: Option<u64>,
    pub(crate) at: String,
}

impl ApiEvent {
    pub(crate) fn database(kind: &str, database_id: &str, message: impl Into<String>) -> Self {
        Self {
            kind: kind.to_owned(),
            database_id: Some(database_id.to_owned()),
            table: None,
            message: message.into(),
            rows: None,
            bytes: None,
            eta_seconds: None,
            at: Utc::now().to_rfc3339(),
        }
    }
}

/// How long a stream trusts what it last read about one database's owner.
///
/// Progress events arrive every replication cycle and each one would
/// otherwise cost a metadata read per open stream. Short enough that a
/// database handed to a workspace (a restore assigns its owner after
/// creating it) starts appearing within seconds.
const OWNERSHIP_TTL: Duration = Duration::from_secs(5);

/// Which events one stream may carry.
///
/// Every subscriber reads the same node-wide broadcast, so the stream itself
/// is the only place a caller's reach can be applied. A database identifier,
/// a table name and a replication message all describe somebody's source;
/// a dashboard session sees those of its own workspace's databases and an API
/// key those of the one database it was issued for.
enum EventScope {
    /// An API key: the one database it is scoped to.
    Database(String),
    /// A dashboard session: the databases its workspace owns.
    Workspace {
        state: ApiState,
        workspace_id: String,
        /// Database id to whether this workspace owns it, and when that was
        /// read.
        owned: HashMap<String, (bool, Instant)>,
    },
}

impl EventScope {
    fn of(principal: &AuthPrincipal, state: &ApiState) -> Result<Self, ApiError> {
        if let Some(database_id) = principal.database_scope() {
            return Ok(Self::Database(database_id.to_owned()));
        }
        Ok(Self::Workspace {
            state: state.clone(),
            workspace_id: principal.require_workspace()?.to_owned(),
            owned: HashMap::new(),
        })
    }

    fn admits(&mut self, event: &ApiEvent) -> bool {
        match self {
            Self::Database(allowed) => event.database_id.as_deref() == Some(allowed.as_str()),
            Self::Workspace {
                state,
                workspace_id,
                owned,
            } => {
                let Some(database_id) = event.database_id.as_deref() else {
                    // Nothing ties this event to a workspace, so it passes
                    // only when it names no table either.
                    return event.table.is_none();
                };
                if let Some((verdict, read_at)) = owned.get(database_id)
                    && read_at.elapsed() < OWNERSHIP_TTL
                {
                    return *verdict;
                }
                // A failed read hides the event: not knowing who owns a
                // database is no reason to show it.
                let verdict = state
                    .metadata()
                    .ok()
                    .and_then(|metadata| {
                        metadata
                            .database_in_workspace(database_id, workspace_id)
                            .ok()
                    })
                    .is_some_and(|database| database.is_some());
                // Identifiers are never reused, so entries only accumulate
                // with databases; drop the stale ones before the map can
                // outgrow the node's database list by much.
                if owned.len() >= 256 {
                    owned.retain(|_, (_, read_at)| read_at.elapsed() < OWNERSHIP_TTL);
                }
                owned.insert(database_id.to_owned(), (verdict, Instant::now()));
                verdict
            }
        }
    }
}

pub(crate) async fn sse(
    Extension(principal): Extension<AuthPrincipal>,
    State(state): State<ApiState>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    principal.require_scope("read")?;
    let scope = EventScope::of(&principal, &state)?;
    let receiver = state.subscribe()?;
    let stream = stream::unfold((receiver, scope), |(mut receiver, mut scope)| async move {
        loop {
            match receiver.recv().await {
                Ok(event) if scope.admits(&event) => {
                    let event = Event::default()
                        .event(event.kind.clone())
                        .json_data(event)
                        .unwrap_or_else(|error| {
                            Event::default()
                                .event("error")
                                .data(format!("event encoding failed: {error}"))
                        });
                    return Some((Ok(event), (receiver, scope)));
                }
                Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
            }
        }
    });
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

pub(crate) async fn websocket(
    Extension(principal): Extension<AuthPrincipal>,
    State(state): State<ApiState>,
    upgrade: WebSocketUpgrade,
) -> Result<Response, ApiError> {
    principal.require_scope("read")?;
    let scope = EventScope::of(&principal, &state)?;
    let receiver = state.subscribe()?;
    Ok(upgrade
        .on_upgrade(move |socket| send_events(socket, receiver, scope))
        .into_response())
}

async fn send_events(
    mut socket: WebSocket,
    mut receiver: tokio::sync::broadcast::Receiver<ApiEvent>,
    mut scope: EventScope,
) {
    loop {
        let event = match receiver.recv().await {
            Ok(event) => event,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
        };
        if !scope.admits(&event) {
            continue;
        }
        let Ok(encoded) = serde_json::to_string(&event) else {
            continue;
        };
        if socket.send(Message::Text(encoded.into())).await.is_err() {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::{
        body::Body,
        http::{Request, StatusCode, header},
    };
    use http_body_util::BodyExt as _;
    use serde_json::Value;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tower::ServiceExt as _;

    use super::{ApiEvent, EventScope};
    use crate::ApiState;

    fn event(database_id: Option<&str>, table: Option<&str>, message: &str) -> ApiEvent {
        ApiEvent {
            kind: "replication.progress".to_owned(),
            database_id: database_id.map(str::to_owned),
            table: table.map(str::to_owned),
            message: message.to_owned(),
            rows: None,
            bytes: None,
            eta_seconds: None,
            at: String::new(),
        }
    }

    #[test]
    fn database_scoped_streams_hide_global_and_cross_database_events() {
        let mut scope = EventScope::Database("db-a".to_owned());
        assert!(scope.admits(&event(Some("db-a"), None, "")));
        assert!(!scope.admits(&event(Some("db-b"), None, "")));
        assert!(!scope.admits(&event(None, None, "")));
    }

    async fn post(app: &axum::Router, uri: &str, bearer: Option<&str>, body: &str) -> Value {
        let mut request = Request::builder()
            .method("POST")
            .uri(uri)
            .header(header::CONTENT_TYPE, "application/json");
        if let Some(bearer) = bearer {
            request = request.header(header::AUTHORIZATION, format!("Bearer {bearer}"));
        }
        let response = app
            .clone()
            .oneshot(request.body(Body::from(body.to_owned())).unwrap())
            .await
            .unwrap();
        assert!(
            response.status().is_success(),
            "{uri}: {}",
            response.status()
        );
        let body = response.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&body).expect("JSON body")
    }

    /// Two workspaces with one database each, and a session in each.
    struct TwoWorkspaces {
        _data: tempfile::TempDir,
        state: ApiState,
        app: axum::Router,
        first_database: String,
        second_token: String,
        second_database: String,
    }

    async fn two_workspaces() -> TwoWorkspaces {
        let data = tempfile::tempdir().expect("API data directory");
        let state = ApiState::new(
            data.path(),
            data.path().join("pintail-meta.db"),
            b"test-jwt-secret-with-enough-entropy",
            &"42".repeat(32),
        )
        .expect("configured API state");
        let app = crate::router_with_state(state.clone());
        let first_token = post(
            &app,
            "/api/auth/setup",
            None,
            r#"{"email":"admin@example.com","password":"correct horse battery"}"#,
        )
        .await["token"]
            .as_str()
            .expect("setup token")
            .to_owned();
        let second_token = post(
            &app,
            "/api/workspaces",
            Some(&first_token),
            r#"{"name":"Second"}"#,
        )
        .await["token"]
            .as_str()
            .expect("workspace token")
            .to_owned();
        let mut databases = Vec::new();
        for (token, name) in [(&first_token, "ledger"), (&second_token, "catalogue")] {
            let created = post(
                &app,
                "/api/databases",
                Some(token),
                &format!(
                    r#"{{"name":"{name}","dsn":"mysql://pintail:secret@db/{name}","mode":"auto"}}"#
                ),
            )
            .await;
            databases.push(created["id"].as_str().expect("database id").to_owned());
        }
        let second_database = databases.pop().expect("second database");
        let first_database = databases.pop().expect("first database");
        TwoWorkspaces {
            _data: data,
            state,
            app,
            first_database,
            second_token,
            second_database,
        }
    }

    /// What the other workspace's activity looks like on the broadcast, then
    /// one event the second workspace owns. A stream scoped to the second
    /// workspace must deliver the last one FIRST: anything earlier arriving
    /// is a leak.
    fn publish_foreign_then_own(fixture: &TwoWorkspaces) {
        fixture.state.publish(event(
            Some(&fixture.first_database),
            Some("payments"),
            "foreign table copied",
        ));
        // Names a table and no database: it cannot be placed in a workspace.
        fixture
            .state
            .publish(event(None, Some("payments"), "unplaced table"));
        fixture.state.publish(event(
            Some(&fixture.second_database),
            Some("products"),
            "own table copied",
        ));
        // No database and no table: nothing workspace-specific to hide.
        fixture.state.publish(event(None, None, "node notice"));
    }

    fn assert_own_then_notice(fixture: &TwoWorkspaces, first: &Value, second: &Value) {
        assert_eq!(first["database_id"], fixture.second_database.as_str());
        assert_eq!(first["table"], "products");
        assert_eq!(second["message"], "node notice");
        assert_eq!(second["database_id"], Value::Null);
    }

    #[tokio::test]
    async fn an_sse_stream_carries_only_its_own_workspace_events() {
        let fixture = two_workspaces().await;
        let response = fixture
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/events")
                    .header(
                        header::AUTHORIZATION,
                        format!("Bearer {}", fixture.second_token),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        publish_foreign_then_own(&fixture);

        let mut body = response.into_body();
        let mut text = String::new();
        let mut events = Vec::new();
        while events.len() < 2 {
            let frame = tokio::time::timeout(Duration::from_secs(10), body.frame())
                .await
                .expect("an event within the deadline")
                .expect("an open stream")
                .expect("a readable frame");
            if let Some(data) = frame.data_ref() {
                text.push_str(std::str::from_utf8(data).expect("UTF-8 frame"));
            }
            while let Some(end) = text.find("\n\n") {
                let block: String = text.drain(..end + 2).collect();
                if let Some(json) = block.lines().find_map(|line| line.strip_prefix("data: ")) {
                    events.push(serde_json::from_str::<Value>(json).expect("event JSON"));
                }
            }
        }
        assert_own_then_notice(&fixture, &events[0], &events[1]);
        assert!(!text.contains(&fixture.first_database));
    }

    /// Reads one unmasked server text frame.
    async fn read_text_frame(stream: &mut tokio::net::TcpStream) -> Value {
        let mut header = [0_u8; 2];
        stream.read_exact(&mut header).await.expect("frame header");
        assert_eq!(header[0], 0x81, "a final text frame");
        let length = match header[1] {
            126 => {
                let mut extended = [0_u8; 2];
                stream.read_exact(&mut extended).await.expect("length");
                usize::from(u16::from_be_bytes(extended))
            }
            short => {
                assert!(short < 126, "a frame this test can size");
                usize::from(short)
            }
        };
        let mut payload = vec![0_u8; length];
        stream
            .read_exact(&mut payload)
            .await
            .expect("frame payload");
        serde_json::from_slice(&payload).expect("event JSON")
    }

    #[tokio::test]
    async fn a_websocket_stream_carries_only_its_own_workspace_events() {
        let fixture = two_workspaces().await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("HTTP listener");
        let address = listener.local_addr().expect("HTTP address");
        let app = fixture.app.clone();
        let server = tokio::spawn(async move { axum::serve(listener, app).await });

        let mut stream = tokio::net::TcpStream::connect(address)
            .await
            .expect("connect");
        let upgrade = format!(
            "GET /api/ws HTTP/1.1\r\nHost: {address}\r\nConnection: Upgrade\r\n\
             Upgrade: websocket\r\nSec-WebSocket-Version: 13\r\n\
             Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
             Authorization: Bearer {}\r\n\r\n",
            fixture.second_token
        );
        stream.write_all(upgrade.as_bytes()).await.expect("upgrade");
        // The response head, byte by byte so no frame is swallowed with it.
        let mut head = Vec::new();
        while !head.ends_with(b"\r\n\r\n") {
            let mut byte = [0_u8; 1];
            stream.read_exact(&mut byte).await.expect("response head");
            head.push(byte[0]);
        }
        assert!(head.starts_with(b"HTTP/1.1 101"), "the upgrade is accepted");
        publish_foreign_then_own(&fixture);

        let read = async {
            let first = read_text_frame(&mut stream).await;
            let second = read_text_frame(&mut stream).await;
            (first, second)
        };
        let (first, second) = tokio::time::timeout(Duration::from_secs(10), read)
            .await
            .expect("two events within the deadline");
        assert_own_then_notice(&fixture, &first, &second);
        server.abort();
    }
}
