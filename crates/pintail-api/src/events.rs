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

use crate::{
    ApiState,
    auth::{AuthPrincipal, current_authority, is_node_admin},
    error::ApiError,
};

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

/// How long a stream trusts what it last read: which workspace owns a
/// database, and whether its own caller still has any standing.
///
/// Progress events arrive every replication cycle and each one would
/// otherwise cost metadata reads per open stream. Short enough that a
/// database handed to a workspace (a restore assigns its owner after
/// creating it) starts appearing within seconds, and that a removed member's
/// stream ends within seconds.
#[cfg(not(test))]
const TRUST_TTL: Duration = Duration::from_secs(5);
#[cfg(test)]
const TRUST_TTL: Duration = Duration::from_millis(150);

/// Who is listening, which decides what they may hear.
///
/// Every subscriber reads the same node-wide broadcast, so the stream itself
/// is the only place a caller's reach can be applied. A database identifier,
/// a table name and a replication message all describe somebody's source.
/// There is deliberately no kind that hears everything.
#[derive(Debug, Eq, PartialEq)]
enum Audience {
    /// An API key: events of the one database it was issued for.
    DatabaseKey { database_id: String },
    /// A workspace member: events of databases that workspace owns.
    WorkspaceMember { workspace_id: String },
    /// A node administrator: their workspace's databases, and the events
    /// that belong to no database - the node's own.
    NodeAdministrator { workspace_id: String },
}

impl Audience {
    fn of(state: &ApiState, principal: &AuthPrincipal) -> Result<Self, ApiError> {
        if let Some(database_id) = principal.database_scope() {
            return Ok(Self::DatabaseKey {
                database_id: database_id.to_owned(),
            });
        }
        let workspace_id = principal.require_workspace()?.to_owned();
        Ok(if is_node_admin(state, principal)? {
            Self::NodeAdministrator { workspace_id }
        } else {
            Self::WorkspaceMember { workspace_id }
        })
    }
}

/// One open stream's view of what it may carry, and until when.
struct EventScope {
    state: ApiState,
    principal: AuthPrincipal,
    audience: Audience,
    /// When the caller's standing was last read.
    confirmed: Instant,
    /// Database id to whether the caller's workspace owns it, and when that
    /// was read.
    owned: HashMap<String, (bool, Instant)>,
}

impl EventScope {
    fn of(principal: &AuthPrincipal, state: &ApiState) -> Result<Self, ApiError> {
        Ok(Self {
            audience: Audience::of(state, principal)?,
            state: state.clone(),
            principal: principal.clone(),
            confirmed: Instant::now(),
            owned: HashMap::new(),
        })
    }

    /// Whether the caller may still hold this stream at all.
    ///
    /// Asked on a timer and before a delivery, and answered from metadata
    /// once the last answer is older than [`TRUST_TTL`]. A disabled account,
    /// a removed member, and a key that is revoked, expired or no longer
    /// grants `read` all end the stream; a role change is taken up in place,
    /// so a demoted node administrator stops hearing the node's events.
    fn still_stands(&mut self) -> bool {
        if self.confirmed.elapsed() < TRUST_TTL {
            return true;
        }
        let Some(current) = current_authority(&self.state, &self.principal) else {
            return false;
        };
        if current.require_scope("read").is_err() {
            return false;
        }
        let Ok(audience) = Audience::of(&self.state, &current) else {
            return false;
        };
        self.principal = current;
        self.audience = audience;
        self.confirmed = Instant::now();
        true
    }

    fn admits(&mut self, event: &ApiEvent) -> bool {
        let workspace_id = match &self.audience {
            Audience::DatabaseKey { database_id } => {
                return event.database_id.as_deref() == Some(database_id.as_str());
            }
            Audience::WorkspaceMember { workspace_id } => {
                if event.database_id.is_none() {
                    return false;
                }
                workspace_id
            }
            Audience::NodeAdministrator { workspace_id } => {
                if event.database_id.is_none() {
                    return true;
                }
                workspace_id
            }
        };
        let Some(database_id) = event.database_id.as_deref() else {
            return false;
        };
        if let Some((verdict, read_at)) = self.owned.get(database_id)
            && read_at.elapsed() < TRUST_TTL
        {
            return *verdict;
        }
        // A failed read hides the event: not knowing who owns a database is
        // no reason to show it.
        let verdict = self
            .state
            .metadata()
            .ok()
            .and_then(|metadata| {
                metadata
                    .database_in_workspace(database_id, workspace_id)
                    .ok()
            })
            .is_some_and(|database| database.is_some());
        // Identifiers are never reused, so entries only accumulate with
        // databases; drop the stale ones before the map can outgrow the
        // node's database list by much.
        if self.owned.len() >= 256 {
            self.owned
                .retain(|_, (_, read_at)| read_at.elapsed() < TRUST_TTL);
        }
        self.owned
            .insert(database_id.to_owned(), (verdict, Instant::now()));
        verdict
    }
}

/// One subscriber: the broadcast, what it may carry, and the timer that
/// re-reads the caller's standing while nothing is being published.
struct Subscription {
    receiver: tokio::sync::broadcast::Receiver<ApiEvent>,
    scope: EventScope,
    recheck: tokio::time::Interval,
}

impl Subscription {
    fn open(principal: &AuthPrincipal, state: &ApiState) -> Result<Self, ApiError> {
        principal.require_scope("read")?;
        let scope = EventScope::of(principal, state)?;
        let receiver = state.subscribe()?;
        let mut recheck = tokio::time::interval(TRUST_TTL);
        recheck.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        Ok(Self {
            receiver,
            scope,
            recheck,
        })
    }

    /// The next event this caller may see, or `None` once the stream is
    /// over: the broadcast closed, or the caller lost their standing.
    async fn next(&mut self) -> Option<ApiEvent> {
        loop {
            tokio::select! {
                _ = self.recheck.tick() => {
                    if !self.scope.still_stands() {
                        return None;
                    }
                }
                received = self.receiver.recv() => match received {
                    Ok(event) => {
                        if !self.scope.still_stands() {
                            return None;
                        }
                        if self.scope.admits(&event) {
                            return Some(event);
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                },
            }
        }
    }
}

pub(crate) async fn sse(
    Extension(principal): Extension<AuthPrincipal>,
    State(state): State<ApiState>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let subscription = Subscription::open(&principal, &state)?;
    let stream = stream::unfold(subscription, |mut subscription| async move {
        let event = subscription.next().await?;
        let event = Event::default()
            .event(event.kind.clone())
            .json_data(event)
            .unwrap_or_else(|error| {
                Event::default()
                    .event("error")
                    .data(format!("event encoding failed: {error}"))
            });
        Some((Ok(event), subscription))
    });
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

pub(crate) async fn websocket(
    Extension(principal): Extension<AuthPrincipal>,
    State(state): State<ApiState>,
    upgrade: WebSocketUpgrade,
) -> Result<Response, ApiError> {
    let subscription = Subscription::open(&principal, &state)?;
    Ok(upgrade
        .on_upgrade(move |socket| send_events(socket, subscription))
        .into_response())
}

async fn send_events(mut socket: WebSocket, mut subscription: Subscription) {
    while let Some(event) = subscription.next().await {
        let Ok(encoded) = serde_json::to_string(&event) else {
            continue;
        };
        if socket.send(Message::Text(encoded.into())).await.is_err() {
            return;
        }
    }
    // Said out loud, so a client can tell an ended stream from a lost one.
    let _ = socket.send(Message::Close(None)).await;
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;

    use super::{ApiEvent, Audience, TRUST_TTL};
    use crate::test_support::{EventStream, Next, Node, TRANSPORTS};

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

    /// The node's own event: no database, no table.
    fn notice() -> ApiEvent {
        event(None, None, "node notice")
    }

    fn delivered(next: &Next) -> (&str, &str) {
        let Next::Event(event) = next else {
            panic!("expected an event, got {next:?}");
        };
        (
            event["database_id"].as_str().unwrap_or("-"),
            event["message"].as_str().unwrap_or("-"),
        )
    }

    /// Long enough for a stream to have re-read its caller's standing.
    async fn past_the_trust_interval() {
        tokio::time::sleep(TRUST_TTL * 3).await;
    }

    #[tokio::test]
    async fn every_caller_is_one_explicit_kind_of_audience() {
        let node = Node::new().await;
        let (_, viewer) = node.member(&node.first_workspace, "viewer");
        let database = node.database(&node.admin, "ledger").await;
        let (_, key) = node.api_key(&node.admin, &database).await;
        for (bearer, expected) in [
            (
                &node.admin,
                Audience::NodeAdministrator {
                    workspace_id: node.first_workspace.clone(),
                },
            ),
            (
                &viewer,
                Audience::WorkspaceMember {
                    workspace_id: node.first_workspace.clone(),
                },
            ),
            (
                &key,
                Audience::DatabaseKey {
                    database_id: database.clone(),
                },
            ),
        ] {
            let principal = if bearer.starts_with("pk_") {
                crate::auth::authenticate_api_key(&node.state, bearer)
            } else {
                crate::auth::authenticate_jwt(&node.state, bearer)
            }
            .expect("an authenticated caller");
            assert_eq!(Audience::of(&node.state, &principal).unwrap(), expected);
        }
    }

    /// A member hears their own workspace's databases - and only those: not
    /// another workspace's, and not the node's own events.
    #[tokio::test]
    async fn a_member_stream_carries_only_its_own_workspace_events() {
        for transport in TRANSPORTS {
            let node = Node::new().await;
            let own = node.database(&node.admin, "ledger").await;
            let (_, elsewhere) = node.workspace(&node.admin, "Second").await;
            let foreign = node.database(&elsewhere, "catalogue").await;
            let (_, viewer) = node.member(&node.first_workspace, "viewer");
            let mut stream = EventStream::open(&node, transport, Some(&viewer))
                .await
                .expect("a stream");

            node.state
                .publish(event(Some(&foreign), Some("products"), "foreign copy"));
            node.state.publish(notice());
            node.state
                .publish(event(None, Some("products"), "unplaced table"));
            node.state
                .publish(event(Some(&own), Some("payments"), "own copy"));
            // The first thing delivered is the last thing published:
            // anything earlier arriving is a leak.
            let first = stream.next().await;
            assert_eq!(
                delivered(&first),
                (own.as_str(), "own copy"),
                "{transport:?}"
            );
            assert_eq!(stream.next().await, Next::Quiet, "{transport:?}");
        }
    }

    /// The node's own events reach a node administrator, in whichever
    /// workspace their session is, beside that workspace's databases.
    #[tokio::test]
    async fn node_events_reach_only_a_node_administrator() {
        for transport in TRANSPORTS {
            let node = Node::new().await;
            let first = node.database(&node.admin, "ledger").await;
            let (second_workspace, elsewhere) = node.workspace(&node.admin, "Second").await;
            let second = node.database(&elsewhere, "catalogue").await;
            // An administrator of the second workspace only: not the node's.
            let (_, second_admin) = node.member(&second_workspace, "admin");
            let mut administrator = EventStream::open(&node, transport, Some(&elsewhere))
                .await
                .expect("a stream");
            let mut workspace_admin = EventStream::open(&node, transport, Some(&second_admin))
                .await
                .expect("a stream");

            node.state.publish(event(Some(&first), None, "first"));
            node.state.publish(notice());
            node.state.publish(event(Some(&second), None, "second"));
            assert_eq!(delivered(&administrator.next().await), ("-", "node notice"));
            assert_eq!(
                delivered(&administrator.next().await),
                (second.as_str(), "second")
            );
            assert_eq!(
                delivered(&workspace_admin.next().await),
                (second.as_str(), "second"),
                "{transport:?}"
            );
            assert_eq!(workspace_admin.next().await, Next::Quiet, "{transport:?}");

            // Demoted in the first workspace, the administrator's open
            // stream stops carrying the node's events and stays open.
            node.metadata()
                .update_workspace_member_role(&node.first_workspace, &node.admin_id, "viewer")
                .expect("demote");
            past_the_trust_interval().await;
            node.state.publish(notice());
            node.state.publish(event(Some(&second), None, "after"));
            assert_eq!(
                delivered(&administrator.next().await),
                (second.as_str(), "after"),
                "{transport:?}"
            );
        }
    }

    /// A database key hears its own database: not a sibling in the same
    /// workspace, not the node.
    #[tokio::test]
    async fn a_database_key_stream_carries_only_its_database() {
        for transport in TRANSPORTS {
            let node = Node::new().await;
            let own = node.database(&node.admin, "ledger").await;
            let sibling = node.database(&node.admin, "catalogue").await;
            let (key_id, key) = node.api_key(&node.admin, &own).await;
            let mut stream = EventStream::open(&node, transport, Some(&key))
                .await
                .expect("a stream");

            node.state.publish(event(Some(&sibling), None, "sibling"));
            node.state.publish(notice());
            node.state.publish(event(Some(&own), None, "own"));
            assert_eq!(delivered(&stream.next().await), (own.as_str(), "own"));
            assert_eq!(stream.next().await, Next::Quiet, "{transport:?}");

            // A revoked key's stream ends, with nothing more delivered.
            node.metadata()
                .set_api_key_enabled(&key_id, false)
                .expect("disable the key");
            past_the_trust_interval().await;
            node.state.publish(event(Some(&own), None, "after"));
            stream.expect_closed().await;
        }
    }

    /// An open stream ends once its caller is removed from the workspace,
    /// and delivers nothing published after the interval it re-reads on.
    #[tokio::test]
    async fn a_stream_ends_when_its_member_is_removed() {
        for transport in TRANSPORTS {
            let node = Node::new().await;
            let own = node.database(&node.admin, "ledger").await;
            let (user_id, viewer) = node.member(&node.first_workspace, "viewer");
            let mut stream = EventStream::open(&node, transport, Some(&viewer))
                .await
                .expect("a stream");
            node.state.publish(event(Some(&own), None, "before"));
            assert_eq!(delivered(&stream.next().await), (own.as_str(), "before"));

            assert!(
                node.metadata()
                    .remove_workspace_member(&node.first_workspace, &user_id)
                    .expect("remove")
            );
            past_the_trust_interval().await;
            node.state.publish(event(Some(&own), None, "after"));
            stream.expect_closed().await;
        }
    }

    /// The same for a disabled account - and with nothing published at all:
    /// an idle stream is re-read on a timer, not only on delivery.
    #[tokio::test]
    async fn an_idle_stream_ends_when_its_account_is_disabled() {
        for transport in TRANSPORTS {
            let node = Node::new().await;
            let (user_id, viewer) = node.member(&node.first_workspace, "viewer");
            let mut stream = EventStream::open(&node, transport, Some(&viewer))
                .await
                .expect("a stream");
            assert!(
                node.metadata()
                    .set_user_enabled(&user_id, false)
                    .expect("disable")
            );
            stream.expect_closed().await;
        }
    }

    #[tokio::test]
    async fn an_unauthenticated_caller_opens_no_stream() {
        let node = Node::new().await;
        for transport in TRANSPORTS {
            for bearer in [None, Some("not-a-session")] {
                let refused = EventStream::open(&node, transport, bearer).await.err();
                assert_eq!(refused, Some(StatusCode::UNAUTHORIZED), "{transport:?}");
            }
        }
    }
}
