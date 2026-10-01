use axum::{
    Extension, Json,
    extract::{Path, Query, State},
};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::{
    ApiState, audit,
    auth::{AuthPrincipal, issue_token},
    error::ApiError,
    state::random_identifier,
};

#[derive(Serialize)]
pub(crate) struct WorkspaceResponse {
    id: String,
    name: String,
    slug: String,
    role: String,
}

#[derive(Deserialize)]
pub(crate) struct CreateWorkspaceRequest {
    name: String,
}

#[derive(Serialize)]
pub(crate) struct SwitchedWorkspaceResponse {
    token: String,
    workspace: WorkspaceResponse,
}

#[derive(Serialize)]
pub(crate) struct MemberResponse {
    user_id: String,
    email: String,
    role: String,
}

#[derive(Deserialize)]
pub(crate) struct ChangeMemberRoleRequest {
    role: String,
}

#[derive(Deserialize)]
pub(crate) struct AuditLogQuery {
    #[serde(default = "default_audit_limit")]
    limit: u64,
}

const fn default_audit_limit() -> u64 {
    200
}

#[derive(Serialize)]
pub(crate) struct AuditEventResponse {
    id: String,
    actor_type: String,
    actor_label: String,
    action: String,
    target_type: Option<String>,
    target_id: Option<String>,
    detail_json: Option<String>,
    created_at: String,
    client_ip: Option<String>,
}

/// Lists every workspace the caller belongs to, for the sidebar switcher.
pub(crate) async fn list(
    Extension(principal): Extension<AuthPrincipal>,
    State(state): State<ApiState>,
) -> Result<Json<Vec<WorkspaceResponse>>, ApiError> {
    let workspaces = state
        .metadata()?
        .workspaces_for_user(&principal.subject)
        .map_err(ApiError::internal)?
        .into_iter()
        .map(|(workspace, role)| WorkspaceResponse {
            id: workspace.id,
            name: workspace.name,
            slug: workspace.slug,
            role,
        })
        .collect();
    Ok(Json(workspaces))
}

/// Creates a new workspace and switches the caller into it immediately.
pub(crate) async fn create(
    Extension(principal): Extension<AuthPrincipal>,
    State(state): State<ApiState>,
    Json(request): Json<CreateWorkspaceRequest>,
) -> Result<Json<SwitchedWorkspaceResponse>, ApiError> {
    let name = request.name.trim();
    if name.is_empty() || name.chars().count() > 80 {
        return Err(ApiError::bad_request(
            "workspace name must be 1-80 characters",
        ));
    }
    let workspace_id = random_identifier("ws_", 16);
    let slug = workspace_id.trim_start_matches("ws_").to_owned();
    let now = Utc::now().to_rfc3339();
    let metadata = state.metadata()?;
    metadata
        .create_workspace(&workspace_id, name, &slug, &now)
        .map_err(ApiError::internal)?;
    metadata
        .add_workspace_member(&workspace_id, &principal.subject, "admin", &now)
        .map_err(ApiError::internal)?;
    let token = issue_token(&state, &principal.subject, "admin", &workspace_id)?;
    audit::record_in(
        &state,
        &workspace_id,
        &principal,
        "workspace.create",
        Some(("workspace", &workspace_id)),
        Some(serde_json::json!({"name": name})),
    );
    Ok(Json(SwitchedWorkspaceResponse {
        token,
        workspace: WorkspaceResponse {
            id: workspace_id,
            name: name.to_owned(),
            slug,
            role: "admin".to_owned(),
        },
    }))
}

/// Switches the caller's active session into another workspace they belong
/// to, minting a token scoped to it.
pub(crate) async fn switch(
    Extension(principal): Extension<AuthPrincipal>,
    State(state): State<ApiState>,
    Path(workspace_id): Path<String>,
) -> Result<Json<SwitchedWorkspaceResponse>, ApiError> {
    let metadata = state.metadata()?;
    let workspace = metadata
        .workspace_by_id(&workspace_id)
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found("workspace does not exist"))?;
    let role = metadata
        .workspace_member_role(&workspace_id, &principal.subject)
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::forbidden("you are not a member of this workspace"))?;
    let token = issue_token(&state, &principal.subject, &role, &workspace_id)?;
    Ok(Json(SwitchedWorkspaceResponse {
        token,
        workspace: WorkspaceResponse {
            id: workspace.id,
            name: workspace.name,
            slug: workspace.slug,
            role,
        },
    }))
}

/// Lists the members of the caller's current workspace.
pub(crate) async fn members(
    Extension(principal): Extension<AuthPrincipal>,
    State(state): State<ApiState>,
) -> Result<Json<Vec<MemberResponse>>, ApiError> {
    let workspace_id = principal.require_workspace()?;
    let members = state
        .metadata()?
        .list_workspace_members(workspace_id)
        .map_err(ApiError::internal)?
        .into_iter()
        .map(|member| MemberResponse {
            user_id: member.user_id,
            email: member.email,
            role: member.role,
        })
        .collect();
    Ok(Json(members))
}

/// Lists the audit trail for the caller's current workspace, most recent
/// first. Admin only: entries can include query SQL text and other
/// sensitive detail.
pub(crate) async fn audit_log(
    Extension(principal): Extension<AuthPrincipal>,
    State(state): State<ApiState>,
    Query(query): Query<AuditLogQuery>,
) -> Result<Json<Vec<AuditEventResponse>>, ApiError> {
    principal.require_admin()?;
    let workspace_id = principal.require_workspace()?;
    let limit = query.limit.clamp(1, 1_000);
    let events = state
        .metadata()?
        .audit_log_in_workspace(workspace_id, limit)
        .map_err(ApiError::internal)?
        .into_iter()
        .map(|event| AuditEventResponse {
            id: event.id,
            actor_type: event.actor_type,
            actor_label: event.actor_label,
            action: event.action,
            target_type: event.target_type,
            target_id: event.target_id,
            detail_json: event.detail_json,
            created_at: event.created_at,
            client_ip: event.client_ip,
        })
        .collect();
    Ok(Json(events))
}

/// Removes a member from the caller's current workspace. Admin only; the
/// caller cannot remove themselves this way.
pub(crate) async fn remove_member(
    Extension(principal): Extension<AuthPrincipal>,
    State(state): State<ApiState>,
    Path(user_id): Path<String>,
) -> Result<axum::http::StatusCode, ApiError> {
    principal.require_admin()?;
    let workspace_id = principal.require_workspace()?;
    if user_id == principal.subject {
        return Err(ApiError::bad_request(
            "you cannot remove yourself from the workspace",
        ));
    }
    let removed = state
        .metadata()?
        .remove_workspace_member(workspace_id, &user_id)
        .map_err(ApiError::internal)?;
    if removed {
        audit::record(
            &state,
            &principal,
            "workspace.remove_member",
            Some(("user", &user_id)),
            None,
        );
        Ok(axum::http::StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::not_found("member does not exist"))
    }
}

/// Changes one member's role in the caller's current workspace. Admin only;
/// the caller cannot change their own.
///
/// That self-exclusion is what keeps a workspace administrable. Only an admin
/// reaches this, and an admin cannot demote themselves here, so no sequence of
/// calls can leave a workspace with nobody able to make the next change - a
/// property worth more than the convenience of stepping down through the same
/// route, which can be done by another admin.
pub(crate) async fn change_member_role(
    Extension(principal): Extension<AuthPrincipal>,
    State(state): State<ApiState>,
    Path(user_id): Path<String>,
    Json(request): Json<ChangeMemberRoleRequest>,
) -> Result<axum::http::StatusCode, ApiError> {
    principal.require_admin()?;
    let workspace_id = principal.require_workspace()?;
    if user_id == principal.subject {
        return Err(ApiError::bad_request("you cannot change your own role"));
    }
    let changed = state
        .metadata()?
        .update_workspace_member_role(workspace_id, &user_id, &request.role)
        // The store rejects an unknown role name, which is the caller's
        // mistake to fix rather than an internal fault to hide.
        .map_err(|failure| ApiError::bad_request(failure.to_string()))?;
    if changed {
        audit::record(
            &state,
            &principal,
            "workspace.change_member_role",
            Some(("user", &user_id)),
            Some(serde_json::json!({ "role": request.role })),
        );
        Ok(axum::http::StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::not_found("member does not exist"))
    }
}

#[cfg(test)]
mod tests {
    use axum::{
        body::Body,
        http::{Request, StatusCode, header},
    };
    use http_body_util::BodyExt as _;
    use serde_json::Value;
    use tower::ServiceExt as _;

    use crate::{ApiState, auth::issue_token};

    async fn call(
        app: &axum::Router,
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
        let response = app
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

    fn token_of(response: &(StatusCode, Value)) -> String {
        assert!(response.0.is_success(), "{response:?}");
        response.1["token"].as_str().expect("a token").to_owned()
    }

    const OAUTH: &str = "/api/settings/oauth/google";
    const WIRE_TLS: &str = "/api/settings/wire-tls";
    const OAUTH_BODY: &str = r#"{"enabled":false,"client_id":"client.example"}"#;
    const WIRE_TLS_BODY: &str = r#"{"hostnames":"replica.example.com"}"#;

    /// Creating a workspace makes its creator that workspace's
    /// administrator, and nothing more. The node's own settings - the OAuth
    /// client every workspace signs in through, the names on the one wire
    /// certificate - stay with the administrators of the node's first
    /// workspace, in whichever workspace their session happens to be.
    #[tokio::test]
    async fn a_workspace_creator_does_not_administer_the_node() {
        let data = tempfile::tempdir().expect("API data directory");
        let state = ApiState::new(
            data.path(),
            data.path().join("pintail-meta.db"),
            b"test-jwt-secret-with-enough-entropy",
            &"42".repeat(32),
        )
        .expect("configured API state");
        let app = crate::router_with_state(state.clone());
        let setup = call(
            &app,
            "POST",
            "/api/auth/setup",
            None,
            Some(r#"{"email":"admin@example.com","password":"correct horse battery"}"#),
        )
        .await;
        let admin = token_of(&setup);
        let first_workspace = setup.1["user"]["workspace_id"]
            .as_str()
            .expect("first workspace")
            .to_owned();

        // A second account that only views the first workspace.
        let metadata = state.metadata().expect("metadata");
        metadata
            .create_user(
                "usr_viewer",
                "viewer@example.com",
                "unused",
                "viewer",
                "now",
            )
            .expect("viewer account");
        metadata
            .add_workspace_member(&first_workspace, "usr_viewer", "viewer", "now")
            .expect("viewer membership");
        let viewer =
            issue_token(&state, "usr_viewer", "viewer", &first_workspace).expect("viewer session");

        // The viewer creates a workspace and is its administrator...
        let created = call(
            &app,
            "POST",
            "/api/workspaces",
            Some(&viewer),
            Some(r#"{"name":"Side project"}"#),
        )
        .await;
        let own = token_of(&created);
        let (status, session) = call(&app, "GET", "/api/session", Some(&own), None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(session["role"], "admin");
        assert_eq!(session["node_admin"], false);
        // ...which lets them run that workspace,
        let (status, _) = call(&app, "GET", "/api/workspaces/audit-log", Some(&own), None).await;
        assert_eq!(status, StatusCode::OK);
        // and reaches no node-wide setting, to read or to write.
        for (method, uri, body) in [
            ("GET", OAUTH, None),
            ("PUT", OAUTH, Some(OAUTH_BODY)),
            ("GET", WIRE_TLS, None),
            ("PUT", WIRE_TLS, Some(WIRE_TLS_BODY)),
        ] {
            let (status, _) = call(&app, method, uri, Some(&own), body).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{method} {uri}");
        }
        let (_, settings) = call(&app, "GET", OAUTH, Some(&admin), None).await;
        assert_eq!(
            settings["client_id"], "",
            "the refused write changed nothing"
        );

        // The first administrator keeps the node from their first workspace
        // and from one they create afterwards.
        let elsewhere = token_of(
            &call(
                &app,
                "POST",
                "/api/workspaces",
                Some(&admin),
                Some(r#"{"name":"Second"}"#),
            )
            .await,
        );
        for session in [&admin, &elsewhere] {
            let (status, principal) = call(&app, "GET", "/api/session", Some(session), None).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(principal["node_admin"], true);
            for (uri, body) in [(OAUTH, OAUTH_BODY), (WIRE_TLS, WIRE_TLS_BODY)] {
                let (status, _) = call(&app, "PUT", uri, Some(session), Some(body)).await;
                assert_eq!(status, StatusCode::OK, "PUT {uri}");
            }
        }

        // Node administration follows the first workspace's membership: a
        // demotion there ends it at once.
        metadata
            .update_workspace_member_role(
                &first_workspace,
                setup.1["user"]["id"].as_str().expect("admin id"),
                "viewer",
            )
            .expect("demote");
        let (status, _) = call(&app, "PUT", OAUTH, Some(&elsewhere), Some(OAUTH_BODY)).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }
}
