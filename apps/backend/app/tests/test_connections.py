from urllib.parse import parse_qs, urlparse

from fastapi.testclient import TestClient

from app.db.models import ConnectionProvider, ConnectionStatus
from app.db.session import get_session_factory
from app.services.connections import ConnectionService


def test_connection_capabilities_report_oauth_configuration(authenticated_client: TestClient) -> None:
    response = authenticated_client.get("/api/connections/capabilities")
    assert response.status_code == 200
    assert response.json()["gmail_oauth_configured"] is True


def test_connection_payload_is_not_exposed(
    authenticated_client: TestClient,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "app.integrations.zotero.ZoteroClient.get_current_key_info",
        lambda self: type(
            "KeyInfo",
            (),
            {
                "user_id": 12345,
                "username": "reader",
                "access": {"user": {"library": True, "write": True}},
            },
        )(),
    )
    saved = authenticated_client.post(
        "/api/connections/zotero",
        json={
            "label": "Primary Zotero",
            "payload": {"api_key": "secret-token", "library_id": "12345"},
            "metadata_json": {"library_type": "users"},
        },
    )
    assert saved.status_code == 201

    fetched = authenticated_client.get("/api/connections/zotero")
    assert fetched.status_code == 200
    payload = fetched.json()
    assert payload["label"] == "Primary Zotero"
    assert "encrypted_payload" not in payload
    assert payload["metadata_json"]["library_type"] == "users"
    assert payload["metadata_json"]["library_id"] == "12345"
    assert payload["metadata_json"]["connected_username"] == "reader"
    assert payload["metadata_json"]["connected_user_id"] == "12345"
    assert payload["metadata_json"]["can_write"] is True
    assert payload["status"] == "connected"


def test_legacy_zotero_connection_is_verified_on_fetch(
    authenticated_client: TestClient,
    monkeypatch,
) -> None:
    with get_session_factory()() as db:
        service = ConnectionService(db)
        service.store_connection(
            provider=ConnectionProvider.ZOTERO,
            label="Primary Zotero",
            payload={"api_key": "secret-token", "library_id": "12345", "library_type": "users"},
            metadata_json={"library_type": "users"},
            status=ConnectionStatus.CONNECTED,
        )

    monkeypatch.setattr(
        "app.integrations.zotero.ZoteroClient.get_current_key_info",
        lambda self: type(
            "KeyInfo",
            (),
            {
                "user_id": 12345,
                "username": "reader",
                "access": {"user": {"library": True, "write": True}},
            },
        )(),
    )

    fetched = authenticated_client.get("/api/connections/zotero")
    assert fetched.status_code == 200
    payload = fetched.json()
    assert payload["status"] == "connected"
    assert payload["metadata_json"]["connected_username"] == "reader"
    assert payload["metadata_json"]["connected_user_id"] == "12345"
    assert payload["metadata_json"]["can_write"] is True


def test_zotero_connection_uses_user_id_when_library_id_is_omitted(
    authenticated_client: TestClient,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "app.integrations.zotero.ZoteroClient.get_current_key_info",
        lambda self: type(
            "KeyInfo",
            (),
            {
                "user_id": 12345,
                "username": "reader",
                "access": {"user": {"library": True, "write": True}},
            },
        )(),
    )

    saved = authenticated_client.post(
        "/api/connections/zotero",
        json={
            "label": "Primary Zotero",
            "payload": {"api_key": "secret-token", "library_id": "", "library_type": "users"},
            "metadata_json": {"library_type": "users"},
        },
    )
    assert saved.status_code == 201
    payload = saved.json()
    assert payload["status"] == "connected"
    assert payload["metadata_json"]["library_id"] == "12345"

    with get_session_factory()() as db:
        stored_payload = ConnectionService(db).get_payload(ConnectionProvider.ZOTERO)

    assert stored_payload is not None
    assert stored_payload["library_id"] == "12345"


def test_zotero_connection_resolves_my_library_alias_to_user_id(
    authenticated_client: TestClient,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "app.integrations.zotero.ZoteroClient.get_current_key_info",
        lambda self: type(
            "KeyInfo",
            (),
            {
                "user_id": 12345,
                "username": "reader",
                "access": {"user": {"library": True, "write": True}},
            },
        )(),
    )

    saved = authenticated_client.post(
        "/api/connections/zotero",
        json={
            "label": "Primary Zotero",
            "payload": {"api_key": "secret-token", "library_id": "My Library", "library_type": "users"},
            "metadata_json": {"library_type": "users"},
        },
    )
    assert saved.status_code == 201
    payload = saved.json()
    assert payload["status"] == "connected"
    assert payload["metadata_json"]["library_id"] == "12345"

    with get_session_factory()() as db:
        stored_payload = ConnectionService(db).get_payload(ConnectionProvider.ZOTERO)

    assert stored_payload is not None
    assert stored_payload["library_id"] == "12345"


def test_zotero_connection_allows_clearing_a_saved_library_id_to_re_resolve_user_library(
    authenticated_client: TestClient,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "app.integrations.zotero.ZoteroClient.get_current_key_info",
        lambda self: type(
            "KeyInfo",
            (),
            {
                "user_id": 12345,
                "username": "reader",
                "access": {"user": {"library": True, "write": True}},
            },
        )(),
    )

    first = authenticated_client.post(
        "/api/connections/zotero",
        json={
            "label": "Primary Zotero",
            "payload": {"api_key": "secret-token", "library_id": "99999", "library_type": "users"},
            "metadata_json": {"library_type": "users"},
        },
    )
    assert first.status_code == 201
    assert first.json()["status"] == "error"

    second = authenticated_client.post(
        "/api/connections/zotero",
        json={
            "label": "Primary Zotero",
            "payload": {"api_key": "", "library_id": "", "library_type": "users"},
            "metadata_json": {"library_type": "users"},
        },
    )
    assert second.status_code == 201
    payload = second.json()
    assert payload["status"] == "connected"
    assert payload["metadata_json"]["library_id"] == "12345"

    with get_session_factory()() as db:
        stored_payload = ConnectionService(db).get_payload(ConnectionProvider.ZOTERO)

    assert stored_payload is not None
    assert stored_payload["library_id"] == "12345"


def test_zotero_connection_marks_mismatched_user_library_as_error(
    authenticated_client: TestClient,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "app.integrations.zotero.ZoteroClient.get_current_key_info",
        lambda self: type(
            "KeyInfo",
            (),
            {
                "user_id": 99999,
                "username": "reader",
                "access": {"user": {"library": True, "write": True}},
            },
        )(),
    )

    saved = authenticated_client.post(
        "/api/connections/zotero",
        json={
            "label": "Primary Zotero",
            "payload": {"api_key": "secret-token", "library_id": "12345"},
            "metadata_json": {"library_type": "users"},
        },
    )
    assert saved.status_code == 201
    payload = saved.json()
    assert payload["status"] == "error"
    assert "configured library ID is 12345" in payload["metadata_json"]["last_error"]


def test_zotero_connection_reuses_existing_secret_when_api_key_is_omitted(
    authenticated_client: TestClient,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "app.integrations.zotero.ZoteroClient.get_current_key_info",
        lambda self: type(
            "KeyInfo",
            (),
            {
                "user_id": 12345,
                "username": "reader",
                "access": {"user": {"library": True, "write": True}},
            },
        )(),
    )

    first = authenticated_client.post(
        "/api/connections/zotero",
        json={
            "label": "Primary Zotero",
            "payload": {"api_key": "secret-token", "library_id": "12345"},
            "metadata_json": {"library_type": "users"},
        },
    )
    assert first.status_code == 201

    second = authenticated_client.post(
        "/api/connections/zotero",
        json={
            "label": "Primary Zotero",
            "payload": {"api_key": "", "library_id": "12345", "library_type": "users"},
            "metadata_json": {
                "library_type": "users",
                "collection_name": "Research Center / Papers",
                "auto_tag_vocabulary": ["area/agents", "method/tool_use"],
            },
        },
    )
    assert second.status_code == 201
    second_payload = second.json()
    assert second_payload["status"] == "connected"
    assert second_payload["metadata_json"]["collection_name"] == "Research Center / Papers"
    assert second_payload["metadata_json"]["auto_tag_vocabulary"] == ["area/agents", "method/tool_use"]

    with get_session_factory()() as db:
        payload = ConnectionService(db).get_payload(ConnectionProvider.ZOTERO)

    assert payload is not None
    assert payload["api_key"] == "secret-token"


def test_gmail_oauth_start_redirects_to_google(authenticated_client: TestClient) -> None:
    response = authenticated_client.get("/api/connections/gmail/oauth/start", follow_redirects=False)
    assert response.status_code == 307
    location = response.headers["location"]
    parsed = urlparse(location)
    params = parse_qs(parsed.query)
    assert parsed.scheme == "https"
    assert "accounts.google.com" in parsed.netloc
    assert params["client_id"] == ["gmail-client-id"]
    assert "state" in params


def test_gmail_oauth_callback_persists_connection(
    authenticated_client: TestClient,
    monkeypatch,
) -> None:
    start = authenticated_client.get("/api/connections/gmail/oauth/start", follow_redirects=False)
    state = parse_qs(urlparse(start.headers["location"]).query)["state"][0]

    monkeypatch.setattr(
        "app.integrations.gmail_oauth.GmailOAuthClient.exchange_code",
        lambda self, *, code, redirect_uri: {
            "access_token": "gmail-access-token",
            "refresh_token": "gmail-refresh-token",
            "scope": "https://www.googleapis.com/auth/gmail.readonly",
            "token_type": "Bearer",
            "expires_at": "2030-01-01T00:00:00+00:00",
        },
    )
    monkeypatch.setattr(
        "app.integrations.gmail_oauth.GmailOAuthClient.fetch_profile",
        lambda self, access_token: {"emailAddress": "reader@example.com"},
    )

    callback = authenticated_client.get(
        "/api/connections/gmail/oauth/callback",
        params={"code": "test-code", "state": state},
        follow_redirects=False,
    )
    assert callback.status_code == 307
    assert callback.headers["location"] == "http://localhost:5173/connections?gmail=connected"

    connection = authenticated_client.get("/api/connections/gmail")
    assert connection.status_code == 200
    payload = connection.json()
    assert payload["status"] == "connected"
    assert payload["metadata_json"]["connected_email"] == "reader@example.com"

    settings_only = authenticated_client.post(
        "/api/connections/gmail",
        json={
            "label": "Primary Gmail",
            "payload": {},
            "metadata_json": {"senders": ["alerts@example.com"], "labels": ["research-newsletters"]},
        },
    )
    assert settings_only.status_code == 201
    updated = settings_only.json()
    assert updated["status"] == "connected"
    assert updated["metadata_json"]["senders"] == ["alerts@example.com"]


def test_gmail_app_password_connection_can_be_saved(
    authenticated_client: TestClient,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "app.services.connections.GmailImapConnector.test_connection",
        lambda self: None,
    )

    saved = authenticated_client.post(
        "/api/connections/gmail",
        json={
            "label": "Primary Gmail",
            "payload": {
                "auth_mode": "app_password",
                "email": "reader@example.com",
                "app_password": "gmail-app-password",
            },
            "metadata_json": {"senders": ["alerts@example.com"], "labels": ["research-newsletters"]},
        },
    )
    assert saved.status_code == 201
    payload = saved.json()
    assert payload["status"] == "connected"
    assert payload["metadata_json"]["auth_mode"] == "app_password"
    assert payload["metadata_json"]["connected_email"] == "reader@example.com"


def test_gmail_app_password_connection_reuses_existing_secret_when_password_is_blank(
    authenticated_client: TestClient,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "app.services.connections.GmailImapConnector.test_connection",
        lambda self: None,
    )

    first = authenticated_client.post(
        "/api/connections/gmail",
        json={
            "label": "Primary Gmail",
            "payload": {
                "auth_mode": "app_password",
                "email": "reader@example.com",
                "app_password": "gmail-app-password",
            },
            "metadata_json": {"labels": ["research-newsletters"]},
        },
    )
    assert first.status_code == 201

    second = authenticated_client.post(
        "/api/connections/gmail",
        json={
            "label": "Primary Gmail",
            "payload": {
                "auth_mode": "app_password",
                "email": "reader@example.com",
                "app_password": "",
            },
            "metadata_json": {"senders": ["alerts@example.com"]},
        },
    )
    assert second.status_code == 201
    updated = second.json()
    assert updated["status"] == "connected"
    assert updated["metadata_json"]["senders"] == ["alerts@example.com"]

    with get_session_factory()() as db:
        stored_payload = ConnectionService(db).get_payload(ConnectionProvider.GMAIL)

    assert stored_payload is not None
    assert stored_payload["auth_mode"] == "app_password"
    assert stored_payload["app_password"] == "gmail-app-password"
