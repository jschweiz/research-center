from __future__ import annotations

from datetime import UTC, datetime, timedelta
from urllib.parse import urlencode

import httpx

from app.core.config import get_settings

AUTHORIZATION_ENDPOINT = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_ENDPOINT = "https://oauth2.googleapis.com/token"
GMAIL_SCOPE = "https://www.googleapis.com/auth/gmail.readonly"
PROFILE_ENDPOINT = "https://gmail.googleapis.com/gmail/v1/users/me/profile"


class GmailOAuthClient:
    def __init__(self) -> None:
        self.settings = get_settings()

    def is_configured(self) -> bool:
        return bool(self.settings.gmail_oauth_client_id and self.settings.gmail_oauth_client_secret)

    def build_authorization_url(self, *, redirect_uri: str, state: str) -> str:
        if not self.is_configured():
            raise RuntimeError("Gmail OAuth is not configured.")
        query = urlencode(
            {
                "client_id": self.settings.gmail_oauth_client_id,
                "redirect_uri": redirect_uri,
                "response_type": "code",
                "scope": GMAIL_SCOPE,
                "access_type": "offline",
                "include_granted_scopes": "true",
                "prompt": "consent",
                "state": state,
            }
        )
        return f"{AUTHORIZATION_ENDPOINT}?{query}"

    def exchange_code(self, *, code: str, redirect_uri: str) -> dict:
        if not self.is_configured():
            raise RuntimeError("Gmail OAuth is not configured.")
        response = httpx.post(
            TOKEN_ENDPOINT,
            data={
                "code": code,
                "client_id": self.settings.gmail_oauth_client_id,
                "client_secret": self.settings.gmail_oauth_client_secret,
                "redirect_uri": redirect_uri,
                "grant_type": "authorization_code",
            },
            timeout=20,
        )
        response.raise_for_status()
        return self._normalize_token_payload(response.json())

    def refresh_access_token(self, refresh_token: str) -> dict:
        if not self.is_configured():
            raise RuntimeError("Gmail OAuth is not configured.")
        response = httpx.post(
            TOKEN_ENDPOINT,
            data={
                "client_id": self.settings.gmail_oauth_client_id,
                "client_secret": self.settings.gmail_oauth_client_secret,
                "refresh_token": refresh_token,
                "grant_type": "refresh_token",
            },
            timeout=20,
        )
        response.raise_for_status()
        payload = self._normalize_token_payload(response.json())
        payload["refresh_token"] = payload.get("refresh_token") or refresh_token
        return payload

    def fetch_profile(self, access_token: str) -> dict:
        response = httpx.get(
            PROFILE_ENDPOINT,
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=20,
        )
        response.raise_for_status()
        return response.json()

    def _normalize_token_payload(self, payload: dict) -> dict:
        expires_in = int(payload.get("expires_in", 3600))
        expires_at = datetime.now(UTC) + timedelta(seconds=expires_in)
        normalized = {
            "access_token": payload.get("access_token"),
            "refresh_token": payload.get("refresh_token"),
            "token_type": payload.get("token_type", "Bearer"),
            "scope": payload.get("scope", GMAIL_SCOPE),
            "expires_at": expires_at.isoformat(),
        }
        return {key: value for key, value in normalized.items() if value is not None}
