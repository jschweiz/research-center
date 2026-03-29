import hmac
from datetime import UTC, datetime

from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

from app.core.config import Settings, get_settings


class SessionManager:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.serializer = self._serializer("research-center-session")

    def _serializer(self, salt: str) -> URLSafeTimedSerializer:
        return URLSafeTimedSerializer(self.settings.secret_key, salt=salt)

    def verify_credentials(self, email: str, password: str) -> bool:
        email_ok = hmac.compare_digest(email.lower(), self.settings.admin_email.lower())
        password_ok = hmac.compare_digest(password, self.settings.admin_password)
        return email_ok and password_ok

    def issue_token(self, email: str) -> str:
        return self.serializer.dumps(
            {
                "email": email.lower(),
                "issued_at": datetime.now(tz=UTC).isoformat(),
            }
        )

    def load_token(self, token: str, max_age_seconds: int = 60 * 60 * 24 * 30) -> str | None:
        try:
            payload = self.serializer.loads(token, max_age=max_age_seconds)
        except (BadSignature, SignatureExpired):
            return None
        email = str(payload.get("email", "")).lower()
        if email != self.settings.admin_email.lower():
            return None
        return email

    def issue_scoped_token(self, payload: dict, *, salt: str) -> str:
        return self._serializer(salt).dumps(payload)

    def load_scoped_token(self, token: str, *, salt: str, max_age_seconds: int) -> dict | None:
        try:
            payload = self._serializer(salt).loads(token, max_age=max_age_seconds)
        except (BadSignature, SignatureExpired):
            return None
        return payload if isinstance(payload, dict) else None


def get_session_manager() -> SessionManager:
    return SessionManager(get_settings())
