import base64
import hashlib

from cryptography.fernet import Fernet

from app.core.config import get_settings


def _build_key(secret: str) -> bytes:
    try:
        decoded = secret.encode("utf-8")
        Fernet(decoded)
        return decoded
    except Exception:
        digest = hashlib.sha256(secret.encode("utf-8")).digest()
        return base64.urlsafe_b64encode(digest)


def get_fernet() -> Fernet:
    settings = get_settings()
    return Fernet(_build_key(settings.encryption_key))
