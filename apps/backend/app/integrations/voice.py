from __future__ import annotations

import hashlib
import json
import logging
import os
from base64 import b64decode, urlsafe_b64encode
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from app.core.config import get_settings
from app.services.ai_budget import AIBudgetService

ADC_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
JWT_BEARER_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:jwt-bearer"
logger = logging.getLogger(__name__)


def _base64url_encode(raw: bytes) -> str:
    return urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


class VoiceClient:
    provider_name = "google-cloud"

    def __init__(self) -> None:
        self.settings = get_settings()
        self.budget_service = AIBudgetService()
        self._cached_access_token: str | None = None
        self._cached_access_token_expiry: datetime | None = None

    @property
    def configured(self) -> bool:
        return self._load_google_credentials() is not None

    @property
    def voice_name(self) -> str:
        configured_name = (self.settings.google_tts_voice_name or "").strip()
        if configured_name:
            return configured_name
        gender = self.settings.google_tts_ssml_gender.lower()
        if gender == "ssml_voice_gender_unspecified":
            return self.settings.google_tts_language_code
        return f"{self.settings.google_tts_language_code} {gender}"

    @property
    def pricing_tier(self) -> str:
        configured_tier = (self.settings.google_tts_pricing_tier or "auto").strip().lower()
        if configured_tier and configured_tier != "auto":
            return configured_tier
        normalized_name = "".join(character for character in self.voice_name.lower() if character.isalnum())
        if "instantcustom" in normalized_name:
            return "instant_custom"
        if "chirp" in normalized_name and "hd" in normalized_name:
            return "chirp_hd"
        if "studio" in normalized_name:
            return "studio"
        if "polyglot" in normalized_name:
            return "polyglot"
        if "neural2" in normalized_name:
            return "neural2"
        if "wavenet" in normalized_name:
            return "wavenet"
        return "standard"

    def tts_cost_rate_per_million_chars_usd(self) -> float:
        return {
            "standard": self.settings.google_tts_standard_cost_per_million_chars_usd,
            "wavenet": self.settings.google_tts_wavenet_cost_per_million_chars_usd,
            "neural2": self.settings.google_tts_neural2_cost_per_million_chars_usd,
            "polyglot": self.settings.google_tts_polyglot_cost_per_million_chars_usd,
            "studio": self.settings.google_tts_studio_cost_per_million_chars_usd,
            "chirp_hd": self.settings.google_tts_chirp_hd_cost_per_million_chars_usd,
            "instant_custom": self.settings.google_tts_instant_custom_cost_per_million_chars_usd,
        }.get(self.pricing_tier, self.settings.google_tts_standard_cost_per_million_chars_usd)

    def estimate_character_count(self, script: str | None) -> int:
        raw_script = script or ""
        if not raw_script.strip():
            return 0
        return len(raw_script)

    def estimate_synthesis_cost_usd(self, script: str | None) -> float:
        character_count = self.estimate_character_count(script)
        if character_count == 0:
            return 0.0
        return round((character_count / 1_000_000) * self.tts_cost_rate_per_million_chars_usd(), 6)

    @property
    def output_format(self) -> str:
        encoding = self.settings.google_tts_audio_encoding
        return {
            "MP3": "mp3",
            "OGG_OPUS": "ogg",
            "LINEAR16": "wav",
        }.get(encoding, "bin")

    @property
    def media_type(self) -> str:
        encoding = self.settings.google_tts_audio_encoding
        return {
            "MP3": "audio/mpeg",
            "OGG_OPUS": "audio/ogg",
            "LINEAR16": "audio/wav",
        }.get(encoding, "application/octet-stream")

    @property
    def synthesis_cache_fingerprint(self) -> str:
        cache_inputs = {
            "language_code": self.settings.google_tts_language_code,
            "voice_name": self.voice_name,
            "ssml_gender": self.settings.google_tts_ssml_gender,
            "audio_encoding": self.settings.google_tts_audio_encoding,
            "speaking_rate": self.settings.google_tts_speaking_rate,
            "pitch": self.settings.google_tts_pitch,
        }
        encoded = json.dumps(cache_inputs, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()[:12]

    def cache_key_for_digest(self, digest_id: str) -> str:
        return f"{digest_id}-{self.synthesis_cache_fingerprint}"

    def cache_path_for_digest(self, digest_id: str) -> Path:
        return self.settings.audio_cache_dir / f"{self.cache_key_for_digest(digest_id)}.{self.output_format}"

    def clear_cached_audio(self, digest_id: str) -> None:
        cache_dir = self.settings.audio_cache_dir
        if not cache_dir.exists():
            return
        for path in cache_dir.glob(f"{digest_id}*"):
            if not path.is_file():
                continue
            stem = path.stem
            if stem == digest_id or stem.startswith(f"{digest_id}-"):
                path.unlink()

    def ensure_cached_audio(self, digest_id: str, script: str) -> Path:
        path = self.cache_path_for_digest(digest_id)
        if path.exists() and path.stat().st_size > 0:
            return path
        audio_bytes = self.synthesize_to_bytes(script)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(audio_bytes)
        return path

    def synthesize_to_bytes(self, script: str) -> bytes:
        if not self.configured:
            raise RuntimeError(
                "Google Cloud TTS is not configured. Set GOOGLE_CLOUD_TTS_CREDENTIALS_JSON, "
                "GOOGLE_APPLICATION_CREDENTIALS, or run gcloud auth application-default login."
            )
        if not script.strip():
            raise RuntimeError("Audio summary script is empty.")

        character_count = self.estimate_character_count(script)
        estimated_cost_usd = self.estimate_synthesis_cost_usd(script)
        reservation = self.budget_service.reserve_estimated_cost(
            provider=self.provider_name,
            operation="synthesize_audio",
            estimated_cost_usd=estimated_cost_usd,
            metadata={
                "voice_name": self.voice_name,
                "pricing_tier": self.pricing_tier,
                "character_count": character_count,
            },
        )
        try:
            response = httpx.post(
                f"{self.settings.google_tts_api_base_url}/text:synthesize",
                headers={
                    "Authorization": f"Bearer {self._get_access_token()}",
                    "Content-Type": "application/json",
                },
                json=self._build_synthesis_payload(script),
                timeout=self.settings.google_tts_timeout_seconds,
            )
            response.raise_for_status()
            payload = response.json()
            audio_content = payload.get("audioContent")
            if not isinstance(audio_content, str) or not audio_content.strip():
                raise RuntimeError("Google Cloud TTS returned an empty audio payload.")
            self.budget_service.consume_reservation(
                reservation,
                actual_cost_usd=estimated_cost_usd,
            )
            reservation = None
            return self._decode_audio_content(audio_content)
        except Exception:
            if reservation is not None:
                self.budget_service.release_reservation(reservation)
            logger.exception(
                "voice.synthesis.failed",
                extra={
                    "voice_name": self.voice_name,
                    "pricing_tier": self.pricing_tier,
                    "character_count": character_count,
                },
            )
            raise

    def _build_synthesis_payload(self, script: str) -> dict[str, Any]:
        voice: dict[str, Any] = {
            "languageCode": self.settings.google_tts_language_code,
            "ssmlGender": self.settings.google_tts_ssml_gender,
        }
        configured_name = (self.settings.google_tts_voice_name or "").strip()
        if configured_name:
            voice["name"] = configured_name
        return {
            "input": {"text": script},
            "voice": voice,
            "audioConfig": {
                "audioEncoding": self.settings.google_tts_audio_encoding,
                "speakingRate": self.settings.google_tts_speaking_rate,
                "pitch": self.settings.google_tts_pitch,
            },
        }

    def _get_access_token(self) -> str:
        if (
            self._cached_access_token
            and self._cached_access_token_expiry
            and datetime.now(UTC) < self._cached_access_token_expiry
        ):
            return self._cached_access_token

        credentials = self._load_google_credentials()
        if credentials is None:
            raise RuntimeError(
                "Google Cloud TTS credentials were not found. Configure Google ADC or provide "
                "GOOGLE_CLOUD_TTS_CREDENTIALS_JSON."
            )

        credential_type = str(credentials.get("type") or "")
        if credential_type == "service_account":
            token_payload = self._exchange_service_account_credentials(credentials)
        elif credential_type == "authorized_user":
            token_payload = self._refresh_authorized_user_credentials(credentials)
        else:
            raise RuntimeError(
                f"Unsupported Google credential type: {credential_type or 'unknown'}."
            )

        access_token = token_payload.get("access_token")
        if not isinstance(access_token, str) or not access_token.strip():
            raise RuntimeError("Google OAuth token response did not include an access token.")
        expires_in = int(token_payload.get("expires_in", 3600))
        self._cached_access_token = access_token
        self._cached_access_token_expiry = datetime.now(UTC) + timedelta(
            seconds=max(30, expires_in - 60)
        )
        return access_token

    def _load_google_credentials(self) -> dict[str, Any] | None:
        inline_json = (self.settings.google_cloud_tts_credentials_json or "").strip()
        if inline_json:
            payload = json.loads(inline_json)
            if isinstance(payload, dict):
                return payload
            raise RuntimeError("GOOGLE_CLOUD_TTS_CREDENTIALS_JSON must contain a JSON object.")

        credentials_path = (
            self.settings.google_application_credentials
            or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        )
        if credentials_path:
            return self._read_credentials_file(Path(credentials_path))

        default_adc_path = (
            Path.home() / ".config" / "gcloud" / "application_default_credentials.json"
        )
        if default_adc_path.exists():
            return self._read_credentials_file(default_adc_path)

        return None

    def _read_credentials_file(self, path: Path) -> dict[str, Any]:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return payload
        raise RuntimeError(f"Credential file at {path} must contain a JSON object.")

    def _exchange_service_account_credentials(self, credentials: dict[str, Any]) -> dict[str, Any]:
        client_email = str(credentials.get("client_email") or "").strip()
        private_key = str(credentials.get("private_key") or "").strip()
        token_uri = str(
            credentials.get("token_uri") or self.settings.google_oauth_token_url
        ).strip()
        if not client_email or not private_key:
            raise RuntimeError(
                "Google service-account credentials are missing client_email or private_key."
            )

        now = int(datetime.now(UTC).timestamp())
        header = {"alg": "RS256", "typ": "JWT"}
        payload = {
            "iss": client_email,
            "scope": ADC_SCOPE,
            "aud": token_uri,
            "exp": now + 3600,
            "iat": now,
        }
        signing_input = ".".join(
            [
                _base64url_encode(json.dumps(header, separators=(",", ":")).encode("utf-8")),
                _base64url_encode(json.dumps(payload, separators=(",", ":")).encode("utf-8")),
            ]
        )
        signer = serialization.load_pem_private_key(private_key.encode("utf-8"), password=None)
        signature = signer.sign(
            signing_input.encode("utf-8"),
            padding.PKCS1v15(),
            hashes.SHA256(),
        )
        assertion = f"{signing_input}.{_base64url_encode(signature)}"

        response = httpx.post(
            token_uri,
            data={
                "grant_type": JWT_BEARER_GRANT_TYPE,
                "assertion": assertion,
            },
            timeout=self.settings.google_tts_timeout_seconds,
        )
        response.raise_for_status()
        return response.json()

    def _refresh_authorized_user_credentials(self, credentials: dict[str, Any]) -> dict[str, Any]:
        refresh_token = str(credentials.get("refresh_token") or "").strip()
        client_id = str(credentials.get("client_id") or "").strip()
        client_secret = str(credentials.get("client_secret") or "").strip()
        token_uri = str(
            credentials.get("token_uri") or self.settings.google_oauth_token_url
        ).strip()
        if not refresh_token or not client_id or not client_secret:
            raise RuntimeError(
                "Google authorized-user credentials are missing refresh_token, "
                "client_id, or client_secret."
            )

        response = httpx.post(
            token_uri,
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "refresh_token": refresh_token,
                "grant_type": "refresh_token",
            },
            timeout=self.settings.google_tts_timeout_seconds,
        )
        response.raise_for_status()
        return response.json()

    def _decode_audio_content(self, audio_content: str) -> bytes:
        padding_length = (-len(audio_content)) % 4
        return b64decode(audio_content + ("=" * padding_length))
