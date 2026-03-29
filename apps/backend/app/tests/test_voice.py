import base64
import json
from pathlib import Path

import pytest

from app.core.config import Settings, get_settings
from app.integrations.voice import VoiceClient
from app.services.ai_budget import AIBudgetExceededError


class _FakeTokenResponse:
    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return {
            "access_token": "test-google-access-token",
            "expires_in": 3600,
            "token_type": "Bearer",
        }


class _FakeSynthesisResponse:
    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return {
            "audioContent": base64.b64encode(b"voice-bytes").decode("ascii"),
        }


def test_voice_client_uses_google_adc_and_text_to_speech_endpoint(
    client,
    monkeypatch,
    tmp_path: Path,
) -> None:
    adc_dir = tmp_path / ".config" / "gcloud"
    adc_dir.mkdir(parents=True)
    (adc_dir / "application_default_credentials.json").write_text(
        json.dumps(
            {
                "type": "authorized_user",
                "client_id": "test-client-id",
                "client_secret": "test-client-secret",
                "refresh_token": "test-refresh-token",
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    monkeypatch.setenv("GOOGLE_CLOUD_TTS_CREDENTIALS_JSON", "")
    monkeypatch.setenv("GOOGLE_TTS_LANGUAGE_CODE", "en-US")
    monkeypatch.setenv("GOOGLE_TTS_VOICE_NAME", "en-US-Studio-O")
    monkeypatch.setenv("GOOGLE_TTS_SSML_GENDER", "FEMALE")
    monkeypatch.setenv("GOOGLE_TTS_AUDIO_ENCODING", "MP3")
    get_settings.cache_clear()

    captured: list[dict] = []

    def _fake_post(url, *, headers=None, json=None, data=None, timeout):
        captured.append(
            {
                "url": url,
                "headers": headers,
                "json": json,
                "data": data,
                "timeout": timeout,
            }
        )
        if url.endswith("/token"):
            return _FakeTokenResponse()
        return _FakeSynthesisResponse()

    monkeypatch.setattr("httpx.post", _fake_post)
    payload = VoiceClient().synthesize_to_bytes("A short research audio summary.")

    assert payload == b"voice-bytes"
    assert len(captured) == 2
    assert captured[0]["url"].endswith("/token")
    assert captured[0]["data"]["grant_type"] == "refresh_token"
    assert captured[0]["data"]["refresh_token"] == "test-refresh-token"
    assert captured[1]["url"].endswith("/text:synthesize")
    assert captured[1]["headers"]["Authorization"] == "Bearer test-google-access-token"
    assert captured[1]["json"]["voice"]["languageCode"] == "en-US"
    assert captured[1]["json"]["voice"]["name"] == "en-US-Studio-O"
    assert captured[1]["json"]["voice"]["ssmlGender"] == "FEMALE"
    assert captured[1]["json"]["audioConfig"]["audioEncoding"] == "MP3"

    get_settings.cache_clear()


def test_voice_client_cache_key_changes_when_voice_settings_change(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("AUDIO_CACHE_DIR", str(tmp_path))
    monkeypatch.setenv("GOOGLE_TTS_LANGUAGE_CODE", "en-US")
    monkeypatch.setenv("GOOGLE_TTS_VOICE_NAME", "en-US-Wavenet-H")
    monkeypatch.setenv("GOOGLE_TTS_SSML_GENDER", "FEMALE")
    monkeypatch.setenv("GOOGLE_TTS_AUDIO_ENCODING", "MP3")
    monkeypatch.setenv("GOOGLE_TTS_SPEAKING_RATE", "0.96")
    monkeypatch.setenv("GOOGLE_TTS_PITCH", "0.0")
    get_settings.cache_clear()

    wavenet_client = VoiceClient()
    wavenet_path = wavenet_client.cache_path_for_digest("digest-123")
    wavenet_path.parent.mkdir(parents=True, exist_ok=True)
    wavenet_path.write_bytes(b"wavenet-audio")

    monkeypatch.setenv("GOOGLE_TTS_VOICE_NAME", "en-US-Studio-O")
    get_settings.cache_clear()

    synthesis_calls: list[str] = []

    def _fake_synthesize(self, script: str) -> bytes:
        synthesis_calls.append(script)
        return b"studio-audio"

    monkeypatch.setattr(VoiceClient, "synthesize_to_bytes", _fake_synthesize)

    studio_client = VoiceClient()
    studio_path = studio_client.ensure_cached_audio("digest-123", "Morning brief.")

    assert studio_path != wavenet_path
    assert studio_path.read_bytes() == b"studio-audio"
    assert synthesis_calls == ["Morning brief."]
    assert wavenet_path.read_bytes() == b"wavenet-audio"

    studio_client.clear_cached_audio("digest-123")

    assert not wavenet_path.exists()
    assert not studio_path.exists()

    get_settings.cache_clear()


def test_settings_load_google_application_credentials_from_env_file(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "GOOGLE_APPLICATION_CREDENTIALS=/tmp/google-service-account.json\n",
        encoding="utf-8",
    )

    settings = Settings(_env_file=env_file)

    assert settings.google_application_credentials == "/tmp/google-service-account.json"


def test_voice_client_estimates_tts_cost_from_voice_tier(monkeypatch) -> None:
    monkeypatch.setenv("GOOGLE_TTS_VOICE_NAME", "en-US-Neural2-J")
    monkeypatch.setenv("GOOGLE_TTS_PRICING_TIER", "auto")
    get_settings.cache_clear()

    client = VoiceClient()

    assert client.pricing_tier == "neural2"
    assert client.estimate_character_count("Hello world.") == 12
    assert client.estimate_synthesis_cost_usd("Hello world.") == 0.000192

    get_settings.cache_clear()


def test_voice_client_blocks_provider_calls_when_daily_budget_is_exhausted(
    client,
    monkeypatch,
) -> None:
    monkeypatch.setenv(
        "GOOGLE_CLOUD_TTS_CREDENTIALS_JSON",
        json.dumps(
            {
                "type": "authorized_user",
                "client_id": "test-client-id",
                "client_secret": "test-client-secret",
                "refresh_token": "test-refresh-token",
            }
        ),
    )
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    monkeypatch.setenv("GOOGLE_TTS_LANGUAGE_CODE", "en-US")
    monkeypatch.setenv("GOOGLE_TTS_VOICE_NAME", "en-US-Studio-O")
    monkeypatch.setenv("GOOGLE_TTS_SSML_GENDER", "FEMALE")
    monkeypatch.setenv("GOOGLE_TTS_AUDIO_ENCODING", "MP3")
    monkeypatch.setenv("AI_DAILY_COST_LIMIT_USD", "0.0")
    get_settings.cache_clear()

    def _unexpected_post(*args, **kwargs):
        raise AssertionError("Google TTS should not be called once the daily AI budget is exhausted.")

    monkeypatch.setattr("httpx.post", _unexpected_post)

    with pytest.raises(AIBudgetExceededError):
        VoiceClient().synthesize_to_bytes("A short research audio summary.")

    get_settings.cache_clear()
