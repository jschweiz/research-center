from pathlib import Path

import pytest
from pydantic import ValidationError

from app.core.config import Settings


def test_settings_env_file_points_to_backend_root() -> None:
    env_file = Settings.model_config.get("env_file")

    assert isinstance(env_file, Path)
    assert env_file.is_absolute()
    assert env_file.name == ".env"
    assert env_file.parent.name == "backend"
    assert env_file.parent.parent.name == "apps"


def test_production_settings_reject_default_secrets() -> None:
    with pytest.raises(ValidationError):
        Settings(
            _env_file=None,
            app_env="production",
            secret_key="change-me",
            encryption_key="production-encryption",
            admin_password="not-change-me",
            frontend_origin="https://frontend.example.com",
        )


def test_production_settings_require_https_frontend_origin() -> None:
    with pytest.raises(ValidationError):
        Settings(
            _env_file=None,
            app_env="production",
            secret_key="production-secret",
            encryption_key="production-encryption",
            admin_password="not-change-me",
            frontend_origin="http://frontend.example.com",
        )


def test_metrics_path_must_start_with_slash() -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, metrics_path="metrics")


def test_default_vault_settings_target_repo_submodule() -> None:
    settings = Settings(_env_file=None)

    assert settings.vault_root_dir.is_absolute()
    assert settings.vault_root_dir.name == "vault"
    assert settings.vault_root_dir.parent.name == "research-center"
    assert settings.vault_git_remote_url == "https://github.com/jschweiz/research-vault"
    assert settings.vault_git_branch == "main"


def test_relative_vault_and_local_state_paths_resolve_from_repo_root() -> None:
    settings = Settings(
        _env_file=None,
        vault_root_dir=Path("vault"),
        local_state_dir=Path("apps/backend/.local-state"),
    )

    assert settings.vault_root_dir.is_absolute()
    assert settings.vault_root_dir.name == "vault"
    assert settings.local_state_dir.is_absolute()
    assert settings.local_state_dir.parts[-3:] == ("apps", "backend", ".local-state")


def test_blank_optional_env_values_are_treated_as_unset(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "GEMINI_API_KEY=",
                "GOOGLE_APPLICATION_CREDENTIALS=",
                "GOOGLE_CLOUD_TTS_CREDENTIALS_JSON=",
                "METRICS_TOKEN=",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    settings = Settings(
        _env_file=env_file,
        gemini_api_key=None,
        google_application_credentials=None,
        google_cloud_tts_credentials_json=None,
        metrics_token=None,
    )

    assert settings.gemini_api_key is None
    assert settings.google_application_credentials is None
    assert settings.google_cloud_tts_credentials_json is None
    assert settings.metrics_token is None
