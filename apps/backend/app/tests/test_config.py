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
