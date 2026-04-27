from datetime import timedelta
from functools import lru_cache
from os import path as os_path
from pathlib import Path
from typing import Any, Literal

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine.url import make_url

BACKEND_ROOT = Path(__file__).resolve().parents[2]
REPO_ROOT = BACKEND_ROOT.parent.parent


def _default_vault_root_dir() -> Path:
    return REPO_ROOT / "vault"


def _default_local_state_dir() -> Path:
    return BACKEND_ROOT / ".local-state"


def _default_codex_add_dirs() -> list[Path]:
    return [BACKEND_ROOT]


def _normalize_database_url(database_url: str) -> str:
    if not database_url.startswith("sqlite"):
        return database_url

    url = make_url(database_url)
    database = url.database
    if not database or database == ":memory:":
        return database_url

    database_path = Path(database).expanduser()
    if not database_path.is_absolute():
        database_path = (REPO_ROOT / database_path).resolve()

    return url.set(database=str(database_path)).render_as_string(hide_password=False)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=BACKEND_ROOT / ".env", env_file_encoding="utf-8", extra="ignore"
    )

    app_env: Literal["development", "test", "production"] = "development"
    app_name: str = "Research Center"
    api_prefix: str = "/api"
    frontend_origin: str = "http://localhost:5173"
    database_url: str = "sqlite+pysqlite:///./research_center.db"
    secret_key: str = "change-me"
    encryption_key: str = "change-me"
    admin_email: str = "admin@example.com"
    admin_password: str = "change-me"
    login_rate_limit_max_attempts: int = Field(default=5, ge=1, le=100)
    login_rate_limit_window_minutes: int = Field(default=15, ge=1, le=1440)
    login_rate_limit_lockout_minutes: int = Field(default=30, ge=1, le=10080)
    auto_create_schema: bool = True
    seed_demo_data: bool = False
    timezone: str = "Europe/Zurich"
    gemini_api_key: str | None = None
    gemini_model: str = "gemini-2.5-flash"
    gemini_base_url: str = "https://generativelanguage.googleapis.com/v1beta"
    gemini_timeout_seconds: int = 30
    ollama_base_url: str = "http://localhost:11434"
    ollama_model: str = "gemma4:e2b"
    ollama_timeout_seconds: int = 45
    ai_daily_cost_limit_usd: float = Field(default=10.0, ge=0.0, le=100000.0)
    ai_budget_reservation_ttl_minutes: int = Field(default=120, ge=1, le=1440)
    ai_trace_retention_days: int = Field(default=30, ge=1, le=3650)
    llm_input_token_cost_per_million_usd: float = Field(default=0.3, ge=0.0)
    llm_output_token_cost_per_million_usd: float = Field(default=2.5, ge=0.0)
    llm_total_token_cost_per_million_usd: float = Field(default=0.3, ge=0.0)
    google_application_credentials: str | None = None
    google_cloud_tts_credentials_json: str | None = None
    gmail_ingest_email: str | None = None
    gmail_ingest_app_password: str | None = None
    gmail_ingest_access_token: str | None = None
    google_tts_api_base_url: str = "https://texttospeech.googleapis.com/v1"
    google_oauth_token_url: str = "https://oauth2.googleapis.com/token"
    google_tts_timeout_seconds: int = 60
    google_tts_language_code: str = "en-US"
    google_tts_voice_name: str | None = "en-US-Studio-O"
    google_tts_pricing_tier: Literal[
        "auto",
        "standard",
        "wavenet",
        "neural2",
        "polyglot",
        "studio",
        "chirp_hd",
        "instant_custom",
    ] = "auto"
    google_tts_standard_cost_per_million_chars_usd: float = Field(default=4.0, ge=0.0)
    google_tts_wavenet_cost_per_million_chars_usd: float = Field(default=4.0, ge=0.0)
    google_tts_neural2_cost_per_million_chars_usd: float = Field(default=16.0, ge=0.0)
    google_tts_polyglot_cost_per_million_chars_usd: float = Field(default=16.0, ge=0.0)
    google_tts_studio_cost_per_million_chars_usd: float = Field(default=160.0, ge=0.0)
    google_tts_chirp_hd_cost_per_million_chars_usd: float = Field(default=30.0, ge=0.0)
    google_tts_instant_custom_cost_per_million_chars_usd: float = Field(default=60.0, ge=0.0)
    google_tts_ssml_gender: Literal["SSML_VOICE_GENDER_UNSPECIFIED", "MALE", "FEMALE"] = "FEMALE"
    google_tts_audio_encoding: Literal["MP3", "OGG_OPUS", "LINEAR16"] = "MP3"
    google_tts_speaking_rate: float = Field(default=1.0, ge=0.25, le=2.0)
    google_tts_pitch: float = Field(default=0.0, ge=-20.0, le=20.0)
    gmail_oauth_client_id: str | None = None
    gmail_oauth_client_secret: str | None = None
    sentence_transformer_model: str = "sentence-transformers/all-MiniLM-L6-v2"
    enable_embeddings: bool = False
    sentry_dsn: str | None = None
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    log_format: Literal["auto", "text", "json"] = "auto"
    metrics_enabled: bool = True
    metrics_path: str = "/metrics"
    metrics_token: str | None = None
    database_backup_dir: Path = BACKEND_ROOT / ".backups" / "database"
    database_backup_retention_count: int = Field(default=14, ge=1, le=365)
    digest_default_hour: int = Field(default=7, ge=0, le=23)
    digest_default_minute: int = Field(default=0, ge=0, le=59)
    session_cookie_name: str = "research_center_session"
    audio_cache_dir: Path = BACKEND_ROOT / ".cache" / "audio_briefs"
    vault_root_dir: Path = Field(default_factory=_default_vault_root_dir)
    local_state_dir: Path = Field(default_factory=_default_local_state_dir)
    default_job_lease_ttl_seconds: int = Field(default=600, ge=60, le=86400)
    local_server_base_url: str = "http://localhost:8000"
    hosted_viewer_url: str | None = None
    local_pairing_token_ttl_minutes: int = Field(default=30, ge=1, le=1440)
    local_control_token_max_age_days: int = Field(default=180, ge=1, le=3650)
    local_web_mode: Literal["local"] = "local"
    web_dist_dir: Path = BACKEND_ROOT.parent / "web" / "dist"
    published_web_dist_dir: Path = BACKEND_ROOT.parent / "web" / "dist-published"
    vault_source_pipelines_enabled: bool = True
    vault_git_enabled: bool = True
    vault_git_remote_name: str = "origin"
    vault_git_remote_url: str = "https://github.com/jschweiz/research-vault"
    vault_git_branch: str = "main"
    vault_git_commit_prefix: str = "vault-sync"
    codex_binary: str = "codex"
    codex_model: str | None = None
    codex_profile: str | None = None
    codex_timeout_minutes: int = Field(default=20, ge=1, le=240)
    codex_search_enabled: bool = True
    codex_add_dirs: list[Path] = Field(default_factory=_default_codex_add_dirs)
    codex_compile_batch_size: int = Field(default=12, ge=1, le=50)
    cloudkit_container_identifier: str | None = None
    cloudkit_environment: Literal["development", "production"] = "development"
    cloudkit_database: Literal["public"] = "public"
    cloudkit_api_token: str | None = None
    cloudkit_server_to_server_key_id: str | None = None
    cloudkit_server_to_server_private_key_pem: str | None = None
    cloudkit_record_type: str = "PublishedEdition"

    @model_validator(mode="before")
    @classmethod
    def normalize_blank_optional_values(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        normalized: dict[str, Any] = {}
        for key, value in data.items():
            field = cls.model_fields.get(key)
            if (
                field is not None
                and field.default is None
                and isinstance(value, str)
                and not value.strip()
            ):
                normalized[key] = None
            else:
                normalized[key] = value
        return normalized

    @property
    def is_production(self) -> bool:
        return self.app_env == "production"

    @property
    def use_json_logging(self) -> bool:
        if self.log_format == "json":
            return True
        if self.log_format == "text":
            return False
        return self.is_production

    @property
    def default_job_lease_ttl(self) -> timedelta:
        return timedelta(seconds=self.default_job_lease_ttl_seconds)

    @property
    def cloudkit_read_configured(self) -> bool:
        return bool(self.cloudkit_container_identifier and self.cloudkit_api_token)

    @property
    def cloudkit_write_configured(self) -> bool:
        return bool(
            self.cloudkit_read_configured
            and self.cloudkit_server_to_server_key_id
            and self.cloudkit_server_to_server_private_key_pem
        )

    @field_validator("codex_add_dirs", mode="before")
    @classmethod
    def parse_codex_add_dirs(cls, value: Any) -> Any:
        if value is None:
            return _default_codex_add_dirs()
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return _default_codex_add_dirs()
            if stripped.startswith("["):
                return stripped
            return [
                Path(part.strip())
                for chunk in stripped.splitlines()
                for part in chunk.split(",")
                if part.strip()
            ]
        return value

    @model_validator(mode="after")
    def normalize_paths(self) -> "Settings":
        self.database_url = _normalize_database_url(self.database_url)
        for field_name in (
            "audio_cache_dir",
            "database_backup_dir",
            "vault_root_dir",
            "local_state_dir",
            "web_dist_dir",
            "published_web_dist_dir",
        ):
            value = getattr(self, field_name)
            normalized = value.expanduser()
            if not normalized.is_absolute():
                base = REPO_ROOT if field_name in {"vault_root_dir", "local_state_dir"} else BACKEND_ROOT
                normalized = (base / normalized).resolve()
            setattr(self, field_name, normalized)
        normalized_codex_dirs: list[Path] = []
        for path in self.codex_add_dirs:
            normalized = path.expanduser()
            if not normalized.is_absolute():
                base = REPO_ROOT if not os_path.isabs(str(path)) else BACKEND_ROOT
                normalized = (base / normalized).resolve()
            normalized_codex_dirs.append(normalized)
        self.codex_add_dirs = normalized_codex_dirs or _default_codex_add_dirs()
        return self

    @model_validator(mode="after")
    def validate_production_security(self) -> "Settings":
        if not self.metrics_path.startswith("/"):
            raise ValueError("METRICS_PATH must start with '/'.")

        if not self.is_production:
            return self

        insecure_fields = [
            field_name
            for field_name, value in (
                ("SECRET_KEY", self.secret_key),
                ("ENCRYPTION_KEY", self.encryption_key),
                ("ADMIN_PASSWORD", self.admin_password),
            )
            if value == "change-me"
        ]
        if insecure_fields:
            field_list = ", ".join(insecure_fields)
            raise ValueError(
                f"Production settings must override default values for: {field_list}."
            )
        if self.frontend_origin.lower().startswith("http://"):
            raise ValueError("Production FRONTEND_ORIGIN must use https.")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
