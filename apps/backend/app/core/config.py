from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

BACKEND_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=BACKEND_ROOT / ".env", env_file_encoding="utf-8", extra="ignore"
    )

    app_env: Literal["development", "test", "production"] = "development"
    app_name: str = "Research Center"
    api_prefix: str = "/api"
    frontend_origin: str = "http://localhost:5173"
    database_url: str = "sqlite+pysqlite:///./research_center.db"
    celery_broker_url: str = "redis://localhost:6379/0"
    celery_result_backend: str = "redis://localhost:6379/1"
    secret_key: str = "change-me"
    encryption_key: str = "change-me"
    admin_email: str = "admin@example.com"
    admin_password: str = "change-me"
    login_rate_limit_max_attempts: int = Field(default=5, ge=1, le=100)
    login_rate_limit_window_minutes: int = Field(default=15, ge=1, le=1440)
    login_rate_limit_lockout_minutes: int = Field(default=30, ge=1, le=10080)
    auto_create_schema: bool = True
    seed_demo_data: bool = True
    timezone: str = "Europe/Zurich"
    gemini_api_key: str | None = None
    gemini_model: str = "gemini-2.5-flash"
    gemini_base_url: str = "https://generativelanguage.googleapis.com/v1beta"
    gemini_timeout_seconds: int = 30
    ai_daily_cost_limit_usd: float = Field(default=10.0, ge=0.0, le=100000.0)
    ai_budget_reservation_ttl_minutes: int = Field(default=120, ge=1, le=1440)
    llm_input_token_cost_per_million_usd: float = Field(default=0.3, ge=0.0)
    llm_output_token_cost_per_million_usd: float = Field(default=2.5, ge=0.0)
    llm_total_token_cost_per_million_usd: float = Field(default=0.3, ge=0.0)
    google_application_credentials: str | None = None
    google_cloud_tts_credentials_json: str | None = None
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
    worker_metrics_host: str = "127.0.0.1"
    worker_metrics_port: int | None = Field(default=None, ge=1, le=65535)
    database_backup_dir: Path = BACKEND_ROOT / ".backups" / "database"
    database_backup_retention_count: int = Field(default=14, ge=1, le=365)
    digest_default_hour: int = Field(default=7, ge=0, le=23)
    digest_default_minute: int = Field(default=0, ge=0, le=59)
    session_cookie_name: str = "research_center_session"
    audio_cache_dir: Path = BACKEND_ROOT / ".cache" / "audio_briefs"

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
