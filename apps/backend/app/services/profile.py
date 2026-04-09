from __future__ import annotations

from datetime import UTC, datetime, time

from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.models import DataMode, ProfileSetting
from app.schemas.profile import (
    AlphaXivSearchSettings,
    AudioBriefSettings,
    BriefSectionSettings,
    ProfileRead,
    ProfileUpdate,
    PromptGuidanceSettings,
    RankingThresholdSettings,
)

DEFAULT_RANKING_WEIGHTS = {
    "relevance": 0.3,
    "novelty": 0.15,
    "source_quality": 0.15,
    "author_match": 0.1,
    "topic_match": 0.15,
    "zotero_affinity": 0.15,
}
DEFAULT_RANKING_THRESHOLDS = RankingThresholdSettings().model_dump()
DEFAULT_BRIEF_SECTIONS = BriefSectionSettings().model_dump()
DEFAULT_AUDIO_BRIEF_SETTINGS = AudioBriefSettings().model_dump()
DEFAULT_PROMPT_GUIDANCE = PromptGuidanceSettings().model_dump()
DEFAULT_ALPHAXIV_SEARCH_SETTINGS = AlphaXivSearchSettings().model_dump()
DEFAULT_FAVORITE_TOPICS = [
    "language models",
    "evaluation",
    "reasoning",
    "research tooling",
]
BRIEF_CACHE_FIELDS = {
    "timezone",
    "summary_depth",
    "brief_sections",
    "audio_brief_settings",
    "prompt_guidance",
}


def _normalize_ranking_weights(value: object) -> dict[str, float]:
    raw = value if isinstance(value, dict) else {}
    normalized = dict(DEFAULT_RANKING_WEIGHTS)
    for key, default in DEFAULT_RANKING_WEIGHTS.items():
        candidate = raw.get(key, default)
        try:
            parsed = float(candidate)
        except (TypeError, ValueError):
            parsed = default
        normalized[key] = round(max(parsed, 0.0), 4)
    return normalized


def _merge_nested_config(
    model: type[BaseModel],
    current: object,
    update: BaseModel | None,
) -> dict[str, object]:
    current_data = model.model_validate(current or {}).model_dump()
    if update is None:
        return current_data
    update_data = update.model_dump(exclude_unset=True)
    return model.model_validate(current_data | update_data).model_dump()


def _default_profile_read() -> ProfileRead:
    settings = get_settings()
    now = datetime.now(UTC)
    return ProfileRead(
        id="default-profile",
        favorite_topics=list(DEFAULT_FAVORITE_TOPICS),
        favorite_authors=[],
        favorite_sources=[],
        ignored_topics=[],
        digest_time=time(
            hour=settings.digest_default_hour,
            minute=settings.digest_default_minute,
        ),
        timezone=settings.timezone,
        data_mode=DataMode.SEED if settings.seed_demo_data else DataMode.LIVE,
        summary_depth="balanced",
        ranking_weights=DEFAULT_RANKING_WEIGHTS,
        ranking_thresholds=RankingThresholdSettings(),
        brief_sections=BriefSectionSettings(),
        audio_brief_settings=AudioBriefSettings(),
        prompt_guidance=PromptGuidanceSettings(),
        alphaxiv_search_settings=AlphaXivSearchSettings(),
        created_at=now,
        updated_at=now,
    )


def load_profile_snapshot() -> ProfileRead:
    from app.db.session import get_session_factory

    try:
        with get_session_factory()() as db:
            return ProfileRead.model_validate(ProfileService(db).get_profile())
    except Exception:
        return _default_profile_read()


class ProfileService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.settings = get_settings()

    def get_profile(self) -> ProfileSetting:
        profile = self.db.scalar(select(ProfileSetting).limit(1))
        if profile:
            normalized = False
            expected_weights = _normalize_ranking_weights(profile.ranking_weights)
            expected_thresholds = RankingThresholdSettings.model_validate(profile.ranking_thresholds or {}).model_dump()
            expected_brief_sections = BriefSectionSettings.model_validate(profile.brief_sections or {}).model_dump()
            expected_audio_settings = AudioBriefSettings.model_validate(profile.audio_brief_settings or {}).model_dump()
            expected_prompt_guidance = PromptGuidanceSettings.model_validate(profile.prompt_guidance or {}).model_dump()
            expected_alphaxiv_search_settings = AlphaXivSearchSettings.model_validate(
                profile.alphaxiv_search_settings or {}
            ).model_dump()
            if profile.ranking_weights != expected_weights:
                profile.ranking_weights = expected_weights
                normalized = True
            if profile.ranking_thresholds != expected_thresholds:
                profile.ranking_thresholds = expected_thresholds
                normalized = True
            if profile.brief_sections != expected_brief_sections:
                profile.brief_sections = expected_brief_sections
                normalized = True
            if profile.audio_brief_settings != expected_audio_settings:
                profile.audio_brief_settings = expected_audio_settings
                normalized = True
            if profile.prompt_guidance != expected_prompt_guidance:
                profile.prompt_guidance = expected_prompt_guidance
                normalized = True
            if profile.alphaxiv_search_settings != expected_alphaxiv_search_settings:
                profile.alphaxiv_search_settings = expected_alphaxiv_search_settings
                normalized = True
            if normalized:
                self.db.add(profile)
                self.db.commit()
                self.db.refresh(profile)
            return profile
        profile = ProfileSetting(
            favorite_topics=list(DEFAULT_FAVORITE_TOPICS),
            favorite_authors=[],
            favorite_sources=[],
            ignored_topics=[],
            digest_time=time(
                hour=self.settings.digest_default_hour, minute=self.settings.digest_default_minute
            ),
            timezone=self.settings.timezone,
            data_mode=DataMode.SEED if self.settings.seed_demo_data else DataMode.LIVE,
            summary_depth="balanced",
            ranking_weights=DEFAULT_RANKING_WEIGHTS,
            ranking_thresholds=DEFAULT_RANKING_THRESHOLDS,
            brief_sections=DEFAULT_BRIEF_SECTIONS,
            audio_brief_settings=DEFAULT_AUDIO_BRIEF_SETTINGS,
            prompt_guidance=DEFAULT_PROMPT_GUIDANCE,
            alphaxiv_search_settings=DEFAULT_ALPHAXIV_SEARCH_SETTINGS,
        )
        self.db.add(profile)
        self.db.commit()
        self.db.refresh(profile)
        return profile

    def update_profile(self, payload: ProfileUpdate) -> ProfileSetting:
        profile = self.get_profile()
        update_data = payload.model_dump(exclude_unset=True)
        original_timezone = profile.timezone
        changed_fields: set[str] = set(update_data)

        if "favorite_topics" in update_data:
            profile.favorite_topics = payload.favorite_topics or []
        if "favorite_authors" in update_data:
            profile.favorite_authors = payload.favorite_authors or []
        if "favorite_sources" in update_data:
            profile.favorite_sources = payload.favorite_sources or []
        if "ignored_topics" in update_data:
            profile.ignored_topics = payload.ignored_topics or []
        if "digest_time" in update_data:
            profile.digest_time = payload.digest_time or profile.digest_time
        if "timezone" in update_data:
            profile.timezone = payload.timezone or profile.timezone
        if "data_mode" in update_data:
            profile.data_mode = payload.data_mode or profile.data_mode
        if "summary_depth" in update_data:
            profile.summary_depth = payload.summary_depth or profile.summary_depth
        if "ranking_weights" in update_data:
            profile.ranking_weights = _normalize_ranking_weights(payload.ranking_weights)
        if "ranking_thresholds" in update_data:
            profile.ranking_thresholds = _merge_nested_config(
                RankingThresholdSettings,
                profile.ranking_thresholds,
                payload.ranking_thresholds,
            )
        if "brief_sections" in update_data:
            profile.brief_sections = _merge_nested_config(
                BriefSectionSettings,
                profile.brief_sections,
                payload.brief_sections,
            )
        if "audio_brief_settings" in update_data:
            profile.audio_brief_settings = _merge_nested_config(
                AudioBriefSettings,
                profile.audio_brief_settings,
                payload.audio_brief_settings,
            )
        if "prompt_guidance" in update_data:
            profile.prompt_guidance = _merge_nested_config(
                PromptGuidanceSettings,
                profile.prompt_guidance,
                payload.prompt_guidance,
            )
        if "alphaxiv_search_settings" in update_data:
            profile.alphaxiv_search_settings = _merge_nested_config(
                AlphaXivSearchSettings,
                profile.alphaxiv_search_settings,
                payload.alphaxiv_search_settings,
            )
        self.db.add(profile)
        self.db.commit()
        if (
            ("timezone" in changed_fields and profile.timezone != original_timezone)
            or BRIEF_CACHE_FIELDS.intersection(changed_fields)
        ):
            from app.services.briefs import BriefService

            BriefService().purge_cached_digests()
            self.db.refresh(profile)
        self.db.refresh(profile)
        return profile
