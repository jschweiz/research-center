from __future__ import annotations

from datetime import time

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.models import DataMode, ProfileSetting
from app.schemas.profile import ProfileUpdate

DEFAULT_RANKING_WEIGHTS = {
    "relevance": 0.3,
    "novelty": 0.15,
    "source_quality": 0.15,
    "author_match": 0.1,
    "topic_match": 0.15,
    "zotero_affinity": 0.15,
}


class ProfileService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.settings = get_settings()

    def get_profile(self) -> ProfileSetting:
        profile = self.db.scalar(select(ProfileSetting).limit(1))
        if profile:
            return profile
        profile = ProfileSetting(
            favorite_topics=["language models", "evaluation", "reasoning", "research tooling"],
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
        )
        self.db.add(profile)
        self.db.commit()
        self.db.refresh(profile)
        return profile

    def update_profile(self, payload: ProfileUpdate) -> ProfileSetting:
        profile = self.get_profile()
        update_data = payload.model_dump(exclude_unset=True)
        original_timezone = profile.timezone
        for field, value in update_data.items():
            setattr(profile, field, value)
        self.db.add(profile)
        self.db.commit()
        if "timezone" in update_data and profile.timezone != original_timezone:
            from app.services.briefs import BriefService

            BriefService(self.db).purge_cached_digests()
            self.db.refresh(profile)
        self.db.refresh(profile)
        return profile
