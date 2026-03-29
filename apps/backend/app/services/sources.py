from __future__ import annotations

from sqlalchemy import select, update
from sqlalchemy.orm import Session, selectinload

from app.db.models import IngestionRun, Item, Source, SourceRule, SourceType
from app.schemas.sources import SourceCreate, SourceUpdate


class SourceService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def get_source(self, source_id: str) -> Source | None:
        return self.db.scalar(
            select(Source)
            .options(selectinload(Source.rules))
            .where(Source.id == source_id, Source.type != SourceType.MANUAL)
        )

    def list_sources(self, *, include_manual: bool = False) -> list[Source]:
        statement = select(Source)
        if not include_manual:
            statement = statement.where(Source.type != SourceType.MANUAL)
        statement = statement.order_by(Source.priority.desc(), Source.name.asc())
        return list(self.db.scalars(statement).all())

    def create_source(self, payload: SourceCreate) -> Source:
        source = Source(
            type=payload.type,
            name=payload.name,
            url=payload.url,
            query=payload.query,
            description=payload.description,
            active=payload.active,
            priority=payload.priority,
            tags=payload.tags,
            config_json=payload.config_json,
        )
        for rule in payload.rules:
            source.rules.append(
                SourceRule(rule_type=rule.rule_type, value=rule.value, active=rule.active)
            )

        self.db.add(source)
        self.db.commit()
        self.db.refresh(source)
        return source

    def update_source(self, source_id: str, payload: SourceUpdate) -> Source | None:
        source = self.db.get(Source, source_id)
        if not source:
            return None

        update_data = payload.model_dump(exclude_unset=True)
        rules = update_data.pop("rules", None)
        for field, value in update_data.items():
            setattr(source, field, value)

        if rules is not None:
            source.rules.clear()
            for rule in rules:
                source.rules.append(
                    SourceRule(
                        rule_type=rule["rule_type"],
                        value=rule["value"],
                        active=rule["active"],
                    )
                )

        self.db.add(source)
        self.db.commit()
        self.db.refresh(source)
        return source

    def delete_source(self, source_id: str) -> bool:
        source = self.db.get(Source, source_id)
        if not source:
            return False

        # Preserve historical runs and ingested items while removing the registry entry.
        self.db.execute(update(Item).where(Item.source_id == source_id).values(source_id=None))
        self.db.execute(
            update(IngestionRun).where(IngestionRun.source_id == source_id).values(source_id=None)
        )
        self.db.delete(source)
        self.db.commit()
        return True
