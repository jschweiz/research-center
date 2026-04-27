from fastapi.testclient import TestClient
from sqlalchemy import select

from app.db.models import Source, SourceType
from app.db.session import get_session_factory
from app.services.default_sources import DEFAULT_SOURCE_SPECS, upsert_default_sources


def test_upsert_default_sources_is_idempotent_and_sets_catalog_metadata(client: TestClient) -> None:
    with get_session_factory()() as db:
        first_summary = upsert_default_sources(db)
        db.commit()
        second_summary = upsert_default_sources(db)
        db.commit()

        sources = {
            source.name: source
            for source in db.scalars(select(Source).order_by(Source.name.asc())).all()
        }

    assert len(first_summary) == len(DEFAULT_SOURCE_SPECS)
    assert all(action == "created" for _, action in first_summary)
    assert all(action == "updated" for _, action in second_summary)
    assert len(sources) == len(DEFAULT_SOURCE_SPECS)

    assert sources["Frontier AI Papers"].type == SourceType.ARXIV
    assert sources["Frontier AI Papers"].query is not None
    assert sources["Anthropic News"].config_json["discovery_mode"] == "website_index"
    assert sources["Anthropic News"].config_json["article_path_prefixes"] == ["/news/"]
    assert sources["Google AI Blog"].config_json["website_url"] == (
        "https://blog.google/innovation-and-ai/technology/ai/"
    )
    assert sources["The Batch Research"].config_json["discovery_mode"] == "website_index"
    assert sources["The Batch Research"].config_json["article_path_prefixes"] == ["/the-batch/"]
    assert sources["Mistral AI News"].url == "https://mistral.ai/news"
    assert sources["Meta AI Engineering"].url == "https://engineering.fb.com/tag/ai/feed/"
    assert sources["TLDR AI"].rules[0].rule_type == "label"
    assert sources["TLDR AI"].rules[0].value == "tldr-ai"
    assert sources["AlphaSignal"].query == "from:news@alphasignal.ai"
    assert sources["Testing Catalog"].query == "from:testingcatalog@ghost.io"
