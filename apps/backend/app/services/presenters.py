from __future__ import annotations

import json
import math
import re
from datetime import date, datetime

from bs4 import BeautifulSoup

from app.db.models import DataMode, Digest, Item, ScoreBucket, TriageStatus
from app.schemas.briefs import (
    AudioBriefChapterRead,
    AudioBriefRead,
    DigestEntryRead,
    DigestRead,
    PaperTableEntryRead,
)
from app.schemas.items import (
    ItemDetailRead,
    ItemInsightRead,
    ItemListEntry,
    ItemScoreRead,
    RelatedMention,
)
from app.services.brief_dates import coverage_day_for_edition
from app.services.text import compact_signal_note, normalize_item_title

WORD_RE = re.compile(r"\b\w+\b")
SPACE_RE = re.compile(r"\s+")
NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
HTML_ORG_MARKERS = (
    "citation_author_institution",
    "citation_author_affiliation",
    "citation_author_organization",
    "citation_publisher",
    "article:publisher",
    "application/ld+json",
    "\"publisher\"",
    "\"affiliation\"",
    "\"worksfor\"",
    "\"sourceorganization\"",
)
AFFILIATION_META_SELECTORS = (
    'meta[name="citation_author_institution"]',
    'meta[name="citation_author_affiliation"]',
    'meta[name="citation_author_organization"]',
)
PUBLISHER_META_SELECTORS = (
    'meta[name="dc.publisher"]',
    'meta[name="citation_publisher"]',
    'meta[property="article:publisher"]',
)
PAPER_CREDIBILITY_HIGH_ORGS = (
    "openai",
    "anthropic",
    "deepmind",
    "google research",
    "meta ai",
    "fair",
    "microsoft research",
    "stanford",
    "mit",
    "cmu",
    "carnegie mellon",
    "berkeley",
    "eth zurich",
    "oxford",
    "cambridge",
    "princeton",
    "allen institute",
    "nvidia",
    "mistral",
    "mistral ai",
)
PAPER_CREDIBILITY_MEDIUM_ORGS = (
    "google",
    "meta",
    "microsoft",
    "aws",
    "amazon",
    "hugging face",
    "cohere",
    "inflection",
    "tsinghua",
    "harvard",
    "yale",
    "columbia",
    "ucla",
    "uc san diego",
    "epfl",
    "max planck",
)


def _clean_candidate(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    collapsed = SPACE_RE.sub(" ", re.sub(r"<[^>]+>", " ", value)).strip(" \t\r\n\"'")
    if not collapsed or collapsed.lower() in {"unknown", "none", "null", "n/a"}:
        return None
    return collapsed


def _normalize_candidate(value: str) -> str:
    return NON_ALNUM_RE.sub("", value.lower())


def _dedupe_candidates(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        normalized = _normalize_candidate(value)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(value)
    return result


def _extract_json_ld_names(payload: object, keys: tuple[str, ...]) -> list[str]:
    candidates: list[str] = []

    def collect(node: object) -> None:
        if isinstance(node, str):
            candidate = _clean_candidate(node)
            if candidate:
                candidates.append(candidate)
            return
        if isinstance(node, list):
            for item in node:
                collect(item)
            return
        if isinstance(node, dict):
            candidate = _clean_candidate(node.get("name"))
            if candidate:
                candidates.append(candidate)
            for key in keys:
                if key in node:
                    collect(node.get(key))

    def walk(node: object) -> None:
        if isinstance(node, list):
            for item in node:
                walk(item)
            return
        if not isinstance(node, dict):
            return
        for key in keys:
            if key in node:
                collect(node.get(key))
        for key in ("author", "creator", "@graph", "mainEntity"):
            if key in node:
                walk(node.get(key))

    walk(payload)
    return _dedupe_candidates(candidates)


def _extract_html_organization_candidates(html: str) -> list[str]:
    if not html or not any(marker in html.lower() for marker in HTML_ORG_MARKERS):
        return []

    soup = BeautifulSoup(html, "html.parser")
    candidates: list[str] = []

    for selector in (*AFFILIATION_META_SELECTORS, *PUBLISHER_META_SELECTORS):
        for element in soup.select(selector):
            candidate = _clean_candidate(element.get("content") or element.get_text(" ", strip=True))
            if candidate:
                candidates.append(candidate)

    for script in soup.select('script[type="application/ld+json"]'):
        raw_text = script.string or script.get_text(" ", strip=True)
        if not raw_text:
            continue
        try:
            payload = json.loads(raw_text)
        except json.JSONDecodeError:
            continue
        candidates.extend(
            _extract_json_ld_names(
                payload,
                ("affiliation", "worksFor", "sourceOrganization", "memberOf"),
            )
        )
        candidates.extend(_extract_json_ld_names(payload, ("publisher",)))

    return _dedupe_candidates(candidates)


def _extract_sender_candidate(value: object) -> str | None:
    candidate = _clean_candidate(value)
    if not candidate:
        return None
    if "@" in candidate and " " not in candidate:
        return None
    return candidate


def resolve_item_organization_name(item: Item) -> str | None:
    metadata = item.metadata_json if isinstance(item.metadata_json, dict) else {}
    raw_payload = (
        item.content.raw_payload
        if item.content and isinstance(item.content.raw_payload, dict)
        else {}
    )

    direct_candidates: list[str] = []
    for container in (metadata, raw_payload):
        for key in ("organization_name", "organization", "institution", "company"):
            candidate = _clean_candidate(container.get(key))
            if candidate:
                direct_candidates.append(candidate)

    sender_candidate = _extract_sender_candidate(
        metadata.get("sender") or raw_payload.get("sender")
    )
    if sender_candidate:
        direct_candidates.append(sender_candidate)

    deduped = _dedupe_candidates(direct_candidates)
    if not deduped:
        html = raw_payload.get("html")
        if isinstance(html, str):
            deduped = _extract_html_organization_candidates(html)
    if not deduped:
        publisher_candidates: list[str] = []
        for key in ("crossref_publisher", "publisher"):
            candidate = _clean_candidate(metadata.get(key))
            if candidate:
                publisher_candidates.append(candidate)
        deduped = _dedupe_candidates(publisher_candidates)
    if not deduped:
        return None

    source_name_normalized = _normalize_candidate(item.source_name)
    preferred = [
        candidate
        for candidate in deduped
        if _normalize_candidate(candidate) != source_name_normalized
    ]
    return preferred[0] if preferred else deduped[0]


def _count_related_mentions(item: Item) -> int:
    seen_urls = {item.canonical_url}
    count = 0

    for related_item in item.cluster.items if item.cluster else []:
        if related_item.id == item.id or related_item.canonical_url in seen_urls:
            continue
        seen_urls.add(related_item.canonical_url)
        count += 1

    duplicate_mentions = item.metadata_json.get("duplicate_mentions", []) if item.metadata_json else []
    for mention in duplicate_mentions:
        canonical_url = mention.get("canonical_url")
        if not canonical_url or canonical_url in seen_urls:
            continue
        seen_urls.add(canonical_url)
        count += 1

    return count


def build_related_mentions(item: Item) -> list[RelatedMention]:
    related: list[RelatedMention] = []
    seen_urls = {item.canonical_url}
    current_title = normalize_item_title(item.title, content_type=item.content_type)

    for related_item in item.cluster.items if item.cluster else []:
        if related_item.id == item.id or related_item.canonical_url in seen_urls:
            continue
        seen_urls.add(related_item.canonical_url)
        related.append(
            RelatedMention(
                item_id=related_item.id,
                title=normalize_item_title(related_item.title, content_type=related_item.content_type),
                source_name=related_item.source_name,
                canonical_url=related_item.canonical_url,
            )
        )

    duplicate_mentions = item.metadata_json.get("duplicate_mentions", []) if item.metadata_json else []
    for index, mention in enumerate(duplicate_mentions, start=1):
        canonical_url = mention.get("canonical_url")
        if not canonical_url or canonical_url in seen_urls:
            continue
        seen_urls.add(canonical_url)
        related.append(
            RelatedMention(
                item_id=f"duplicate-{item.id}-{index}",
                title=normalize_item_title(mention.get("title") or current_title, content_type=item.content_type),
                source_name=mention.get("source_name") or "Duplicate mention",
                canonical_url=canonical_url,
            )
        )
    return related


def build_item_list_entry(item: Item) -> ItemListEntry:
    title = normalize_item_title(item.title, content_type=item.content_type)
    return ItemListEntry(
        id=item.id,
        title=title,
        source_name=item.source_name,
        organization_name=resolve_item_organization_name(item),
        authors=item.authors,
        published_at=item.published_at,
        canonical_url=item.canonical_url,
        content_type=item.content_type,
        triage_status=item.triage_status,
        starred=item.starred,
        extraction_confidence=item.extraction_confidence,
        short_summary=item.insight.short_summary if item.insight else None,
        bucket=item.score.bucket if item.score else ScoreBucket.ARCHIVE,
        total_score=item.score.total_score if item.score else 0.0,
        reason_trace=item.score.reason_trace if item.score else {},
        also_mentioned_in_count=_count_related_mentions(item),
    )


def _zotero_tags_for_item(item: Item) -> list[str]:
    best_match = max(item.zotero_matches, key=lambda match: match.similarity_score, default=None)
    if best_match is None:
        return []
    raw_tags = best_match.metadata_json.get("tags") if isinstance(best_match.metadata_json, dict) else []
    if not isinstance(raw_tags, list):
        return []
    seen: set[str] = set()
    tags: list[str] = []
    for value in raw_tags:
        if not isinstance(value, str):
            continue
        cleaned = value.strip()
        normalized = cleaned.lower()
        if not cleaned or normalized in seen:
            continue
        seen.add(normalized)
        tags.append(cleaned)
        if len(tags) >= 4:
            break
    return tags


def _organization_component(item: Item) -> float:
    organization_name = (resolve_item_organization_name(item) or "").strip().lower()
    if organization_name:
        if any(token in organization_name for token in PAPER_CREDIBILITY_HIGH_ORGS):
            return 1.0
        if any(token in organization_name for token in PAPER_CREDIBILITY_MEDIUM_ORGS):
            return 0.75
        return 0.5

    metadata = item.metadata_json if isinstance(item.metadata_json, dict) else {}
    if metadata.get("semantic_scholar_venue") or metadata.get("crossref_publisher"):
        return 0.3
    return 0.0


def compute_paper_credibility_score(item: Item) -> int:
    metadata = item.metadata_json if isinstance(item.metadata_json, dict) else {}
    citation_count = metadata.get("semantic_scholar_citation_count")
    try:
        citation_value = max(float(citation_count or 0), 0.0)
    except (TypeError, ValueError):
        citation_value = 0.0
    citation_component = min(math.log10(citation_value + 1.0) / 3.0, 1.0)
    source_component = item.score.source_quality_score if item.score else 0.0
    score = 100 * (
        0.45 * _organization_component(item)
        + 0.35 * citation_component
        + 0.20 * source_component
    )
    return round(score)


def build_paper_table_entry(item: Item, rank: int) -> PaperTableEntryRead:
    return PaperTableEntryRead(
        item=build_item_list_entry(item),
        rank=rank,
        zotero_tags=_zotero_tags_for_item(item),
        credibility_score=compute_paper_credibility_score(item),
    )


def build_item_detail(item: Item) -> ItemDetailRead:
    normalized_title = normalize_item_title(item.title, content_type=item.content_type)
    normalized_why_it_matters = (
        compact_signal_note(
            item.insight.why_it_matters,
            title=normalized_title,
            summary=item.insight.short_summary or "",
            fallback_text=item.content.cleaned_text if item.content else "",
        )
        if item.insight
        else None
    )
    score = (
        ItemScoreRead(
            relevance_score=item.score.relevance_score,
            novelty_score=item.score.novelty_score,
            source_quality_score=item.score.source_quality_score,
            author_match_score=item.score.author_match_score,
            topic_match_score=item.score.topic_match_score,
            zotero_affinity_score=item.score.zotero_affinity_score,
            total_score=item.score.total_score,
            bucket=item.score.bucket,
            reason_trace=item.score.reason_trace,
        )
        if item.score
        else ItemScoreRead()
    )
    insight = (
        ItemInsightRead(
            short_summary=item.insight.short_summary,
            why_it_matters=normalized_why_it_matters,
            whats_new=item.insight.whats_new,
            caveats=item.insight.caveats,
            follow_up_questions=item.insight.follow_up_questions,
            contribution=item.insight.contribution,
            method=item.insight.method,
            result=item.insight.result,
            limitation=item.insight.limitation,
            possible_extension=item.insight.possible_extension,
            deeper_summary=item.insight.deeper_summary,
            experiment_ideas=item.insight.experiment_ideas,
        )
        if item.insight
        else ItemInsightRead()
    )

    return ItemDetailRead(
        id=item.id,
        title=normalized_title,
        source_name=item.source_name,
        organization_name=resolve_item_organization_name(item),
        authors=item.authors,
        published_at=item.published_at,
        canonical_url=item.canonical_url,
        content_type=item.content_type,
        triage_status=item.triage_status,
        starred=item.starred,
        ingest_status=item.ingest_status,
        extraction_confidence=item.extraction_confidence,
        cleaned_text=item.content.cleaned_text if item.content else None,
        outbound_links=item.content.outbound_links if item.content else [],
        raw_payload_retention_until=item.content.raw_payload_retention_until if item.content else None,
        score=score,
        insight=insight,
        also_mentioned_in=build_related_mentions(item),
        zotero_matches=[match.metadata_json | {"title": match.title} for match in item.zotero_matches],
    )


def estimate_audio_duration_seconds(script: str | None) -> int | None:
    if not script:
        return None
    word_count = len(WORD_RE.findall(script))
    if not word_count:
        return None
    # Slightly slower than conversational speech to match research-summary pacing.
    return max(30, round(word_count / 2.35))


def build_audio_brief_read(digest: Digest) -> AudioBriefRead | None:
    if (
        not digest.audio_brief_status
        and not digest.audio_brief_script
        and not digest.audio_artifact_url
        and not digest.audio_brief_error
    ):
        return None
    status = digest.audio_brief_status or "pending"
    has_provider_backed_audio = bool(
        digest.audio_artifact_url or (digest.audio_artifact_provider and digest.audio_artifact_voice)
    )
    if status == "succeeded" and not has_provider_backed_audio:
        status = "pending"
    chapters = [
        AudioBriefChapterRead(
            item_id=str(chapter.get("item_id") or ""),
            item_title=(
                normalize_item_title(str(chapter.get("item_title") or ""), content_type="paper")
                if str(chapter.get("section") or "") == "papers_table"
                else str(chapter.get("item_title") or "")
            ),
            section=str(chapter.get("section") or ""),
            rank=int(chapter.get("rank") or 0),
            headline=str(chapter.get("headline") or ""),
            narration=str(chapter.get("narration") or ""),
            offset_seconds=int(chapter.get("offset_seconds") or 0),
        )
        for chapter in digest.audio_brief_chapters or []
        if chapter.get("item_id")
    ]
    return AudioBriefRead(
        status=status,
        script=digest.audio_brief_script,
        chapters=chapters,
        estimated_duration_seconds=estimate_audio_duration_seconds(digest.audio_brief_script),
        audio_url=digest.audio_artifact_url,
        audio_duration_seconds=digest.audio_duration_seconds,
        provider=digest.audio_artifact_provider,
        voice=digest.audio_artifact_voice,
        error=digest.audio_brief_error,
        generated_at=digest.audio_brief_generated_at,
        metadata=digest.audio_metadata_json or {},
    )


def build_digest_entries(digest: Digest) -> dict[str, list[DigestEntryRead]]:
    grouped: dict[str, list[DigestEntryRead]] = {
        "headlines": [],
        "editorial_shortlist": [],
        "interesting_side_signals": [],
        "remaining_reads": [],
    }
    section_priority = {
        "editorial_shortlist": 0,
        "headlines": 1,
        "interesting_side_signals": 2,
        "remaining_reads": 3,
    }
    for entry in sorted(
        digest.entries,
        key=lambda current: (section_priority.get(str(current.section), 99), current.rank),
    ):
        if entry.item.triage_status == TriageStatus.ARCHIVED:
            continue
        payload = DigestEntryRead(
            item=build_item_list_entry(entry.item),
            note=compact_signal_note(
                entry.note or (entry.item.insight.why_it_matters if entry.item.insight else None),
                title=normalize_item_title(entry.item.title, content_type=entry.item.content_type),
                summary=entry.item.insight.short_summary if entry.item.insight else "",
                fallback_text=entry.item.content.cleaned_text if entry.item.content else "",
            ),
            rank=entry.rank,
        )
        if entry.section == "headlines":
            grouped["headlines"].append(payload)
        elif entry.section == "editorial_shortlist":
            grouped["editorial_shortlist"].append(payload)
        elif entry.section == "interesting_side_signals":
            grouped["interesting_side_signals"].append(payload)
        else:
            grouped["remaining_reads"].append(payload)
    return grouped


def build_period_digest_read(
    *,
    digest_id: str,
    period_type: str,
    data_mode: DataMode,
    title: str,
    editorial_note: str | None,
    suggested_follow_ups: list[str],
    audio_brief: AudioBriefRead | None,
    generated_at: datetime,
    headlines: list[DigestEntryRead],
    editorial_shortlist: list[DigestEntryRead],
    interesting_side_signals: list[DigestEntryRead],
    remaining_reads: list[DigestEntryRead],
    papers_table: list[PaperTableEntryRead],
    brief_date: date | None = None,
    week_start: date | None = None,
    week_end: date | None = None,
    coverage_start: date | None = None,
    coverage_end: date | None = None,
) -> DigestRead:
    return DigestRead(
        id=digest_id,
        period_type=period_type,
        brief_date=brief_date,
        week_start=week_start,
        week_end=week_end,
        coverage_start=coverage_start or (coverage_day_for_edition(brief_date) if brief_date else week_start),
        coverage_end=coverage_end or (coverage_day_for_edition(brief_date) if brief_date else week_end),
        data_mode=data_mode,
        title=title,
        editorial_note=editorial_note,
        suggested_follow_ups=suggested_follow_ups,
        audio_brief=audio_brief,
        generated_at=generated_at,
        headlines=headlines,
        editorial_shortlist=editorial_shortlist,
        interesting_side_signals=interesting_side_signals,
        remaining_reads=remaining_reads,
        papers_table=papers_table,
    )


def build_digest_read(digest: Digest, *, papers_table: list[PaperTableEntryRead] | None = None) -> DigestRead:
    grouped = build_digest_entries(digest)
    coverage_day = coverage_day_for_edition(digest.brief_date)
    return build_period_digest_read(
        digest_id=digest.id,
        period_type="day",
        brief_date=digest.brief_date,
        coverage_start=coverage_day,
        coverage_end=coverage_day,
        data_mode=digest.data_mode,
        title=digest.title,
        editorial_note=digest.editorial_note,
        suggested_follow_ups=digest.suggested_follow_ups,
        audio_brief=build_audio_brief_read(digest),
        generated_at=digest.generated_at,
        papers_table=papers_table or [],
        **grouped,
    )
