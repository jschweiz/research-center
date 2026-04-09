from __future__ import annotations

import html
import shutil
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from app.db.models import IngestionRunType, RunStatus
from app.schemas.published import (
    PublishedAvailabilityRead,
    PublishedDigestEntryRead,
    PublishedDigestRead,
    PublishedEditionManifestRead,
    PublishedEditionSummaryRead,
)
from app.services.vault_alphaxiv import AlphaXivPaperResolver
from app.services.vault_briefs import VaultBriefService
from app.services.vault_runtime import (
    RunRecorder,
    to_published_item_detail,
    to_published_item_list_entry,
    utcnow,
)
from app.vault.models import PublishedIndex
from app.vault.store import VaultStore

SCHEMA_VERSION = 3
INDEX_FILENAME = "index.html"
MANIFEST_FILENAME = "manifest.json"
MARKDOWN_FILENAME = "brief.md"
AUDIO_FILENAME = "audio.mp3"
BRIEF_JSON_FILENAME = "brief.json"
ITEMS_DIRNAME = "items"


@dataclass(frozen=True)
class PublicationBundleResult:
    root: Path
    latest_dir: Path
    history_dir: Path
    archive_path: Path


class VaultPublisherService:
    def __init__(self) -> None:
        self.store = VaultStore()
        self.briefs = VaultBriefService()
        self.runs = RunRecorder(self.store)
        self.store.ensure_layout()

    def publish_latest(self) -> PublishedEditionSummaryRead:
        target = self._latest_publishable_date()
        if target is None:
            target = self.briefs.current_edition_date()
        return self.publish_date(target)

    def publish_date(self, brief_date: date) -> PublishedEditionSummaryRead:
        run = self.runs.start(
            run_type=IngestionRunType.CLEANUP,
            operation_kind="viewer_publish",
            trigger="manual_publish",
            title="Viewer publish",
            summary=f"Rendering viewer artifacts for {brief_date.isoformat()}.",
        )
        lease = None
        try:
            lease = self.store.acquire_lease(name="publish", owner="mac", ttl_seconds=600)
            digest = self.briefs.get_or_generate_by_date(brief_date)
            if digest is None:
                raise RuntimeError(f"No brief exists for {brief_date.isoformat()}.")
            manifest = self.build_manifest(digest)
            bundle = self._write_publication_bundle(manifest)
            published_index = self._update_published_index(manifest.edition)
            run.affected_edition_days = [brief_date]
            run.basic_info.extend(
                [
                    {"label": "Edition", "value": manifest.edition.edition_id},
                    {"label": "Viewer dir", "value": str(bundle.root)},
                    {"label": "Latest manifest", "value": str(bundle.latest_dir / MANIFEST_FILENAME)},
                ]
            )
            self.runs.finish(
                run,
                status=RunStatus.SUCCEEDED,
                summary=f"Rendered viewer artifacts for {brief_date.isoformat()}.",
            )
            return published_index.latest or manifest.edition
        except Exception as exc:
            run.errors.append(str(exc))
            self.runs.finish(run, status=RunStatus.FAILED, summary=f"Viewer publish failed for {brief_date.isoformat()}.")
            raise
        finally:
            if lease is not None:
                self.store.release_lease(lease)

    def build_manifest(self, digest) -> PublishedEditionManifestRead:
        item_ids = self._published_item_ids(digest)
        items_index = self.store.load_items_index()
        item_lookup = {item.id: item for item in items_index.items}
        alphaxiv = AlphaXivPaperResolver(store=self.store, items=items_index.items)
        published_items = {}
        for item_id in item_ids:
            item = item_lookup.get(item_id)
            if item is None:
                continue
            raw = self.store.read_raw_document_relative(item.raw_doc_path)
            refreshed = self._refresh_item_from_raw(item, raw_document=raw)
            published_items[item_id] = to_published_item_detail(
                refreshed,
                alphaxiv=alphaxiv.resolve(refreshed, raw_document=raw),
            )
        current_summary = PublishedEditionSummaryRead(
            edition_id=f"day:{digest.brief_date.isoformat()}",
            record_name=self._edition_slug(f"day:{digest.brief_date.isoformat()}"),
            period_type=digest.period_type,
            brief_date=digest.brief_date,
            week_start=digest.week_start,
            week_end=digest.week_end,
            title=digest.title,
            generated_at=digest.generated_at,
            published_at=utcnow(),
            has_audio=bool(digest.audio_brief and digest.audio_brief.status == RunStatus.SUCCEEDED.value),
            schema_version=SCHEMA_VERSION,
        )
        published_index = self.store.load_published_index()
        available = [current_summary]
        for edition in published_index.editions:
            if edition.edition_id != current_summary.edition_id:
                available.append(edition)
        return PublishedEditionManifestRead(
            schema_version=SCHEMA_VERSION,
            edition=current_summary,
            availability=PublishedAvailabilityRead.model_validate(
                self.briefs.list_availability().model_dump(mode="json")
            ),
            available_editions=available[:60],
            digest=self._to_published_digest(digest, alphaxiv=alphaxiv, item_lookup=item_lookup),
            items=published_items,
        )

    def list_published_summaries(self, *, limit: int = 20) -> list[PublishedEditionSummaryRead]:
        return self.store.load_published_index().editions[:limit]

    def get_latest_published_summary(self) -> PublishedEditionSummaryRead | None:
        return self.store.load_published_index().latest

    def _update_published_index(self, latest: PublishedEditionSummaryRead) -> PublishedIndex:
        current = self.store.load_published_index()
        editions = [latest] + [edition for edition in current.editions if edition.edition_id != latest.edition_id]
        updated = PublishedIndex(generated_at=utcnow(), latest=latest, editions=editions)
        self.store.save_published_index(updated)
        return updated

    def _write_publication_bundle(self, manifest: PublishedEditionManifestRead) -> PublicationBundleResult:
        root = self.store.viewer_dir
        latest_dir = root / "latest"
        history_dir = root / "history" / self._edition_slug(manifest.edition.edition_id)
        root.mkdir(parents=True, exist_ok=True)
        latest_dir.mkdir(parents=True, exist_ok=True)
        history_dir.mkdir(parents=True, exist_ok=True)

        self._write_root_index(root)
        self._write_edition_bundle(latest_dir, manifest=manifest, history=False)
        self._write_edition_bundle(history_dir, manifest=manifest, history=True)
        archive_path = root / "archive.json"
        self.store.write_json(archive_path, self._build_archive_payload(manifest))
        return PublicationBundleResult(
            root=root,
            latest_dir=latest_dir,
            history_dir=history_dir,
            archive_path=archive_path,
        )

    def _write_root_index(self, root: Path) -> None:
        self.store.write_text(
            root / INDEX_FILENAME,
            """<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta http-equiv="refresh" content="0; url=latest/index.html" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Research Center Viewer</title>
  </head>
  <body>
    <p>Opening the latest viewer snapshot… <a href="latest/index.html">Continue</a></p>
  </body>
</html>
""",
        )

    def _write_edition_bundle(
        self,
        bundle_dir: Path,
        *,
        manifest: PublishedEditionManifestRead,
        history: bool,
    ) -> None:
        items_dir = bundle_dir / ITEMS_DIRNAME
        if items_dir.exists():
            shutil.rmtree(items_dir)
        items_dir.mkdir(parents=True, exist_ok=True)

        self.store.write_bytes(
            bundle_dir / MANIFEST_FILENAME,
            manifest.model_dump_json(indent=2).encode("utf-8"),
        )
        self.store.write_text(bundle_dir / MARKDOWN_FILENAME, self._render_markdown_brief(manifest))
        self.store.write_text(bundle_dir / INDEX_FILENAME, self._render_html_brief(manifest, history=history))
        self.store.write_bytes(
            bundle_dir / BRIEF_JSON_FILENAME,
            manifest.digest.model_dump_json(indent=2).encode("utf-8"),
        )

        if manifest.digest.audio_brief and manifest.digest.audio_brief.audio_url:
            source = self.store.brief_dir_for_date(manifest.digest.brief_date) / manifest.digest.audio_brief.audio_url
            if source.exists():
                shutil.copy2(source, bundle_dir / AUDIO_FILENAME)

        for item in manifest.items.values():
            stem = self._safe_filename(item.id)
            self.store.write_bytes(
                items_dir / f"{stem}.json",
                item.model_dump_json(indent=2).encode("utf-8"),
            )
            self.store.write_text(items_dir / f"{stem}.md", self._render_item_markdown(item))
            self.store.write_text(items_dir / f"{stem}.html", self._render_item_html(item))

    def _build_archive_payload(self, manifest: PublishedEditionManifestRead) -> dict[str, object]:
        def build_entry(summary: PublishedEditionSummaryRead) -> dict[str, object]:
            slug = self._edition_slug(summary.edition_id)
            base = "latest" if summary.edition_id == manifest.edition.edition_id else f"history/{slug}"
            payload = summary.model_dump(mode="json")
            payload.update(
                {
                    "bundle_slug": slug,
                    "html_path": f"{base}/{INDEX_FILENAME}",
                    "manifest_path": f"{base}/{MANIFEST_FILENAME}",
                    "brief_json_path": f"{base}/{BRIEF_JSON_FILENAME}",
                    "brief_markdown_path": f"{base}/{MARKDOWN_FILENAME}",
                    "audio_path": f"{base}/{AUDIO_FILENAME}" if summary.has_audio else None,
                }
            )
            return payload

        return {
            "generated_at": utcnow().isoformat(),
            "latest": build_entry(manifest.edition),
            "editions": [build_entry(entry) for entry in manifest.available_editions],
        }

    def _to_published_digest(
        self,
        digest,
        *,
        alphaxiv: AlphaXivPaperResolver,
        item_lookup,
    ) -> PublishedDigestRead:
        items_lookup = item_lookup

        def _entries(values) -> list[PublishedDigestEntryRead]:
            entries: list[PublishedDigestEntryRead] = []
            for entry in values:
                item = items_lookup.get(entry.item.id)
                if item is None:
                    continue
                raw = self.store.read_raw_document_relative(item.raw_doc_path)
                refreshed = self._refresh_item_from_raw(item, raw_document=raw)
                paper = alphaxiv.resolve(
                    refreshed,
                    raw_document=raw,
                )
                entries.append(
                    PublishedDigestEntryRead(
                        item=to_published_item_list_entry(
                            refreshed,
                            summary_override=paper.short_summary if paper else None,
                        ),
                        note=entry.note,
                        rank=entry.rank,
                    )
                )
            return entries

        papers = []
        for entry in digest.papers_table:
            item = items_lookup.get(entry.item.id)
            if item is None:
                continue
            raw = self.store.read_raw_document_relative(item.raw_doc_path)
            refreshed = self._refresh_item_from_raw(item, raw_document=raw)
            paper = alphaxiv.resolve(
                refreshed,
                raw_document=raw,
            )
            papers.append(
                {
                    "item": to_published_item_list_entry(
                        refreshed,
                        summary_override=paper.short_summary if paper else None,
                    ),
                    "rank": entry.rank,
                    "zotero_tags": entry.zotero_tags,
                    "credibility_score": entry.credibility_score,
                }
            )
        return PublishedDigestRead.model_validate(
            {
                **digest.model_dump(mode="json"),
                "editorial_shortlist": [entry.model_dump(mode="json") for entry in _entries(digest.editorial_shortlist)],
                "headlines": [entry.model_dump(mode="json") for entry in _entries(digest.headlines)],
                "interesting_side_signals": [entry.model_dump(mode="json") for entry in _entries(digest.interesting_side_signals)],
                "remaining_reads": [entry.model_dump(mode="json") for entry in _entries(digest.remaining_reads)],
                "papers_table": papers,
            }
        )

    @staticmethod
    def _published_item_ids(digest) -> list[str]:
        ids: list[str] = []
        for entries in (
            digest.editorial_shortlist,
            digest.headlines,
            digest.interesting_side_signals,
            digest.remaining_reads,
            digest.papers_table,
        ):
            for entry in entries:
                item_id = entry.item.id
                if item_id not in ids:
                    ids.append(item_id)
        return ids

    @staticmethod
    def _render_markdown_brief(manifest: PublishedEditionManifestRead) -> str:
        lines = [
            f"# {manifest.digest.title}",
            "",
            f"Published: {manifest.edition.published_at.isoformat()}",
            f"Coverage: {manifest.digest.coverage_start.isoformat()}",
        ]
        if manifest.digest.editorial_note:
            lines.extend(["", manifest.digest.editorial_note])
        if manifest.digest.audio_brief and manifest.digest.audio_brief.audio_url:
            lines.extend(["", f"Audio: {AUDIO_FILENAME}"])
        for title, entries in (
            ("Editorial shortlist", manifest.digest.editorial_shortlist),
            ("Headlines", manifest.digest.headlines),
            ("Interesting side signals", manifest.digest.interesting_side_signals),
            ("Remaining reads", manifest.digest.remaining_reads),
        ):
            if not entries:
                continue
            lines.extend(["", f"## {title}", ""])
            for entry in entries:
                lines.append(f"- [{entry.item.title}]({entry.item.canonical_url})")
                if entry.note:
                    lines.append(f"  - {entry.note}")
        return "\n".join(lines).strip() + "\n"

    def _render_html_brief(self, manifest: PublishedEditionManifestRead, *, history: bool) -> str:
        archive_links = "".join(
            (
                f"<li><a href='../{summary.record_name}/{INDEX_FILENAME}'>{html.escape(summary.title)}</a></li>"
                if history
                else f"<li>{html.escape(summary.title)}</li>"
            )
            for summary in manifest.available_editions[:12]
        )
        sections = "".join(
            self._render_html_section(title, entries)
            for title, entries in (
                ("Editorial shortlist", manifest.digest.editorial_shortlist),
                ("Headlines", manifest.digest.headlines),
                ("Interesting side signals", manifest.digest.interesting_side_signals),
                ("Remaining reads", manifest.digest.remaining_reads),
            )
        )
        audio = (
            "<section><h2>Audio</h2><audio controls preload='metadata' src='audio.mp3'></audio></section>"
            if manifest.digest.audio_brief and manifest.digest.audio_brief.audio_url
            else ""
        )
        return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>{html.escape(manifest.digest.title)}</title>
    <style>
      body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 0; background: #f7f3ea; color: #1f1b16; }}
      main {{ max-width: 1120px; margin: 0 auto; padding: 32px 20px 72px; }}
      section {{ background: rgba(255,255,255,0.72); border: 1px solid rgba(0,0,0,0.08); border-radius: 20px; padding: 20px; margin-top: 20px; }}
      h1, h2 {{ margin-top: 0; }}
      .meta {{ color: #6b6155; }}
      ul {{ padding-left: 20px; }}
      a {{ color: #0f766e; }}
    </style>
  </head>
  <body>
    <main>
      <section>
        <p class="meta">Published {html.escape(manifest.edition.published_at.isoformat())}</p>
        <h1>{html.escape(manifest.digest.title)}</h1>
        <p>{html.escape(manifest.digest.editorial_note or 'Latest synced viewer brief.')}</p>
      </section>
      {audio}
      {sections}
      <section>
        <h2>Archive</h2>
        <ul>{archive_links}</ul>
      </section>
    </main>
  </body>
</html>
"""

    @staticmethod
    def _render_html_section(title: str, entries: list[PublishedDigestEntryRead]) -> str:
        if not entries:
            return ""
        items = "".join(
            f"<li><a href='{html.escape(entry.item.canonical_url)}'>{html.escape(entry.item.title)}</a>"
            f"{f'<p>{html.escape(entry.note)}</p>' if entry.note else ''}</li>"
            for entry in entries
        )
        return f"<section><h2>{html.escape(title)}</h2><ul>{items}</ul></section>"

    @staticmethod
    def _render_item_markdown(item) -> str:
        summary = item.alphaxiv.short_summary if item.alphaxiv and item.alphaxiv.short_summary else item.insight.short_summary
        text = item.alphaxiv.filed_text if item.alphaxiv and item.alphaxiv.filed_text else item.cleaned_text
        lines = [
            f"# {item.title}",
            "",
            f"Source: {item.source_name}",
            f"Published: {item.published_at.isoformat() if item.published_at else 'Unknown'}",
            f"Canonical URL: {item.canonical_url}",
        ]
        if summary:
            lines.extend(["", "## Summary", "", summary])
        if item.alphaxiv and item.alphaxiv.audio_url:
            lines.extend(["", "## Audio", "", item.alphaxiv.audio_url])
        if text:
            lines.extend(["", "## Text", "", text])
        return "\n".join(lines).strip() + "\n"

    @staticmethod
    def _render_item_html(item) -> str:
        summary = html.escape(
            item.alphaxiv.short_summary if item.alphaxiv and item.alphaxiv.short_summary else item.insight.short_summary or "No summary."
        )
        text = html.escape(
            item.alphaxiv.filed_text if item.alphaxiv and item.alphaxiv.filed_text else item.cleaned_text or "No text available."
        )
        audio = (
            f"<p><audio controls preload='metadata' src='{html.escape(item.alphaxiv.audio_url)}'></audio></p>"
            if item.alphaxiv and item.alphaxiv.audio_url
            else ""
        )
        return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>{html.escape(item.title)}</title>
  </head>
  <body>
    <main>
      <h1>{html.escape(item.title)}</h1>
      <p>{summary}</p>
      {audio}
      <pre>{text}</pre>
    </main>
  </body>
</html>
"""

    def _refresh_item_from_raw(self, item, *, raw_document=None):
        raw = raw_document if raw_document is not None else self.store.read_raw_document_relative(item.raw_doc_path)
        if raw is None:
            return item
        return item.model_copy(
            update={
                "cleaned_text": raw.body,
                "asset_paths": raw.frontmatter.asset_paths,
                "short_summary": raw.frontmatter.short_summary,
                "lightweight_enrichment_status": raw.frontmatter.lightweight_enrichment_status,
                "lightweight_enriched_at": raw.frontmatter.lightweight_enriched_at,
            }
        )

    def _latest_publishable_date(self) -> date | None:
        availability: PublishedAvailabilityRead = self.briefs.list_availability()
        return availability.default_day

    @staticmethod
    def _edition_slug(edition_id: str) -> str:
        return edition_id.replace(":", "-")

    @staticmethod
    def _safe_filename(value: str) -> str:
        return "".join(character if character.isalnum() or character in {"-", "_"} else "-" for character in value)
