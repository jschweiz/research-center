import { ArrowRight, Microscope, Newspaper, SquareArrowOutUpRight } from "lucide-react";
import { Link } from "react-router-dom";

import { SkimmableText } from "../components/SkimmableText";
import { resolveExternalUrl } from "../lib/external-links";
import type { PublishedItemListEntry } from "../runtime/types";

interface PublishedFeedItemProps {
  item: PublishedItemListEntry;
  note?: string | null;
  rank?: number;
  recordName: string;
}

const contentTypeLabel: Record<PublishedItemListEntry["content_type"], string> = {
  article: "Article",
  news: "News",
  newsletter: "Newsletter",
  paper: "Paper",
  post: "Post",
  signal: "Signal",
  thread: "Thread",
};

function formatPublishedAt(value: string | null) {
  if (!value) return "Undated";

  const date = new Date(value);
  const includeYear = date.getFullYear() !== new Date().getFullYear();
  return new Intl.DateTimeFormat(undefined, {
    day: "numeric",
    month: "short",
    ...(includeYear ? { year: "numeric" } : {}),
  }).format(date);
}

function buildSummary(item: PublishedItemListEntry, note?: string | null) {
  return note?.trim() || item.short_summary?.trim() || "Summary pending.";
}

function PublishedStoryTypeIcon({ item }: { item: PublishedItemListEntry }) {
  if (item.content_type === "paper") {
    return <Microscope className="h-3.5 w-3.5" />;
  }
  return <Newspaper className="h-3.5 w-3.5" />;
}

function PublishedStoryEyebrow({
  item,
  rank,
}: {
  item: PublishedItemListEntry;
  rank?: number;
}) {
  return (
    <div className="pv-story-eyebrow">
      {typeof rank === "number" ? <span className="pv-rank-chip">{String(rank).padStart(2, "0")}</span> : null}
      <span>{item.source_name}</span>
      <span>{contentTypeLabel[item.content_type]}</span>
      <span>{formatPublishedAt(item.published_at)}</span>
    </div>
  );
}

export function PublishedLeadStory({ item, note, rank, recordName }: PublishedFeedItemProps) {
  const canonicalUrl = resolveExternalUrl(item.canonical_url);
  const summaryText = buildSummary(item, note);
  const byline = item.organization_name?.trim() || item.authors.join(", ").trim() || item.source_name;

  return (
    <article className="pv-lead-story">
      <div className="pv-lead-header">
        <PublishedStoryEyebrow item={item} rank={rank} />
        <a
          aria-label={`Open source for ${item.title}`}
          className="pv-lead-source"
          href={canonicalUrl}
          rel="noreferrer"
          target="_blank"
        >
          <PublishedStoryTypeIcon item={item} />
          <SquareArrowOutUpRight className="h-3.5 w-3.5" />
        </a>
      </div>

      <h3 className="pv-lead-title">
        <Link className="pv-story-link" to={`/items/${item.id}?record=${encodeURIComponent(recordName)}`}>
          {item.title}
        </Link>
      </h3>

      <SkimmableText className="pv-lead-summary">{summaryText}</SkimmableText>

      <div className="pv-lead-footer">
        <span className="pv-story-byline">{byline}</span>
        <Link className="pv-lead-read" to={`/items/${item.id}?record=${encodeURIComponent(recordName)}`}>
          <span>Read story</span>
          <ArrowRight className="h-3.5 w-3.5" />
        </Link>
      </div>
    </article>
  );
}

export function PublishedStoryRow({ item, note, rank, recordName }: PublishedFeedItemProps) {
  const canonicalUrl = resolveExternalUrl(item.canonical_url);
  const summaryText = buildSummary(item, note);

  return (
    <article className="pv-story-row">
      <Link className="pv-story-row-main" to={`/items/${item.id}?record=${encodeURIComponent(recordName)}`}>
        <PublishedStoryEyebrow item={item} rank={rank} />
        <h3 className="pv-story-row-title">{item.title}</h3>
        <SkimmableText className="pv-story-row-summary">{summaryText}</SkimmableText>
      </Link>

      <a
        aria-label={`Open source for ${item.title}`}
        className="pv-story-row-source"
        href={canonicalUrl}
        rel="noreferrer"
        target="_blank"
      >
        <SquareArrowOutUpRight className="h-4 w-4" />
      </a>
    </article>
  );
}
