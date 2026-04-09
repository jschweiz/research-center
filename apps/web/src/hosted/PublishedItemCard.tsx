import { Link } from "react-router-dom";
import { ArrowRight, Microscope, Newspaper, SquareArrowOutUpRight } from "lucide-react";
import clsx from "clsx";

import type { PublishedItemListEntry } from "../runtime/types";
import { SkimmableText } from "../components/SkimmableText";

interface PublishedItemCardProps {
  item: PublishedItemListEntry;
  recordName: string;
  note?: string | null;
  hero?: boolean;
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
  return new Intl.DateTimeFormat("en-GB", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
  }).format(new Date(value));
}

export function PublishedItemCard({
  item,
  recordName,
  note,
  hero = false,
}: PublishedItemCardProps) {
  const byline = item.organization_name?.trim() || item.authors.join(", ").trim() || item.source_name;

  return (
    <article
      className={clsx(
        "editorial-panel",
        hero && "bg-[rgba(255,255,255,0.62)] shadow-[0_30px_80px_rgba(17,19,18,0.14)]",
      )}
    >
      <div className="flex items-start justify-between gap-4">
        <div className="space-y-3">
          <div className="flex flex-wrap items-center gap-2 text-[11px] uppercase tracking-[0.24em] text-[var(--muted)]">
            <span>{item.source_name}</span>
            <span className="rounded-full border border-[var(--ink)]/10 px-3 py-1">{contentTypeLabel[item.content_type]}</span>
            {item.kind && item.kind !== item.content_type ? <span>{item.kind}</span> : null}
          </div>
          <h3 className={clsx("font-display text-[var(--ink)]", hero ? "text-4xl leading-tight" : "text-2xl leading-tight")}>
            <Link className="hover:text-[var(--accent)]" to={`/items/${item.id}?record=${encodeURIComponent(recordName)}`}>
              {item.title}
            </Link>
          </h3>
        </div>

        <div className="flex h-12 w-12 items-center justify-center rounded-full border border-[var(--ink)]/10 bg-[var(--paper-strong)]">
          {item.content_type === "paper" ? <Microscope className="h-5 w-5" /> : <Newspaper className="h-5 w-5" />}
        </div>
      </div>

      <div className="mt-4 flex flex-wrap items-center gap-3 font-mono text-xs uppercase tracking-[0.18em] text-[var(--muted)]">
        <span>{byline}</span>
        <span>{formatPublishedAt(item.published_at)}</span>
      </div>

      <SkimmableText className="mt-5 text-base leading-7 text-[var(--muted-strong)]">
        {item.short_summary || note || "Summary pending."}
      </SkimmableText>

      {note ? (
        <SkimmableText className="mt-4 rounded-2xl border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.55)] px-4 py-3 text-sm leading-6 text-[var(--ink)]">
          {note}
        </SkimmableText>
      ) : null}

      <div className="mt-6 flex flex-wrap gap-2">
        <Link className="secondary-button" to={`/items/${item.id}?record=${encodeURIComponent(recordName)}`}>
          <ArrowRight className="h-4 w-4" />
          Open detail
        </Link>
        <a className="secondary-button" href={item.canonical_url} rel="noreferrer" target="_blank">
          <SquareArrowOutUpRight className="h-4 w-4" />
          Open source
        </a>
      </div>
    </article>
  );
}
