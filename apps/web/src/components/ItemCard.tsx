import { Link } from "react-router-dom";
import { ArrowRight, Microscope, Newspaper } from "lucide-react";
import clsx from "clsx";

import type { ItemListEntry } from "../api/types";
import { QuickActions } from "./QuickActions";
import { SkimmableText } from "./SkimmableText";

interface ItemCardProps {
  item: ItemListEntry;
  note?: string | null;
  hero?: boolean;
  byline?: string;
  publishedAtFormat?: "date" | "datetime";
  compactActions?: boolean;
  showScore?: boolean;
}

const contentTypeLabel: Record<ItemListEntry["content_type"], string> = {
  article: "Article",
  news: "News",
  newsletter: "Newsletter",
  paper: "Paper",
  post: "Post",
  signal: "Signal",
  thread: "Thread",
};

function formatPublishedAt(value: string | null, format: "date" | "datetime") {
  if (!value) return "Undated";

  return new Intl.DateTimeFormat(
    "en-GB",
    format === "date"
      ? {
          day: "2-digit",
          month: "2-digit",
          year: "numeric",
        }
      : {
          day: "2-digit",
          month: "2-digit",
          year: "numeric",
          hour: "2-digit",
          minute: "2-digit",
        },
  ).format(new Date(value));
}

export function ItemCard({
  item,
  note,
  hero = false,
  byline,
  publishedAtFormat = "datetime",
  compactActions = false,
  showScore = false,
}: ItemCardProps) {
  void showScore;
  const resolvedByline = byline?.trim() || item.authors.join(", ").trim();

  return (
    <article
      className={clsx(
        "editorial-panel",
        !hero && "h-full",
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
            <Link className="hover:text-[var(--accent)]" to={`/items/${item.id}`}>
              {item.title}
            </Link>
          </h3>
        </div>
        <div className="flex h-12 w-12 items-center justify-center rounded-full border border-[var(--ink)]/10 bg-[var(--paper-strong)]">
          {item.content_type === "paper" ? <Microscope className="h-5 w-5" /> : <Newspaper className="h-5 w-5" />}
        </div>
      </div>

      <div className="mt-4 flex flex-wrap items-center gap-3 font-mono text-xs uppercase tracking-[0.18em] text-[var(--muted)]">
        {resolvedByline ? <span>{resolvedByline}</span> : null}
        <span>{formatPublishedAt(item.published_at, publishedAtFormat)}</span>
      </div>

      <SkimmableText className="mt-5 text-base leading-7 text-[var(--muted-strong)]">
        {item.short_summary || note || "Summary pending."}
      </SkimmableText>
      {note ? (
        <SkimmableText className="mt-4 rounded-2xl border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.55)] px-4 py-3 text-sm leading-6 text-[var(--ink)]">
          {note}
        </SkimmableText>
      ) : null}

      <div className="mt-6">
        <QuickActions compact={compactActions} itemId={item.id} starred={item.starred} triageStatus={item.triage_status} url={item.canonical_url} />
      </div>
      <Link className="mt-6 inline-flex items-center gap-2 font-mono text-xs uppercase tracking-[0.26em] text-[var(--ink)]" to={`/items/${item.id}`}>
        Open detail
        <ArrowRight className="h-4 w-4" />
      </Link>
    </article>
  );
}
