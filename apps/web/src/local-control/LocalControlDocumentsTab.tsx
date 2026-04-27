import { useDeferredValue, useEffect, useLayoutEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Check, ExternalLink, LibraryBig, LoaderCircle, Star } from "lucide-react";
import { Link, useLocation, useSearchParams } from "react-router-dom";

import type { ItemListEntry } from "../api/types";
import { resolveExternalUrl } from "../lib/external-links";
import { LocalControlError, localControlClient } from "./client";

type DocumentPreset = {
  label: string;
  status?: string;
  type?: ItemListEntry["content_type"];
};

type DocumentSortOption = "newest" | "oldest" | "importance";
type DocumentDateRangeOption = "today" | "yesterday" | "this-week" | "this-month" | "this-year" | "last-year" | "all";
type DocumentSubDocumentsFilter = "show" | "hide";
type DocumentPageSizeOption = 20 | 50 | 100;

const DOCUMENTS_SCROLL_STORAGE_KEY = "local-control-documents-scroll";
const DOCUMENT_PAGE_SIZE_OPTIONS: DocumentPageSizeOption[] = [20, 50, 100];

const DOCUMENT_PRESETS: DocumentPreset[] = [
  { label: "All" },
  { label: "Articles", type: "article" },
  { label: "News", type: "news" },
  { label: "Papers", type: "paper" },
  { label: "Newsletters", type: "newsletter" },
  { label: "Posts", type: "post" },
  { label: "Signals", type: "signal" },
  { label: "Threads", type: "thread" },
  { label: "Archived", status: "archived" },
];

const DOCUMENT_TYPE_OPTIONS: Array<{
  label: string;
  value: "" | ItemListEntry["content_type"];
}> = [
  { label: "All document types", value: "" },
  { label: "Article", value: "article" },
  { label: "News", value: "news" },
  { label: "Newsletter", value: "newsletter" },
  { label: "Paper", value: "paper" },
  { label: "Post", value: "post" },
  { label: "Signal", value: "signal" },
  { label: "Thread", value: "thread" },
];

const DOCUMENT_DATE_RANGE_OPTIONS: Array<{
  label: string;
  value: DocumentDateRangeOption;
}> = [
  { label: "Today", value: "today" },
  { label: "Yesterday", value: "yesterday" },
  { label: "This week", value: "this-week" },
  { label: "This month", value: "this-month" },
  { label: "This year", value: "this-year" },
  { label: "Last year", value: "last-year" },
  { label: "All", value: "all" },
];

const CONTENT_TYPE_LABEL: Record<ItemListEntry["content_type"], string> = {
  article: "Article",
  news: "News",
  newsletter: "Newsletter",
  paper: "Paper",
  post: "Post",
  signal: "Signal",
  thread: "Thread",
};

function formatDocumentDetailPath(itemId: string, search: string) {
  return search ? `/documents/${itemId}${search}` : `/documents/${itemId}`;
}

function parseContentTypeFilter(value: string | null): "" | ItemListEntry["content_type"] {
  switch (value) {
    case "article":
    case "news":
    case "newsletter":
    case "paper":
    case "post":
    case "signal":
    case "thread":
      return value;
    default:
      return "";
  }
}

function parseSort(value: string | null): DocumentSortOption {
  switch (value) {
    case "newest":
    case "oldest":
    case "importance":
      return value;
    default:
      return "importance";
  }
}

function parseDateRange(value: string | null): DocumentDateRangeOption {
  switch (value) {
    case "today":
    case "yesterday":
    case "this-week":
    case "this-month":
    case "this-year":
    case "last-year":
    case "all":
      return value;
    default:
      return "this-week";
  }
}

function parseSubDocumentsFilter(value: string | null): DocumentSubDocumentsFilter {
  return value === "hide" ? "hide" : "show";
}

function parsePage(value: string | null) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 1) {
    return 1;
  }
  return parsed;
}

function parsePageSize(value: string | null): DocumentPageSizeOption {
  switch (value) {
    case "20":
      return 20;
    case "50":
      return 50;
    case "100":
      return 100;
    default:
      return 50;
  }
}

function formatLocalIsoDate(value: Date) {
  const year = value.getFullYear();
  const month = String(value.getMonth() + 1).padStart(2, "0");
  const day = String(value.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function shiftDays(value: Date, days: number) {
  const next = new Date(value);
  next.setDate(next.getDate() + days);
  return next;
}

function resolveDateRangeBounds(range: DocumentDateRangeOption) {
  if (range === "all") {
    return { from: undefined, to: undefined };
  }

  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const to = formatLocalIsoDate(today);
  const currentYear = today.getFullYear();

  switch (range) {
    case "today":
      return { from: to, to };
    case "yesterday": {
      const yesterday = formatLocalIsoDate(shiftDays(today, -1));
      return { from: yesterday, to: yesterday };
    }
    case "this-week":
      return { from: formatLocalIsoDate(shiftDays(today, -6)), to };
    case "this-month":
      return { from: formatLocalIsoDate(shiftDays(today, -29)), to };
    case "this-year":
      return { from: `${currentYear}-01-01`, to };
    case "last-year":
      return {
        from: `${currentYear - 1}-01-01`,
        to: `${currentYear - 1}-12-31`,
      };
    default:
      return { from: undefined, to: undefined };
  }
}

function persistDocumentsScroll(search: string) {
  if (typeof window === "undefined") return;

  window.sessionStorage.setItem(
    DOCUMENTS_SCROLL_STORAGE_KEY,
    JSON.stringify({
      search,
      scrollY: window.scrollY,
    }),
  );
}

function consumeDocumentsScroll(search: string) {
  if (typeof window === "undefined") return null;

  const raw = window.sessionStorage.getItem(DOCUMENTS_SCROLL_STORAGE_KEY);
  if (!raw) return null;

  try {
    const parsed = JSON.parse(raw) as { scrollY?: number; search?: string };
    if (parsed.search !== search || typeof parsed.scrollY !== "number") {
      return null;
    }
    window.sessionStorage.removeItem(DOCUMENTS_SCROLL_STORAGE_KEY);
    return parsed.scrollY;
  } catch {
    window.sessionStorage.removeItem(DOCUMENTS_SCROLL_STORAGE_KEY);
    return null;
  }
}

function formatDocumentDate(value: string | null) {
  if (!value) return "Undated";
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
    year: "numeric",
  }).format(new Date(value));
}

function formatLabel(value: string) {
  return value.replace(/_/g, " ");
}

function describeRequestError(error: unknown, fallback: string) {
  if (error instanceof LocalControlError) {
    return error.message;
  }
  if (error instanceof Error && error.message) {
    return error.message;
  }
  return fallback;
}

function readReasonTraceString(reasonTrace: Record<string, unknown>, key: string) {
  const value = reasonTrace[key];
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function readReasonTraceStringList(reasonTrace: Record<string, unknown>, key: string) {
  const value = reasonTrace[key];
  if (!Array.isArray(value)) return [];
  return value
    .filter((entry): entry is string => typeof entry === "string" && entry.trim().length > 0)
    .map((entry) => entry.trim());
}

function formatFallbackScoreRationale(reasonTrace: Record<string, unknown>) {
  const fragments: string[] = [];
  const topicMatches = typeof reasonTrace.topic_matches === "number" ? reasonTrace.topic_matches : 0;
  const authorMatches = typeof reasonTrace.author_matches === "number" ? reasonTrace.author_matches : 0;
  const favoriteSourceMatch = reasonTrace.favorite_source_match === true;
  const ignoredPenalty = typeof reasonTrace.ignored_penalty === "number" ? reasonTrace.ignored_penalty : 0;

  if (favoriteSourceMatch) fragments.push("favorite source matched");
  if (topicMatches > 0) fragments.push(`${topicMatches} topic match${topicMatches === 1 ? "" : "es"}`);
  if (authorMatches > 0) fragments.push(`${authorMatches} author match${authorMatches === 1 ? "" : "es"}`);
  if (ignoredPenalty > 0) fragments.push(`${ignoredPenalty.toFixed(2)} ignored-topic penalty`);

  if (!fragments.length) {
    return "No detailed scoring rationale was stored for this item.";
  }

  return `${fragments.join(", ")}.`;
}

function getScoreRationale(item: Pick<ItemListEntry, "reason_trace">) {
  return (
    readReasonTraceString(item.reason_trace, "judge_reason") ??
    readReasonTraceString(item.reason_trace, "llm_reason") ??
    formatFallbackScoreRationale(item.reason_trace)
  );
}

function getScoreEvidence(item: Pick<ItemListEntry, "reason_trace">) {
  return readReasonTraceStringList(item.reason_trace, "judge_evidence_quotes")[0] ?? null;
}

function ScoreTooltip({
  item,
}: {
  item: Pick<ItemListEntry, "total_score" | "score_breakdown" | "reason_trace">;
}) {
  const breakdown = [
    { label: "Relevance", value: item.score_breakdown.relevance_score },
    { label: "Novelty", value: item.score_breakdown.novelty_score },
    { label: "Source", value: item.score_breakdown.source_quality_score },
    { label: "Author", value: item.score_breakdown.author_match_score },
    { label: "Topic", value: item.score_breakdown.topic_match_score },
  ];
  const rationale = getScoreRationale(item);
  const evidence = getScoreEvidence(item);
  const judgeModel = readReasonTraceString(item.reason_trace, "judge_model");
  const heading = judgeModel && !judgeModel.startsWith("heuristic:") ? "Model rationale" : "Scoring rationale";

  return (
    <div className="group/score relative inline-flex">
      <button
        aria-label={`${item.total_score.toFixed(2)} score details`}
        className="inline-flex items-center rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.86)] px-2.5 py-1 font-mono text-[10px] uppercase tracking-[0.14em] text-[var(--muted-strong)] transition hover:-translate-y-0.5 hover:border-[var(--accent)]/22 hover:text-[var(--ink)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--accent)]/18"
        type="button"
      >
        {item.total_score.toFixed(2)} score
      </button>
      <div className="pointer-events-none absolute left-0 top-full z-30 mt-3 w-[22rem] max-w-[min(22rem,calc(100vw-3rem))] translate-y-2 opacity-0 transition duration-150 group-hover/score:translate-y-0 group-hover/score:opacity-100 group-focus-within/score:translate-y-0 group-focus-within/score:opacity-100">
        <div className="overflow-hidden rounded-[1.35rem] border border-[var(--ink)]/10 bg-[linear-gradient(180deg,rgba(255,255,255,0.98),rgba(247,242,236,0.96))] shadow-[0_24px_60px_rgba(17,19,18,0.16)] backdrop-blur-sm">
          <div className="border-b border-[var(--ink)]/8 bg-[rgba(255,255,255,0.56)] px-4 py-3">
            <p className="font-mono text-[9px] uppercase tracking-[0.18em] text-[var(--muted)]">{heading}</p>
            <p className="mt-1.5 text-[13px] leading-5 text-[var(--ink)]">{rationale}</p>
          </div>
          <div className="space-y-3 px-4 py-4">
            {evidence ? (
              <div className="rounded-[1rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.62)] px-3 py-3">
                <p className="font-mono text-[9px] uppercase tracking-[0.16em] text-[var(--muted)]">Evidence</p>
                <p className="mt-1.5 text-[12px] leading-5 text-[var(--muted-strong)]">&ldquo;{evidence}&rdquo;</p>
              </div>
            ) : null}
            <div className="grid grid-cols-2 gap-2">
              {breakdown.map((entry) => (
                <div
                  key={entry.label}
                  className="rounded-[0.95rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.52)] px-3 py-2.5"
                >
                  <p className="font-mono text-[9px] uppercase tracking-[0.16em] text-[var(--muted)]">{entry.label}</p>
                  <p className="mt-1 text-sm font-medium text-[var(--ink)]">{entry.value.toFixed(2)}</p>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

const COMPACT_ACTION_CLASS =
  "flex h-8 w-8 shrink-0 items-center justify-center rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.78)] text-[var(--muted)] transition hover:-translate-y-0.5 hover:border-[var(--accent)]/26 hover:text-[var(--accent)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--accent)]/18 disabled:cursor-not-allowed disabled:opacity-60";

const TITLE_CLAMP_STYLE = {
  display: "-webkit-box",
  WebkitBoxOrient: "vertical" as const,
  WebkitLineClamp: 2,
  overflow: "hidden",
};

const SUMMARY_CLAMP_STYLE = {
  display: "-webkit-box",
  WebkitBoxOrient: "vertical" as const,
  WebkitLineClamp: 2,
  overflow: "hidden",
};

function presetIsActive(
  preset: DocumentPreset,
  {
    statusFilter,
    contentTypeFilter,
  }: {
    statusFilter: string;
    contentTypeFilter: "" | ItemListEntry["content_type"];
  },
) {
  if (preset.label === "Archived") {
    return statusFilter === "archived";
  }
  if (preset.label === "All") {
    return !statusFilter && !contentTypeFilter;
  }
  return !statusFilter && contentTypeFilter === (preset.type ?? "");
}

function InlineChip({
  label,
  tone = "default",
}: {
  label: string;
  tone?: "default" | "success" | "warning";
}) {
  const className =
    tone === "success"
      ? "border-[rgba(22,163,74,0.2)] bg-[rgba(22,163,74,0.08)] text-[#166534]"
      : tone === "warning"
        ? "border-[var(--accent)]/18 bg-[rgba(154,52,18,0.08)] text-[var(--accent)]"
        : "border-[var(--ink)]/10 bg-[rgba(255,255,255,0.72)] text-[var(--muted-strong)]";

  return (
    <span className={`inline-flex rounded-full border px-2.5 py-0.5 font-mono text-[9px] uppercase tracking-[0.14em] ${className}`}>
      {label}
    </span>
  );
}

function DocumentTableRow({
  item,
  search,
}: {
  item: ItemListEntry;
  search: string;
}) {
  const queryClient = useQueryClient();
  const [optimisticRead, setOptimisticRead] = useState(item.read);
  const [optimisticStarred, setOptimisticStarred] = useState(item.starred);
  const [zoteroState, setZoteroState] = useState<"idle" | "saved" | "needs_review">("idle");
  const [zoteroDetail, setZoteroDetail] = useState<string | null>(null);

  useEffect(() => {
    setOptimisticRead(item.read);
  }, [item.read]);

  useEffect(() => {
    setOptimisticStarred(item.starred);
  }, [item.starred]);

  const refreshDocuments = async () => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: ["local-control", "documents"] }),
      queryClient.invalidateQueries({ queryKey: ["local-control", "document", item.id] }),
    ]);
  };

  const markRead = useMutation({
    mutationFn: () => localControlClient.markDocumentRead(item.id),
    onMutate: () => {
      const previousRead = optimisticRead;
      setOptimisticRead(true);
      return { previousRead };
    },
    onError: (_error, _variables, context) => {
      setOptimisticRead(context?.previousRead ?? item.read);
    },
    onSuccess: refreshDocuments,
  });

  const star = useMutation({
    mutationFn: () => localControlClient.starDocument(item.id),
    onMutate: () => {
      const previousRead = optimisticRead;
      const previousStarred = optimisticStarred;
      setOptimisticRead(true);
      setOptimisticStarred(!previousStarred);
      return { previousRead, previousStarred };
    },
    onError: (_error, _variables, context) => {
      setOptimisticRead(context?.previousRead ?? item.read);
      setOptimisticStarred(context?.previousStarred ?? item.starred);
    },
    onSuccess: refreshDocuments,
  });

  const saveToZotero = useMutation({
    mutationFn: () => localControlClient.saveDocumentToZotero(item.id),
    onMutate: () => {
      const previousZoteroState = zoteroState;
      const previousZoteroDetail = zoteroDetail;
      setZoteroDetail(null);
      return { previousZoteroDetail, previousZoteroState };
    },
    onError: (error, _variables, context) => {
      setZoteroState(context?.previousZoteroState ?? "idle");
      setZoteroDetail(describeRequestError(error, "Could not save this document to Zotero."));
    },
    onSuccess: async (response) => {
      if (response.triage_status === "saved") {
        setOptimisticRead(true);
        setZoteroState("saved");
      } else if (response.triage_status === "needs_review") {
        setZoteroState("needs_review");
      } else {
        setZoteroState("idle");
      }
      setZoteroDetail(response.detail);
      await refreshDocuments();
    },
  });

  const authorLabel = item.authors.join(", ").trim();
  const bucketTone =
    item.bucket === "must_read" ? "success" : item.bucket === "archive" ? "warning" : "default";
  const triageTone = item.triage_status === "archived" ? "warning" : "default";
  const triageLabel = item.triage_status === "unread" && optimisticRead ? null : formatLabel(item.triage_status);
  const interactionBusy = markRead.isPending || star.isPending || saveToZotero.isPending;
  const starLabel = star.isPending ? "Updating importance" : optimisticStarred ? "Important" : "Mark important";
  const starButtonClass = optimisticStarred
    ? `${COMPACT_ACTION_CLASS} border-[#d97706]/28 bg-[rgba(245,158,11,0.16)] text-[#9a3412] shadow-[0_10px_24px_rgba(217,119,6,0.14)] hover:border-[#d97706]/38 hover:text-[#9a3412]`
    : COMPACT_ACTION_CLASS;
  const zoteroLabel = saveToZotero.isPending
    ? "Adding to Zotero"
    : zoteroState === "saved"
      ? "Saved to Zotero"
      : zoteroState === "needs_review"
        ? "Zotero needs review"
        : "Add to Zotero";
  const zoteroTitle = zoteroDetail ?? zoteroLabel;
  const zoteroButtonClass = saveToZotero.isPending
    ? `${COMPACT_ACTION_CLASS} border-[var(--accent)]/24 text-[var(--accent)]`
    : zoteroState === "saved"
      ? `${COMPACT_ACTION_CLASS} border-[rgba(22,163,74,0.24)] bg-[rgba(22,163,74,0.12)] text-[#166534] shadow-[0_10px_24px_rgba(22,163,74,0.12)] hover:border-[rgba(22,163,74,0.34)] hover:text-[#166534]`
      : zoteroState === "needs_review"
        ? `${COMPACT_ACTION_CLASS} border-[#d97706]/24 bg-[rgba(245,158,11,0.12)] text-[#9a3412] shadow-[0_10px_24px_rgba(217,119,6,0.1)] hover:border-[#d97706]/34 hover:text-[#9a3412]`
        : COMPACT_ACTION_CLASS;

  const markAsRead = () => {
    if (optimisticRead || markRead.isPending) return;
    markRead.mutate();
  };

  return (
    <tr className="border-b border-[var(--ink)]/8 align-top last:border-b-0 hover:bg-[rgba(255,255,255,0.36)]">
      <td className="px-4 py-4">
        <div className="max-w-[31rem]">
          <Link
            className="block font-display text-[0.92rem] leading-[1.28] text-[var(--ink)] transition hover:text-[var(--accent)] md:text-[0.98rem]"
            onClick={() => {
              persistDocumentsScroll(search);
              markAsRead();
            }}
            to={formatDocumentDetailPath(item.id, search)}
            style={TITLE_CLAMP_STYLE}
          >
            {item.title}
          </Link>
          {item.short_summary ? (
            <p className="mt-1.5 text-[13px] leading-5 text-[var(--muted-strong)]" style={SUMMARY_CLAMP_STYLE}>
              {item.short_summary}
            </p>
          ) : null}
        </div>
      </td>
      <td className="px-4 py-4">
        <p className="truncate text-[14px] leading-5 text-[var(--muted-strong)]">{item.source_name}</p>
        <p className="mt-1 truncate font-mono text-[10px] uppercase tracking-[0.14em] text-[var(--muted)]">
          {authorLabel || "Unknown author"}
        </p>
      </td>
      <td className="px-4 py-4 align-middle">
        <InlineChip label={CONTENT_TYPE_LABEL[item.content_type]} />
      </td>
      <td className="px-4 py-4">
        <div className="flex flex-wrap gap-1.5">
          <InlineChip label={formatLabel(item.bucket)} tone={bucketTone} />
          {triageLabel ? <InlineChip label={triageLabel} tone={triageTone} /> : null}
        </div>
        <div className="mt-1.5 flex items-center gap-1.5 text-[11px] leading-5 text-[var(--muted)]">
          <ScoreTooltip item={item} /> · {item.also_mentioned_in_count} related
        </div>
      </td>
      <td className="px-4 py-4 font-mono text-[10px] uppercase tracking-[0.14em] text-[var(--muted-strong)] whitespace-nowrap">
        {formatDocumentDate(item.published_at)}
      </td>
      <td className="px-4 py-4">
        <div className="flex items-center gap-1.5">
          <button
            aria-label={starLabel}
            aria-pressed={optimisticStarred}
            className={starButtonClass}
            disabled={interactionBusy}
            onClick={() => star.mutate()}
            title={starLabel}
            type="button"
          >
            <Star className={`h-[13px] w-[13px] ${optimisticStarred ? "fill-current text-[#d97706]" : ""}`} />
            <span className="sr-only">{starLabel}</span>
          </button>
          <button
            aria-label={zoteroTitle}
            className={zoteroButtonClass}
            disabled={interactionBusy || zoteroState === "saved"}
            onClick={() => saveToZotero.mutate()}
            title={zoteroTitle}
            type="button"
          >
            {saveToZotero.isPending ? (
              <LoaderCircle className="h-[13px] w-[13px] animate-spin" />
            ) : zoteroState === "saved" ? (
              <Check className="h-[13px] w-[13px]" />
            ) : (
              <LibraryBig className="h-[13px] w-[13px]" />
            )}
            <span className="sr-only">{zoteroLabel}</span>
          </button>
          <a
            aria-label="Open source"
            className={COMPACT_ACTION_CLASS}
            href={resolveExternalUrl(item.canonical_url)}
            onClick={() => markAsRead()}
            rel="noreferrer"
            target="_blank"
            title="Open source"
          >
            <ExternalLink className="h-[13px] w-[13px]" />
            <span className="sr-only">Open source</span>
          </a>
        </div>
      </td>
    </tr>
  );
}

function DocumentsTable({
  items,
  search,
}: {
  items: ItemListEntry[];
  search: string;
}) {
  return (
    <div className="editorial-panel overflow-visible px-0 py-0">
      <div className="overflow-x-auto overflow-y-visible">
        <table className="min-w-[840px] w-full table-fixed border-collapse">
          <thead>
            <tr className="border-b border-[var(--ink)]/8 bg-[rgba(255,255,255,0.48)] text-left">
              <th className="w-[35%] px-4 py-4 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]" scope="col">
                Document
              </th>
              <th className="w-[17%] px-4 py-4 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]" scope="col">
                Source
              </th>
              <th className="w-[8%] px-4 py-4 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]" scope="col">
                Type
              </th>
              <th className="w-[19%] px-4 py-4 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]" scope="col">
                Status
              </th>
              <th className="w-[8%] px-4 py-4 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]" scope="col">
                Date
              </th>
              <th className="w-[13%] px-4 py-4 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]" scope="col">
                Actions
              </th>
            </tr>
          </thead>
          <tbody>
            {items.map((item) => (
              <DocumentTableRow item={item} key={item.id} search={search} />
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export function LocalControlDocumentsTab() {
  const location = useLocation();
  const [searchParams, setSearchParams] = useSearchParams();
  const statusFilter = searchParams.get("status") ?? "";
  const contentTypeFilter = parseContentTypeFilter(searchParams.get("type"));
  const sort = parseSort(searchParams.get("sort"));
  const dateRange = parseDateRange(searchParams.get("range"));
  const subDocumentsFilter = parseSubDocumentsFilter(searchParams.get("subdocs"));
  const page = parsePage(searchParams.get("page"));
  const pageSize = parsePageSize(searchParams.get("page_size"));
  const query = searchParams.get("q") ?? "";
  const sourceId = searchParams.get("source") ?? "";
  const deferredQuery = useDeferredValue(query.trim());
  const dateRangeBounds = useMemo(() => resolveDateRangeBounds(dateRange), [dateRange]);

  const updateSearchParams = (updates: {
    contentTypeFilter?: "" | ItemListEntry["content_type"];
    dateRange?: DocumentDateRangeOption;
    page?: number;
    pageSize?: DocumentPageSizeOption;
    query?: string;
    resetPage?: boolean;
    sort?: DocumentSortOption;
    sourceId?: string;
    statusFilter?: string;
    subDocumentsFilter?: DocumentSubDocumentsFilter;
  }) => {
    const next = new URLSearchParams(searchParams);

    if (updates.resetPage) {
      next.delete("page");
    }

    if (updates.query !== undefined) {
      if (updates.query) {
        next.set("q", updates.query);
      } else {
        next.delete("q");
      }
    }

    if (updates.sourceId !== undefined) {
      if (updates.sourceId) {
        next.set("source", updates.sourceId);
      } else {
        next.delete("source");
      }
    }

    if (updates.contentTypeFilter !== undefined) {
      if (updates.contentTypeFilter) {
        next.set("type", updates.contentTypeFilter);
      } else {
        next.delete("type");
      }
    }

    if (updates.statusFilter !== undefined) {
      if (updates.statusFilter) {
        next.set("status", updates.statusFilter);
      } else {
        next.delete("status");
      }
    }

    if (updates.dateRange !== undefined) {
      next.delete("from");
      next.delete("to");
      if (updates.dateRange !== "this-week") {
        next.set("range", updates.dateRange);
      } else {
        next.delete("range");
      }
    }

    if (updates.sort !== undefined) {
      if (updates.sort === "importance") {
        next.delete("sort");
      } else {
        next.set("sort", updates.sort);
      }
    }

    if (updates.subDocumentsFilter !== undefined) {
      if (updates.subDocumentsFilter === "hide") {
        next.set("subdocs", "hide");
      } else {
        next.delete("subdocs");
      }
    }

    if (updates.page !== undefined) {
      if (updates.page > 1) {
        next.set("page", String(updates.page));
      } else {
        next.delete("page");
      }
    }

    if (updates.pageSize !== undefined) {
      if (updates.pageSize === 50) {
        next.delete("page_size");
      } else {
        next.set("page_size", String(updates.pageSize));
      }
    }

    setSearchParams(next, { replace: true });
  };

  const sourcesQuery = useQuery({
    queryKey: ["local-control", "sources"],
    queryFn: localControlClient.getSources,
    retry: false,
  });
  const documentsQuery = useQuery({
    queryKey: [
      "local-control",
      "documents",
      statusFilter,
      contentTypeFilter,
      sourceId,
      dateRange,
      subDocumentsFilter,
      sort,
      deferredQuery,
      page,
      pageSize,
    ],
    queryFn: () =>
      localControlClient.getDocuments({
        status: statusFilter || undefined,
        content_type: contentTypeFilter || undefined,
        source_id: sourceId || undefined,
        from: dateRangeBounds.from,
        to: dateRangeBounds.to,
        hide_sub_documents: subDocumentsFilter === "hide" ? true : undefined,
        page,
        page_size: pageSize,
        sort,
        q: deferredQuery || undefined,
      }),
    retry: false,
  });

  const documents = documentsQuery.data?.items ?? [];
  const totalDocuments = documentsQuery.data?.total ?? 0;
  const totalPages = Math.max(1, Math.ceil(totalDocuments / pageSize));
  const pageStart = totalDocuments === 0 ? 0 : (page - 1) * pageSize + 1;
  const pageEnd = totalDocuments === 0 ? 0 : pageStart + documents.length - 1;

  useEffect(() => {
    if (documentsQuery.isLoading || totalDocuments === 0 || page <= totalPages) {
      return;
    }
    updateSearchParams({ page: totalPages });
  }, [documentsQuery.isLoading, page, totalDocuments, totalPages]);

  useLayoutEffect(() => {
    if (documentsQuery.isLoading) return;

    const scrollY = consumeDocumentsScroll(location.search);
    if (scrollY === null) return;

    const frame = window.requestAnimationFrame(() => {
      window.scrollTo({ top: scrollY, behavior: "auto" });
    });

    return () => window.cancelAnimationFrame(frame);
  }, [documents.length, documentsQuery.isError, documentsQuery.isLoading, location.search]);

  const emptyMessage = useMemo(() => {
    const contentTypeLabel = contentTypeFilter ? CONTENT_TYPE_LABEL[contentTypeFilter].toLowerCase() : null;
    if (query.trim()) {
      return subDocumentsFilter === "hide"
        ? `No vault documents match “${query.trim()}” once sub-documents are hidden.`
        : `No vault documents match “${query.trim()}”.`;
    }
    if (sourceId) {
      const sourceName = sourcesQuery.data?.find((source) => source.id === sourceId)?.name ?? "that source";
      return subDocumentsFilter === "hide"
        ? `No documents from ${sourceName} remain once sub-documents are hidden.`
        : `No documents from ${sourceName} match this view yet.`;
    }
    if (dateRange !== "all") {
      return subDocumentsFilter === "hide"
        ? "No vault documents remain in the selected time range once sub-documents are hidden."
        : "No vault documents fall within the selected time range.";
    }
    if (statusFilter === "archived") {
      if (contentTypeLabel) {
        return subDocumentsFilter === "hide"
          ? `No archived ${contentTypeLabel} documents remain once sub-documents are hidden.`
          : `No archived ${contentTypeLabel} documents yet.`;
      }
      return subDocumentsFilter === "hide"
        ? "No archived vault documents remain once sub-documents are hidden."
        : "No archived vault documents yet.";
    }
    if (contentTypeLabel) {
      return subDocumentsFilter === "hide"
        ? `No ${contentTypeLabel} documents remain once sub-documents are hidden.`
        : `No ${contentTypeLabel} documents are available for this view yet.`;
    }
    return subDocumentsFilter === "hide"
      ? "No vault documents remain once sub-documents are hidden."
      : "No vault documents are available for this view yet.";
  }, [contentTypeFilter, dateRange, query, sourceId, sourcesQuery.data, statusFilter, subDocumentsFilter]);

  return (
    <div className="space-y-6">
      <section className="editorial-panel">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <p className="section-kicker">Vault index</p>
            <h3 className="section-title">All documents present in the vault</h3>
            <p className="mt-4 max-w-3xl text-sm leading-7 text-[var(--muted)]">
              This is the full read-only working table for the local vault. Filter by source, content type, and status to inspect what the Mac has already materialized into the items index.
            </p>
          </div>
          <div className="rounded-[1.5rem] border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.56)] px-5 py-5">
            <p className="field-label">Current slice</p>
            <p className="mt-2 text-sm leading-6 text-[var(--muted-strong)]">
              {pageStart.toLocaleString()}-{pageEnd.toLocaleString()} of {totalDocuments.toLocaleString()} document{totalDocuments === 1 ? "" : "s"}
            </p>
            <p className="text-sm leading-6 text-[var(--muted)]">
              Page {Math.min(page, totalPages).toLocaleString()} of {totalPages.toLocaleString()} · {sourcesQuery.data?.length ?? 0} registered source
              {sourcesQuery.data?.length === 1 ? "" : "s"}
            </p>
          </div>
        </div>

        <div className="mt-6 grid gap-3 md:grid-cols-2 xl:grid-cols-[minmax(0,1fr)_220px_220px_220px_180px_140px]">
          <input
            className="field-input"
            onChange={(event) => updateSearchParams({ query: event.target.value, resetPage: true })}
            placeholder="Search titles, summaries, or body text"
            value={query}
          />
          <select
            className="field-input"
            onChange={(event) => updateSearchParams({ sourceId: event.target.value, resetPage: true })}
            value={sourceId}
          >
            <option value="">{sourcesQuery.isLoading ? "Loading sources..." : "All sources"}</option>
            {sourcesQuery.data?.map((source) => (
              <option key={source.id} value={source.id}>
                {source.name}
              </option>
            ))}
          </select>
          <select
            className="field-input"
            onChange={(event) =>
              updateSearchParams({
                contentTypeFilter: event.target.value as "" | ItemListEntry["content_type"],
                resetPage: true,
              })
            }
            value={contentTypeFilter}
          >
            {DOCUMENT_TYPE_OPTIONS.map((option) => (
              <option key={option.value || "all"} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
          <select
            className="field-input"
            onChange={(event) => updateSearchParams({ dateRange: parseDateRange(event.target.value), resetPage: true })}
            value={dateRange}
          >
            {DOCUMENT_DATE_RANGE_OPTIONS.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
          <select
            className="field-input"
            onChange={(event) => updateSearchParams({ sort: parseSort(event.target.value), resetPage: true })}
            value={sort}
          >
            <option value="newest">Newest</option>
            <option value="oldest">Oldest</option>
            <option value="importance">Importance</option>
          </select>
          <select
            className="field-input"
            onChange={(event) =>
              updateSearchParams({
                pageSize: parsePageSize(event.target.value),
                resetPage: true,
              })
            }
            value={pageSize}
          >
            {DOCUMENT_PAGE_SIZE_OPTIONS.map((option) => (
              <option key={option} value={option}>
                {option} rows
              </option>
            ))}
          </select>
        </div>

        <div className="mt-5 flex flex-wrap gap-2">
          {DOCUMENT_PRESETS.map((preset) => (
            <button
              key={preset.label}
              className={`filter-pill ${presetIsActive(preset, { statusFilter, contentTypeFilter }) ? "filter-pill-active" : ""}`}
              onClick={() => {
                updateSearchParams({
                  statusFilter: preset.status ?? "",
                  contentTypeFilter: preset.type ?? "",
                  resetPage: true,
                });
              }}
              type="button"
            >
              {preset.label}
            </button>
          ))}
          <button
            aria-pressed={subDocumentsFilter === "hide"}
            className={`filter-pill ${subDocumentsFilter === "hide" ? "filter-pill-active" : ""}`}
            onClick={() =>
              updateSearchParams({
                subDocumentsFilter: subDocumentsFilter === "hide" ? "show" : "hide",
                resetPage: true,
              })
            }
            type="button"
          >
            {subDocumentsFilter === "hide" ? "Sub-documents hidden" : "Hide sub-documents"}
          </button>
        </div>
      </section>

      {documentsQuery.isLoading ? <div className="page-loading">Loading vault documents…</div> : null}
      {documentsQuery.isError ? (
        <div className="editorial-panel">
          <p className="section-kicker">Documents unavailable</p>
          <p className="mt-3 text-sm leading-6 text-[var(--danger)]">
            {describeRequestError(documentsQuery.error, "The vault document table could not be loaded.")}
          </p>
        </div>
      ) : null}
      {!documentsQuery.isLoading && !documentsQuery.isError && documents.length === 0 ? (
        <div className="page-empty">{emptyMessage}</div>
      ) : null}
      {!documentsQuery.isLoading && !documentsQuery.isError && documents.length > 0 ? (
        <div className="space-y-4">
          <DocumentsTable items={documents} search={location.search} />
          <div className="editorial-panel flex flex-col gap-3 px-5 py-4 md:flex-row md:items-center md:justify-between">
            <p className="text-sm leading-6 text-[var(--muted)]">
              Showing {pageStart.toLocaleString()}-{pageEnd.toLocaleString()} of {totalDocuments.toLocaleString()} documents.
            </p>
            <div className="flex items-center gap-2">
              <button
                className="secondary-button px-3 py-2 text-[11px] whitespace-nowrap"
                disabled={page <= 1}
                onClick={() => updateSearchParams({ page: page - 1 })}
                type="button"
              >
                Previous
              </button>
              <span className="font-mono text-[11px] uppercase tracking-[0.14em] text-[var(--muted)]">
                Page {Math.min(page, totalPages)} / {totalPages}
              </span>
              <button
                className="secondary-button px-3 py-2 text-[11px] whitespace-nowrap"
                disabled={page >= totalPages}
                onClick={() => updateSearchParams({ page: page + 1 })}
                type="button"
              >
                Next
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
