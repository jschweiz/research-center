import { useDeferredValue, useLayoutEffect, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { ExternalLink } from "lucide-react";
import { Link, useLocation, useSearchParams } from "react-router-dom";

import type { ItemListEntry } from "../api/types";
import { LocalControlError, localControlClient } from "./client";

type DocumentPreset = {
  label: string;
  status?: string;
  type?: ItemListEntry["content_type"];
};

type DocumentSortOption = "newest" | "oldest" | "importance";
type DocumentDateRangeOption = "today" | "this-week" | "this-month" | "this-year" | "last-year" | "all";

const DOCUMENTS_SCROLL_STORAGE_KEY = "local-control-documents-scroll";

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

function DocumentsTable({
  items,
  search,
}: {
  items: ItemListEntry[];
  search: string;
}) {
  return (
    <div className="editorial-panel overflow-hidden px-0 py-0">
      <div className="overflow-x-auto">
        <table className="min-w-[980px] w-full table-fixed border-collapse">
          <thead>
            <tr className="border-b border-[var(--ink)]/8 bg-[rgba(255,255,255,0.48)] text-left">
              <th className="w-[38%] px-5 py-4 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]" scope="col">
                Document
              </th>
              <th className="w-[18%] px-5 py-4 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]" scope="col">
                Source
              </th>
              <th className="w-[10%] px-5 py-4 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]" scope="col">
                Type
              </th>
              <th className="w-[18%] px-5 py-4 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]" scope="col">
                Status
              </th>
              <th className="w-[10%] px-5 py-4 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]" scope="col">
                Date
              </th>
              <th className="w-[6%] px-5 py-4 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]" scope="col">
                Open
              </th>
            </tr>
          </thead>
          <tbody>
            {items.map((item) => {
              const authorLabel = item.authors.join(", ").trim();
              const bucketTone =
                item.bucket === "must_read"
                  ? "success"
                  : item.bucket === "archive"
                    ? "warning"
                    : "default";
              const triageTone = item.triage_status === "archived" ? "warning" : "default";

              return (
                <tr
                  key={item.id}
                  className="border-b border-[var(--ink)]/8 align-top last:border-b-0 hover:bg-[rgba(255,255,255,0.36)]"
                >
                  <td className="px-5 py-4">
                    <div className="max-w-[34rem]">
                      <Link
                        className="block font-display text-[0.92rem] leading-[1.28] text-[var(--ink)] transition hover:text-[var(--accent)] md:text-[0.98rem]"
                        onClick={() => persistDocumentsScroll(search)}
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
                  <td className="px-5 py-4">
                    <p className="truncate text-[14px] leading-5 text-[var(--muted-strong)]">{item.source_name}</p>
                    <p className="mt-1 truncate font-mono text-[10px] uppercase tracking-[0.14em] text-[var(--muted)]">
                      {authorLabel || "Unknown author"}
                    </p>
                  </td>
                  <td className="px-5 py-4 align-middle">
                    <InlineChip label={CONTENT_TYPE_LABEL[item.content_type]} />
                  </td>
                  <td className="px-5 py-4">
                    <div className="flex flex-wrap gap-1.5">
                      <InlineChip label={formatLabel(item.bucket)} tone={bucketTone} />
                      <InlineChip label={formatLabel(item.triage_status)} tone={triageTone} />
                    </div>
                    <p className="mt-1.5 text-[11px] leading-5 text-[var(--muted)]">
                      {item.total_score.toFixed(2)} score · {item.also_mentioned_in_count} related
                    </p>
                  </td>
                  <td className="px-5 py-4 font-mono text-[11px] uppercase tracking-[0.14em] text-[var(--muted-strong)] whitespace-nowrap">
                    {formatDocumentDate(item.published_at)}
                  </td>
                  <td className="px-5 py-4">
                    <a
                      className="secondary-button w-fit px-3 py-2 text-[11px] whitespace-nowrap"
                      href={item.canonical_url}
                      rel="noreferrer"
                      target="_blank"
                    >
                      <ExternalLink className="h-3.5 w-3.5" />
                      Open
                    </a>
                  </td>
                </tr>
              );
            })}
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
  const query = searchParams.get("q") ?? "";
  const sourceId = searchParams.get("source") ?? "";
  const deferredQuery = useDeferredValue(query.trim());
  const dateRangeBounds = useMemo(() => resolveDateRangeBounds(dateRange), [dateRange]);

  const updateSearchParams = (updates: {
    contentTypeFilter?: "" | ItemListEntry["content_type"];
    dateRange?: DocumentDateRangeOption;
    query?: string;
    sort?: DocumentSortOption;
    sourceId?: string;
    statusFilter?: string;
  }) => {
    const next = new URLSearchParams(searchParams);

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
      sort,
      deferredQuery,
    ],
    queryFn: () =>
      localControlClient.getDocuments({
        status: statusFilter || undefined,
        content_type: contentTypeFilter || undefined,
        source_id: sourceId || undefined,
        from: dateRangeBounds.from,
        to: dateRangeBounds.to,
        sort,
        q: deferredQuery || undefined,
      }),
    retry: false,
  });

  useLayoutEffect(() => {
    if (documentsQuery.isLoading) return;

    const scrollY = consumeDocumentsScroll(location.search);
    if (scrollY === null) return;

    const frame = window.requestAnimationFrame(() => {
      window.scrollTo({ top: scrollY, behavior: "auto" });
    });

    return () => window.cancelAnimationFrame(frame);
  }, [documentsQuery.data?.length, documentsQuery.isError, documentsQuery.isLoading, location.search]);

  const emptyMessage = useMemo(() => {
    const contentTypeLabel = contentTypeFilter ? CONTENT_TYPE_LABEL[contentTypeFilter].toLowerCase() : null;
    if (query.trim()) {
      return `No vault documents match “${query.trim()}”.`;
    }
    if (sourceId) {
      const sourceName = sourcesQuery.data?.find((source) => source.id === sourceId)?.name ?? "that source";
      return `No documents from ${sourceName} match this view yet.`;
    }
    if (dateRange !== "all") {
      return "No vault documents fall within the selected time range.";
    }
    if (statusFilter === "archived") {
      return contentTypeLabel ? `No archived ${contentTypeLabel} documents yet.` : "No archived vault documents yet.";
    }
    if (contentTypeLabel) {
      return `No ${contentTypeLabel} documents are available for this view yet.`;
    }
    return "No vault documents are available for this view yet.";
  }, [contentTypeFilter, dateRange, query, sourceId, sourcesQuery.data, statusFilter]);

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
              {(documentsQuery.data?.length ?? 0).toLocaleString()} document{documentsQuery.data?.length === 1 ? "" : "s"}
            </p>
            <p className="text-sm leading-6 text-[var(--muted)]">
              {sourcesQuery.data?.length ?? 0} registered source{sourcesQuery.data?.length === 1 ? "" : "s"}
            </p>
          </div>
        </div>

        <div className="mt-6 grid gap-3 md:grid-cols-2 xl:grid-cols-[minmax(0,1fr)_220px_220px_220px_180px]">
          <input
            className="field-input"
            onChange={(event) => updateSearchParams({ query: event.target.value })}
            placeholder="Search titles, summaries, or body text"
            value={query}
          />
          <select
            className="field-input"
            onChange={(event) => updateSearchParams({ sourceId: event.target.value })}
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
            onChange={(event) => updateSearchParams({ dateRange: parseDateRange(event.target.value) })}
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
            onChange={(event) => updateSearchParams({ sort: parseSort(event.target.value) })}
            value={sort}
          >
            <option value="newest">Newest</option>
            <option value="oldest">Oldest</option>
            <option value="importance">Importance</option>
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
                });
              }}
              type="button"
            >
              {preset.label}
            </button>
          ))}
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
      {!documentsQuery.isLoading && !documentsQuery.isError && (documentsQuery.data?.length ?? 0) === 0 ? (
        <div className="page-empty">{emptyMessage}</div>
      ) : null}
      {!documentsQuery.isLoading && !documentsQuery.isError && (documentsQuery.data?.length ?? 0) > 0 ? (
        <DocumentsTable items={documentsQuery.data ?? []} search={location.search} />
      ) : null}
    </div>
  );
}
