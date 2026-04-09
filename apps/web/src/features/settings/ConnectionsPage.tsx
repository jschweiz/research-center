import { FormEvent, ReactNode, useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { CheckCircle2, ChevronDown, ChevronUp, History, Play, Workflow } from "lucide-react";
import { useSearchParams } from "react-router-dom";

import { api } from "../../api/client";
import type { IngestionRunHistoryEntry, RunStatus, Source, SourceProbeResult, SourceRawKind, SourceType } from "../../api/types";
import { SkimmableText } from "../../components/SkimmableText";
import defaultZoteroAutoTagVocabulary from "../../constants/zoteroAutoTagVocabulary.json";

const DEFAULT_ZOTERO_AUTO_TAG_VOCABULARY = defaultZoteroAutoTagVocabulary as string[];
const EMAIL_RE = /^[^@\s]+@[^@\s]+\.[^@\s]+$/;

type SourceFormState = {
  name: string;
  type: SourceType;
  rawKind: SourceRawKind;
  url: string;
  website: string;
  discoveryMode: "rss_feed" | "website_index";
  query: string;
  description: string;
  maxItems: string;
  tags: string;
  active: boolean;
};

type Notice = {
  tone: "error" | "success";
  message: string;
};

type SourceProbeReport =
  | {
      tone: "error";
      message: string;
    }
  | {
      tone: "success";
      result: SourceProbeResult;
    };

function ConnectionCard({
  label,
  headline,
  summary,
  configured,
  open,
  onToggle,
  notice,
  chips,
  children,
}: {
  label: string;
  headline: string;
  summary: string;
  configured: boolean;
  open: boolean;
  onToggle: () => void;
  notice: Notice | null;
  chips: ReactNode;
  children: ReactNode;
}) {
  const toneClass = configured
    ? "border-[rgba(22,163,74,0.22)] bg-[linear-gradient(180deg,rgba(240,253,244,0.94),rgba(220,252,231,0.72))] shadow-[0_18px_44px_rgba(22,163,74,0.08)]"
    : "border-[var(--ink)]/8 bg-[rgba(255,255,255,0.56)]";
  const labelClass = configured ? "text-[#166534]" : "text-[var(--muted)]";
  const statusClass = configured
    ? "border-[rgba(22,163,74,0.2)] bg-white/78 text-[#166534]"
    : "border-[var(--ink)]/10 bg-[rgba(255,255,255,0.74)] text-[var(--muted-strong)]";
  const summaryClass = configured ? "text-[#166534]" : "text-[var(--muted)]";
  const dividerClass = configured ? "border-[rgba(22,163,74,0.14)]" : "border-[var(--ink)]/8";

  return (
    <article className={`rounded-[1.9rem] border px-5 py-5 transition ${toneClass}`}>
      <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <p className={`font-mono text-[11px] uppercase tracking-[0.24em] ${labelClass}`}>{label}</p>
            <span className={`inline-flex items-center gap-2 rounded-full border px-3 py-1 font-mono text-[11px] uppercase tracking-[0.18em] ${statusClass}`}>
              {configured ? <CheckCircle2 className="h-3.5 w-3.5" /> : null}
              <span>{configured ? "Connected" : "Setup required"}</span>
            </span>
          </div>
          <h4 className="mt-4 font-display text-3xl leading-tight text-[var(--ink)]">{headline}</h4>
          <SkimmableText className={`mt-3 max-w-xl text-sm leading-6 ${summaryClass}`}>{summary}</SkimmableText>
        </div>

        <button
          className={`secondary-button px-3 py-2 text-[11px] ${configured ? "border-[rgba(22,163,74,0.18)] bg-white/80 text-[#166534] hover:border-[rgba(22,163,74,0.3)]" : ""}`}
          onClick={onToggle}
          type="button"
        >
          {open ? "Collapse" : configured ? "Show settings" : "Open setup"}
          {open ? <ChevronUp className="h-3.5 w-3.5" /> : <ChevronDown className="h-3.5 w-3.5" />}
        </button>
      </div>

      {notice ? (
        <div
          className={`mt-5 rounded-2xl border px-4 py-3 text-sm leading-6 ${
            notice.tone === "error"
              ? "border-[var(--danger)]/20 bg-[rgba(255,255,255,0.66)] text-[var(--danger)]"
              : configured
                ? "border-[rgba(22,163,74,0.2)] bg-white/72 text-[#166534]"
                : "border-[var(--ink)]/8 bg-[rgba(255,255,255,0.66)] text-[var(--muted-strong)]"
          }`}
        >
          {notice.message}
        </div>
      ) : null}

      <div className="mt-5 flex flex-wrap gap-2">{chips}</div>

      {open ? <div className={`mt-5 border-t pt-5 ${dividerClass}`}>{children}</div> : null}
    </article>
  );
}

function csvToList(value: string) {
  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function readStringMetadata(value: unknown) {
  return typeof value === "string" && value.trim() ? value : null;
}

function readBooleanMetadata(value: unknown) {
  return typeof value === "boolean" ? value : null;
}

function readStringListMetadata(value: unknown) {
  if (!Array.isArray(value)) return null;
  return value.filter((entry): entry is string => typeof entry === "string" && entry.trim().length > 0);
}

function formatTagVocabulary(tags: string[]) {
  return tags.join("\n");
}

function parseTagVocabularyInput(value: string) {
  const trimmed = value.trim();
  if (!trimmed) return [];
  if (trimmed.startsWith("[")) {
    try {
      const parsed = JSON.parse(trimmed);
      if (Array.isArray(parsed)) {
        return Array.from(
          new Set(parsed.filter((entry): entry is string => typeof entry === "string" && entry.trim().length > 0).map((entry) => entry.trim())),
        );
      }
    } catch {
      // Fall back to line parsing.
    }
  }
  return Array.from(
    new Set(
      trimmed
        .split(/[\n,]+/)
        .map((entry) => entry.trim())
        .filter(Boolean),
    ),
  );
}

function readErrorMessage(error: unknown) {
  return error instanceof Error && error.message ? error.message : "Could not check this source.";
}

function createEmptySourceForm(): SourceFormState {
  return {
    name: "",
    type: "website",
    rawKind: "blog-post",
    url: "",
    website: "",
    discoveryMode: "rss_feed",
    query: "",
    description: "",
    maxItems: "20",
    tags: "",
    active: true,
  };
}

function mapSourceToForm(source: Source): SourceFormState {
  const discoveryMode =
    readStringMetadata(source.config_json.discovery_mode) === "website_index" ? "website_index" : "rss_feed";
  return {
    name: source.name,
    type: source.type,
    rawKind:
      source.raw_kind === "newsletter" || source.raw_kind === "paper" || source.raw_kind === "article" || source.raw_kind === "thread" || source.raw_kind === "signal"
        ? source.raw_kind
        : "blog-post",
    url: source.url ?? "",
    website: readStringMetadata(source.config_json.website_url) ?? "",
    discoveryMode,
    query: source.query ?? "",
    description: source.description ?? "",
    maxItems: String(source.max_items),
    tags: source.tags.join(", "),
    active: source.active,
  };
}

function formatSourceTypeLabel(type: SourceType) {
  switch (type) {
    case "website":
      return "Website";
    case "gmail_newsletter":
      return "Gmail";
    default:
      return type;
  }
}

function buildSourceConfig(sourceForm: SourceFormState, existingConfig: Record<string, unknown> = {}) {
  const config = { ...existingConfig };
  const trimmedWebsite = sourceForm.website.trim();
  const trimmedQuery = sourceForm.query.trim();

  if (sourceForm.type === "website") {
    config.discovery_mode = sourceForm.discoveryMode;
    if (trimmedWebsite) {
      config.website_url = trimmedWebsite;
    } else {
      delete config.website_url;
    }
    return config;
  }

  delete config.website_url;
  delete config.discovery_mode;
  delete config.senders;
  delete config.raw_query;

  if (trimmedQuery) {
    const parts = trimmedQuery
      .split(/[\n,]+/)
      .map((entry) => entry.trim())
      .filter(Boolean);
    const senderList = parts.length && parts.every((entry) => EMAIL_RE.test(entry)) ? parts : null;

    if (senderList?.length) {
      config.senders = senderList;
    } else if (EMAIL_RE.test(trimmedQuery)) {
      config.senders = [trimmedQuery];
    } else {
      config.raw_query = trimmedQuery;
    }
  }

  return config;
}

function getSourceLocatorEntries(source: Pick<Source, "type" | "url" | "query" | "config_json">) {
  const entries: Array<{ label: string; value: string }> = [];
  const website = readStringMetadata(source.config_json.website_url);
  const discoveryMode = readStringMetadata(source.config_json.discovery_mode);
  const senderList = readStringListMetadata(source.config_json.senders);
  const rawQuery = readStringMetadata(source.config_json.raw_query);

  if (source.type === "gmail_newsletter") {
    if (senderList?.length) {
      entries.push({ label: "Senders", value: senderList.join(", ") });
    }
    if (rawQuery) {
      entries.push({ label: "Gmail query", value: rawQuery });
    }
    return entries;
  }

  if (source.type === "website") {
    if (source.url) {
      entries.push({ label: discoveryMode === "website_index" ? "Index URL" : "Feed URL", value: source.url });
    }
    if (website) {
      entries.push({ label: "Website", value: website });
    }
    if (discoveryMode) {
      entries.push({
        label: "Discovery mode",
        value: discoveryMode === "website_index" ? "Website index" : "RSS/feed",
      });
    }
    return entries;
  }

  if (source.url ?? source.query) {
    entries.push({ label: "Locator", value: source.url ?? source.query ?? "" });
  }

  return entries;
}

function formatRawKindLabel(value: string) {
  return value.replace(/[-_]/g, " ");
}

function formatRunStatusChipLabel(status: RunStatus) {
  switch (status) {
    case "failed":
      return "Failed";
    case "interrupted":
      return "Interrupted";
    case "running":
      return "Running";
    case "pending":
      return "Pending";
    default:
      return "Synced";
  }
}

function runStatusChipClassName(status: RunStatus) {
  switch (status) {
    case "failed":
      return "border-[var(--danger)]/18 bg-[rgba(159,18,57,0.08)] text-[var(--danger)]";
    case "interrupted":
      return "border-[rgba(120,53,15,0.18)] bg-[rgba(120,53,15,0.08)] text-[#78350f]";
    case "running":
      return "border-[var(--teal)]/18 bg-[rgba(14,77,100,0.08)] text-[var(--teal)]";
    case "pending":
      return "border-[var(--accent)]/18 bg-[rgba(154,52,18,0.08)] text-[var(--accent)]";
    default:
      return "border-[var(--ink)]/10 bg-[rgba(17,19,18,0.05)] text-[var(--muted-strong)]";
  }
}

function formatDateTimeLabel(value: string | null) {
  if (!value) return "Not finished yet";
  return new Date(value).toLocaleString();
}

function formatTokenCount(value: number) {
  return value.toLocaleString();
}

function formatUsdCost(value: number) {
  if (!Number.isFinite(value) || value <= 0) return "$0";
  if (value < 0.0001) return "<$0.0001";
  if (value < 0.01) return `$${value.toFixed(4)}`;
  return `$${value.toFixed(2)}`;
}

function formatLogTimeLabel(value: string) {
  return new Date(value).toLocaleTimeString();
}

function buildOptimisticIngestRun(id: string): IngestionRunHistoryEntry {
  const now = new Date().toISOString();
  return {
    id,
    run_type: "ingest",
    status: "running",
    operation_kind: "ingest_cycle",
    trigger: "ingest",
    title: "Ingest cycle",
    summary: "Starting ingest cycle…",
    started_at: now,
    finished_at: null,
    affected_edition_days: [],
    total_titles: 0,
    source_count: 0,
    failed_source_count: 0,
    created_count: 0,
    updated_count: 0,
    duplicate_mention_count: 0,
    extractor_fallback_count: 0,
    ai_prompt_tokens: 0,
    ai_completion_tokens: 0,
    ai_total_tokens: 0,
    ai_cost_usd: 0,
    tts_cost_usd: 0,
    total_cost_usd: 0,
    average_extraction_confidence: null,
    basic_info: [
      { label: "Sources", value: "Preparing run" },
      { label: "Current source", value: "Waiting for worker" },
    ],
    logs: [
      {
        logged_at: now,
        level: "info",
        message: "Ingest request sent. Waiting for the worker to start.",
      },
    ],
    steps: [],
    source_stats: [],
    errors: [],
    output_paths: [],
    changed_file_count: 0,
  };
}

function sortRunsByStartedAtDesc(runs: IngestionRunHistoryEntry[]) {
  return [...runs].sort((left, right) => new Date(right.started_at).getTime() - new Date(left.started_at).getTime());
}

function buildOptimisticSourceLatestRun(
  source: Source,
  id: string,
  startedAt: string,
): NonNullable<Source["latest_extraction_run"]> {
  return {
    id,
    status: "running",
    operation_kind: "raw_fetch",
    summary: `Running source inject for ${source.name}.`,
    started_at: startedAt,
    finished_at: null,
    emitted_kinds:
      source.latest_extraction_run?.emitted_kinds.length
        ? source.latest_extraction_run.emitted_kinds
        : [source.raw_kind],
  };
}

function buildOptimisticSourceInjectRun(source: Source, id: string, startedAt: string): IngestionRunHistoryEntry {
  return {
    id,
    run_type: "ingest",
    status: "running",
    operation_kind: "raw_fetch",
    trigger: "manual_source_fetch",
    title: `Source inject · ${source.name}`,
    summary: `Running source inject for ${source.name}.`,
    started_at: startedAt,
    finished_at: null,
    affected_edition_days: [],
    total_titles: 0,
    source_count: 1,
    failed_source_count: 0,
    created_count: 0,
    updated_count: 0,
    duplicate_mention_count: 0,
    extractor_fallback_count: 0,
    ai_prompt_tokens: 0,
    ai_completion_tokens: 0,
    ai_total_tokens: 0,
    ai_cost_usd: 0,
    tts_cost_usd: 0,
    total_cost_usd: 0,
    average_extraction_confidence: null,
    basic_info: [
      { label: "Source", value: source.name },
      { label: "Status", value: "Waiting for the latest extraction log" },
    ],
    logs: [
      {
        logged_at: startedAt,
        level: "info",
        message: `Source inject requested for ${source.name}. Waiting for the worker to finish.`,
      },
    ],
    steps: [],
    source_stats: [],
    errors: [],
    output_paths: [],
    changed_file_count: 0,
  };
}

function parseIsoDate(value: string) {
  const [year, month, day] = value.split("-").map(Number);
  return new Date(Date.UTC(year, month - 1, day, 12, 0, 0));
}

function shiftIsoDate(value: string, offsetDays: number) {
  const date = parseIsoDate(value);
  date.setUTCDate(date.getUTCDate() + offsetDays);
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function collapseIsoDateRanges(values: string[]) {
  const uniqueValues = Array.from(new Set(values)).sort();
  if (!uniqueValues.length) return null;

  const ranges: Array<{ start: string; end: string }> = [];
  let start = uniqueValues[0];
  let end = uniqueValues[0];

  for (const value of uniqueValues.slice(1)) {
    if (value === shiftIsoDate(end, 1)) {
      end = value;
      continue;
    }
    ranges.push({ start, end });
    start = value;
    end = value;
  }
  ranges.push({ start, end });

  return ranges
    .map((range) =>
      range.start === range.end
        ? formatBriefDayLabel(range.start)
        : `${formatBriefDayLabel(range.start)} - ${formatBriefDayLabel(range.end)}`,
    )
    .join(", ");
}

function describeAssociatedDates(run: IngestionRunHistoryEntry) {
  if (!run.affected_edition_days.length) return null;
  const edition = collapseIsoDateRanges(run.affected_edition_days);
  const coverage = collapseIsoDateRanges(run.affected_edition_days.map((value) => shiftIsoDate(value, -1)));
  return {
    edition,
    coverage,
  };
}

function formatRunDuration(startedAt: string, finishedAt: string | null) {
  if (!finishedAt) return "Still running";
  const durationMs = new Date(finishedAt).getTime() - new Date(startedAt).getTime();
  if (!Number.isFinite(durationMs) || durationMs < 0) return "Unknown duration";
  const totalSeconds = Math.round(durationMs / 1000);
  if (totalSeconds < 60) return `${totalSeconds}s`;
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return `${minutes}m ${seconds}s`;
}

function formatRunStatusLabel(status: RunStatus) {
  switch (status) {
    case "failed":
      return "Failure";
    case "interrupted":
      return "Interrupted";
    case "running":
      return "Running";
    case "pending":
      return "Pending";
    default:
      return "Success";
  }
}

function formatBriefDayLabel(value: string) {
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
    year: "numeric",
  }).format(new Date(`${value}T12:00:00`));
}

function formatEditionTargetLabel(value: string) {
  const [year, month, day] = value.split("-");
  return `${Number(day)}/${month}/${year}`;
}

export function ConnectionsPage() {
  const [searchParams] = useSearchParams();
  const queryClient = useQueryClient();
  const sourcesQuery = useQuery({ queryKey: ["sources"], queryFn: () => api.getSources() });
  const capabilitiesQuery = useQuery({
    queryKey: ["connections", "capabilities"],
    queryFn: api.getConnectionCapabilities,
  });
  const briefAvailabilityQuery = useQuery({
    queryKey: ["briefs", "availability"],
    queryFn: api.getBriefAvailability,
  });
  const gmailQuery = useQuery({ queryKey: ["connections", "gmail"], queryFn: api.getGmailConnection });
  const zoteroQuery = useQuery({ queryKey: ["connections", "zotero"], queryFn: api.getZoteroConnection });
  const ingestionRunsQuery = useQuery({
    queryKey: ["ops", "ingestion-runs"],
    queryFn: api.getIngestionRuns,
    refetchInterval: (query) => {
      const runs = query.state.data as IngestionRunHistoryEntry[] | undefined;
      return runs?.some((run) => run.status === "running") ? 1500 : false;
    },
    refetchIntervalInBackground: true,
  });
  const [sourceForm, setSourceForm] = useState<SourceFormState>(createEmptySourceForm);
  const [editingSourceId, setEditingSourceId] = useState<string | null>(null);
  const [sourceNotice, setSourceNotice] = useState<Notice | null>(null);
  const [sourceProbeReports, setSourceProbeReports] = useState<Record<string, SourceProbeReport>>({});
  const [probingSourceIds, setProbingSourceIds] = useState<Record<string, boolean>>({});
  const [removingSourceId, setRemovingSourceId] = useState<string | null>(null);
  const [togglingSourceId, setTogglingSourceId] = useState<string | null>(null);
  const [injectingSourceId, setInjectingSourceId] = useState<string | null>(null);
  const [loadingLatestLogSourceId, setLoadingLatestLogSourceId] = useState<string | null>(null);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [regenerateBriefDate, setRegenerateBriefDate] = useState("");
  const [gmailEmail, setGmailEmail] = useState("");
  const [gmailAppPassword, setGmailAppPassword] = useState("");
  const [zoteroApiKey, setZoteroApiKey] = useState("");
  const [zoteroLibraryId, setZoteroLibraryId] = useState("");
  const [zoteroCollectionName, setZoteroCollectionName] = useState("");
  const [zoteroAutoTagVocabulary, setZoteroAutoTagVocabulary] = useState(formatTagVocabulary(DEFAULT_ZOTERO_AUTO_TAG_VOCABULARY));
  const [zoteroAutoTagVocabularyHydrated, setZoteroAutoTagVocabularyHydrated] = useState(false);
  const [connectionPanels, setConnectionPanels] = useState({ gmail: true, zotero: true });
  const [connectionPanelsReady, setConnectionPanelsReady] = useState(false);

  const refreshAll = async () => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: ["sources"] }),
      queryClient.invalidateQueries({ queryKey: ["connections"] }),
      queryClient.invalidateQueries({ queryKey: ["briefs"] }),
      queryClient.invalidateQueries({ queryKey: ["items"] }),
      queryClient.invalidateQueries({ queryKey: ["item"] }),
      queryClient.invalidateQueries({ queryKey: ["ops", "ingestion-runs"] }),
    ]);
  };

  const resetSourceEditor = () => {
    setEditingSourceId(null);
    setSourceForm(createEmptySourceForm());
    setSourceNotice(null);
  };

  const createSource = useMutation({
    mutationFn: (payload: Record<string, unknown>) => api.createSource(payload),
    onSuccess: async () => {
      setEditingSourceId(null);
      setSourceForm(createEmptySourceForm());
      setSourceNotice({ tone: "success", message: "Source added." });
      await refreshAll();
    },
    onError: (error) => {
      setSourceNotice({ tone: "error", message: error.message });
    },
  });
  const updateSource = useMutation({
    mutationFn: ({ id, payload }: { id: string; payload: Record<string, unknown> }) => api.updateSource(id, payload),
    onSuccess: async (source) => {
      setEditingSourceId(source.id);
      setSourceForm(mapSourceToForm(source));
      setSourceNotice({ tone: "success", message: "Source updated." });
      setSourceProbeReports((current) => {
        const next = { ...current };
        delete next[source.id];
        return next;
      });
      await refreshAll();
    },
    onError: (error) => {
      setSourceNotice({ tone: "error", message: error.message });
    },
  });
  const toggleSource = useMutation({
    mutationFn: ({ id, active }: { id: string; active: boolean }) => api.updateSource(id, { active }),
    onSuccess: async (source) => {
      if (editingSourceId === source.id) {
        setSourceForm((current) => ({ ...current, active: source.active }));
      }
      setSourceNotice({
        tone: "success",
        message: source.active ? "Source resumed." : "Source paused.",
      });
      await refreshAll();
    },
    onError: (error) => {
      setSourceNotice({ tone: "error", message: error.message });
    },
    onSettled: () => {
      setTogglingSourceId(null);
    },
  });
  const removeSource = useMutation({
    mutationFn: (id: string) => api.deleteSource(id),
    onSuccess: async (_, sourceId) => {
      if (editingSourceId === sourceId) {
        resetSourceEditor();
      }
      setSourceNotice({ tone: "success", message: "Source removed." });
      setSourceProbeReports((current) => {
        const next = { ...current };
        delete next[sourceId];
        return next;
      });
      setProbingSourceIds((current) => {
        const next = { ...current };
        delete next[sourceId];
        return next;
      });
      await refreshAll();
    },
    onError: (error) => {
      setSourceNotice({ tone: "error", message: error.message });
    },
    onSettled: () => {
      setRemovingSourceId(null);
    },
  });
  const saveGmail = useMutation({
    mutationFn: (payload: Record<string, unknown>) => api.saveGmailConnection(payload),
    onSuccess: async (connection) => {
      setGmailAppPassword("");
      setConnectionPanels((current) => ({ ...current, gmail: connection.status !== "connected" }));
      const savedEmail = readStringMetadata(connection.metadata_json.connected_email);
      if (savedEmail) {
        setGmailEmail(savedEmail);
      }
      await refreshAll();
    },
  });
  const saveZotero = useMutation({
    mutationFn: (payload: Record<string, unknown>) => api.saveZoteroConnection(payload),
    onSuccess: async (connection) => {
      setZoteroApiKey("");
      setConnectionPanels((current) => ({ ...current, zotero: connection.status !== "connected" }));
      const savedLibraryId = readStringMetadata(connection.metadata_json.library_id);
      if (savedLibraryId) {
        setZoteroLibraryId(savedLibraryId);
      }
      setZoteroCollectionName(readStringMetadata(connection.metadata_json.collection_name) ?? "");
      setZoteroAutoTagVocabulary(
        formatTagVocabulary(readStringListMetadata(connection.metadata_json.auto_tag_vocabulary) ?? []),
      );
      setZoteroAutoTagVocabularyHydrated(true);
      await refreshAll();
    },
  });
  const ingestNow = useMutation({
    mutationFn: api.ingestNow,
    onMutate: async () => {
      await queryClient.cancelQueries({ queryKey: ["ops", "ingestion-runs"] });
      const previousRuns = queryClient.getQueryData<IngestionRunHistoryEntry[]>(["ops", "ingestion-runs"]) ?? [];
      const optimisticRunId = `optimistic-ingest-${Date.now()}`;
      queryClient.setQueryData<IngestionRunHistoryEntry[]>(["ops", "ingestion-runs"], [
        buildOptimisticIngestRun(optimisticRunId),
        ...previousRuns,
      ]);
      return { previousRuns, optimisticRunId };
    },
    onSuccess: async (job, _vars, context) => {
      if (context?.optimisticRunId) {
        queryClient.setQueryData<IngestionRunHistoryEntry[]>(["ops", "ingestion-runs"], (current = []) =>
          current.filter((run) => run.id !== context.optimisticRunId),
        );
        setSelectedRunId((current) =>
          current === context.optimisticRunId && job.operation_run_id ? job.operation_run_id : current,
        );
      }
      await refreshAll();
    },
    onError: (_error, _vars, context) => {
      if (context?.previousRuns) {
        queryClient.setQueryData(["ops", "ingestion-runs"], context.previousRuns);
      }
      if (context?.optimisticRunId) {
        setSelectedRunId((current) => (current === context.optimisticRunId ? null : current));
      }
    },
  });
  const enrichAll = useMutation({
    mutationFn: api.enrichAll,
    onSuccess: async (job) => {
      await refreshAll();
      if (job.operation_run_id) {
        setSelectedRunId(job.operation_run_id);
      }
    },
  });
  const retryFailed = useMutation({
    mutationFn: api.retryFailedJobs,
    onSuccess: refreshAll,
  });
  const regenerateEdition = useMutation({
    mutationFn: (briefDate: string) => api.regenerateBrief(briefDate),
    onSuccess: refreshAll,
  });
  const clearContent = useMutation({
    mutationFn: api.clearContent,
    onSuccess: refreshAll,
  });

  const sourceSummary = useMemo(() => {
    const total = sourcesQuery.data?.length ?? 0;
    const active = sourcesQuery.data?.filter((source) => source.active).length ?? 0;
    const paused = total - active;
    return {
      total,
      active,
      paused,
      countLabel: `${active} active source${active === 1 ? "" : "s"}`,
      detailLabel:
        paused > 0
          ? `${paused} paused · ${total} total`
          : total > 0
            ? "All sources are live"
            : "No sources configured yet",
    };
  }, [sourcesQuery.data]);

  const opNotice =
    ingestNow.isError
      ? { tone: "error" as const, message: ingestNow.error.message }
      : enrichAll.isError
        ? { tone: "error" as const, message: enrichAll.error.message }
      : retryFailed.isError
        ? { tone: "error" as const, message: retryFailed.error.message }
        : regenerateEdition.isError
          ? { tone: "error" as const, message: regenerateEdition.error.message }
          : clearContent.isError
            ? { tone: "error" as const, message: clearContent.error.message }
            : ingestNow.isSuccess
              ? { tone: "success" as const, message: ingestNow.data.detail }
              : enrichAll.isSuccess
                ? { tone: "success" as const, message: enrichAll.data.detail }
              : retryFailed.isSuccess
                ? { tone: "success" as const, message: retryFailed.data.detail }
                : regenerateEdition.isSuccess
                  ? { tone: "success" as const, message: regenerateEdition.data.detail }
                  : clearContent.isSuccess
                    ? { tone: "success" as const, message: clearContent.data.detail }
                    : null;
  const operationActionBusy =
    ingestNow.isPending
    || enrichAll.isPending
    || retryFailed.isPending
    || regenerateEdition.isPending
    || clearContent.isPending;

  const regenerateOptions = useMemo(() => {
    if (briefAvailabilityQuery.data?.days.length) return briefAvailabilityQuery.data.days;
    if (briefAvailabilityQuery.data?.default_day) {
      return [
        {
          brief_date: briefAvailabilityQuery.data.default_day,
          coverage_start: briefAvailabilityQuery.data.default_day,
          coverage_end: briefAvailabilityQuery.data.default_day,
        },
      ];
    }
    return [];
  }, [briefAvailabilityQuery.data]);

  const gmailNotice =
    saveGmail.isError
      ? { tone: "error" as const, message: saveGmail.error.message }
      : saveGmail.isSuccess
        ? {
            tone: "success" as const,
            message:
              readStringMetadata(saveGmail.data?.metadata_json.auth_mode) === "app_password"
                ? "Gmail connection verified."
                : "Gmail settings saved.",
          }
        : null;

  const zoteroSuccess =
    saveZotero.isSuccess && saveZotero.data.status === "connected"
      ? "Zotero connection verified."
      : null;

  useEffect(() => {
    if (!gmailQuery.data) return;
    const connectedEmail = readStringMetadata(gmailQuery.data.metadata_json.connected_email);
    setGmailEmail(connectedEmail ?? "");
  }, [gmailQuery.data]);

  useEffect(() => {
    const savedLibraryId = readStringMetadata(zoteroQuery.data?.metadata_json.library_id);
    if (!savedLibraryId || zoteroLibraryId) return;
    setZoteroLibraryId(savedLibraryId);
  }, [zoteroLibraryId, zoteroQuery.data]);

  useEffect(() => {
    const savedCollectionName = readStringMetadata(zoteroQuery.data?.metadata_json.collection_name);
    if (!savedCollectionName || zoteroCollectionName) return;
    setZoteroCollectionName(savedCollectionName);
  }, [zoteroCollectionName, zoteroQuery.data]);

  useEffect(() => {
    if (zoteroAutoTagVocabularyHydrated || zoteroQuery.isPending) return;
    const savedTagVocabulary = readStringListMetadata(zoteroQuery.data?.metadata_json.auto_tag_vocabulary);
    if (savedTagVocabulary) {
      setZoteroAutoTagVocabulary(formatTagVocabulary(savedTagVocabulary));
    } else if (Array.isArray(zoteroQuery.data?.metadata_json.auto_tag_vocabulary)) {
      setZoteroAutoTagVocabulary("");
    } else {
      setZoteroAutoTagVocabulary(formatTagVocabulary(DEFAULT_ZOTERO_AUTO_TAG_VOCABULARY));
    }
    setZoteroAutoTagVocabularyHydrated(true);
  }, [zoteroAutoTagVocabularyHydrated, zoteroQuery.data, zoteroQuery.isPending]);

  useEffect(() => {
    if (!regenerateOptions.length) return;
    if (regenerateOptions.some((option) => option.brief_date === regenerateBriefDate)) return;
    setRegenerateBriefDate(regenerateOptions[0].brief_date);
  }, [regenerateBriefDate, regenerateOptions]);

  useEffect(() => {
    if (connectionPanelsReady || gmailQuery.isPending || zoteroQuery.isPending) return;
    setConnectionPanels({
      gmail: gmailQuery.data?.status !== "connected",
      zotero: zoteroQuery.data?.status !== "connected",
    });
    setConnectionPanelsReady(true);
  }, [connectionPanelsReady, gmailQuery.data?.status, gmailQuery.isPending, zoteroQuery.data?.status, zoteroQuery.isPending]);

  useEffect(() => {
    if (!selectedRunId) return;
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setSelectedRunId(null);
      }
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [selectedRunId]);

  const sourceQueryError = sourcesQuery.error || capabilitiesQuery.error || gmailQuery.error || zoteroQuery.error;
  if ((sourcesQuery.isLoading || capabilitiesQuery.isLoading || gmailQuery.isLoading || zoteroQuery.isLoading)
    && !sourcesQuery.data
    && !capabilitiesQuery.data
    && !gmailQuery.data
    && !zoteroQuery.data) {
    return <div className="page-loading">Loading connections…</div>;
  }
  if (sourceQueryError) {
    return <div className="page-empty">Connections are unavailable right now. Refresh or check the backend.</div>;
  }

  const gmailState = searchParams.get("gmail");
  const gmailReason = searchParams.get("reason");
  const gmailOauthConfigured = capabilitiesQuery.data?.gmail_oauth_configured ?? false;
  const gmailOauthRedirectUri = (() => {
    try {
      return new URL(api.oauthUrl("/connections/gmail/oauth/callback"), window.location.href).toString();
    } catch {
      return "/api/connections/gmail/oauth/callback";
    }
  })();
  const gmailConnectedEmail =
    typeof gmailQuery.data?.metadata_json.connected_email === "string"
      ? gmailQuery.data.metadata_json.connected_email
      : null;
  const gmailAuthMode = readStringMetadata(gmailQuery.data?.metadata_json.auth_mode);
  const gmailLastError = readStringMetadata(gmailQuery.data?.metadata_json.last_error);
  const gmailSourceCount = sourcesQuery.data?.filter((source) => source.type === "gmail_newsletter").length ?? 0;
  const gmailUsesAppPassword = gmailAuthMode === "app_password";
  const gmailConfigured = gmailQuery.data?.status === "connected";
  const gmailStatusSummary =
    gmailLastError && gmailQuery.data?.status === "error"
      ? gmailLastError
      : gmailConnectedEmail
        ? gmailUsesAppPassword
          ? `Connected as ${gmailConnectedEmail} with a Gmail app password.`
          : `Connected as ${gmailConnectedEmail} via Google OAuth.`
        : gmailOauthConfigured
          ? "Use Google OAuth for a durable hosted connection, or enter a Gmail app password below."
          : "Google OAuth is not configured here, but you can connect Gmail directly with an app password below.";
  const gmailStatusNotice =
    gmailState === "connected"
      ? { tone: "success" as const, message: "Gmail connected. The next ingest run will use the stored OAuth credentials." }
      : gmailState
        ? { tone: "error" as const, message: `Gmail connection failed${gmailReason ? `: ${gmailReason.replaceAll("_", " ")}` : "."}` }
        : gmailNotice;
  const zoteroUsername = readStringMetadata(zoteroQuery.data?.metadata_json.connected_username);
  const zoteroUserId = readStringMetadata(zoteroQuery.data?.metadata_json.connected_user_id);
  const zoteroLibraryIdValue = readStringMetadata(zoteroQuery.data?.metadata_json.library_id);
  const zoteroCollectionNameValue = readStringMetadata(zoteroQuery.data?.metadata_json.collection_name);
  const zoteroAutoTagVocabularyValue = readStringListMetadata(zoteroQuery.data?.metadata_json.auto_tag_vocabulary);
  const zoteroVerifiedAt = readStringMetadata(zoteroQuery.data?.metadata_json.verified_at);
  const zoteroLastError = readStringMetadata(zoteroQuery.data?.metadata_json.last_error);
  const zoteroCanWrite = readBooleanMetadata(zoteroQuery.data?.metadata_json.can_write);
  const zoteroConfigured = zoteroQuery.data?.status === "connected";
  const zoteroAutoTagCount =
    zoteroAutoTagVocabularyValue !== null ? zoteroAutoTagVocabularyValue.length : DEFAULT_ZOTERO_AUTO_TAG_VOCABULARY.length;
  const zoteroSummary =
    zoteroQuery.data?.status === "connected"
      ? zoteroUsername
        ? `Verified as ${zoteroUsername}${zoteroUserId ? ` (user ${zoteroUserId})` : ""}.`
        : "Zotero key verified for this library."
      : zoteroLastError ?? "Paste a Zotero Web API key to verify the connection. Personal libraries can be resolved automatically.";
  const zoteroNotice =
    saveZotero.isError
      ? { tone: "error" as const, message: saveZotero.error.message }
      : saveZotero.isSuccess && saveZotero.data.status === "error"
        ? { tone: "error" as const, message: readStringMetadata(saveZotero.data.metadata_json.last_error) ?? "Zotero verification failed." }
        : zoteroSuccess
          ? { tone: "success" as const, message: zoteroSuccess }
          : null;
  const connectedConnectionCount = Number(gmailConfigured) + Number(zoteroConfigured);
  const recentOperationCount = ingestionRunsQuery.data?.length ?? 0;
  const sourceMutationBusy =
    createSource.isPending || updateSource.isPending || toggleSource.isPending || removeSource.isPending;
  const selectedRun = ingestionRunsQuery.data?.find((run) => run.id === selectedRunId) ?? null;
  const selectedRunAssociatedDates = selectedRun ? describeAssociatedDates(selectedRun) : null;
  const editingSource = sourcesQuery.data?.find((source) => source.id === editingSourceId) ?? null;
  const toggleConnectionPanel = (panel: "gmail" | "zotero") => {
    setConnectionPanels((current) => ({ ...current, [panel]: !current[panel] }));
  };

  const upsertRunInHistoryCache = (run: IngestionRunHistoryEntry) => {
    queryClient.setQueryData<IngestionRunHistoryEntry[]>(["ops", "ingestion-runs"], (current = []) => {
      const rest = current.filter((entry) => entry.id !== run.id);
      return sortRunsByStartedAtDesc([run, ...rest]);
    });
  };

  const startEditingSource = (source: Source) => {
    setEditingSourceId(source.id);
    setSourceForm(mapSourceToForm(source));
    setSourceNotice(null);
  };

  const handleSourceTypeChange = (type: SourceType) => {
    if (sourceNotice) setSourceNotice(null);
    setSourceForm((current) => {
      if (current.type === type) return current;
      return {
        ...current,
        type,
        rawKind: type === "gmail_newsletter" ? "newsletter" : current.rawKind === "newsletter" ? "blog-post" : current.rawKind,
        url: type === "website" ? current.url : "",
        website: type === "website" ? current.website : "",
        query: type === "gmail_newsletter" ? current.query : "",
      };
    });
  };

  const handleToggleSource = (source: Source) => {
    setSourceNotice(null);
    setTogglingSourceId(source.id);
    toggleSource.mutate({ id: source.id, active: !source.active });
  };

  const handleRemoveSource = (source: Source) => {
    if (!window.confirm(`Remove "${source.name}" from the registry? Historical items stay available.`)) {
      return;
    }
    setSourceNotice(null);
    setRemovingSourceId(source.id);
    removeSource.mutate(source.id);
  };

  const handleProbeSource = async (source: Source) => {
    setProbingSourceIds((current) => ({ ...current, [source.id]: true }));
    try {
      const result = await api.probeSource(source.id);
      setSourceProbeReports((current) => ({
        ...current,
        [source.id]: {
          tone: "success",
          result,
        },
      }));
    } catch (error) {
      setSourceProbeReports((current) => ({
        ...current,
        [source.id]: {
          tone: "error",
          message: readErrorMessage(error),
        },
      }));
    } finally {
      setProbingSourceIds((current) => ({ ...current, [source.id]: false }));
    }
  };

  const handleInjectSource = async (source: Source) => {
    setSourceNotice(null);
    setInjectingSourceId(source.id);
    await queryClient.cancelQueries({ queryKey: ["sources"] });
    await queryClient.cancelQueries({ queryKey: ["ops", "ingestion-runs"] });
    const previousSources = queryClient.getQueryData<Source[]>(["sources"]);
    const previousRuns = queryClient.getQueryData<IngestionRunHistoryEntry[]>(["ops", "ingestion-runs"]);
    const optimisticRunId = `optimistic-source-inject-${source.id}-${Date.now()}`;
    const startedAt = new Date().toISOString();
    const optimisticLatestRun = buildOptimisticSourceLatestRun(source, optimisticRunId, startedAt);
    const optimisticRun = buildOptimisticSourceInjectRun(source, optimisticRunId, startedAt);

    queryClient.setQueryData<Source[]>(["sources"], (current = []) =>
      current.map((entry) =>
        entry.id === source.id
          ? {
              ...entry,
              latest_extraction_run: optimisticLatestRun,
            }
          : entry,
      ),
    );
    queryClient.setQueryData<IngestionRunHistoryEntry[]>(["ops", "ingestion-runs"], (current = []) =>
      sortRunsByStartedAtDesc([optimisticRun, ...current.filter((run) => run.id !== optimisticRunId)]),
    );
    try {
      const response = await api.injectSource(source.id);
      setSourceNotice({ tone: "success", message: response.detail });
      await refreshAll();
      if (response.operation_run_id) {
        setSelectedRunId(response.operation_run_id);
      }
    } catch (error) {
      if (previousSources) {
        queryClient.setQueryData(["sources"], previousSources);
      }
      if (previousRuns) {
        queryClient.setQueryData(["ops", "ingestion-runs"], previousRuns);
      }
      setSourceNotice({ tone: "error", message: readErrorMessage(error) });
    } finally {
      setInjectingSourceId(null);
    }
  };

  const handleOpenLatestLog = async (source: Source) => {
    setSourceNotice(null);
    setLoadingLatestLogSourceId(source.id);
    try {
      const response = await api.getSourceLatestLog(source.id);
      upsertRunInHistoryCache(response.run);
      setSelectedRunId(response.run.id);
    } catch (error) {
      setSourceNotice({ tone: "error", message: readErrorMessage(error) });
    } finally {
      setLoadingLatestLogSourceId(null);
    }
  };

  const handleClearContent = () => {
    if (
      !window.confirm(
        "Delete all stored content, generated briefs, and operational history? Sources, Gmail/Zotero connections, and profile settings will stay in place.",
      )
    ) {
      return;
    }
    clearContent.mutate();
  };

  const handleSourceSubmit = (event: FormEvent) => {
    event.preventDefault();

    const trimmedName = sourceForm.name.trim();
    const trimmedUrl = sourceForm.url.trim();
    const trimmedWebsite = sourceForm.website.trim();
    const trimmedQuery = sourceForm.query.trim();
    const maxItems = Number.parseInt(sourceForm.maxItems, 10);
    const rawKind = sourceForm.type === "gmail_newsletter" ? "newsletter" : sourceForm.rawKind;

    if (!trimmedName) {
      setSourceNotice({ tone: "error", message: "Source name is required." });
      return;
    }
    if (sourceForm.type === "gmail_newsletter" && !trimmedQuery) {
      setSourceNotice({ tone: "error", message: "Provide the sender email or Gmail query for this source." });
      return;
    }
    if (sourceForm.type === "website" && !trimmedUrl) {
      setSourceNotice({ tone: "error", message: "Provide the feed or index URL for this source." });
      return;
    }
    if (!rawKind.trim()) {
      setSourceNotice({ tone: "error", message: "Choose the raw document kind this source should write." });
      return;
    }
    if (Number.isNaN(maxItems) || maxItems < 1 || maxItems > 100) {
      setSourceNotice({ tone: "error", message: "Max items must be a number between 1 and 100." });
      return;
    }

    setSourceNotice(null);
    const payload = {
      name: trimmedName,
      raw_kind: rawKind,
      url: sourceForm.type === "website" ? trimmedUrl || null : null,
      query: sourceForm.type === "gmail_newsletter" ? trimmedQuery || null : null,
      description: sourceForm.description.trim() || null,
      max_items: maxItems,
      tags: csvToList(sourceForm.tags),
      active: sourceForm.active,
      config_json: buildSourceConfig(
        {
          ...sourceForm,
          website: trimmedWebsite,
          query: trimmedQuery,
        },
        editingSource?.config_json ?? {},
      ),
    };

    if (editingSourceId) {
      updateSource.mutate({ id: editingSourceId, payload });
      return;
    }

    createSource.mutate({
      ...payload,
      type: sourceForm.type,
    });
  };

  return (
    <>
      <div className="space-y-6 pb-10">
        <div className="mx-auto w-full max-w-6xl">
          <section className="editorial-panel overflow-hidden">
            <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
              <div>
                <p className="section-kicker">Connections</p>
                <h3 className="section-title">Gmail and Zotero</h3>
                <SkimmableText className="mt-4 max-w-4xl text-base leading-7 text-[var(--muted)]">
                  Connect the inbox and the library first. Configured services collapse into green status cards, while missing ones stay open until they are wired up.
                </SkimmableText>
              </div>
              <div className="grid gap-3 sm:grid-cols-3">
                <article className="rounded-[1.45rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.52)] px-4 py-4">
                  <p className="font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">Connected</p>
                  <p className="mt-3 font-display text-3xl leading-none text-[var(--ink)]">{String(connectedConnectionCount).padStart(2, "0")}</p>
                  <p className="mt-2 text-sm leading-6 text-[var(--muted)]">of 2 services ready</p>
                </article>
                <article className="rounded-[1.45rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.52)] px-4 py-4">
                  <p className="font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">Sources</p>
                  <p className="mt-3 font-display text-3xl leading-none text-[var(--ink)]">{String(sourceSummary.total).padStart(2, "0")}</p>
                  <p className="mt-2 text-sm leading-6 text-[var(--muted)]">{sourceSummary.active} active right now</p>
                </article>
                <article className="rounded-[1.45rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.52)] px-4 py-4">
                  <p className="font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">Operations</p>
                  <p className="mt-3 font-display text-3xl leading-none text-[var(--ink)]">{String(recentOperationCount).padStart(2, "0")}</p>
                  <p className="mt-2 text-sm leading-6 text-[var(--muted)]">recent source, ingest, brief, and audio runs</p>
                </article>
              </div>
            </div>

            <div className="mt-6 space-y-4">
              <ConnectionCard
                chips={
                  <>
                    <span className="rounded-full border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.74)] px-3 py-1 font-mono text-[11px] uppercase tracking-[0.16em] text-[var(--muted)]">
                      {gmailConnectedEmail ?? "No inbox connected"}
                    </span>
                    <span className="rounded-full border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.74)] px-3 py-1 font-mono text-[11px] uppercase tracking-[0.16em] text-[var(--muted)]">
                      {gmailAuthMode === "app_password" ? "App password" : gmailAuthMode === "oauth" ? "OAuth" : "No auth mode yet"}
                    </span>
                    <span className="rounded-full border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.74)] px-3 py-1 font-mono text-[11px] uppercase tracking-[0.16em] text-[var(--muted)]">
                      {gmailSourceCount} Gmail source{gmailSourceCount === 1 ? "" : "s"}
                    </span>
                  </>
                }
                configured={gmailConfigured}
                headline="Inbox routing"
                label="Gmail"
                notice={gmailStatusNotice}
                onToggle={() => toggleConnectionPanel("gmail")}
                open={connectionPanels.gmail}
                summary={
                  gmailConfigured
                    ? gmailStatusSummary
                    : "Connect Gmail so newsletter sources can ingest directly from your mailbox without manual forwarding."
                }
              >
                <form
                  className="space-y-5"
                  onSubmit={(event) => {
                    event.preventDefault();
                    const trimmedEmail = gmailEmail.trim();
                    const trimmedAppPassword = gmailAppPassword.trim();
                    const shouldUseAppPassword = gmailUsesAppPassword || Boolean(trimmedEmail || trimmedAppPassword);
                    saveGmail.mutate({
                      label: "Primary Gmail",
                      payload: shouldUseAppPassword
                        ? {
                            auth_mode: "app_password",
                            email: trimmedEmail,
                            app_password: trimmedAppPassword,
                          }
                        : {},
                      metadata_json: {},
                    });
                  }}
                >
                  <div className="grid gap-3 sm:grid-cols-2">
                    <div className="rounded-[1.45rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.58)] px-4 py-4">
                      <p className="field-label">Connected inbox</p>
                      <p className="mt-2 text-sm leading-6 text-[var(--ink)]">{gmailConnectedEmail ?? "Not connected yet"}</p>
                    </div>
                    <div className="rounded-[1.45rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.58)] px-4 py-4">
                      <p className="field-label">Auth mode</p>
                      <p className="mt-2 text-sm leading-6 text-[var(--ink)]">
                        {gmailAuthMode === "app_password" ? "App password" : gmailAuthMode === "oauth" ? "OAuth" : "Settings only"}
                      </p>
                    </div>
                    <div className="rounded-[1.45rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.58)] px-4 py-4">
                      <p className="field-label">Registry coverage</p>
                      <p className="mt-2 text-sm leading-6 text-[var(--ink)]">{gmailSourceCount} Gmail sources in the queue</p>
                    </div>
                    <div className="rounded-[1.45rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.58)] px-4 py-4">
                      <p className="field-label">Last error</p>
                      <p className="mt-2 text-sm leading-6 text-[var(--ink)]">{gmailLastError ?? "No recent Gmail error"}</p>
                    </div>
                  </div>

                  <div className="grid gap-4 lg:grid-cols-[minmax(0,0.9fr)_minmax(0,1.1fr)]">
                    <div className="rounded-[1.55rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.5)] p-5">
                      <p className="field-label">Google OAuth</p>
                      <SkimmableText className="mt-3 text-sm leading-6 text-[var(--muted)]">
                        Use OAuth for the most durable hosted connection. If OAuth is not configured in this environment, fall back to a Gmail app password instead.
                      </SkimmableText>
                      {!gmailOauthConfigured ? (
                        <div className="mt-4 rounded-2xl border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.72)] px-4 py-4 text-sm leading-6 text-[var(--muted-strong)]">
                          <p>
                            This button stays disabled until the backend sets <code>GMAIL_OAUTH_CLIENT_ID</code> and <code>GMAIL_OAUTH_CLIENT_SECRET</code> in
                            <code> apps/backend/.env</code> and restarts.
                          </p>
                          <p className="mt-3">
                            In Google Cloud, add this authorized redirect URI:
                          </p>
                          <code className="mt-2 block break-all rounded-xl border border-[var(--ink)]/8 bg-white/80 px-3 py-2 font-mono text-xs text-[var(--ink)]">
                            {gmailOauthRedirectUri}
                          </code>
                        </div>
                      ) : null}
                      <div className="mt-5">
                        <button
                          className={`secondary-button ${gmailOauthConfigured ? "" : "cursor-not-allowed opacity-60"}`}
                          disabled={!gmailOauthConfigured}
                          onClick={() => {
                            if (!gmailOauthConfigured) return;
                            window.location.assign(api.oauthUrl("/connections/gmail/oauth/start"));
                          }}
                          type="button"
                        >
                          {gmailConfigured && gmailAuthMode === "oauth" ? "Reconnect Gmail OAuth" : "Connect with Google OAuth"}
                        </button>
                      </div>
                    </div>

                    <div className="rounded-[1.55rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.5)] p-5">
                      <p className="field-label">Direct Gmail sign-in</p>
                      <SkimmableText className="mt-3 text-sm leading-6 text-[var(--muted)]">
                        Use a Gmail app password if you do not want to configure Google Cloud. Leave the password blank on later saves to keep the current secret.
                      </SkimmableText>
                      <div className="mt-4 grid gap-4 sm:grid-cols-2">
                        <label>
                          <span className="field-label">Gmail address</span>
                          <input
                            className="field-input"
                            onChange={(event) => setGmailEmail(event.target.value)}
                            placeholder="you@gmail.com"
                            value={gmailEmail}
                          />
                        </label>
                        <label>
                          <span className="field-label">App password</span>
                          <input
                            className="field-input"
                            onChange={(event) => setGmailAppPassword(event.target.value)}
                            placeholder={gmailUsesAppPassword ? "Leave blank to keep the saved app password" : "16-character Gmail app password"}
                            type="password"
                            value={gmailAppPassword}
                          />
                        </label>
                      </div>
                    </div>
                  </div>

                  <div className="rounded-[1.55rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.5)] p-5">
                    <p className="field-label">Per-source filtering</p>
                    <SkimmableText className="mt-3 text-sm leading-6 text-[var(--muted)]">
                      Configure each Gmail source with its own sender email or Gmail search query. Connection settings only handle authentication now.
                    </SkimmableText>
                  </div>

                  <div className="flex justify-end">
                    <button className="primary-button" disabled={saveGmail.isPending} type="submit">
                      {saveGmail.isPending ? "Saving Gmail settings..." : "Save Gmail settings"}
                    </button>
                  </div>
                </form>
              </ConnectionCard>

              <ConnectionCard
                chips={
                  <>
                    <span className="rounded-full border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.74)] px-3 py-1 font-mono text-[11px] uppercase tracking-[0.16em] text-[var(--muted)]">
                      {zoteroUsername ?? "No verified Zotero user"}
                    </span>
                    <span className="rounded-full border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.74)] px-3 py-1 font-mono text-[11px] uppercase tracking-[0.16em] text-[var(--muted)]">
                      {zoteroCollectionNameValue ?? "Library root"}
                    </span>
                    <span className="rounded-full border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.74)] px-3 py-1 font-mono text-[11px] uppercase tracking-[0.16em] text-[var(--muted)]">
                      {zoteroCanWrite ? "Write enabled" : "Read state only"}
                    </span>
                  </>
                }
                configured={zoteroConfigured}
                headline="Save-out target"
                label="Zotero"
                notice={zoteroNotice}
                onToggle={() => toggleConnectionPanel("zotero")}
                open={connectionPanels.zotero}
                summary={
                  zoteroConfigured
                    ? zoteroSummary
                    : "Verify a Zotero key so the research workflow can ship selected items into the right library and collection."
                }
              >
                <form
                  className="space-y-5"
                  onSubmit={(event) => {
                    event.preventDefault();
                    saveZotero.mutate({
                      label: "Primary Zotero",
                      payload: { api_key: zoteroApiKey.trim(), library_id: zoteroLibraryId.trim(), library_type: "users" },
                      metadata_json: {
                        library_type: "users",
                        collection_name: zoteroCollectionName.trim() || null,
                        auto_tag_vocabulary: parseTagVocabularyInput(zoteroAutoTagVocabulary),
                      },
                    });
                  }}
                >
                  <div className="grid gap-3 sm:grid-cols-2">
                    <div className="rounded-[1.45rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.58)] px-4 py-4">
                      <p className="field-label">Username</p>
                      <p className="mt-2 text-sm leading-6 text-[var(--ink)]">{zoteroUsername ?? "Not verified yet"}</p>
                    </div>
                    <div className="rounded-[1.45rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.58)] px-4 py-4">
                      <p className="field-label">User Library ID</p>
                      <p className="mt-2 text-sm leading-6 text-[var(--ink)]">{zoteroLibraryIdValue ?? zoteroUserId ?? "Not stored yet"}</p>
                    </div>
                    <div className="rounded-[1.45rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.58)] px-4 py-4">
                      <p className="field-label">Upload collection</p>
                      <p className="mt-2 text-sm leading-6 text-[var(--ink)]">{zoteroCollectionNameValue ?? "Library root"}</p>
                    </div>
                    <div className="rounded-[1.45rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.58)] px-4 py-4">
                      <p className="field-label">Last verified</p>
                      <p className="mt-2 text-sm leading-6 text-[var(--ink)]">
                        {zoteroVerifiedAt ? new Date(zoteroVerifiedAt).toLocaleString() : "Not verified yet"}
                      </p>
                    </div>
                  </div>

                  <div className="rounded-[1.55rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.5)] p-5">
                    <p className="field-label">Verification</p>
                    <SkimmableText className="mt-3 text-sm leading-6 text-[var(--muted)]">
                      Stored keys stay hidden after save. Paste a new key only when you want to rotate credentials or connect a different Zotero account.
                    </SkimmableText>
                  </div>

                  <label>
                    <span className="field-label">API key</span>
                    <textarea className="field-input min-h-24" onChange={(event) => setZoteroApiKey(event.target.value)} value={zoteroApiKey} />
                  </label>

                  <div className="grid gap-4 sm:grid-cols-2">
                    <label>
                      <span className="field-label">User Library ID (optional)</span>
                      <input
                        className="field-input"
                        onChange={(event) => setZoteroLibraryId(event.target.value)}
                        placeholder="Leave blank for your personal library"
                        value={zoteroLibraryId}
                      />
                    </label>
                    <label>
                      <span className="field-label">Collection (optional)</span>
                      <input
                        className="field-input"
                        onChange={(event) => setZoteroCollectionName(event.target.value)}
                        placeholder="Library root"
                        value={zoteroCollectionName}
                      />
                    </label>
                  </div>

                  <p className="text-sm leading-6 text-[var(--muted)]">
                    Use <code>Parent / Child</code> to target or create nested collections. Leave the library ID blank instead of entering <code>My Library</code>.
                  </p>

                  <label>
                    <span className="field-label">Auto-tag Vocabulary</span>
                    <textarea
                      className="field-input min-h-48"
                      onChange={(event) => setZoteroAutoTagVocabulary(event.target.value)}
                      placeholder="One tag per line"
                      value={zoteroAutoTagVocabulary}
                    />
                  </label>

                  <SkimmableText className="text-sm leading-6 text-[var(--muted)]">
                    New Zotero saves merge the manual tags from the action with inferred tags from this vocabulary. Paste one tag per line or a JSON array.
                  </SkimmableText>

                  <div className="flex justify-end">
                    <button className="primary-button" disabled={saveZotero.isPending} type="submit">
                      {saveZotero.isPending
                        ? "Verifying Zotero..."
                        : zoteroConfigured
                          ? "Update Zotero connection"
                          : "Verify Zotero connection"}
                    </button>
                  </div>
                </form>
              </ConnectionCard>
            </div>
          </section>
        </div>

        <div className="page-breakout space-y-6">
          <section className="editorial-panel overflow-hidden">
          <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
            <div>
              <p className="section-kicker">Sources</p>
              <h3 className="section-title">Current list of sources</h3>
              <SkimmableText className="mt-4 max-w-4xl text-base leading-7 text-[var(--muted)]">
                Review every feed in one place, preview locators, run a single-source inject, inspect the latest extraction log, pause ingest without losing context, and register new sources below the current queue.
              </SkimmableText>
            </div>
            <div className="flex flex-wrap items-center gap-2">
              <span className="rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.56)] px-3 py-1.5 font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--muted)]">
                {sourceSummary.active} active
              </span>
              <span className="rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.56)] px-3 py-1.5 font-mono text-[10px] uppercase tracking-[0.22em] text-[var(--muted)]">
                {sourceSummary.total} total
              </span>
              {editingSourceId ? (
                <button className="secondary-button px-2.5 py-1.5 text-[10px]" disabled={sourceMutationBusy} onClick={resetSourceEditor} type="button">
                  New blank source
                </button>
              ) : null}
            </div>
          </div>

          {sourceNotice ? (
            <div
              className={`mt-5 rounded-2xl border px-4 py-3 text-sm leading-6 ${
                sourceNotice.tone === "error"
                  ? "border-[var(--danger)]/20 bg-[rgba(255,255,255,0.56)] text-[var(--danger)]"
                  : "border-[var(--ink)]/8 bg-[rgba(255,255,255,0.56)] text-[var(--muted-strong)]"
              }`}
            >
              {sourceNotice.message}
            </div>
          ) : null}

          <div className="mt-6 space-y-4">
            <div className="min-w-0 space-y-4">
              {sourcesQuery.data?.length ? (
                sourcesQuery.data.map((source) => {
                  const locatorEntries = getSourceLocatorEntries(source);
                  const probeReport = sourceProbeReports[source.id];
                  const isProbingSource = Boolean(probingSourceIds[source.id]);
                  const isInjectingSource = injectingSourceId === source.id;
                  const isLoadingLatestLog = loadingLatestLogSourceId === source.id;
                  const latestExtractionRun = source.latest_extraction_run;

                  return (
                    <article
                      key={source.id}
                      className={`min-w-0 rounded-[1.7rem] border px-4 py-4 ${
                        editingSourceId === source.id
                          ? "border-[var(--accent)]/28 bg-[rgba(255,255,255,0.72)] shadow-[0_20px_44px_rgba(17,19,18,0.07)]"
                          : "border-[var(--ink)]/8 bg-[rgba(255,255,255,0.55)]"
                      } ${source.active ? "" : "bg-[rgba(255,255,255,0.42)]"}`}
                    >
                      <div className="flex flex-wrap items-start justify-between gap-3">
                        <div className="min-w-0">
                          <div className="flex flex-wrap items-center gap-2">
                            <p className="text-lg font-medium text-[var(--ink)]">{source.name}</p>
                            <span
                              className={`rounded-full border px-2.5 py-0.5 font-mono text-[10px] uppercase tracking-[0.16em] ${
                                source.active
                                  ? "border-[var(--ink)]/10 bg-[rgba(17,19,18,0.05)] text-[var(--muted-strong)]"
                                  : "border-[var(--accent)]/18 bg-[rgba(154,52,18,0.08)] text-[var(--accent)]"
                              }`}
                            >
                              {source.active ? "Active" : "Paused"}
                            </span>
                            <span className="rounded-full border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.74)] px-2.5 py-0.5 font-mono text-[10px] uppercase tracking-[0.16em] text-[var(--muted)]">
                              {formatSourceTypeLabel(source.type)}
                            </span>
                            <span className="rounded-full border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.74)] px-2.5 py-0.5 font-mono text-[10px] uppercase tracking-[0.16em] text-[var(--muted)]">
                              {formatRawKindLabel(source.raw_kind)}
                            </span>
                            <span className="rounded-full border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.74)] px-2.5 py-0.5 font-mono text-[10px] uppercase tracking-[0.16em] text-[var(--muted)]">
                              Max {source.max_items}
                            </span>
                            {source.has_custom_pipeline ? (
                              <span
                                className="inline-flex items-center gap-1.5 rounded-full border border-[rgba(154,52,18,0.14)] bg-[rgba(154,52,18,0.08)] px-2.5 py-0.5 font-mono text-[10px] uppercase tracking-[0.16em] text-[var(--accent)]"
                                title={source.custom_pipeline_id ?? "Custom pipeline"}
                              >
                                <Workflow className="h-3.5 w-3.5" />
                                <span>Custom pipeline</span>
                              </span>
                            ) : null}
                          </div>
                          {source.description ? <p className="mt-2.5 text-sm leading-6 text-[var(--muted)]">{source.description}</p> : null}
                        </div>
                        {latestExtractionRun ? (
                          <span
                            className={`rounded-full border px-2.5 py-0.5 font-mono text-[10px] uppercase tracking-[0.18em] ${runStatusChipClassName(latestExtractionRun.status)}`}
                          >
                            {formatRunStatusChipLabel(latestExtractionRun.status)}
                          </span>
                        ) : (
                          <span className="rounded-full border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.7)] px-2.5 py-0.5 font-mono text-[10px] uppercase tracking-[0.18em] text-[var(--muted)]">
                            No extraction yet
                          </span>
                        )}
                      </div>

                      {locatorEntries.length ? (
                        <div className="mt-3 grid gap-3 rounded-[1.25rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.62)] px-3.5 py-3.5 sm:grid-cols-2">
                          {locatorEntries.map((entry) => (
                            <div key={`${source.id}-${entry.label}`}>
                              <p className="field-label">{entry.label}</p>
                              <p className="mt-1.5 break-all font-mono text-[11px] leading-5 text-[var(--muted-strong)]">{entry.value}</p>
                            </div>
                          ))}
                        </div>
                      ) : null}

                      <div className="mt-3 flex flex-wrap items-center gap-2">
                        {source.tags.map((tag) => (
                          <span
                            key={`${source.id}-${tag}`}
                            className="rounded-full border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.74)] px-2.5 py-1 text-[11px] text-[var(--muted-strong)]"
                          >
                            {tag}
                          </span>
                        ))}
                        <span className="text-[11px] leading-5 text-[var(--muted)]">
                          {source.last_synced_at
                            ? `Last synced ${new Date(source.last_synced_at).toLocaleString()}`
                            : `Updated ${new Date(source.updated_at).toLocaleString()}`}
                        </span>
                      </div>

                      <div className="mt-3 rounded-[1.25rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.62)] px-3.5 py-3.5">
                        <div className="flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
                          <div className="min-w-0">
                            <p className="field-label">Latest extraction</p>
                            <p className="mt-1.5 text-sm leading-6 text-[var(--ink)]">
                              {latestExtractionRun ? latestExtractionRun.summary : "No source-specific extraction run has been recorded yet."}
                            </p>
                          </div>
                          {latestExtractionRun ? (
                            <span
                              className={`w-fit rounded-full border px-3 py-1 font-mono text-[10px] uppercase tracking-[0.16em] ${runStatusChipClassName(latestExtractionRun.status)}`}
                            >
                              {formatRunStatusChipLabel(latestExtractionRun.status)}
                            </span>
                          ) : null}
                        </div>
                        <p className="mt-2 text-xs leading-5 text-[var(--muted)]">
                          {latestExtractionRun
                            ? `Started ${formatDateTimeLabel(latestExtractionRun.started_at)}${latestExtractionRun.finished_at ? ` · Finished ${formatDateTimeLabel(latestExtractionRun.finished_at)}` : ""}`
                            : "Use Inject source to run this source on demand and create its first extraction log."}
                        </p>
                      </div>

                      <div className="mt-4 flex flex-wrap gap-2">
                        <button
                          className="secondary-button px-2.5 py-1.5 text-[10px]"
                          disabled={isProbingSource || isInjectingSource}
                          onClick={() => handleProbeSource(source)}
                          type="button"
                        >
                          {isProbingSource ? "Previewing..." : "Preview source"}
                        </button>
                        <button
                          className="secondary-button px-2.5 py-1.5 text-[10px]"
                          disabled={isInjectingSource || isLoadingLatestLog}
                          onClick={() => handleInjectSource(source)}
                          type="button"
                        >
                          <Play className="h-3.5 w-3.5" />
                          {isInjectingSource ? "Injecting..." : "Inject source"}
                        </button>
                        <button
                          className="secondary-button px-2.5 py-1.5 text-[10px]"
                          disabled={isInjectingSource || isLoadingLatestLog || !latestExtractionRun}
                          onClick={() => handleOpenLatestLog(source)}
                          type="button"
                          title={latestExtractionRun ? "Open the latest extraction log for this source" : "No extraction log yet"}
                        >
                          <History className="h-3.5 w-3.5" />
                          {isLoadingLatestLog ? "Loading log..." : "Latest extraction log"}
                        </button>
                        <button
                          className="secondary-button px-2.5 py-1.5 text-[10px]"
                          disabled={sourceMutationBusy || isInjectingSource}
                          onClick={() => startEditingSource(source)}
                          type="button"
                        >
                          {editingSourceId === source.id ? "Editing source" : "Edit source"}
                        </button>
                        <button
                          className={`secondary-button px-2.5 py-1.5 text-[10px] ${source.active ? "" : "filter-pill-active"}`}
                          disabled={sourceMutationBusy || isInjectingSource}
                          onClick={() => handleToggleSource(source)}
                          type="button"
                        >
                          {togglingSourceId === source.id ? "Saving..." : source.active ? "Pause source" : "Resume source"}
                        </button>
                        <button
                          className="secondary-button border-[var(--danger)]/22 px-2.5 py-1.5 text-[10px] text-[var(--danger)] hover:border-[var(--danger)]/38"
                          disabled={sourceMutationBusy || isInjectingSource}
                          onClick={() => handleRemoveSource(source)}
                          type="button"
                        >
                          {removingSourceId === source.id ? "Removing..." : "Remove source"}
                        </button>
                      </div>

                      {probeReport ? (
                        <div
                          className={`mt-4 rounded-[1.25rem] border px-3.5 py-3.5 ${
                            probeReport.tone === "error"
                              ? "border-[var(--danger)]/20 bg-[rgba(255,255,255,0.66)] text-[var(--danger)]"
                              : "border-[var(--ink)]/8 bg-[rgba(255,255,255,0.68)]"
                          }`}
                        >
                          {probeReport.tone === "error" ? (
                            <p className="text-sm leading-6">{probeReport.message}</p>
                          ) : (
                            <>
                              <div className="flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
                                <div>
                                  <p className="field-label">Fetch report</p>
                                  <p className="mt-1.5 text-sm leading-6 text-[var(--ink)]">{probeReport.result.detail}</p>
                                </div>
                                <span className="rounded-full border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.76)] px-3 py-1 font-mono text-[10px] uppercase tracking-[0.16em] text-[var(--muted)]">
                                  Checked {new Date(probeReport.result.checked_at).toLocaleString()}
                                </span>
                              </div>

                              {probeReport.result.sample_titles.length ? (
                                <div className="mt-3 grid gap-2 lg:grid-cols-2">
                                  {probeReport.result.sample_titles.map((title, index) => (
                                    <div
                                      key={`${source.id}-probe-${title}-${index}`}
                                      className="rounded-[1rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.72)] px-3 py-3"
                                    >
                                      <p className="field-label">Sample {index + 1}</p>
                                      <p className="mt-1.5 text-sm leading-6 text-[var(--muted-strong)]">{title}</p>
                                    </div>
                                  ))}
                                </div>
                              ) : (
                                <p className="mt-3 text-xs leading-5 text-[var(--muted)]">No items matched the current source settings.</p>
                              )}
                            </>
                          )}
                        </div>
                      ) : null}
                    </article>
                  );
                })
              ) : (
                <div className="rounded-3xl border border-dashed border-[var(--ink)]/12 bg-[rgba(255,255,255,0.4)] px-5 py-8 text-sm leading-7 text-[var(--muted)]">
                  No sources are configured yet. Use the editor below to register the first feed.
                </div>
              )}
            </div>

            <form className="rounded-[1.8rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.48)] px-4 py-4" onSubmit={handleSourceSubmit}>
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div>
                  <p className="section-kicker">{editingSourceId ? "Edit source" : "Add source"}</p>
                  <h4 className="section-title text-[2.2rem]">{editingSourceId ? sourceForm.name || "Update source" : "Register a new source"}</h4>
                </div>
                {editingSourceId ? (
                  <button className="secondary-button px-2.5 py-1.5 text-[10px]" disabled={sourceMutationBusy} onClick={resetSourceEditor} type="button">
                    Cancel
                  </button>
                ) : null}
              </div>

              <SkimmableText className="mt-4 text-sm leading-6 text-[var(--muted)]">
                {editingSourceId
                  ? "Adjust the source fields and state here. The source type stays fixed after creation."
                  : "Pick the source type first, then fill only the fields that matter for that feed."}
              </SkimmableText>

              {editingSource?.has_custom_pipeline ? (
                <div className="mt-4 rounded-[1.25rem] border border-[rgba(154,52,18,0.14)] bg-[rgba(154,52,18,0.08)] px-4 py-4">
                  <div className="flex items-start gap-3">
                    <Workflow className="mt-0.5 h-4 w-4 shrink-0 text-[var(--accent)]" />
                    <div>
                      <p className="field-label text-[var(--accent)]">Custom pipeline</p>
                      <p className="mt-1.5 text-sm leading-6 text-[var(--muted-strong)]">
                        This source uses the dedicated extraction pipeline <code>{editingSource.custom_pipeline_id}</code>.
                      </p>
                    </div>
                  </div>
                </div>
              ) : null}

              <div className="mt-5">
                <p className="field-label">Source type</p>
                <div className={`mt-3 grid gap-3 md:grid-cols-2 ${editingSourceId ? "opacity-65" : ""}`}>
                  {[
                    {
                      type: "website" as const,
                      label: "Website",
                      description: "RSS feeds or index pages that write blog posts, articles, papers, threads, or signals into raw/.",
                    },
                    {
                      type: "gmail_newsletter" as const,
                      label: "Gmail",
                      description: "One sender or Gmail search query per source.",
                    },
                  ].map((option) => {
                    const selected = sourceForm.type === option.type;
                    return (
                      <button
                        key={option.type}
                        className={`rounded-[1.25rem] border px-4 py-3 text-left transition ${
                          selected
                            ? "border-[var(--accent)]/32 bg-[rgba(154,52,18,0.08)] shadow-[0_12px_26px_rgba(154,52,18,0.08)]"
                            : "border-[var(--ink)]/8 bg-[rgba(255,255,255,0.58)] hover:border-[var(--accent)]/18"
                        }`}
                        disabled={Boolean(editingSourceId)}
                        onClick={() => handleSourceTypeChange(option.type)}
                        type="button"
                      >
                        <p className="font-mono text-[11px] uppercase tracking-[0.16em] text-[var(--muted-strong)]">{option.label}</p>
                        <p className="mt-2 text-sm leading-6 text-[var(--muted)]">{option.description}</p>
                      </button>
                    );
                  })}
                </div>
              </div>

              <div className="mt-5 grid gap-4 sm:grid-cols-2">
                <label>
                  <span className="field-label">Name</span>
                  <input
                    className="field-input min-h-[2.75rem] rounded-[1rem] px-4 py-3 text-sm"
                    onChange={(event) => {
                      if (sourceNotice) setSourceNotice(null);
                      setSourceForm({ ...sourceForm, name: event.target.value });
                    }}
                    placeholder={
                      sourceForm.type === "gmail_newsletter"
                        ? "TLDR AI"
                        : sourceForm.rawKind === "paper"
                          ? "AlphaXiv Papers"
                          : "OpenAI News"
                    }
                    value={sourceForm.name}
                  />
                </label>
                <label>
                  <span className="field-label">Max items per run</span>
                  <input
                    className="field-input min-h-[2.75rem] rounded-[1rem] px-4 py-3 text-sm"
                    inputMode="numeric"
                    onChange={(event) => {
                      if (sourceNotice) setSourceNotice(null);
                      setSourceForm({ ...sourceForm, maxItems: event.target.value });
                    }}
                    placeholder="20"
                    value={sourceForm.maxItems}
                  />
                </label>
                <label>
                  <span className="field-label">Raw document kind</span>
                  <select
                    className="field-input min-h-[2.75rem] rounded-[1rem] px-4 py-3 text-sm"
                    disabled={sourceForm.type === "gmail_newsletter"}
                    onChange={(event) => {
                      if (sourceNotice) setSourceNotice(null);
                      setSourceForm({ ...sourceForm, rawKind: event.target.value as SourceRawKind });
                    }}
                    value={sourceForm.type === "gmail_newsletter" ? "newsletter" : sourceForm.rawKind}
                  >
                    {sourceForm.type === "gmail_newsletter" ? (
                      <option value="newsletter">Newsletter</option>
                    ) : (
                      <>
                        <option value="blog-post">Blog post</option>
                        <option value="article">Article</option>
                        <option value="paper">Paper</option>
                        <option value="thread">Thread</option>
                        <option value="signal">Signal</option>
                      </>
                    )}
                  </select>
                  <p className="mt-2 text-xs leading-5 text-[var(--muted)]">
                    {sourceForm.type === "gmail_newsletter"
                      ? "Gmail sources always write newsletter raw documents."
                      : "Use paper for AlphaXiv-style research feeds and blog post for company news sites."}
                  </p>
                </label>
                <label>
                  <span className="field-label">Tags</span>
                  <input
                    className="field-input min-h-[2.75rem] rounded-[1rem] px-4 py-3 text-sm"
                    onChange={(event) => {
                      if (sourceNotice) setSourceNotice(null);
                      setSourceForm({ ...sourceForm, tags: event.target.value });
                    }}
                    placeholder="openai, official, research"
                    value={sourceForm.tags}
                  />
                </label>

                {sourceForm.type === "website" ? (
                  <>
                    <div className="sm:col-span-2">
                      <span className="field-label">Discovery mode</span>
                      <div className="mt-3 grid gap-3 md:grid-cols-2">
                        {[
                          {
                            value: "rss_feed" as const,
                            label: "RSS / feed",
                            description: "Read an Atom or RSS feed directly.",
                          },
                          {
                            value: "website_index" as const,
                            label: "Website index",
                            description: "Crawl an index page and extract matching links.",
                          },
                        ].map((option) => {
                          const selected = sourceForm.discoveryMode === option.value;
                          return (
                            <button
                              key={option.value}
                              className={`rounded-[1.25rem] border px-4 py-3 text-left transition ${
                                selected
                                  ? "border-[var(--accent)]/32 bg-[rgba(154,52,18,0.08)] shadow-[0_12px_26px_rgba(154,52,18,0.08)]"
                                  : "border-[var(--ink)]/8 bg-[rgba(255,255,255,0.58)] hover:border-[var(--accent)]/18"
                              }`}
                              onClick={() => {
                                if (sourceNotice) setSourceNotice(null);
                                setSourceForm({ ...sourceForm, discoveryMode: option.value });
                              }}
                              type="button"
                            >
                              <p className="font-mono text-[11px] uppercase tracking-[0.16em] text-[var(--muted-strong)]">{option.label}</p>
                              <p className="mt-2 text-sm leading-6 text-[var(--muted)]">{option.description}</p>
                            </button>
                          );
                        })}
                      </div>
                    </div>
                    <label>
                      <span className="field-label">{sourceForm.discoveryMode === "website_index" ? "Index URL" : "Feed URL"}</span>
                      <input
                        className="field-input min-h-[2.75rem] rounded-[1rem] px-4 py-3 text-sm"
                        onChange={(event) => {
                          if (sourceNotice) setSourceNotice(null);
                          setSourceForm({ ...sourceForm, url: event.target.value });
                        }}
                        placeholder={
                          sourceForm.discoveryMode === "website_index"
                            ? "https://www.anthropic.com/research"
                            : "https://openai.com/news/rss.xml"
                        }
                        value={sourceForm.url}
                      />
                    </label>
                    <label>
                      <span className="field-label">Website home (optional)</span>
                      <input
                        className="field-input min-h-[2.75rem] rounded-[1rem] px-4 py-3 text-sm"
                        onChange={(event) => {
                          if (sourceNotice) setSourceNotice(null);
                          setSourceForm({ ...sourceForm, website: event.target.value });
                        }}
                        placeholder="https://openai.com/news"
                        value={sourceForm.website}
                      />
                    </label>
                    <label className="sm:col-span-2">
                      <span className="field-label">Extraction brief</span>
                      <textarea
                        className="field-input min-h-28 rounded-[1rem] px-4 py-3 text-sm"
                        onChange={(event) => {
                          if (sourceNotice) setSourceNotice(null);
                          setSourceForm({ ...sourceForm, description: event.target.value });
                        }}
                        placeholder={
                          sourceForm.rawKind === "paper"
                            ? "Capture the paper metadata, abstract, claims, methods, and links for each research entry."
                            : "What should be extracted from this site or feed?"
                        }
                        value={sourceForm.description}
                      />
                    </label>
                  </>
                ) : null}

                {sourceForm.type === "gmail_newsletter" ? (
                  <>
                    <label className="sm:col-span-2">
                      <span className="field-label">Sender email or Gmail query</span>
                      <input
                        className="field-input min-h-[2.75rem] rounded-[1rem] px-4 py-3 text-sm"
                        onChange={(event) => {
                          if (sourceNotice) setSourceNotice(null);
                          setSourceForm({ ...sourceForm, query: event.target.value });
                        }}
                        placeholder="newsletter@example.com or from:newsletter@example.com label:tldr-ai"
                        value={sourceForm.query}
                      />
                      <p className="mt-2 text-xs leading-5 text-[var(--muted)]">
                        Use a plain sender email for strict sender-only reads. Advanced Gmail search syntax still works when you need labels.
                      </p>
                    </label>
                    <label className="sm:col-span-2">
                      <span className="field-label">Extraction brief</span>
                      <textarea
                        className="field-input min-h-28 rounded-[1rem] px-4 py-3 text-sm"
                        onChange={(event) => {
                          if (sourceNotice) setSourceNotice(null);
                          setSourceForm({ ...sourceForm, description: event.target.value });
                        }}
                        placeholder="Summarize product launches, research links, benchmarks, and notable opinions from this newsletter."
                        value={sourceForm.description}
                      />
                    </label>
                  </>
                ) : null}
              </div>

              <div className="mt-4 rounded-[1.2rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.62)] px-3.5 py-3.5">
                <div className="flex flex-wrap items-center justify-between gap-3">
                  <div>
                    <p className="field-label">Status</p>
                    <p className="mt-1.5 text-sm leading-6 text-[var(--muted)]">
                      Paused sources stay visible here but stop participating in ingest runs.
                    </p>
                  </div>
                  <button
                    className={`secondary-button px-2.5 py-1.5 text-[10px] ${sourceForm.active ? "filter-pill-active" : ""}`}
                    onClick={() => {
                      if (sourceNotice) setSourceNotice(null);
                      setSourceForm({ ...sourceForm, active: !sourceForm.active });
                    }}
                    type="button"
                  >
                    {sourceForm.active ? "Active" : "Paused"}
                  </button>
                </div>
              </div>

              <div className="mt-4 flex flex-wrap gap-2">
                <button className="primary-button px-3 py-2 text-[10px]" disabled={sourceMutationBusy} type="submit">
                  {createSource.isPending
                    ? "Adding source..."
                    : updateSource.isPending
                      ? "Saving changes..."
                      : editingSourceId
                        ? "Save changes"
                        : "Add source"}
                </button>
                {editingSourceId ? (
                  <button className="secondary-button px-2.5 py-1.5 text-[10px]" disabled={sourceMutationBusy} onClick={resetSourceEditor} type="button">
                    Stop editing
                  </button>
                ) : null}
              </div>
            </form>
          </div>
          </section>

          <section className="editorial-panel overflow-hidden">
          <div className="flex flex-col gap-4 xl:flex-row xl:items-end xl:justify-between">
            <div>
              <p className="section-kicker">Recent operations</p>
              <h3 className="section-title">Operational history</h3>
              <SkimmableText className="mt-4 max-w-4xl text-base leading-7 text-[var(--muted)]">
                Review the execution log across source extracts, ingest cycles, brief generation, and audio generation, including the edition coverage each run touched and the estimated LLM plus voice spend behind it.
              </SkimmableText>
            </div>
            <div className="flex flex-wrap items-center gap-2">
              <span className="rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.56)] px-4 py-2 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">
                {String(recentOperationCount).padStart(2, "0")} operations
              </span>
              <button
                className="secondary-button"
                disabled={operationActionBusy}
                onClick={() => ingestNow.mutate()}
                type="button"
              >
                {ingestNow.isPending ? "Running ingest..." : "Run ingest now"}
              </button>
              <button
                className="secondary-button"
                disabled={operationActionBusy}
                onClick={() => enrichAll.mutate()}
                type="button"
              >
                {enrichAll.isPending ? "Running enrichment..." : "Run full enrichment"}
              </button>
              <button
                className="secondary-button"
                disabled={operationActionBusy}
                onClick={() => retryFailed.mutate()}
                type="button"
              >
                {retryFailed.isPending ? "Retrying..." : "Retry errored ingests"}
              </button>
            </div>
          </div>

          <div className="mt-5 rounded-[1.7rem] border border-[var(--danger)]/16 bg-[rgba(255,255,255,0.44)] px-4 py-4">
            <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
              <div>
                <p className="field-label text-[var(--danger)]">Content reset</p>
                <SkimmableText className="mt-2 max-w-3xl text-sm leading-6 text-[var(--muted)]">
                  Delete stored papers, articles, newsletters, posts, threads, and signals, generated briefs, and the operational history. Sources,
                  Gmail and Zotero connections, and profile settings stay as they are.
                </SkimmableText>
              </div>
              <button
                className="secondary-button border-[var(--danger)]/22 text-[var(--danger)] hover:border-[var(--danger)]/38"
                disabled={operationActionBusy}
                onClick={handleClearContent}
                type="button"
              >
                {clearContent.isPending ? "Clearing content..." : "Clear stored content"}
              </button>
            </div>
          </div>

          <div className="mt-5 grid gap-3 border-t border-[var(--ink)]/8 pt-5 lg:grid-cols-[minmax(0,280px)_auto] lg:items-end">
            <label>
              <span className="field-label">Regenerate edition</span>
              <select
                className="field-input mt-2"
                disabled={!regenerateOptions.length || regenerateEdition.isPending || clearContent.isPending}
                onChange={(event) => setRegenerateBriefDate(event.target.value)}
                value={regenerateBriefDate}
              >
                {regenerateOptions.map((option) => (
                  <option key={option.brief_date} value={option.brief_date}>
                    {formatEditionTargetLabel(option.brief_date)}
                  </option>
                ))}
              </select>
            </label>
            <button
              className="secondary-button w-fit"
              disabled={!regenerateBriefDate || operationActionBusy}
              onClick={() => regenerateEdition.mutate(regenerateBriefDate)}
              type="button"
            >
              {regenerateEdition.isPending ? "Regenerating edition..." : "Regenerate selected edition"}
            </button>
          </div>

          {opNotice ? (
            <div
              className={`mt-5 rounded-2xl border px-4 py-3 text-sm leading-6 ${
                opNotice.tone === "error"
                  ? "border-[var(--danger)]/20 bg-[rgba(255,255,255,0.56)] text-[var(--danger)]"
                  : "border-[var(--ink)]/8 bg-[rgba(255,255,255,0.56)] text-[var(--muted-strong)]"
              }`}
            >
              {opNotice.message}
            </div>
          ) : null}

          {ingestionRunsQuery.isLoading && !ingestionRunsQuery.data ? (
            <div className="mt-6 rounded-3xl border border-dashed border-[var(--ink)]/12 bg-[rgba(255,255,255,0.42)] px-5 py-8 text-sm leading-7 text-[var(--muted)]">
              Loading operation history…
            </div>
          ) : ingestionRunsQuery.isError ? (
            <div className="mt-6 rounded-3xl border border-[var(--danger)]/18 bg-[rgba(255,255,255,0.42)] px-5 py-8 text-sm leading-7 text-[var(--danger)]">
              {ingestionRunsQuery.error.message}
            </div>
          ) : ingestionRunsQuery.data?.length ? (
            <div className="mt-6 overflow-x-auto rounded-3xl border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.42)]">
              <table className="min-w-full border-collapse">
                <thead>
                  <tr className="border-b border-[var(--ink)]/8 text-left">
                    <th className="px-4 py-3 font-mono text-[11px] uppercase tracking-[0.2em] text-[var(--muted)]">Executed</th>
                    <th className="px-4 py-3 font-mono text-[11px] uppercase tracking-[0.2em] text-[var(--muted)]">Associated range</th>
                    <th className="px-4 py-3 font-mono text-[11px] uppercase tracking-[0.2em] text-[var(--muted)]">Operation</th>
                    <th className="px-4 py-3 font-mono text-[11px] uppercase tracking-[0.2em] text-[var(--muted)]">Basic info</th>
                    <th className="px-4 py-3 font-mono text-[11px] uppercase tracking-[0.2em] text-[var(--muted)]">Est. cost</th>
                    <th className="px-4 py-3 font-mono text-[11px] uppercase tracking-[0.2em] text-[var(--muted)]">Status</th>
                  </tr>
                </thead>
                <tbody>
                  {ingestionRunsQuery.data.map((run) => {
                    const associatedDates = describeAssociatedDates(run);
                    return (
                      <tr
                        key={run.id}
                        className="cursor-pointer border-b border-[var(--ink)]/6 transition hover:bg-[rgba(255,255,255,0.52)] focus-within:bg-[rgba(255,255,255,0.52)]"
                        onClick={() => setSelectedRunId(run.id)}
                        onKeyDown={(event) => {
                          if (event.key === "Enter" || event.key === " ") {
                            event.preventDefault();
                            setSelectedRunId(run.id);
                          }
                        }}
                        tabIndex={0}
                      >
                        <td className="px-4 py-4 text-sm leading-6 text-[var(--muted-strong)]">{formatDateTimeLabel(run.started_at)}</td>
                        <td className="px-4 py-4">
                          {associatedDates ? (
                            <>
                              <p className="text-sm leading-6 text-[var(--muted-strong)]">Edition {associatedDates.edition}</p>
                              <p className="mt-1 text-xs leading-5 text-[var(--muted)]">Coverage {associatedDates.coverage}</p>
                            </>
                          ) : (
                            <p className="text-sm leading-6 text-[var(--muted)]">General / no edition range</p>
                          )}
                        </td>
                        <td className="px-4 py-4">
                          <p className="text-sm font-medium text-[var(--ink)]">{run.title}</p>
                          <p className="mt-1 text-xs leading-5 text-[var(--muted)]">
                            {run.trigger ? `Trigger: ${run.trigger.replace(/_/g, " ")}` : "Recorded operation"}
                          </p>
                        </td>
                        <td className="px-4 py-4">
                          <p className="text-sm leading-6 text-[var(--muted-strong)]">{run.summary}</p>
                        </td>
                        <td className="px-4 py-4">
                          <p className="text-sm leading-6 text-[var(--muted-strong)]">{formatUsdCost(run.total_cost_usd)}</p>
                          <p className="mt-1 text-xs leading-5 text-[var(--muted)]">
                            LLM {formatUsdCost(run.ai_cost_usd)} · TTS {formatUsdCost(run.tts_cost_usd)}
                          </p>
                        </td>
                        <td className="px-4 py-4">
                          <span
                            className={`rounded-full border px-3 py-1 font-mono text-[11px] uppercase tracking-[0.16em] ${
                              run.status === "failed"
                                ? "border-[var(--danger)]/18 bg-[rgba(159,18,57,0.08)] text-[var(--danger)]"
                                : run.status === "running"
                                  ? "border-[var(--teal)]/18 bg-[rgba(14,77,100,0.08)] text-[var(--teal)]"
                                  : "border-[var(--ink)]/10 bg-[rgba(17,19,18,0.05)] text-[var(--muted-strong)]"
                            }`}
                          >
                            {formatRunStatusLabel(run.status)}
                          </span>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="mt-6 rounded-3xl border border-dashed border-[var(--ink)]/12 bg-[rgba(255,255,255,0.42)] px-5 py-8 text-sm leading-7 text-[var(--muted)]">
              No operations yet. Run ingest, regenerate a brief, or generate audio to start building the log.
            </div>
          )}
          </section>
        </div>
      </div>

      {selectedRun ? (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-[rgba(17,19,18,0.38)] px-4 py-6 backdrop-blur-sm"
          onClick={() => setSelectedRunId(null)}
        >
          <div
            aria-labelledby={`ingestion-run-${selectedRun.id}`}
            aria-modal="true"
            className="max-h-[90vh] w-full max-w-5xl overflow-hidden rounded-[2rem] border border-[var(--ink)]/10 bg-[rgba(247,240,224,0.96)] shadow-[0_32px_80px_rgba(17,19,18,0.22)]"
            onClick={(event) => event.stopPropagation()}
            role="dialog"
          >
            <div className="flex flex-wrap items-start justify-between gap-4 border-b border-[var(--ink)]/8 px-6 py-5">
              <div>
                <p className="section-kicker">Operation details</p>
                <h4 className="section-title text-3xl" id={`ingestion-run-${selectedRun.id}`}>
                  {selectedRun.title}
                </h4>
                <p className="mt-3 text-sm leading-6 text-[var(--muted)]">
                  Executed {formatDateTimeLabel(selectedRun.started_at)} · Finished {formatDateTimeLabel(selectedRun.finished_at)}
                </p>
              </div>
              <button className="secondary-button" onClick={() => setSelectedRunId(null)} type="button">
                Close
              </button>
            </div>

            <div className="max-h-[calc(90vh-110px)] overflow-y-auto px-6 py-6">
              <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
                <div className="rounded-2xl border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.58)] px-4 py-4">
                  <p className="field-label">Status</p>
                  <p className="mt-2 text-sm leading-6 text-[var(--ink)]">{formatRunStatusLabel(selectedRun.status)}</p>
                </div>
                <div className="rounded-2xl border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.58)] px-4 py-4">
                  <p className="field-label">Executed</p>
                  <p className="mt-2 text-sm leading-6 text-[var(--ink)]">{formatDateTimeLabel(selectedRun.started_at)}</p>
                </div>
                <div className="rounded-2xl border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.58)] px-4 py-4">
                  <p className="field-label">Duration</p>
                  <p className="mt-2 text-sm leading-6 text-[var(--ink)]">{formatRunDuration(selectedRun.started_at, selectedRun.finished_at)}</p>
                </div>
                <div className="rounded-2xl border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.58)] px-4 py-4">
                  <p className="field-label">Associated dates</p>
                  {selectedRunAssociatedDates ? (
                    <>
                      <p className="mt-2 text-sm leading-6 text-[var(--ink)]">Edition {selectedRunAssociatedDates.edition}</p>
                      <p className="mt-1 text-xs leading-5 text-[var(--muted)]">Coverage {selectedRunAssociatedDates.coverage}</p>
                    </>
                  ) : (
                    <p className="mt-2 text-sm leading-6 text-[var(--ink)]">No edition-linked date range</p>
                  )}
                </div>
                <div className="rounded-2xl border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.58)] px-4 py-4">
                  <p className="field-label">Est. total cost</p>
                  <p className="mt-2 text-sm leading-6 text-[var(--ink)]">{formatUsdCost(selectedRun.total_cost_usd)}</p>
                </div>
                <div className="rounded-2xl border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.58)] px-4 py-4">
                  <p className="field-label">LLM cost</p>
                  <p className="mt-2 text-sm leading-6 text-[var(--ink)]">{formatUsdCost(selectedRun.ai_cost_usd)}</p>
                </div>
                <div className="rounded-2xl border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.58)] px-4 py-4">
                  <p className="field-label">TTS cost</p>
                  <p className="mt-2 text-sm leading-6 text-[var(--ink)]">{formatUsdCost(selectedRun.tts_cost_usd)}</p>
                </div>
                <div className="rounded-2xl border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.58)] px-4 py-4">
                  <p className="field-label">AI tokens</p>
                  <p className="mt-2 text-sm leading-6 text-[var(--ink)]">{formatTokenCount(selectedRun.ai_total_tokens)}</p>
                </div>

                {selectedRun.basic_info.map((info) => (
                  <div key={`${selectedRun.id}-${info.label}`} className="rounded-2xl border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.58)] px-4 py-4">
                    <p className="field-label">{info.label}</p>
                    <p className="mt-2 text-sm leading-6 text-[var(--ink)]">{info.value}</p>
                  </div>
                ))}

                {selectedRun.average_extraction_confidence !== null ? (
                  <div className="rounded-2xl border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.58)] px-4 py-4">
                    <p className="field-label">Extraction quality</p>
                    <p className="mt-2 text-sm leading-6 text-[var(--ink)]">
                      {`${Math.round(selectedRun.average_extraction_confidence * 100)}% average confidence`}
                    </p>
                  </div>
                ) : null}
              </div>

              {selectedRun.errors.length ? (
                <div className="mt-6 rounded-3xl border border-[var(--danger)]/18 bg-[rgba(255,255,255,0.62)] px-5 py-5">
                  <p className="field-label text-[var(--danger)]">Failures</p>
                  <div className="mt-3 space-y-2 text-sm leading-6 text-[var(--danger)]">
                    {selectedRun.errors.map((error) => (
                      <p key={error}>{error}</p>
                    ))}
                  </div>
                </div>
              ) : null}

              {selectedRun.logs.length ? (
                <div className="mt-6 rounded-3xl border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.62)] px-5 py-5">
                  <div className="flex items-center justify-between gap-3">
                    <p className="field-label">Execution log</p>
                    {selectedRun.status === "running" ? (
                      <span className="rounded-full border border-[var(--teal)]/18 bg-[rgba(14,77,100,0.08)] px-3 py-1 font-mono text-[11px] uppercase tracking-[0.16em] text-[var(--teal)]">
                        Live
                      </span>
                    ) : null}
                  </div>
                  <div className="mt-4 space-y-3">
                    {selectedRun.logs.map((log, index) => (
                      <div
                        key={`${selectedRun.id}-${log.logged_at}-${index}`}
                        className="rounded-2xl border border-[var(--ink)]/8 bg-[rgba(247,240,224,0.72)] px-4 py-3"
                      >
                        <div className="flex flex-wrap items-center gap-2 text-xs leading-5 text-[var(--muted)]">
                          <span className="font-mono uppercase tracking-[0.16em]">{log.level}</span>
                          <span>{formatLogTimeLabel(log.logged_at)}</span>
                        </div>
                        <p className="mt-1.5 text-sm leading-6 text-[var(--ink)]">{log.message}</p>
                      </div>
                    ))}
                  </div>
                </div>
              ) : null}

              {selectedRun.source_stats.length ? (
                <>
                  <div className="mt-6 overflow-x-auto rounded-3xl border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.5)]">
                    <table className="min-w-full border-collapse">
                      <thead>
                        <tr className="border-b border-[var(--ink)]/8 text-left">
                          <th className="px-4 py-3 font-mono text-[11px] uppercase tracking-[0.18em] text-[var(--muted)]">Source</th>
                          <th className="px-4 py-3 font-mono text-[11px] uppercase tracking-[0.18em] text-[var(--muted)]">Status</th>
                          <th className="px-4 py-3 font-mono text-[11px] uppercase tracking-[0.18em] text-[var(--muted)]">Titles</th>
                          <th className="px-4 py-3 font-mono text-[11px] uppercase tracking-[0.18em] text-[var(--muted)]">Created / updated</th>
                          <th className="px-4 py-3 font-mono text-[11px] uppercase tracking-[0.18em] text-[var(--muted)]">Extractor notes</th>
                          <th className="px-4 py-3 font-mono text-[11px] uppercase tracking-[0.18em] text-[var(--muted)]">AI tokens</th>
                          <th className="px-4 py-3 font-mono text-[11px] uppercase tracking-[0.18em] text-[var(--muted)]">Confidence</th>
                        </tr>
                      </thead>
                      <tbody>
                        {selectedRun.source_stats.map((sourceStat) => (
                          <tr key={`${selectedRun.id}-${sourceStat.source_name}`} className="border-b border-[var(--ink)]/6">
                            <td className="px-4 py-4 text-sm leading-6 text-[var(--ink)]">{sourceStat.source_name}</td>
                            <td className="px-4 py-4 text-sm leading-6 text-[var(--muted-strong)]">{formatRunStatusLabel(sourceStat.status)}</td>
                            <td className="px-4 py-4 text-sm leading-6 text-[var(--muted-strong)]">{sourceStat.ingested_count}</td>
                            <td className="px-4 py-4 text-sm leading-6 text-[var(--muted-strong)]">
                              {sourceStat.created_count} / {sourceStat.updated_count}
                            </td>
                            <td className="px-4 py-4 text-sm leading-6 text-[var(--muted-strong)]">
                              {sourceStat.extractor_fallback_count
                                ? `${sourceStat.extractor_fallback_count} fallbacks`
                                : sourceStat.error ?? "Clean run"}
                            </td>
                            <td className="px-4 py-4 text-sm leading-6 text-[var(--muted-strong)]">{formatTokenCount(sourceStat.ai_total_tokens)}</td>
                            <td className="px-4 py-4 text-sm leading-6 text-[var(--muted-strong)]">
                              {sourceStat.average_extraction_confidence !== null
                                ? `${Math.round(sourceStat.average_extraction_confidence * 100)}%`
                                : "n/a"}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>

                  <div className="mt-6 grid gap-4 lg:grid-cols-2">
                    {selectedRun.source_stats.map((sourceStat) => (
                      <article
                        key={`${selectedRun.id}-${sourceStat.source_name}-items`}
                        className="rounded-3xl border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.54)] px-5 py-5"
                      >
                        <div className="flex flex-wrap items-start justify-between gap-3">
                          <div>
                            <p className="field-label">Source</p>
                            <p className="mt-2 text-lg font-medium text-[var(--ink)]">{sourceStat.source_name}</p>
                          </div>
                          <span
                            className={`rounded-full border px-3 py-1 font-mono text-[11px] uppercase tracking-[0.16em] ${
                              sourceStat.status === "failed"
                                ? "border-[var(--danger)]/18 bg-[rgba(159,18,57,0.08)] text-[var(--danger)]"
                                : "border-[var(--ink)]/10 bg-[rgba(17,19,18,0.05)] text-[var(--muted-strong)]"
                            }`}
                          >
                            {formatRunStatusLabel(sourceStat.status)}
                          </span>
                        </div>
                        {sourceStat.error ? (
                          <p className="mt-4 rounded-2xl border border-[var(--danger)]/18 bg-[rgba(255,255,255,0.66)] px-4 py-3 text-sm leading-6 text-[var(--danger)]">
                            {sourceStat.error}
                          </p>
                        ) : null}
                        {sourceStat.items.length ? (
                          <div className="mt-4 space-y-3">
                            {sourceStat.items.map((item) => (
                              <div
                                key={`${sourceStat.source_name}-${item.title}`}
                                className="rounded-2xl border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.62)] px-4 py-3"
                              >
                                <p className="text-sm font-medium leading-6 text-[var(--ink)]">{item.title}</p>
                                <p className="mt-1 text-xs leading-5 text-[var(--muted)]">
                                  {item.outcome} · {item.content_type} · {Math.round(item.extraction_confidence * 100)}% confidence
                                </p>
                              </div>
                            ))}
                          </div>
                        ) : (
                          <p className="mt-4 text-sm leading-6 text-[var(--muted)]">No titles were extracted for this source in this run.</p>
                        )}
                      </article>
                    ))}
                  </div>
                </>
              ) : null}
            </div>
          </div>
        </div>
      ) : null}
    </>
  );
}
