import { FormEvent, ReactNode, useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  BookOpen,
  Bot,
  CheckCircle2,
  ChevronDown,
  ChevronUp,
  Clock3,
  Database,
  History,
  LoaderCircle,
  Mail,
  Mic2,
  Play,
  RefreshCw,
  Sparkles,
  Upload,
  Workflow,
} from "lucide-react";
import { useNavigate, useSearchParams } from "react-router-dom";

import { api } from "../../api/client";
import type {
  AdvancedOutputKind,
  HealthCheckScope,
  IngestionRunHistoryEntry,
  JobResponse,
  PipelineStatus,
  RunStatus,
  Source,
  SourceProbeResult,
  SourceRawKind,
  SourceType,
} from "../../api/types";
import { SkimmableText } from "../../components/SkimmableText";
import defaultZoteroAutoTagVocabulary from "../../constants/zoteroAutoTagVocabulary.json";
import { hasSuccessfulEditionRun } from "../../lib/edition-output-status";

const DEFAULT_ZOTERO_AUTO_TAG_VOCABULARY = defaultZoteroAutoTagVocabulary as string[];
const EMAIL_RE = /^[^@\s]+@[^@\s]+\.[^@\s]+$/;
const SOURCE_INJECT_MAX_ITEMS_LIMIT = 250;

type WorkspaceMode = "connections" | "pipeline";

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
  | { tone: "error"; message: string }
  | { tone: "success"; result: SourceProbeResult };

type PipelineJobKind =
  | "full_ingest"
  | "fetch_sources"
  | "lightweight_enrich"
  | "rebuild_index"
  | "compile_wiki"
  | "regenerate_brief"
  | "generate_audio"
  | "publish_viewer"
  | "health_check"
  | "answer_query"
  | "file_output";

type PipelineActionRequest =
  | { kind: "full_ingest"; label: string }
  | { kind: "fetch_sources"; label: string }
  | { kind: "lightweight_enrich"; label: string }
  | { kind: "rebuild_index"; label: string }
  | { kind: "compile_wiki"; label: string; limit: number }
  | { kind: "regenerate_brief"; label: string; briefDate: string }
  | { kind: "generate_audio"; label: string; briefDate: string }
  | { kind: "publish_viewer"; label: string; briefDate: string }
  | { kind: "health_check"; label: string; scope: HealthCheckScope; topic?: string }
  | { kind: "answer_query"; label: string; question: string; outputKind: AdvancedOutputKind }
  | { kind: "file_output"; label: string; path: string };

type QueuedPipelineAction = PipelineActionRequest & {
  queuedAt: string;
};

type PipelineActionState = {
  status: "idle" | "running" | "queued";
  queuePosition: number | null;
  currentRun: IngestionRunHistoryEntry | null;
  latestRun: IngestionRunHistoryEntry | null;
};

type SyntheticRecentJob = {
  id: string;
  title: string;
  summary: string;
  trigger: string | null;
  status: RunStatus;
  startedAt: string;
  associatedDates: ReturnType<typeof describeAssociatedDates>;
  totalCostUsd: number;
  aiCostUsd: number;
  ttsCostUsd: number;
  clickable: boolean;
  runId: string | null;
};

const PIPELINE_JOB_KINDS: PipelineJobKind[] = [
  "full_ingest",
  "fetch_sources",
  "lightweight_enrich",
  "rebuild_index",
  "compile_wiki",
  "regenerate_brief",
  "generate_audio",
  "publish_viewer",
  "health_check",
  "answer_query",
  "file_output",
];

function MetricCard({
  label,
  value,
  detail,
}: {
  label: string;
  value: string;
  detail: string;
}) {
  return (
    <article className="rounded-[1.45rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.52)] px-4 py-4">
      <p className="font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">{label}</p>
      <p className="mt-3 font-display text-3xl leading-none text-[var(--ink)]">{value}</p>
      <p className="mt-2 text-sm leading-6 text-[var(--muted)]">{detail}</p>
    </article>
  );
}

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

function PipelineStage({
  step,
  title,
  description,
  children,
}: {
  step: string;
  title: string;
  description: string;
  children: ReactNode;
}) {
  return (
    <section className="rounded-[1.8rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.44)] px-5 py-5">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-start">
        <div className="flex h-14 w-14 shrink-0 items-center justify-center rounded-[1.45rem] border border-[var(--accent)]/18 bg-[rgba(154,52,18,0.08)] font-display text-2xl text-[var(--accent)]">
          {step}
        </div>
        <div className="min-w-0">
          <p className="section-kicker">{`Step ${step}`}</p>
          <h4 className="mt-3 font-display text-3xl leading-tight text-[var(--ink)]">{title}</h4>
          <SkimmableText className="mt-3 max-w-3xl text-sm leading-6 text-[var(--muted)]">{description}</SkimmableText>
        </div>
      </div>
      <div className="mt-6 divide-y divide-[var(--ink)]/8">{children}</div>
    </section>
  );
}

function EditionTargetField({
  value,
  options,
  disabled,
  helperText,
  onChange,
}: {
  value: string;
  options: Array<{ brief_date: string; coverage_start: string; coverage_end: string }>;
  disabled: boolean;
  helperText: string;
  onChange: (value: string) => void;
}) {
  const resolvedValue = value || options[0]?.brief_date || "";

  return (
    <label className="block">
      <span className="field-label">Edition target</span>
      <select
        className="field-input mt-2"
        disabled={disabled}
        onChange={(event) => onChange(event.target.value)}
        value={resolvedValue}
      >
        {options.length ? (
          options.map((option) => (
            <option key={option.brief_date} value={option.brief_date}>
              {formatEditionTargetLabel(option.brief_date)}
            </option>
          ))
        ) : (
          <option value="">No editions available yet</option>
        )}
      </select>
      <p className="mt-2 text-xs leading-5 text-[var(--muted)]">
        {options.length ? helperText : "No edition targets are available yet. Run the earlier pipeline steps first."}
      </p>
    </label>
  );
}

function PipelineActionRow({
  icon,
  title,
  description,
  updates,
  useWhen,
  actionLabel,
  busyLabel,
  state,
  queuePosition,
  disabled,
  controls,
  note,
  badge,
  logsLabel,
  logsDisabled = false,
  onOpenLogs,
  tone = "default",
  onClick,
}: {
  icon: ReactNode;
  title: string;
  description: string;
  updates: string;
  useWhen: string;
  actionLabel: string;
  busyLabel: string;
  state: PipelineActionState["status"];
  queuePosition?: number | null;
  disabled: boolean;
  controls?: ReactNode;
  note?: string;
  badge?: string;
  logsLabel?: string;
  logsDisabled?: boolean;
  onOpenLogs?: () => void;
  tone?: "default" | "placeholder" | "success" | "warning";
  onClick: () => void;
}) {
  const isRunning = state === "running";
  const isQueued = state === "queued";
  const iconToneClass =
    isRunning
      ? "border-[rgba(14,77,100,0.18)] bg-[rgba(14,77,100,0.1)] text-[var(--teal)]"
      : isQueued
        ? "border-[var(--accent)]/20 bg-[rgba(154,52,18,0.1)] text-[var(--accent)]"
        : tone === "success"
          ? "border-[rgba(22,163,74,0.2)] bg-[rgba(22,163,74,0.08)] text-[#166534]"
          : tone === "warning"
            ? "border-[var(--accent)]/20 bg-[rgba(154,52,18,0.1)] text-[var(--accent)]"
        : tone === "placeholder"
          ? "border-[rgba(14,77,100,0.18)] bg-[rgba(14,77,100,0.08)] text-[var(--teal)]"
          : "border-[var(--ink)]/8 bg-[rgba(255,255,255,0.74)] text-[var(--accent)]";
  const rowToneClass =
    isRunning
      ? "border-[rgba(14,77,100,0.18)] bg-[linear-gradient(180deg,rgba(14,77,100,0.08),rgba(255,255,255,0.56))]"
      : isQueued
        ? "border-[var(--accent)]/18 bg-[linear-gradient(180deg,rgba(154,52,18,0.08),rgba(255,255,255,0.56))]"
        : tone === "success"
          ? "border-[rgba(22,163,74,0.18)] bg-[linear-gradient(180deg,rgba(240,253,244,0.9),rgba(255,255,255,0.56))]"
          : tone === "warning"
            ? "border-[var(--accent)]/18 bg-[linear-gradient(180deg,rgba(154,52,18,0.08),rgba(255,255,255,0.56))]"
            : "border-transparent bg-transparent";
  const panelToneClass =
    isRunning
      ? "border-[rgba(14,77,100,0.18)] bg-[rgba(14,77,100,0.06)]"
      : isQueued
        ? "border-[var(--accent)]/18 bg-[rgba(154,52,18,0.06)]"
        : tone === "success"
          ? "border-[rgba(22,163,74,0.18)] bg-[rgba(22,163,74,0.06)]"
          : tone === "warning"
            ? "border-[var(--accent)]/18 bg-[rgba(154,52,18,0.06)]"
        : tone === "placeholder"
          ? "border-[rgba(14,77,100,0.16)] bg-[rgba(14,77,100,0.05)]"
          : "border-[var(--ink)]/8 bg-[rgba(255,255,255,0.6)]";
  const detailToneClass =
    isRunning
      ? "border-[rgba(14,77,100,0.14)] bg-[rgba(255,255,255,0.68)]"
      : isQueued
        ? "border-[var(--accent)]/14 bg-[rgba(255,255,255,0.66)]"
        : tone === "success"
          ? "border-[rgba(22,163,74,0.14)] bg-[rgba(255,255,255,0.74)]"
          : tone === "warning"
            ? "border-[var(--accent)]/14 bg-[rgba(255,255,255,0.66)]"
            : "border-[var(--ink)]/8 bg-[rgba(255,255,255,0.56)]";
  const badgeToneClass =
    isRunning
      ? "border-[rgba(14,77,100,0.18)] bg-[rgba(14,77,100,0.08)] text-[var(--teal)]"
      : isQueued
        ? "border-[var(--accent)]/18 bg-[rgba(154,52,18,0.08)] text-[var(--accent)]"
        : tone === "success"
          ? "border-[rgba(22,163,74,0.2)] bg-[rgba(22,163,74,0.08)] text-[#166534]"
          : tone === "warning"
            ? "border-[var(--accent)]/18 bg-[rgba(154,52,18,0.08)] text-[var(--accent)]"
        : tone === "placeholder"
          ? "border-[rgba(14,77,100,0.18)] bg-[rgba(14,77,100,0.08)] text-[var(--teal)]"
          : "border-[var(--ink)]/8 bg-[rgba(255,255,255,0.74)] text-[var(--muted)]";
  const stateBadge = isRunning ? "Running now" : isQueued && queuePosition ? `Queued #${queuePosition}` : badge;
  const actionText = isRunning ? busyLabel : isQueued && queuePosition ? `Queued #${queuePosition}` : actionLabel;

  return (
    <div className="py-5 first:pt-0 last:pb-0">
      <div className={`rounded-[1.55rem] border px-4 py-4 transition ${rowToneClass}`}>
        <div className="grid gap-4 lg:grid-cols-[minmax(0,1.45fr)_minmax(300px,0.8fr)] lg:items-start">
          <div className="min-w-0">
            <div className="flex items-start gap-3">
              <div className={`mt-0.5 flex h-10 w-10 shrink-0 items-center justify-center rounded-[1.1rem] border ${iconToneClass}`}>
                {icon}
              </div>
              <div className="min-w-0">
                <div className="flex flex-wrap items-center gap-2">
                  <h5 className="text-lg font-medium text-[var(--ink)]">{title}</h5>
                  {stateBadge ? (
                    <span className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 font-mono text-[10px] uppercase tracking-[0.16em] ${badgeToneClass}`}>
                      {isRunning ? <LoaderCircle className="h-3 w-3 animate-spin" /> : null}
                      {isQueued ? <Clock3 className="h-3 w-3" /> : null}
                      <span>{stateBadge}</span>
                    </span>
                  ) : null}
                </div>
                <SkimmableText className="mt-2 text-sm leading-6 text-[var(--muted)]">{description}</SkimmableText>
              </div>
            </div>

            <div className="mt-4 grid gap-3 sm:grid-cols-2">
              <div className={`rounded-[1.2rem] border px-3.5 py-3 ${detailToneClass}`}>
                <p className="field-label">What it updates</p>
                <p className="mt-1.5 text-sm leading-6 text-[var(--muted-strong)]">{updates}</p>
              </div>
              <div className={`rounded-[1.2rem] border px-3.5 py-3 ${detailToneClass}`}>
                <p className="field-label">Use this when</p>
                <p className="mt-1.5 text-sm leading-6 text-[var(--muted-strong)]">{useWhen}</p>
              </div>
            </div>
          </div>

          <div className={`rounded-[1.35rem] border px-4 py-4 ${panelToneClass}`}>
            {controls ? (
              <div>{controls}</div>
            ) : (
              <p className="text-sm leading-6 text-[var(--muted)]">No extra input is needed for this job.</p>
            )}
            {note ? <p className="mt-3 text-xs leading-5 text-[var(--muted)]">{note}</p> : null}
            <div className="mt-4 flex flex-wrap items-center gap-3">
              <button className="secondary-button" disabled={disabled} onClick={onClick} type="button">
                {isRunning ? <LoaderCircle className="h-3.5 w-3.5 animate-spin" /> : null}
                {isQueued ? <Clock3 className="h-3.5 w-3.5" /> : null}
                {actionText}
              </button>
              {logsLabel ? (
                <button className="secondary-button" disabled={logsDisabled} onClick={onOpenLogs} type="button">
                  <History className="h-3.5 w-3.5" />
                  {logsLabel}
                </button>
              ) : null}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function SourceStatusChip({ status }: { status: RunStatus | null }) {
  if (!status) {
    return (
      <span className="rounded-full border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.72)] px-2.5 py-0.5 font-mono text-[10px] uppercase tracking-[0.16em] text-[var(--muted)]">
        No extraction yet
      </span>
    );
  }

  const className =
    status === "failed"
      ? "border-[var(--danger)]/18 bg-[rgba(159,18,57,0.08)] text-[var(--danger)]"
      : status === "interrupted"
        ? "border-[rgba(120,53,15,0.18)] bg-[rgba(120,53,15,0.08)] text-[#78350f]"
      : status === "running"
        ? "border-[var(--teal)]/18 bg-[rgba(14,77,100,0.08)] text-[var(--teal)]"
        : status === "pending"
          ? "border-[var(--accent)]/18 bg-[rgba(154,52,18,0.08)] text-[var(--accent)]"
          : "border-[var(--ink)]/10 bg-[rgba(17,19,18,0.05)] text-[var(--muted-strong)]";
  const label =
    status === "failed"
      ? "Failed"
      : status === "interrupted"
        ? "Interrupted"
        : status === "running"
          ? "Running"
          : status === "pending"
            ? "Pending"
            : "Synced";

  return (
    <span className={`rounded-full border px-2.5 py-0.5 font-mono text-[10px] uppercase tracking-[0.16em] ${className}`}>
      {label}
    </span>
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
  return error instanceof Error && error.message ? error.message : "Request failed.";
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
      source.raw_kind === "newsletter"
      || source.raw_kind === "paper"
      || source.raw_kind === "article"
      || source.raw_kind === "news"
      || source.raw_kind === "thread"
      || source.raw_kind === "signal"
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
  }

  return entries;
}

function formatSourceTypeLabel(type: SourceType) {
  return type === "gmail_newsletter" ? "Gmail" : "Website";
}

function formatRawKindLabel(value: string) {
  return value.replace(/[-_]/g, " ");
}

function defaultModesForSourceType(type: SourceType) {
  return type === "gmail_newsletter"
    ? { classification_mode: "written_content_auto", decomposition_mode: "newsletter_entries" }
    : { classification_mode: "fixed", decomposition_mode: "none" };
}

function formatModeLabel(value: string) {
  return value.replace(/_/g, " ");
}

function formatEmittedKinds(kinds: string[]) {
  return kinds.length ? kinds.map((kind) => formatRawKindLabel(kind)).join(", ") : "No emitted kinds recorded yet";
}

function formatDateTimeLabel(value: string | null) {
  if (!value) return "Not finished yet";
  return new Date(value).toLocaleString();
}

function formatLogTimeLabel(value: string) {
  return new Date(value).toLocaleTimeString();
}

function formatUsdCost(value: number) {
  if (!Number.isFinite(value) || value <= 0) return "$0";
  if (value < 0.0001) return "<$0.0001";
  if (value < 0.01) return `$${value.toFixed(4)}`;
  return `$${value.toFixed(2)}`;
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
  return { edition, coverage };
}

function matchesPipelineRun(run: IngestionRunHistoryEntry, kind: PipelineJobKind) {
  if (kind === "full_ingest") {
    return run.trigger === "default_ingest_pipeline"
      && ["raw_fetch", "lightweight_enrichment", "vault_index"].includes(run.operation_kind);
  }
  if (kind === "fetch_sources") {
    return run.operation_kind === "raw_fetch" && run.trigger === "manual_fetch";
  }
  if (kind === "lightweight_enrich") {
    return run.operation_kind === "lightweight_enrichment" && run.trigger === "manual_lightweight_enrich";
  }
  if (kind === "rebuild_index") {
    return run.operation_kind === "vault_index" && run.trigger === "manual_index";
  }
  if (kind === "compile_wiki") {
    return run.operation_kind === "advanced_compile" && run.trigger === "manual_advanced_compile";
  }
  if (kind === "regenerate_brief") {
    return run.operation_kind === "brief_generation" && run.trigger === "manual_digest";
  }
  if (kind === "generate_audio") {
    return run.operation_kind === "audio_generation" && run.trigger === "manual_audio";
  }
  if (kind === "publish_viewer") {
    return run.operation_kind === "viewer_publish" && run.trigger === "manual_publish";
  }
  if (kind === "health_check") {
    return run.operation_kind === "health_check" && run.trigger === "manual_health_check";
  }
  if (kind === "answer_query") {
    return run.operation_kind === "answer_query" && run.trigger === "manual_answer_query";
  }
  return run.operation_kind === "file_output" && run.trigger === "manual_file_output";
}

function describePipelineActionDates(action: PipelineActionRequest | QueuedPipelineAction) {
  if ("briefDate" in action) {
    return {
      edition: formatBriefDayLabel(action.briefDate),
      coverage: formatBriefDayLabel(shiftIsoDate(action.briefDate, -1)),
    };
  }
  return null;
}

function syntheticRecentJobFromAction(
  action: QueuedPipelineAction,
  options: { status: RunStatus; queuePosition?: number | null; runningLabel?: string },
): SyntheticRecentJob {
  const queuedSuffix =
    options.status === "pending" && options.queuePosition
      ? `Queued locally in position ${options.queuePosition}.`
      : options.status === "running"
        ? options.runningLabel ?? "Waiting for the live run record."
        : "Pending.";
  return {
    id: `synthetic-${action.kind}-${action.queuedAt}`,
    title: action.label,
    summary: queuedSuffix,
    trigger: options.status === "pending" ? "local_queue" : "local_start",
    status: options.status,
    startedAt: action.queuedAt,
    associatedDates: describePipelineActionDates(action),
    totalCostUsd: 0,
    aiCostUsd: 0,
    ttsCostUsd: 0,
    clickable: false,
    runId: null,
  };
}

function sortRunsByStartedAtDesc(runs: IngestionRunHistoryEntry[]) {
  return [...runs].sort((left, right) => new Date(right.started_at).getTime() - new Date(left.started_at).getTime());
}

function buildOptimisticSourceLatestRun(
  source: Source,
  id: string,
  startedAt: string,
  maxItems: number,
): NonNullable<Source["latest_extraction_run"]> {
  return {
    id,
    status: "running",
    operation_kind: "raw_fetch",
    summary: `Running source inject for ${source.name} with a cap of ${maxItems} documents.`,
    started_at: startedAt,
    finished_at: null,
    emitted_kinds:
      source.latest_extraction_run?.emitted_kinds.length
        ? source.latest_extraction_run.emitted_kinds
        : [source.raw_kind],
  };
}

function buildOptimisticSourceInjectRun(
  source: Source,
  id: string,
  startedAt: string,
  maxItems: number,
): IngestionRunHistoryEntry {
  return {
    id,
    run_type: "ingest",
    status: "running",
    operation_kind: "raw_fetch",
    trigger: "manual_source_fetch",
    title: `Source inject · ${source.name}`,
    summary: `Running source inject for ${source.name} with a cap of ${maxItems} documents.`,
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
      { label: "Max items", value: String(maxItems) },
      { label: "Status", value: "Waiting for the latest extraction log" },
    ],
    logs: [
      {
        logged_at: startedAt,
        level: "info",
        message: `Source inject requested for ${source.name} with a cap of ${maxItems} documents. Waiting for the worker to finish.`,
      },
    ],
    steps: [],
    source_stats: [],
    errors: [],
    output_paths: [],
    changed_file_count: 0,
  };
}

function ConnectionsWorkspacePage({ mode }: { mode: WorkspaceMode }) {
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const focusedPipelineSourceId = mode === "pipeline" ? searchParams.get("source") : null;
  const selectedRunId = mode === "pipeline" ? searchParams.get("run") : null;
  const navigateToPipeline = (
    changes: { sourceId?: string | null; runId?: string | null } = {},
    options?: { replace?: boolean },
  ) => {
    const next = new URLSearchParams();
    const sourceId =
      changes.sourceId !== undefined ? changes.sourceId : mode === "pipeline" ? searchParams.get("source") : null;
    const runId = changes.runId !== undefined ? changes.runId : mode === "pipeline" ? searchParams.get("run") : null;

    if (sourceId) {
      next.set("source", sourceId);
    }
    if (runId) {
      next.set("run", runId);
    }

    const nextSearch = next.toString();
    navigate(`/pipeline${nextSearch ? `?${nextSearch}` : ""}`, { replace: options?.replace ?? false });
  };

  const [pipelineQueue, setPipelineQueue] = useState<QueuedPipelineAction[]>([]);
  const [activePipelineAction, setActivePipelineAction] = useState<QueuedPipelineAction | null>(null);

  const sourcesQuery = useQuery({ queryKey: ["sources"], queryFn: () => api.getSources() });
  const capabilitiesQuery = useQuery({
    queryKey: ["connections", "capabilities"],
    queryFn: api.getConnectionCapabilities,
  });
  const briefAvailabilityQuery = useQuery({
    queryKey: ["briefs", "availability"],
    queryFn: api.getBriefAvailability,
  });
  const pipelineStatusQuery = useQuery({
    queryKey: ["ops", "pipeline-status"],
    queryFn: api.getPipelineStatus,
  });
  const advancedRuntimeQuery = useQuery({
    queryKey: ["ops", "advanced-runtime"],
    queryFn: api.getAdvancedRuntime,
  });
  const gmailQuery = useQuery({ queryKey: ["connections", "gmail"], queryFn: api.getGmailConnection });
  const zoteroQuery = useQuery({ queryKey: ["connections", "zotero"], queryFn: api.getZoteroConnection });
  const ingestionRunsQuery = useQuery({
    queryKey: ["ops", "ingestion-runs"],
    queryFn: api.getIngestionRuns,
    refetchInterval: (query) => {
      const runs = query.state.data as IngestionRunHistoryEntry[] | undefined;
      return activePipelineAction || runs?.some((run) => run.status === "running") ? 1500 : false;
    },
    refetchIntervalInBackground: true,
  });

  const [connectionPanels, setConnectionPanels] = useState({ gmail: true, zotero: true });
  const [connectionPanelsReady, setConnectionPanelsReady] = useState(false);
  const [sourceForm, setSourceForm] = useState<SourceFormState>(createEmptySourceForm);
  const [isSourceEditorOpen, setIsSourceEditorOpen] = useState(false);
  const [editingSourceId, setEditingSourceId] = useState<string | null>(null);
  const [sourceNotice, setSourceNotice] = useState<Notice | null>(null);
  const [pipelineNotice, setPipelineNotice] = useState<Notice | null>(null);
  const [sourceProbeReports, setSourceProbeReports] = useState<Record<string, SourceProbeReport>>({});
  const [probingSourceIds, setProbingSourceIds] = useState<Record<string, boolean>>({});
  const [sourceInjectOverrides, setSourceInjectOverrides] = useState<Record<string, string>>({});
  const [removingSourceId, setRemovingSourceId] = useState<string | null>(null);
  const [togglingSourceId, setTogglingSourceId] = useState<string | null>(null);
  const [injectingSourceId, setInjectingSourceId] = useState<string | null>(null);
  const [openingSourceLogId, setOpeningSourceLogId] = useState<string | null>(null);
  const [regenerateBriefDate, setRegenerateBriefDate] = useState("");
  const [advancedCompileLimit, setAdvancedCompileLimit] = useState("8");
  const [healthCheckScope, setHealthCheckScope] = useState<HealthCheckScope>("vault");
  const [healthCheckTopic, setHealthCheckTopic] = useState("");
  const [answerQuestion, setAnswerQuestion] = useState("");
  const [answerOutputKind, setAnswerOutputKind] = useState<AdvancedOutputKind>("answer");
  const [fileOutputPath, setFileOutputPath] = useState("");
  const [gmailEmail, setGmailEmail] = useState("");
  const [gmailAppPassword, setGmailAppPassword] = useState("");
  const [zoteroApiKey, setZoteroApiKey] = useState("");
  const [zoteroLibraryId, setZoteroLibraryId] = useState("");
  const [zoteroCollectionName, setZoteroCollectionName] = useState("");
  const [zoteroAutoTagVocabulary, setZoteroAutoTagVocabulary] = useState(formatTagVocabulary(DEFAULT_ZOTERO_AUTO_TAG_VOCABULARY));
  const [zoteroAutoTagVocabularyHydrated, setZoteroAutoTagVocabularyHydrated] = useState(false);

  const refreshAll = async () => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: ["sources"] }),
      queryClient.invalidateQueries({ queryKey: ["connections"] }),
      queryClient.invalidateQueries({ queryKey: ["briefs"] }),
      queryClient.invalidateQueries({ queryKey: ["items"] }),
      queryClient.invalidateQueries({ queryKey: ["item"] }),
      queryClient.invalidateQueries({ queryKey: ["ops", "pipeline-status"] }),
      queryClient.invalidateQueries({ queryKey: ["ops", "ingestion-runs"] }),
    ]);
  };

  const handleJobSuccess = async (job: JobResponse) => {
    setPipelineNotice({ tone: "success", message: job.detail });
    await refreshAll();
  };

  const handleJobError = (error: unknown) => {
    setPipelineNotice({ tone: "error", message: readErrorMessage(error) });
  };

  const resetSourceEditor = () => {
    setIsSourceEditorOpen(false);
    setEditingSourceId(null);
    setSourceForm(createEmptySourceForm());
    setSourceNotice(null);
  };

  const openNewSourceEditor = () => {
    setEditingSourceId(null);
    setSourceForm(createEmptySourceForm());
    setSourceNotice(null);
    setIsSourceEditorOpen(true);
  };

  const openSourceEditor = (source: Source) => {
    setEditingSourceId(source.id);
    setSourceForm(mapSourceToForm(source));
    setSourceNotice(null);
    setIsSourceEditorOpen(true);
  };

  const openSourceInPipelines = (sourceId: string) => {
    setPipelineNotice(null);
    navigateToPipeline({ sourceId, runId: null });
  };

  const createSource = useMutation({
    mutationFn: (payload: Record<string, unknown>) => api.createSource(payload),
    onSuccess: async () => {
      resetSourceEditor();
      setSourceNotice({ tone: "success", message: "Source added." });
      await refreshAll();
    },
    onError: (error) => setSourceNotice({ tone: "error", message: error.message }),
  });

  const updateSource = useMutation({
    mutationFn: ({ id, payload }: { id: string; payload: Record<string, unknown> }) => api.updateSource(id, payload),
    onSuccess: async (source) => {
      resetSourceEditor();
      setSourceNotice({ tone: "success", message: "Source updated." });
      setSourceProbeReports((current) => {
        const next = { ...current };
        delete next[source.id];
        return next;
      });
      await refreshAll();
    },
    onError: (error) => setSourceNotice({ tone: "error", message: error.message }),
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
    onError: (error) => setSourceNotice({ tone: "error", message: error.message }),
    onSettled: () => setTogglingSourceId(null),
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
      await refreshAll();
    },
    onError: (error) => setSourceNotice({ tone: "error", message: error.message }),
    onSettled: () => setRemovingSourceId(null),
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

  const runFullPipeline = useMutation({
    mutationFn: api.runIngest,
    onSuccess: handleJobSuccess,
    onError: handleJobError,
  });

  const fetchSources = useMutation({
    mutationFn: api.fetchSources,
    onSuccess: handleJobSuccess,
    onError: handleJobError,
  });

  const lightweightEnrich = useMutation({
    mutationFn: api.lightweightEnrich,
    onSuccess: handleJobSuccess,
    onError: handleJobError,
  });

  const rebuildIndex = useMutation({
    mutationFn: api.rebuildItemsIndex,
    onSuccess: handleJobSuccess,
    onError: handleJobError,
  });

  const compileWiki = useMutation({
    mutationFn: (payload?: { limit?: number }) => api.advancedCompile(payload),
    onSuccess: handleJobSuccess,
    onError: handleJobError,
  });

  const healthCheck = useMutation({
    mutationFn: (payload: { scope: HealthCheckScope; topic?: string }) => api.healthCheck(payload),
    onSuccess: handleJobSuccess,
    onError: handleJobError,
  });

  const answerQuery = useMutation({
    mutationFn: (payload: { question: string; output_kind: AdvancedOutputKind }) => api.answerQuery(payload),
    onSuccess: handleJobSuccess,
    onError: handleJobError,
  });

  const fileOutput = useMutation({
    mutationFn: (payload: { path: string }) => api.fileOutput(payload),
    onSuccess: handleJobSuccess,
    onError: handleJobError,
  });

  const regenerateEdition = useMutation({
    mutationFn: (briefDate?: string) => api.regenerateBrief(briefDate),
    onSuccess: handleJobSuccess,
    onError: handleJobError,
  });

  const generateAudio = useMutation({
    mutationFn: (briefDate?: string) => api.generateAudio(briefDate),
    onSuccess: handleJobSuccess,
    onError: handleJobError,
  });

  const publishLatest = useMutation({
    mutationFn: (briefDate?: string) => api.publishLatest(briefDate),
    onSuccess: handleJobSuccess,
    onError: handleJobError,
  });

  const sourceSummary = useMemo(() => {
    const total = sourcesQuery.data?.length ?? 0;
    const active = sourcesQuery.data?.filter((source) => source.active).length ?? 0;
    return {
      total,
      active,
      paused: total - active,
    };
  }, [sourcesQuery.data]);

  const runningOperationCount = ingestionRunsQuery.data?.filter((run) => run.status === "running").length ?? 0;

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
  const activeEditionTarget = regenerateBriefDate || regenerateOptions[0]?.brief_date || "";
  const currentEditionTarget = briefAvailabilityQuery.data?.default_day ?? regenerateOptions[0]?.brief_date ?? null;
  const currentEditionSelected = Boolean(currentEditionTarget && activeEditionTarget === currentEditionTarget);
  const currentEditionOutputState = useMemo(
    () => ({
      brief: currentEditionSelected && hasSuccessfulEditionRun(ingestionRunsQuery.data ?? [], "brief_generation", currentEditionTarget),
      audio: currentEditionSelected && hasSuccessfulEditionRun(ingestionRunsQuery.data ?? [], "audio_generation", currentEditionTarget),
      viewer: currentEditionSelected && hasSuccessfulEditionRun(ingestionRunsQuery.data ?? [], "viewer_publish", currentEditionTarget),
    }),
    [currentEditionSelected, currentEditionTarget, ingestionRunsQuery.data],
  );

  const selectedRun = ingestionRunsQuery.data?.find((run) => run.id === selectedRunId) ?? null;
  const selectedRunAssociatedDates = selectedRun ? describeAssociatedDates(selectedRun) : null;

  const editingSource = sourcesQuery.data?.find((source) => source.id === editingSourceId) ?? null;
  const sourceMutationBusy =
    createSource.isPending || updateSource.isPending || toggleSource.isPending || removeSource.isPending;
  const pipelineActionStates = useMemo<Record<PipelineJobKind, PipelineActionState>>(() => {
    const runs = ingestionRunsQuery.data ?? [];
    const next = {} as Record<PipelineJobKind, PipelineActionState>;
    for (const kind of PIPELINE_JOB_KINDS) {
      const matchingRuns = runs.filter((run) => matchesPipelineRun(run, kind));
      const queueIndex = pipelineQueue.findIndex((action) => action.kind === kind);
      const currentRun = matchingRuns.find((run) => run.status === "running") ?? null;
      const status =
        activePipelineAction?.kind === kind || currentRun
          ? "running"
          : queueIndex >= 0
            ? "queued"
            : "idle";
      next[kind] = {
        status,
        queuePosition: queueIndex >= 0 ? queueIndex + 1 : null,
        currentRun,
        latestRun: matchingRuns[0] ?? null,
      };
    }
    return next;
  }, [activePipelineAction, ingestionRunsQuery.data, pipelineQueue]);

  const displayRunningCount = useMemo(() => {
    if (!activePipelineAction) return runningOperationCount;
    return pipelineActionStates[activePipelineAction.kind].currentRun ? runningOperationCount : runningOperationCount + 1;
  }, [activePipelineAction, pipelineActionStates, runningOperationCount]);

  const queuedOperationCount = pipelineQueue.length;

  const recentJobs = useMemo(() => {
    const runs = ingestionRunsQuery.data ?? [];
    let activeRow: SyntheticRecentJob | null = null;
    const queuedRows: SyntheticRecentJob[] = [];
    if (activePipelineAction && !pipelineActionStates[activePipelineAction.kind].currentRun) {
      activeRow = syntheticRecentJobFromAction(activePipelineAction, {
        status: "running",
        runningLabel: "Starting now. Live logs will appear here as soon as the run record is written.",
      });
    }
    pipelineQueue.forEach((action, index) => {
      queuedRows.push(
        syntheticRecentJobFromAction(action, {
          status: "pending",
          queuePosition: index + 1,
        }),
      );
    });
    return {
      activeRow,
      queuedRows,
      combinedCount: runs.length + queuedRows.length + Number(Boolean(activeRow)),
    };
  }, [activePipelineAction, ingestionRunsQuery.data, pipelineActionStates, pipelineQueue]);

  const recentOperationCount = recentJobs.combinedCount;
  const pipelineStatus: PipelineStatus | null = pipelineStatusQuery.data ?? null;
  const lightweightPendingCount = pipelineStatus?.lightweight_pending_count ?? 0;
  const itemsIndex = pipelineStatus?.items_index;
  const lightweightPendingLabel =
    lightweightPendingCount > 0
      ? `Run lightweight enrich (${lightweightPendingCount})`
      : "Run lightweight enrich";
  const lightweightPendingDescription =
    lightweightPendingCount > 0
      ? `Use the local Ollama model to add small metadata only: missing authors, tags, and a short summary. This counter tracks documents that have not completed lightweight enrichment yet. ${lightweightPendingCount} document${lightweightPendingCount === 1 ? "" : "s"} currently need a first pass.`
      : "Use the local Ollama model to add small metadata only: missing authors, tags, and a short summary. Every raw document has completed lightweight enrichment at least once.";
  const itemsIndexUpToDate = itemsIndex?.up_to_date ?? false;
  const itemsIndexStaleCount = itemsIndex?.stale_document_count ?? 0;
  const itemsIndexTone = itemsIndexUpToDate ? "success" : "warning";
  const itemsIndexBadge = itemsIndexUpToDate ? "Up to date" : "Needs rebuild";
  const itemsIndexDescription = itemsIndexUpToDate
    ? `The structured items index is current${itemsIndex?.generated_at ? ` as of ${formatDateTimeLabel(itemsIndex.generated_at)}` : ""}. Rebuild only if you want to force a refresh.`
    : `The structured items index is stale for ${itemsIndexStaleCount} document${itemsIndexStaleCount === 1 ? "" : "s"}. Rebuild it to refresh the inbox, filters, briefs, wiki compile, and published viewer.`;

  const executePipelineAction = async (action: QueuedPipelineAction) => {
    setActivePipelineAction(action);
    setPipelineNotice(null);
    await queryClient.invalidateQueries({ queryKey: ["ops", "ingestion-runs"] });
    try {
      if (action.kind === "full_ingest") {
        await runFullPipeline.mutateAsync();
      } else if (action.kind === "fetch_sources") {
        await fetchSources.mutateAsync();
      } else if (action.kind === "lightweight_enrich") {
        await lightweightEnrich.mutateAsync();
      } else if (action.kind === "rebuild_index") {
        await rebuildIndex.mutateAsync();
      } else if (action.kind === "compile_wiki") {
        await compileWiki.mutateAsync({ limit: action.limit });
      } else if (action.kind === "regenerate_brief") {
        await regenerateEdition.mutateAsync(action.briefDate);
      } else if (action.kind === "generate_audio") {
        await generateAudio.mutateAsync(action.briefDate);
      } else if (action.kind === "publish_viewer") {
        await publishLatest.mutateAsync(action.briefDate);
      } else if (action.kind === "health_check") {
        await healthCheck.mutateAsync({ scope: action.scope, topic: action.topic });
      } else if (action.kind === "answer_query") {
        await answerQuery.mutateAsync({ question: action.question, output_kind: action.outputKind });
      } else {
        await fileOutput.mutateAsync({ path: action.path });
      }
    } catch {
      // Individual mutations already route errors into the shared pipeline notice.
    } finally {
      setActivePipelineAction(null);
    }
  };

  const queuePipelineAction = (action: PipelineActionRequest) => {
    setPipelineNotice(null);
    const state = pipelineActionStates[action.kind];
    if (state.status === "running") {
      setPipelineNotice({ tone: "success", message: `${action.label} is already running.` });
      return;
    }
    if (state.status === "queued" && state.queuePosition) {
      setPipelineNotice({ tone: "success", message: `${action.label} is already queued in position ${state.queuePosition}.` });
      return;
    }
    const queuedAction: QueuedPipelineAction = { ...action, queuedAt: new Date().toISOString() };
    if (activePipelineAction || runningOperationCount > 0) {
      const position = pipelineQueue.length + 1;
      setPipelineQueue((current) => [...current, queuedAction]);
      setPipelineNotice({ tone: "success", message: `${action.label} queued in position ${position}.` });
      return;
    }
    void executePipelineAction(queuedAction);
  };

  const buildPipelineLogButton = (kind: PipelineJobKind) => {
    const actionState = pipelineActionStates[kind];
    const run = actionState.currentRun ?? (actionState.status === "running" ? null : actionState.latestRun);
    return {
      logsLabel: actionState.status === "running"
        ? run
          ? "Current logs"
          : "Waiting for logs..."
        : run
          ? "Latest logs"
          : undefined,
      logsDisabled: !run,
      onOpenLogs: run ? () => navigateToPipeline({ runId: run.id }, { replace: true }) : undefined,
    };
  };

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
    if (activePipelineAction || !pipelineQueue.length || runningOperationCount > 0) return;
    const [nextAction, ...rest] = pipelineQueue;
    setPipelineQueue(rest);
    void executePipelineAction(nextAction);
  }, [activePipelineAction, pipelineQueue, runningOperationCount]);

  useEffect(() => {
    if (mode !== "connections" || searchParams.get("tab") !== "pipelines") return;
    const next = new URLSearchParams();
    const sourceId = searchParams.get("source");
    const runId = searchParams.get("run");
    if (sourceId) {
      next.set("source", sourceId);
    }
    if (runId) {
      next.set("run", runId);
    }
    const nextSearch = next.toString();
    navigate(`/pipeline${nextSearch ? `?${nextSearch}` : ""}`, { replace: true });
  }, [mode, navigate, searchParams]);

  useEffect(() => {
    if (!selectedRunId) return;
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        navigateToPipeline({ runId: null }, { replace: true });
      }
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [navigateToPipeline, selectedRunId]);

  useEffect(() => {
    if (!isSourceEditorOpen) return;
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setIsSourceEditorOpen(false);
        setEditingSourceId(null);
        setSourceForm(createEmptySourceForm());
        setSourceNotice(null);
      }
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [isSourceEditorOpen]);

  const combinedError =
    sourcesQuery.error
    || capabilitiesQuery.error
    || briefAvailabilityQuery.error
    || gmailQuery.error
    || zoteroQuery.error
    || ingestionRunsQuery.error;

  if (
    (sourcesQuery.isLoading || capabilitiesQuery.isLoading || gmailQuery.isLoading || zoteroQuery.isLoading)
    && !sourcesQuery.data
    && !capabilitiesQuery.data
    && !gmailQuery.data
    && !zoteroQuery.data
  ) {
    return <div className="page-loading">Loading control room…</div>;
  }

  if (combinedError) {
    return <div className="page-empty">The control room is unavailable right now. Refresh or check the backend.</div>;
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
  const gmailConnectedEmail = readStringMetadata(gmailQuery.data?.metadata_json.connected_email);
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
  const gmailNotice =
    gmailState === "connected"
      ? { tone: "success" as const, message: "Gmail connected. Newsletter sources will use the stored OAuth connection on the next run." }
      : gmailState
        ? { tone: "error" as const, message: `Gmail connection failed${gmailReason ? `: ${gmailReason.replaceAll("_", " ")}` : "."}` }
        : saveGmail.isError
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
      : zoteroLastError ?? "Paste a Zotero Web API key to verify the connection.";
  const zoteroNotice =
    saveZotero.isError
      ? { tone: "error" as const, message: saveZotero.error.message }
      : saveZotero.isSuccess && saveZotero.data.status === "error"
        ? { tone: "error" as const, message: readStringMetadata(saveZotero.data.metadata_json.last_error) ?? "Zotero verification failed." }
        : saveZotero.isSuccess
          ? { tone: "success" as const, message: "Zotero connection verified." }
          : null;

  const connectedConnectionCount = Number(gmailConfigured) + Number(zoteroConfigured);

  const handleSourceTypeChange = (type: SourceType) => {
    setSourceNotice(null);
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

  const handleSourceSubmit = (event: FormEvent) => {
    event.preventDefault();

    const trimmedName = sourceForm.name.trim();
    const trimmedUrl = sourceForm.url.trim();
    const trimmedWebsite = sourceForm.website.trim();
    const trimmedQuery = sourceForm.query.trim();
    const maxItems = Number.parseInt(sourceForm.maxItems, 10);
    const rawKind = sourceForm.type === "gmail_newsletter" ? "newsletter" : sourceForm.rawKind;
    const defaultModes = defaultModesForSourceType(sourceForm.type);

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
    if (Number.isNaN(maxItems) || maxItems < 1 || maxItems > 100) {
      setSourceNotice({ tone: "error", message: "Max items must be a number between 1 and 100." });
      return;
    }

    const payload = {
      name: trimmedName,
      raw_kind: rawKind,
      classification_mode: defaultModes.classification_mode,
      decomposition_mode: defaultModes.decomposition_mode,
      url: sourceForm.type === "website" ? trimmedUrl || null : null,
      query: sourceForm.type === "gmail_newsletter" ? trimmedQuery || null : null,
      description: sourceForm.description.trim() || null,
      max_items: maxItems,
      tags: csvToList(sourceForm.tags),
      active: sourceForm.active,
      config_json: buildSourceConfig(
        { ...sourceForm, website: trimmedWebsite, query: trimmedQuery },
        editingSource?.config_json ?? {},
      ),
    };

    setSourceNotice(null);
    if (editingSourceId) {
      updateSource.mutate({ id: editingSourceId, payload });
      return;
    }
    createSource.mutate({ ...payload, type: sourceForm.type });
  };

  const handleProbeSource = async (source: Source) => {
    setSourceNotice(null);
    setProbingSourceIds((current) => ({ ...current, [source.id]: true }));
    try {
      const result = await api.probeSource(source.id);
      setSourceProbeReports((current) => ({
        ...current,
        [source.id]: { tone: "success", result },
      }));
    } catch (error) {
      setSourceProbeReports((current) => ({
        ...current,
        [source.id]: { tone: "error", message: readErrorMessage(error) },
      }));
    } finally {
      setProbingSourceIds((current) => ({ ...current, [source.id]: false }));
    }
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

  const sourceInjectMaxItemsValue = (source: Source) => sourceInjectOverrides[source.id] ?? String(source.max_items);

  const handleSourceInjectMaxItemsChange = (sourceId: string, value: string) => {
    setSourceInjectOverrides((current) => {
      if (!value.trim()) {
        if (!(sourceId in current)) return current;
        const next = { ...current };
        delete next[sourceId];
        return next;
      }
      if (current[sourceId] === value) return current;
      return { ...current, [sourceId]: value };
    });
  };

  const handleInjectSource = async (source: Source) => {
    setPipelineNotice(null);
    const requestedMaxItems = Number.parseInt(sourceInjectMaxItemsValue(source), 10);
    if (Number.isNaN(requestedMaxItems) || requestedMaxItems < 1 || requestedMaxItems > SOURCE_INJECT_MAX_ITEMS_LIMIT) {
      setPipelineNotice({
        tone: "error",
        message: `Docs per inject must be a number between 1 and ${SOURCE_INJECT_MAX_ITEMS_LIMIT}.`,
      });
      return;
    }
    setInjectingSourceId(source.id);
    await queryClient.cancelQueries({ queryKey: ["sources"] });
    await queryClient.cancelQueries({ queryKey: ["ops", "ingestion-runs"] });
    const previousSources = queryClient.getQueryData<Source[]>(["sources"]);
    const previousRuns = queryClient.getQueryData<IngestionRunHistoryEntry[]>(["ops", "ingestion-runs"]);
    const optimisticRunId = `optimistic-source-inject-${source.id}-${Date.now()}`;
    const startedAt = new Date().toISOString();
    const optimisticLatestRun = buildOptimisticSourceLatestRun(source, optimisticRunId, startedAt, requestedMaxItems);
    const optimisticRun = buildOptimisticSourceInjectRun(source, optimisticRunId, startedAt, requestedMaxItems);

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
      const response = await api.injectSource(source.id, { max_items: requestedMaxItems });
      await handleJobSuccess(response);
    } catch (error) {
      if (previousSources) {
        queryClient.setQueryData(["sources"], previousSources);
      }
      if (previousRuns) {
        queryClient.setQueryData(["ops", "ingestion-runs"], previousRuns);
      }
      setPipelineNotice({ tone: "error", message: readErrorMessage(error) });
    } finally {
      setInjectingSourceId(null);
    }
  };

  const handleOpenLatestLog = async (source: Source) => {
    setPipelineNotice(null);
    setOpeningSourceLogId(source.id);
    try {
      const response = await api.getSourceLatestLog(source.id);
      queryClient.setQueryData<IngestionRunHistoryEntry[]>(["ops", "ingestion-runs"], (current = []) => {
        const rest = current.filter((run) => run.id !== response.run.id);
        return sortRunsByStartedAtDesc([response.run, ...rest]);
      });
      navigateToPipeline({ sourceId: source.id, runId: response.run.id });
    } catch (error) {
      setPipelineNotice({ tone: "error", message: readErrorMessage(error) });
    } finally {
      setOpeningSourceLogId(null);
    }
  };

  return (
    <>
      <div className="space-y-6 pb-10">
        <section className="editorial-panel overflow-hidden">
          <div className="flex flex-col gap-5 xl:flex-row xl:items-end xl:justify-between">
            <div>
              <p className="section-kicker">{mode === "connections" ? "Connections" : "Pipeline"}</p>
              <h3 className="section-title">
                {mode === "connections" ? "Inbox, library, and source setup" : "Operators, runs, and source injects"}
              </h3>
              <SkimmableText className="mt-4 max-w-4xl text-base leading-7 text-[var(--muted)]">
                {mode === "connections"
                  ? "Keep Gmail, Zotero, and the source registry configured here. Pipeline execution now lives in its own left-nav page so setup and operations stay separate."
                  : "Run the ingest chain, inject individual sources, and inspect recorded jobs here. Configuration lives on the Connections page."}
              </SkimmableText>
            </div>
            <div className="grid gap-3 sm:grid-cols-3">
              {mode === "connections" ? (
                <>
                  <MetricCard
                    label="Connected"
                    value={String(connectedConnectionCount).padStart(2, "0")}
                    detail={`${connectedConnectionCount} of 2 external services ready`}
                  />
                  <MetricCard
                    label="Sources"
                    value={String(sourceSummary.total).padStart(2, "0")}
                    detail={`${sourceSummary.active} active · ${sourceSummary.paused} paused`}
                  />
                  <MetricCard
                    label="Runs"
                    value={String(recentOperationCount).padStart(2, "0")}
                    detail={displayRunningCount ? `${displayRunningCount} running now` : queuedOperationCount ? `${queuedOperationCount} queued now` : "Operational history"}
                  />
                </>
              ) : (
                <>
                  <MetricCard
                    label="Active"
                    value={String(sourceSummary.active).padStart(2, "0")}
                    detail={`${sourceSummary.total} configured sources in the registry`}
                  />
                  <MetricCard
                    label="Running"
                    value={String(displayRunningCount).padStart(2, "0")}
                    detail={displayRunningCount ? "Workers currently in flight" : queuedOperationCount ? `${queuedOperationCount} waiting in queue` : "No workers running right now"}
                  />
                  <MetricCard
                    label="Runs"
                    value={String(recentOperationCount).padStart(2, "0")}
                    detail="Recent operational history"
                  />
                </>
              )}
            </div>
          </div>
        </section>

        {mode === "connections" ? (
          <div className="space-y-6">
            <section className="editorial-panel overflow-hidden">
              <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
                <div>
                  <p className="section-kicker">Connections</p>
                  <h3 className="section-title">Inbox and library fabric</h3>
                  <SkimmableText className="mt-4 max-w-4xl text-base leading-7 text-[var(--muted)]">
                    Gmail handles newsletter ingestion. Zotero gives the review workflow a durable save-out target. Keep both configured here before you touch the worker side.
                  </SkimmableText>
                </div>
                <div className="flex flex-wrap gap-2">
                  <span className="rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.56)] px-4 py-2 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">
                    {gmailSourceCount} Gmail source{gmailSourceCount === 1 ? "" : "s"}
                  </span>
                  <span className="rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.56)] px-4 py-2 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">
                    {zoteroAutoTagCount} auto-tags
                  </span>
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
                        {gmailSourceCount} source{gmailSourceCount === 1 ? "" : "s"}
                      </span>
                    </>
                  }
                  configured={gmailConfigured}
                  headline="Inbox routing"
                  label="Gmail"
                  notice={gmailNotice}
                  onToggle={() => setConnectionPanels((current) => ({ ...current, gmail: !current.gmail }))}
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
                    </div>

                    <div className="grid gap-4 lg:grid-cols-[minmax(0,0.9fr)_minmax(0,1.1fr)]">
                      <div className="rounded-[1.55rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.5)] p-5">
                        <p className="field-label">Google OAuth</p>
                        <SkimmableText className="mt-3 text-sm leading-6 text-[var(--muted)]">
                          Use OAuth for the most durable hosted connection. The vault source pipeline now reuses the stored Gmail OAuth connection directly.
                        </SkimmableText>
                        {!gmailOauthConfigured ? (
                          <div className="mt-4 rounded-2xl border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.72)] px-4 py-4 text-sm leading-6 text-[var(--muted-strong)]">
                            <p>
                              This button stays disabled until the backend sets <code>GMAIL_OAUTH_CLIENT_ID</code> and <code>GMAIL_OAUTH_CLIENT_SECRET</code> in
                              <code> apps/backend/.env</code> and restarts.
                            </p>
                            <p className="mt-3">In Google Cloud, add this authorized redirect URI:</p>
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

                    <div className="flex justify-end">
                      <button className="primary-button" disabled={saveGmail.isPending} type="submit">
                        {saveGmail.isPending ? "Saving Gmail..." : "Save Gmail settings"}
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
                  onToggle={() => setConnectionPanels((current) => ({ ...current, zotero: !current.zotero }))}
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

                    <label>
                      <span className="field-label">Auto-tag Vocabulary</span>
                      <textarea
                        className="field-input min-h-48"
                        onChange={(event) => setZoteroAutoTagVocabulary(event.target.value)}
                        placeholder="One tag per line"
                        value={zoteroAutoTagVocabulary}
                      />
                    </label>

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

            <section className="editorial-panel overflow-hidden">
              <div className="flex flex-col gap-4 xl:flex-row xl:items-end xl:justify-between">
                <div>
                  <p className="section-kicker">Source registry</p>
                  <h3 className="section-title">Configured sources</h3>
                  <SkimmableText className="mt-4 max-w-4xl text-base leading-7 text-[var(--muted)]">
                    Edit the actual discovery config here. Probing stays on the configuration side; manual inject moves to Pipeline where it belongs with the rest of the worker controls.
                  </SkimmableText>
                </div>
                <div className="flex flex-wrap items-center gap-2">
                  <span className="rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.56)] px-4 py-2 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">
                    {sourceSummary.active} active
                  </span>
                  <span className="rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.56)] px-4 py-2 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">
                    {sourceSummary.paused} paused
                  </span>
                  <button className="primary-button px-3 py-2 text-[11px]" onClick={openNewSourceEditor} type="button">
                    Add another feed
                  </button>
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
                {sourcesQuery.data?.length ? (
                  sourcesQuery.data.map((source) => {
                    const locatorEntries = getSourceLocatorEntries(source);
                    const probeReport = sourceProbeReports[source.id];
                    const latestRun = source.latest_extraction_run;
                    return (
                      <article
                        key={source.id}
                        className={`rounded-[1.7rem] border px-4 py-4 ${
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
                                {source.active ? "Status: active" : "Status: paused"}
                              </span>
                              <span className="rounded-full border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.74)] px-2.5 py-0.5 font-mono text-[10px] uppercase tracking-[0.16em] text-[var(--muted)]">
                                {`Source: ${formatSourceTypeLabel(source.type)}`}
                              </span>
                              <span className="rounded-full border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.74)] px-2.5 py-0.5 font-mono text-[10px] uppercase tracking-[0.16em] text-[var(--muted)]">
                                {`Raw kind: ${formatRawKindLabel(source.raw_kind)}`}
                              </span>
                              <span className="rounded-full border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.74)] px-2.5 py-0.5 font-mono text-[10px] uppercase tracking-[0.16em] text-[var(--muted)]">
                                {`Classification: ${formatModeLabel(source.classification_mode)}`}
                              </span>
                              <span className="rounded-full border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.74)] px-2.5 py-0.5 font-mono text-[10px] uppercase tracking-[0.16em] text-[var(--muted)]">
                                {`Decomposition: ${formatModeLabel(source.decomposition_mode)}`}
                              </span>
                              {source.has_custom_pipeline ? (
                                <span className="rounded-full border border-[var(--accent)]/18 bg-[rgba(154,52,18,0.08)] px-2.5 py-0.5 font-mono text-[10px] uppercase tracking-[0.16em] text-[var(--accent)]">
                                  {source.custom_pipeline_id ?? "Custom pipeline"}
                                </span>
                              ) : null}
                            </div>
                            {source.description ? (
                              <p className="mt-2.5 text-sm leading-6 text-[var(--muted)]">{source.description}</p>
                            ) : null}
                            {latestRun ? (
                              <p className="mt-2 text-xs leading-5 text-[var(--muted)]">
                                Latest emitted kinds: {formatEmittedKinds(latestRun.emitted_kinds)}
                              </p>
                            ) : null}
                          </div>
                          <SourceStatusChip status={latestRun?.status ?? null} />
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

                        {source.tags.length ? (
                          <div className="mt-3 rounded-[1.25rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.62)] px-3.5 py-3.5">
                            <p className="field-label">Saved tags</p>
                            <div className="mt-2 flex flex-wrap gap-2">
                              {source.tags.map((tag) => (
                                <span
                                  key={`${source.id}-${tag}`}
                                  className="rounded-full border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.74)] px-2.5 py-1 text-[11px] text-[var(--muted-strong)]"
                                >
                                  {tag}
                                </span>
                              ))}
                            </div>
                          </div>
                        ) : null}

                        <div className="mt-4 flex flex-wrap gap-2">
                          <button
                            className="secondary-button px-3 py-2 text-[11px]"
                            disabled={Boolean(probingSourceIds[source.id])}
                            onClick={() => handleProbeSource(source)}
                            type="button"
                          >
                            {probingSourceIds[source.id] ? "Probing..." : "Probe"}
                          </button>
                          <button
                            className="secondary-button px-3 py-2 text-[11px]"
                            onClick={() => openSourceEditor(source)}
                            type="button"
                          >
                            Edit
                          </button>
                          <button
                            className="secondary-button px-3 py-2 text-[11px]"
                            onClick={() => openSourceInPipelines(source.id)}
                            type="button"
                          >
                            <Workflow className="h-3.5 w-3.5" />
                            Open in pipeline
                          </button>
                          <button
                            className="secondary-button px-3 py-2 text-[11px]"
                            disabled={togglingSourceId === source.id}
                            onClick={() => handleToggleSource(source)}
                            type="button"
                          >
                            {togglingSourceId === source.id ? "Updating..." : source.active ? "Pause" : "Resume"}
                          </button>
                          <button
                            className="secondary-button px-3 py-2 text-[11px]"
                            disabled={removingSourceId === source.id}
                            onClick={() => handleRemoveSource(source)}
                            type="button"
                          >
                            {removingSourceId === source.id ? "Removing..." : "Delete"}
                          </button>
                        </div>

                        {probeReport ? (
                          <div
                            className={`mt-4 rounded-[1.25rem] border px-3.5 py-3.5 text-sm leading-6 ${
                              probeReport.tone === "error"
                                ? "border-[var(--danger)]/20 bg-[rgba(255,255,255,0.58)] text-[var(--danger)]"
                                : "border-[var(--ink)]/8 bg-[rgba(255,255,255,0.62)] text-[var(--muted-strong)]"
                            }`}
                          >
                            {probeReport.tone === "error" ? (
                              probeReport.message
                            ) : (
                              <>
                                <p>{probeReport.result.detail}</p>
                                {probeReport.result.sample_titles.length ? (
                                  <ul className="mt-3 list-disc space-y-1 pl-5 text-xs leading-5 text-[var(--muted)]">
                                    {probeReport.result.sample_titles.map((title) => (
                                      <li key={`${source.id}-${title}`}>{title}</li>
                                    ))}
                                  </ul>
                                ) : null}
                              </>
                            )}
                          </div>
                        ) : null}
                      </article>
                    );
                  })
                ) : (
                  <div className="rounded-3xl border border-dashed border-[var(--ink)]/12 bg-[rgba(255,255,255,0.42)] px-5 py-8 text-sm leading-7 text-[var(--muted)]">
                    No sources are configured yet. Use Add another feed to register the first source.
                  </div>
                )}
              </div>
            </section>
          </div>
        ) : (
          <div className="space-y-6">
            <section className="editorial-panel overflow-hidden">
              <div className="flex flex-col gap-4 xl:flex-row xl:items-end xl:justify-between">
                <div>
                  <p className="section-kicker">Operations runbook</p>
                  <h3 className="section-title">Run workers in order</h3>
                  <SkimmableText className="mt-4 max-w-4xl text-base leading-7 text-[var(--muted)]">
                    This page is laid out like an operator playbook instead of a dashboard. Use the full ingest chain for the normal refresh, or run the numbered steps below from top to bottom when you only need part of the pipeline.
                  </SkimmableText>
                </div>
                <div className="flex flex-wrap gap-2">
                  <span className="rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.56)] px-4 py-2 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">
                    {sourceSummary.active} active sources
                  </span>
                  <span className="rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.56)] px-4 py-2 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">
                    {displayRunningCount ? `${displayRunningCount} running now` : queuedOperationCount ? `${queuedOperationCount} queued now` : "No workers running"}
                  </span>
                </div>
              </div>

              {pipelineNotice ? (
                <div
                  className={`mt-5 rounded-2xl border px-4 py-3 text-sm leading-6 ${
                    pipelineNotice.tone === "error"
                      ? "border-[var(--danger)]/20 bg-[rgba(255,255,255,0.56)] text-[var(--danger)]"
                      : "border-[var(--ink)]/8 bg-[rgba(255,255,255,0.56)] text-[var(--muted-strong)]"
                  }`}
                >
                  {pipelineNotice.message}
                </div>
              ) : null}

              <div className="mt-6 space-y-5">
                <section
                  className={`rounded-[1.85rem] border px-5 py-5 ${
                    pipelineActionStates.full_ingest.status === "running"
                      ? "border-[rgba(14,77,100,0.18)] bg-[linear-gradient(180deg,rgba(14,77,100,0.08),rgba(255,255,255,0.56))]"
                      : pipelineActionStates.full_ingest.status === "queued"
                        ? "border-[var(--accent)]/18 bg-[linear-gradient(180deg,rgba(154,52,18,0.08),rgba(255,255,255,0.56))]"
                        : "border-[var(--accent)]/18 bg-[linear-gradient(180deg,rgba(154,52,18,0.08),rgba(255,255,255,0.56))]"
                  }`}
                >
                  <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
                    <div className="min-w-0">
                      <div className="flex items-start gap-3">
                        <div
                          className={`flex h-12 w-12 shrink-0 items-center justify-center rounded-[1.25rem] border bg-white/70 ${
                            pipelineActionStates.full_ingest.status === "running"
                              ? "border-[rgba(14,77,100,0.2)] text-[var(--teal)]"
                              : "border-[var(--accent)]/20 text-[var(--accent)]"
                          }`}
                        >
                          {pipelineActionStates.full_ingest.status === "running" ? (
                            <LoaderCircle className="h-5 w-5 animate-spin" />
                          ) : pipelineActionStates.full_ingest.status === "queued" ? (
                            <Clock3 className="h-5 w-5" />
                          ) : (
                            <Workflow className="h-5 w-5" />
                          )}
                        </div>
                        <div className="min-w-0">
                          <div className="flex flex-wrap items-center gap-2">
                            <p className={`section-kicker ${pipelineActionStates.full_ingest.status === "running" ? "text-[var(--teal)]" : "text-[var(--accent)]"}`}>
                              Recommended default
                            </p>
                            {pipelineActionStates.full_ingest.status !== "idle" ? (
                              <span
                                className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 font-mono text-[10px] uppercase tracking-[0.16em] ${
                                  pipelineActionStates.full_ingest.status === "running"
                                    ? "border-[rgba(14,77,100,0.18)] bg-[rgba(14,77,100,0.08)] text-[var(--teal)]"
                                    : "border-[var(--accent)]/18 bg-[rgba(154,52,18,0.08)] text-[var(--accent)]"
                                }`}
                              >
                                {pipelineActionStates.full_ingest.status === "running" ? <LoaderCircle className="h-3 w-3 animate-spin" /> : <Clock3 className="h-3 w-3" />}
                                <span>
                                  {pipelineActionStates.full_ingest.status === "running"
                                    ? "Running now"
                                    : `Queued #${pipelineActionStates.full_ingest.queuePosition}`}
                                </span>
                              </span>
                            ) : null}
                          </div>
                          <h4 className="mt-3 font-display text-3xl leading-tight text-[var(--ink)]">Full ingest chain</h4>
                          <SkimmableText className="mt-3 max-w-3xl text-sm leading-6 text-[var(--muted-strong)]">
                            Runs the normal default ingest path only: fetch sources, run lightweight enrichment for stale raw docs, rebuild the items index, and sync the vault once. Wiki compile, brief generation, audio, and publish remain explicit downstream actions.
                          </SkimmableText>
                        </div>
                      </div>

                      <div className="mt-5 flex flex-wrap items-center gap-2">
                        <span className="rounded-full border border-[var(--accent)]/16 bg-white/76 px-3 py-1 font-mono text-[11px] uppercase tracking-[0.18em] text-[var(--accent)]">
                          Fetch sources
                        </span>
                        <span className="font-mono text-[11px] uppercase tracking-[0.18em] text-[var(--muted)]">then</span>
                        <span className="rounded-full border border-[var(--accent)]/16 bg-white/76 px-3 py-1 font-mono text-[11px] uppercase tracking-[0.18em] text-[var(--accent)]">
                          Lightweight enrich
                        </span>
                        <span className="font-mono text-[11px] uppercase tracking-[0.18em] text-[var(--muted)]">then</span>
                        <span className="rounded-full border border-[var(--accent)]/16 bg-white/76 px-3 py-1 font-mono text-[11px] uppercase tracking-[0.18em] text-[var(--accent)]">
                          Rebuild index
                        </span>
                      </div>
                    </div>

                    <div
                      className={`rounded-[1.45rem] border bg-white/74 px-4 py-4 ${
                        pipelineActionStates.full_ingest.status === "running"
                          ? "border-[rgba(14,77,100,0.18)]"
                          : pipelineActionStates.full_ingest.status === "queued"
                            ? "border-[var(--accent)]/18"
                            : "border-[var(--accent)]/16"
                      }`}
                    >
                      <p className="field-label">Use this when</p>
                      <p className="mt-2 max-w-xs text-sm leading-6 text-[var(--muted-strong)]">
                        You want the standard vault refresh and do not need to stop between fetch, lightweight metadata, and indexing.
                      </p>
                      <div className="mt-4 flex flex-wrap gap-3">
                        <button
                          className="primary-button"
                          disabled={pipelineActionStates.full_ingest.status !== "idle"}
                          onClick={() => queuePipelineAction({ kind: "full_ingest", label: "Full ingest chain" })}
                          type="button"
                        >
                          {pipelineActionStates.full_ingest.status === "running" ? (
                            <>
                              <LoaderCircle className="h-3.5 w-3.5 animate-spin" />
                              Running full ingest...
                            </>
                          ) : pipelineActionStates.full_ingest.status === "queued" ? (
                            <>
                              <Clock3 className="h-3.5 w-3.5" />
                              {`Queued #${pipelineActionStates.full_ingest.queuePosition}`}
                            </>
                          ) : (
                            "Run full ingest"
                          )}
                        </button>
                        {buildPipelineLogButton("full_ingest").logsLabel ? (
                          <button
                            className="secondary-button"
                            disabled={buildPipelineLogButton("full_ingest").logsDisabled}
                            onClick={buildPipelineLogButton("full_ingest").onOpenLogs}
                            type="button"
                          >
                            <History className="h-3.5 w-3.5" />
                            {buildPipelineLogButton("full_ingest").logsLabel}
                          </button>
                        ) : null}
                      </div>
                    </div>
                  </div>
                </section>

                <PipelineStage
                  description="These are the first three stages of the vault-native ingest model. Fetch is deterministic only, lightweight enrichment is the one-time Ollama pass, and indexing only materializes helper indexes from durable files."
                  step="1"
                  title="Refresh raw documents"
                >
                  <PipelineActionRow
                    actionLabel="Fetch sources"
                    busyLabel="Fetching sources..."
                    description="Fetch every enabled website and newsletter source into the raw vault, normalize the canonical text, and perform deterministic newsletter decomposition only."
                    disabled={pipelineActionStates.fetch_sources.status !== "idle"}
                    icon={<Mail className="h-4 w-4" />}
                    logsDisabled={buildPipelineLogButton("fetch_sources").logsDisabled}
                    logsLabel={buildPipelineLogButton("fetch_sources").logsLabel}
                    onClick={() => queuePipelineAction({ kind: "fetch_sources", label: "Fetch sources" })}
                    onOpenLogs={buildPipelineLogButton("fetch_sources").onOpenLogs}
                    queuePosition={pipelineActionStates.fetch_sources.queuePosition}
                    state={pipelineActionStates.fetch_sources.status}
                    title="Fetch sources"
                    updates="Raw source folders, source.md files, and derived newsletter child docs."
                    useWhen="New emails, website posts, or paper listings should be discovered without running any LLM or rebuilding the index yet."
                  />
                  <PipelineActionRow
                    actionLabel={lightweightPendingLabel}
                    busyLabel="Running Ollama..."
                    description={lightweightPendingDescription}
                    disabled={pipelineActionStates.lightweight_enrich.status !== "idle"}
                    icon={<Sparkles className="h-4 w-4" />}
                    logsDisabled={buildPipelineLogButton("lightweight_enrich").logsDisabled}
                    logsLabel={buildPipelineLogButton("lightweight_enrich").logsLabel}
                    onClick={() => queuePipelineAction({ kind: "lightweight_enrich", label: "Lightweight enrichment" })}
                    onOpenLogs={buildPipelineLogButton("lightweight_enrich").onOpenLogs}
                    queuePosition={pipelineActionStates.lightweight_enrich.queuePosition}
                    state={pipelineActionStates.lightweight_enrich.status}
                    title="Lightweight enrichment"
                    updates="Only frontmatter metadata such as authors, tags, short_summary, and enrichment status."
                    useWhen="Fetch completed and you want the lightweight summary metadata to be available before indexing, briefing, or browsing."
                  />
                  <PipelineActionRow
                    actionLabel="Rebuild items index"
                    badge={itemsIndexBadge}
                    busyLabel="Indexing..."
                    description={itemsIndexDescription}
                    disabled={pipelineActionStates.rebuild_index.status !== "idle"}
                    icon={<Database className="h-4 w-4" />}
                    logsDisabled={buildPipelineLogButton("rebuild_index").logsDisabled}
                    logsLabel={buildPipelineLogButton("rebuild_index").logsLabel}
                    onClick={() => queuePipelineAction({ kind: "rebuild_index", label: "Rebuild items index" })}
                    onOpenLogs={buildPipelineLogButton("rebuild_index").onOpenLogs}
                    queuePosition={pipelineActionStates.rebuild_index.queuePosition}
                    state={pipelineActionStates.rebuild_index.status}
                    tone={itemsIndexTone}
                    title="Rebuild items index"
                    updates="The materialized helper index only. This stage should not invent any metadata."
                    useWhen="Raw source files or lightweight frontmatter changed and you need those changes reflected in the frontend and downstream generators."
                  />
                </PipelineStage>

                <PipelineStage
                  description="These jobs use the current index to rebuild operator-facing outputs. The edition selector appears only on the actions that render a specific edition."
                  step="2"
                  title="Build outputs"
                >
                  <PipelineActionRow
                    actionLabel="Compile wiki with Codex"
                    busyLabel="Compiling wiki..."
                    controls={(
                      <label>
                        <span className="field-label">Compile batch size</span>
                        <input
                          className="field-input"
                          min={1}
                          onChange={(event) => setAdvancedCompileLimit(event.target.value)}
                          placeholder="8"
                          type="number"
                          value={advancedCompileLimit}
                        />
                        <p className="mt-2 text-xs leading-5 text-[var(--muted)]">
                          Codex will inspect up to this many stale raw docs in one compile pass.
                        </p>
                      </label>
                    )}
                    description="Use Codex to incrementally maintain the wiki from changed raw documents, then rebuild the deterministic wiki indexes."
                    disabled={pipelineActionStates.compile_wiki.status !== "idle" || !advancedCompileLimit}
                    icon={<BookOpen className="h-4 w-4" />}
                    logsDisabled={buildPipelineLogButton("compile_wiki").logsDisabled}
                    logsLabel={buildPipelineLogButton("compile_wiki").logsLabel}
                    onClick={() => queuePipelineAction({ kind: "compile_wiki", label: "Compile wiki with Codex", limit: Number(advancedCompileLimit) || 8 })}
                    onOpenLogs={buildPipelineLogButton("compile_wiki").onOpenLogs}
                    queuePosition={pipelineActionStates.compile_wiki.queuePosition}
                    state={pipelineActionStates.compile_wiki.status}
                    title="Compile wiki with Codex"
                    updates="Wiki markdown plus rebuilt pages and graph indexes."
                    useWhen="Fresh raw docs are ready and you want the Obsidian knowledge base itself updated."
                  />
                  <PipelineActionRow
                    actionLabel="Regenerate brief"
                    busyLabel="Regenerating brief..."
                    controls={
                      <EditionTargetField
                        disabled={!regenerateOptions.length}
                        helperText="Choose which written edition to rebuild from the current index."
                        onChange={setRegenerateBriefDate}
                        options={regenerateOptions}
                        value={activeEditionTarget}
                      />
                    }
                    description="Rebuild the written edition from the current index without rerunning the entire ingest chain."
                    disabled={pipelineActionStates.regenerate_brief.status !== "idle" || !activeEditionTarget}
                    icon={<RefreshCw className="h-4 w-4" />}
                    logsDisabled={buildPipelineLogButton("regenerate_brief").logsDisabled}
                    logsLabel={buildPipelineLogButton("regenerate_brief").logsLabel}
                    onClick={() => queuePipelineAction({ kind: "regenerate_brief", label: "Regenerate edition brief", briefDate: activeEditionTarget })}
                    onOpenLogs={buildPipelineLogButton("regenerate_brief").onOpenLogs}
                    queuePosition={pipelineActionStates.regenerate_brief.queuePosition}
                    state={pipelineActionStates.regenerate_brief.status}
                    tone={currentEditionOutputState.brief ? "success" : "default"}
                    title="Regenerate edition brief"
                    updates="The written brief for one edition."
                    useWhen="Ranking, summaries, or included items changed and the written edition needs a clean rebuild."
                  />
                  <PipelineActionRow
                    actionLabel="Generate audio"
                    busyLabel="Generating audio..."
                    controls={
                      <EditionTargetField
                        disabled={!regenerateOptions.length}
                        helperText="Choose which edition should get a refreshed audio brief."
                        onChange={setRegenerateBriefDate}
                        options={regenerateOptions}
                        value={activeEditionTarget}
                      />
                    }
                    description="Create or refresh the audio brief for the selected edition."
                    disabled={pipelineActionStates.generate_audio.status !== "idle" || !activeEditionTarget}
                    icon={<Mic2 className="h-4 w-4" />}
                    logsDisabled={buildPipelineLogButton("generate_audio").logsDisabled}
                    logsLabel={buildPipelineLogButton("generate_audio").logsLabel}
                    onClick={() => queuePipelineAction({ kind: "generate_audio", label: "Generate audio", briefDate: activeEditionTarget })}
                    onOpenLogs={buildPipelineLogButton("generate_audio").onOpenLogs}
                    queuePosition={pipelineActionStates.generate_audio.queuePosition}
                    state={pipelineActionStates.generate_audio.status}
                    tone={currentEditionOutputState.audio ? "success" : "default"}
                    title="Generate audio"
                    updates="Audio script and rendered audio for one edition."
                    useWhen="The written brief is ready and you want the narration or audio artifact refreshed."
                  />
                  <PipelineActionRow
                    actionLabel="Publish viewer"
                    busyLabel="Publishing viewer..."
                    controls={
                      <EditionTargetField
                        disabled={!regenerateOptions.length}
                        helperText="Choose which edition should be rendered into the published viewer bundle."
                        onChange={setRegenerateBriefDate}
                        options={regenerateOptions}
                        value={activeEditionTarget}
                      />
                    }
                    description="Render the published viewer artifacts for the selected edition and refresh the latest viewer bundle."
                    disabled={pipelineActionStates.publish_viewer.status !== "idle" || !activeEditionTarget}
                    icon={<Upload className="h-4 w-4" />}
                    logsDisabled={buildPipelineLogButton("publish_viewer").logsDisabled}
                    logsLabel={buildPipelineLogButton("publish_viewer").logsLabel}
                    onClick={() => queuePipelineAction({ kind: "publish_viewer", label: "Publish viewer", briefDate: activeEditionTarget })}
                    onOpenLogs={buildPipelineLogButton("publish_viewer").onOpenLogs}
                    queuePosition={pipelineActionStates.publish_viewer.queuePosition}
                    state={pipelineActionStates.publish_viewer.status}
                    tone={currentEditionOutputState.viewer ? "success" : "default"}
                    title="Publish viewer"
                    updates="Viewer artifacts and the latest published bundle."
                    useWhen="You want the web viewer to reflect the current edition output."
                  />
                </PipelineStage>

                <PipelineStage
                  description="These are explicit downstream Codex jobs. They operate on the vault directly, stay vault-first, and do not run as part of default ingest."
                  step="3"
                  title="Advanced enrichment"
                >
                  <div className="rounded-[1.3rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.56)] px-4 py-4">
                    <p className="field-label">Codex runtime</p>
                    <p className="mt-2 text-sm leading-6 text-[var(--muted-strong)]">
                      {advancedRuntimeQuery.data
                        ? advancedRuntimeQuery.data.available
                          ? advancedRuntimeQuery.data.authenticated
                            ? `Codex is ready with ${advancedRuntimeQuery.data.model ?? "the configured model"}${advancedRuntimeQuery.data.profile ? ` using profile ${advancedRuntimeQuery.data.profile}` : ""}. Web search is ${advancedRuntimeQuery.data.search_enabled ? "enabled" : "disabled"}, timeout is ${advancedRuntimeQuery.data.timeout_minutes ?? "?"} minutes, and the default compile batch size is ${advancedRuntimeQuery.data.compile_batch_size ?? "?"}.`
                            : advancedRuntimeQuery.data.detail ?? "Codex is installed but not authenticated."
                          : advancedRuntimeQuery.data.detail ?? "Codex CLI is unavailable."
                        : "Loading Codex runtime status..."}
                    </p>
                    <p className="mt-2 text-xs leading-5 text-[var(--muted)]">
                      Durable outputs land in dated folders under `outputs/answers/`, `outputs/slides/`, and `outputs/charts/`.
                    </p>
                  </div>
                  <PipelineActionRow
                    actionLabel="Run health check"
                    busyLabel="Running health check..."
                    controls={(
                      <div className="space-y-3">
                        <label>
                          <span className="field-label">Scope</span>
                          <select className="field-input" onChange={(event) => setHealthCheckScope(event.target.value as HealthCheckScope)} value={healthCheckScope}>
                            <option value="vault">Vault</option>
                            <option value="wiki">Wiki</option>
                            <option value="raw">Raw</option>
                          </select>
                        </label>
                        <label>
                          <span className="field-label">Topic</span>
                          <input
                            className="field-input"
                            onChange={(event) => setHealthCheckTopic(event.target.value)}
                            placeholder="optional focus like evals or agent memory"
                            type="text"
                            value={healthCheckTopic}
                          />
                        </label>
                      </div>
                    )}
                    description="Ask Codex to inspect the vault for inconsistent facts, missing metadata, weak links, stale pages, duplicate concepts, and article candidates."
                    disabled={pipelineActionStates.health_check.status !== "idle"}
                    icon={<History className="h-4 w-4" />}
                    logsDisabled={buildPipelineLogButton("health_check").logsDisabled}
                    logsLabel={buildPipelineLogButton("health_check").logsLabel}
                    onClick={() => queuePipelineAction({ kind: "health_check", label: "Run health check", scope: healthCheckScope, topic: healthCheckTopic.trim() || undefined })}
                    onOpenLogs={buildPipelineLogButton("health_check").onOpenLogs}
                    queuePosition={pipelineActionStates.health_check.queuePosition}
                    state={pipelineActionStates.health_check.status}
                    title="Run health check"
                    updates="A durable report under `outputs/health-checks/`."
                    useWhen="You want a report-first integrity pass without silently changing the wiki."
                  />
                  <PipelineActionRow
                    actionLabel="Ask Codex"
                    busyLabel="Generating output..."
                    controls={(
                      <div className="space-y-3">
                        <label>
                          <span className="field-label">Question</span>
                          <textarea
                            className="field-input min-h-28"
                            onChange={(event) => setAnswerQuestion(event.target.value)}
                            placeholder="What changed recently around eval agents, and what should be filed into the wiki?"
                            value={answerQuestion}
                          />
                        </label>
                        <label>
                          <span className="field-label">Output kind</span>
                          <select className="field-input" onChange={(event) => setAnswerOutputKind(event.target.value as AdvancedOutputKind)} value={answerOutputKind}>
                            <option value="answer">Answer</option>
                            <option value="slides">Slides</option>
                            <option value="chart">Chart bundle</option>
                          </select>
                        </label>
                      </div>
                    )}
                    description="Run a vault-first Codex Q&A job and persist the result as a durable artifact instead of leaving it only in chat."
                    disabled={pipelineActionStates.answer_query.status !== "idle" || !answerQuestion.trim()}
                    icon={<Bot className="h-4 w-4" />}
                    logsDisabled={buildPipelineLogButton("answer_query").logsDisabled}
                    logsLabel={buildPipelineLogButton("answer_query").logsLabel}
                    onClick={() => queuePipelineAction({ kind: "answer_query", label: "Ask Codex", question: answerQuestion.trim(), outputKind: answerOutputKind })}
                    onOpenLogs={buildPipelineLogButton("answer_query").onOpenLogs}
                    queuePosition={pipelineActionStates.answer_query.queuePosition}
                    state={pipelineActionStates.answer_query.status}
                    title="Ask Codex"
                    updates="An artifact under `outputs/answers/`, `outputs/slides/`, or `outputs/charts/`."
                    useWhen="You want a durable report, deck, or chart bundle grounded in the vault."
                  />
                  <PipelineActionRow
                    actionLabel="File output"
                    busyLabel="Filing into wiki..."
                    controls={(
                      <label>
                        <span className="field-label">Vault-relative output path</span>
                        <input
                          className="field-input"
                          onChange={(event) => setFileOutputPath(event.target.value)}
                          placeholder="outputs/answers/2026-04-07/agent-evals-report.md"
                          type="text"
                          value={fileOutputPath}
                        />
                        <p className="mt-2 text-xs leading-5 text-[var(--muted)]">
                          Point this at an existing artifact inside the vault that Codex should distill back into `wiki/**`.
                        </p>
                      </label>
                    )}
                    description="Take a prior durable output and explicitly file its reusable insight back into the wiki with backlinks to the source artifact."
                    disabled={pipelineActionStates.file_output.status !== "idle" || !fileOutputPath.trim()}
                    icon={<Upload className="h-4 w-4" />}
                    logsDisabled={buildPipelineLogButton("file_output").logsDisabled}
                    logsLabel={buildPipelineLogButton("file_output").logsLabel}
                    onClick={() => queuePipelineAction({ kind: "file_output", label: "File output into wiki", path: fileOutputPath.trim() })}
                    onOpenLogs={buildPipelineLogButton("file_output").onOpenLogs}
                    queuePosition={pipelineActionStates.file_output.queuePosition}
                    state={pipelineActionStates.file_output.status}
                    title="File output into wiki"
                    updates="Selected `wiki/**` pages plus rebuilt pages and graph indexes."
                    useWhen="A generated output contains durable knowledge that should compound in the vault."
                  />
                </PipelineStage>
              </div>
            </section>

            <section className="editorial-panel overflow-hidden">
              <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
                <div>
                  <p className="section-kicker">Source-level runs</p>
                  <h3 className="section-title">Inject one source at a time</h3>
                  <SkimmableText className="mt-4 max-w-4xl text-base leading-7 text-[var(--muted)]">
                    This bypasses the broader pipeline and reruns only one configured source. Use it to validate a new source, retry a single feed or newsletter, or jump straight into the latest extraction log.
                  </SkimmableText>
                </div>
                <div className="flex flex-wrap gap-2">
                  <span className="rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.56)] px-4 py-2 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">
                    {sourcesQuery.data?.length ?? 0} configured
                  </span>
                  <span className="rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.56)] px-4 py-2 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">
                    {sourceSummary.active} ready now
                  </span>
                </div>
              </div>

              {sourcesQuery.data?.length ? (
                <div className="mt-6 overflow-x-auto rounded-[1.9rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.42)]">
                  <table className="min-w-full border-collapse">
                    <thead>
                      <tr className="border-b border-[var(--ink)]/8 text-left">
                        <th className="px-4 py-3 font-mono text-[11px] uppercase tracking-[0.2em] text-[var(--muted)]">Source</th>
                        <th className="px-4 py-3 font-mono text-[11px] uppercase tracking-[0.2em] text-[var(--muted)]">Locator</th>
                        <th className="px-4 py-3 font-mono text-[11px] uppercase tracking-[0.2em] text-[var(--muted)]">Latest extraction</th>
                        <th className="px-4 py-3 font-mono text-[11px] uppercase tracking-[0.2em] text-[var(--muted)]">Actions</th>
                      </tr>
                    </thead>
                    <tbody>
                      {sourcesQuery.data.map((source) => {
                        const locatorEntries = getSourceLocatorEntries(source);
                        const latestRun = source.latest_extraction_run;
                        const isFocusedSource = focusedPipelineSourceId === source.id;

                        return (
                          <tr
                            key={`inject-${source.id}`}
                            className={`border-b border-[var(--ink)]/6 align-top last:border-b-0 ${
                              isFocusedSource ? "bg-[rgba(154,52,18,0.06)]" : ""
                            }`}
                          >
                            <td className="px-4 py-4">
                              <div className="flex flex-wrap items-center gap-2">
                                <p className="text-sm font-medium text-[var(--ink)]">{source.name}</p>
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
                                {source.has_custom_pipeline ? (
                                  <span className="rounded-full border border-[var(--accent)]/18 bg-[rgba(154,52,18,0.08)] px-2.5 py-0.5 font-mono text-[10px] uppercase tracking-[0.16em] text-[var(--accent)]">
                                    {source.custom_pipeline_id ?? "Custom pipeline"}
                                  </span>
                                ) : null}
                                {isFocusedSource ? (
                                  <span className="rounded-full border border-[var(--accent)]/18 bg-[rgba(154,52,18,0.08)] px-2.5 py-0.5 font-mono text-[10px] uppercase tracking-[0.16em] text-[var(--accent)]">
                                    Selected
                                  </span>
                                ) : null}
                              </div>
                              {source.description ? (
                                <p className="mt-2 max-w-md text-sm leading-6 text-[var(--muted)]">{source.description}</p>
                              ) : (
                                <p className="mt-2 text-sm leading-6 text-[var(--muted)]">No description added.</p>
                              )}
                            </td>
                            <td className="px-4 py-4">
                              {locatorEntries.length ? (
                                <div className="space-y-3">
                                  {locatorEntries.map((entry) => (
                                    <div key={`${source.id}-inject-${entry.label}`}>
                                      <p className="field-label">{entry.label}</p>
                                      <p className="mt-1.5 break-all font-mono text-[11px] leading-5 text-[var(--muted-strong)]">{entry.value}</p>
                                    </div>
                                  ))}
                                </div>
                              ) : (
                                <p className="text-sm leading-6 text-[var(--muted)]">No locator metadata recorded.</p>
                              )}
                            </td>
                            <td className="px-4 py-4">
                              <div className="space-y-2">
                                <SourceStatusChip status={latestRun?.status ?? null} />
                                {latestRun ? (
                                  <>
                                    <p className="text-sm leading-6 text-[var(--muted-strong)]">{latestRun.summary}</p>
                                    <p className="text-xs leading-5 text-[var(--muted)]">
                                      {formatDateTimeLabel(latestRun.started_at)}
                                      {latestRun.finished_at ? ` · ${formatRunDuration(latestRun.started_at, latestRun.finished_at)}` : ""}
                                    </p>
                                    <p className="text-xs leading-5 text-[var(--muted)]">
                                      Emitted kinds: {formatEmittedKinds(latestRun.emitted_kinds)}
                                    </p>
                                  </>
                                ) : (
                                  <p className="text-sm leading-6 text-[var(--muted)]">No extraction has been recorded for this source yet.</p>
                                )}
                              </div>
                            </td>
                            <td className="px-4 py-4">
                              <div className="flex min-w-[180px] flex-col items-start gap-2">
                                <label className="w-full rounded-[1.2rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.62)] px-3 py-3">
                                  <span className="field-label">Docs this inject</span>
                                  <input
                                    className="field-input mt-2"
                                    inputMode="numeric"
                                    max={SOURCE_INJECT_MAX_ITEMS_LIMIT}
                                    min={1}
                                    onChange={(event) => handleSourceInjectMaxItemsChange(source.id, event.target.value)}
                                    type="number"
                                    value={sourceInjectMaxItemsValue(source)}
                                  />
                                  <span className="mt-2 block text-xs leading-5 text-[var(--muted)]">
                                    Default {source.max_items} from source settings. Raise this for longer backfills.
                                  </span>
                                </label>
                                <button
                                  className="secondary-button"
                                  disabled={injectingSourceId === source.id}
                                  onClick={() => handleInjectSource(source)}
                                  type="button"
                                >
                                  <Play className="h-3.5 w-3.5" />
                                  {injectingSourceId === source.id ? "Injecting..." : "Inject source"}
                                </button>
                                <button
                                  className="secondary-button"
                                  disabled={injectingSourceId === source.id || openingSourceLogId === source.id || !latestRun}
                                  onClick={() => handleOpenLatestLog(source)}
                                  type="button"
                                >
                                  <History className="h-3.5 w-3.5" />
                                  {openingSourceLogId === source.id ? "Opening..." : "Open latest run"}
                                </button>
                              </div>
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className="mt-6 rounded-3xl border border-dashed border-[var(--ink)]/12 bg-[rgba(255,255,255,0.42)] px-5 py-8 text-sm leading-7 text-[var(--muted)]">
                  No sources are configured yet. Add one on the Connections page before using per-source inject.
                </div>
              )}
            </section>

            <section className="editorial-panel overflow-hidden">
              <div className="flex flex-col gap-4 xl:flex-row xl:items-end xl:justify-between">
                <div>
                  <p className="section-kicker">Operational history</p>
                  <h3 className="section-title">Recent job runs</h3>
                  <SkimmableText className="mt-4 max-w-4xl text-base leading-7 text-[var(--muted)]">
                    Every button above records a run here. Open any row to inspect the detailed log, associated edition coverage, and the exact messages produced during the job.
                  </SkimmableText>
                </div>
                <div className="flex flex-wrap gap-2">
                  <span className="rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.56)] px-4 py-2 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">
                    {String(recentOperationCount).padStart(2, "0")} runs
                  </span>
                  <span className="rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.56)] px-4 py-2 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">
                    {displayRunningCount ? `${displayRunningCount} running` : "Idle"}
                  </span>
                  <span className="rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.56)] px-4 py-2 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">
                    {queuedOperationCount ? `${queuedOperationCount} queued` : "No queued jobs"}
                  </span>
                </div>
              </div>

              {ingestionRunsQuery.isLoading && !ingestionRunsQuery.data && !recentJobs.activeRow && !recentJobs.queuedRows.length ? (
                <div className="mt-6 rounded-3xl border border-dashed border-[var(--ink)]/12 bg-[rgba(255,255,255,0.42)] px-5 py-8 text-sm leading-7 text-[var(--muted)]">
                  Loading operation history…
                </div>
              ) : ingestionRunsQuery.data?.length || recentJobs.activeRow || recentJobs.queuedRows.length ? (
                <div className="mt-6 overflow-x-auto rounded-3xl border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.42)]">
                  <table className="min-w-full border-collapse">
                    <thead>
                      <tr className="border-b border-[var(--ink)]/8 text-left">
                        <th className="px-4 py-3 font-mono text-[11px] uppercase tracking-[0.2em] text-[var(--muted)]">Executed</th>
                        <th className="px-4 py-3 font-mono text-[11px] uppercase tracking-[0.2em] text-[var(--muted)]">Operation</th>
                        <th className="px-4 py-3 font-mono text-[11px] uppercase tracking-[0.2em] text-[var(--muted)]">Edition / coverage</th>
                        <th className="px-4 py-3 font-mono text-[11px] uppercase tracking-[0.2em] text-[var(--muted)]">Cost</th>
                        <th className="px-4 py-3 font-mono text-[11px] uppercase tracking-[0.2em] text-[var(--muted)]">Status</th>
                      </tr>
                    </thead>
                    <tbody>
                      {recentJobs.activeRow ? (
                        <tr key={recentJobs.activeRow.id} className="border-b border-[var(--ink)]/6 align-top bg-[rgba(14,77,100,0.04)]">
                          <td className="px-4 py-4 text-sm leading-6 text-[var(--muted-strong)]">{formatDateTimeLabel(recentJobs.activeRow.startedAt)}</td>
                          <td className="px-4 py-4">
                            <p className="text-sm font-medium text-[var(--ink)]">{recentJobs.activeRow.title}</p>
                            <p className="mt-1 text-xs leading-5 text-[var(--muted)]">Starting locally</p>
                            <p className="mt-2 text-xs leading-5 text-[var(--muted)]">{recentJobs.activeRow.summary}</p>
                          </td>
                          <td className="px-4 py-4">
                            {recentJobs.activeRow.associatedDates ? (
                              <>
                                <p className="text-sm leading-6 text-[var(--muted-strong)]">Edition {recentJobs.activeRow.associatedDates.edition}</p>
                                <p className="mt-1 text-xs leading-5 text-[var(--muted)]">Coverage {recentJobs.activeRow.associatedDates.coverage}</p>
                              </>
                            ) : (
                              <p className="text-sm leading-6 text-[var(--muted)]">General / no edition range</p>
                            )}
                          </td>
                          <td className="px-4 py-4">
                            <p className="text-sm leading-6 text-[var(--muted-strong)]">{formatUsdCost(recentJobs.activeRow.totalCostUsd)}</p>
                            <p className="mt-1 text-xs leading-5 text-[var(--muted)]">
                              LLM {formatUsdCost(recentJobs.activeRow.aiCostUsd)} · TTS {formatUsdCost(recentJobs.activeRow.ttsCostUsd)}
                            </p>
                          </td>
                          <td className="px-4 py-4">
                            <SourceStatusChip status={recentJobs.activeRow.status} />
                          </td>
                        </tr>
                      ) : null}
                      {(ingestionRunsQuery.data ?? []).map((run) => {
                        const associatedDates = describeAssociatedDates(run);
                        return (
                          <tr
                            key={run.id}
                            className="cursor-pointer border-b border-[var(--ink)]/6 transition hover:bg-[rgba(255,255,255,0.52)]"
                            onClick={() => navigateToPipeline({ runId: run.id }, { replace: true })}
                            onKeyDown={(event) => {
                              if (event.key === "Enter" || event.key === " ") {
                                event.preventDefault();
                                navigateToPipeline({ runId: run.id }, { replace: true });
                              }
                            }}
                            tabIndex={0}
                          >
                            <td className="px-4 py-4 text-sm leading-6 text-[var(--muted-strong)]">{formatDateTimeLabel(run.started_at)}</td>
                            <td className="px-4 py-4">
                              <p className="text-sm font-medium text-[var(--ink)]">{run.title}</p>
                              <p className="mt-1 text-xs leading-5 text-[var(--muted)]">
                                {run.trigger ? `Trigger: ${run.trigger.replace(/_/g, " ")}` : "Recorded operation"}
                              </p>
                            </td>
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
                              <p className="text-sm leading-6 text-[var(--muted-strong)]">{formatUsdCost(run.total_cost_usd)}</p>
                              <p className="mt-1 text-xs leading-5 text-[var(--muted)]">
                                LLM {formatUsdCost(run.ai_cost_usd)} · TTS {formatUsdCost(run.tts_cost_usd)}
                              </p>
                            </td>
                            <td className="px-4 py-4">
                              <SourceStatusChip status={run.status} />
                            </td>
                          </tr>
                        );
                      })}
                      {recentJobs.queuedRows.map((row) => (
                        <tr key={row.id} className="border-b border-[var(--ink)]/6 align-top bg-[rgba(255,255,255,0.3)] last:border-b-0">
                          <td className="px-4 py-4 text-sm leading-6 text-[var(--muted-strong)]">{formatDateTimeLabel(row.startedAt)}</td>
                          <td className="px-4 py-4">
                            <p className="text-sm font-medium text-[var(--ink)]">{row.title}</p>
                            <p className="mt-1 text-xs leading-5 text-[var(--muted)]">
                              {row.trigger === "local_queue" ? "Queued locally" : "Starting locally"}
                            </p>
                            <p className="mt-2 text-xs leading-5 text-[var(--muted)]">{row.summary}</p>
                          </td>
                          <td className="px-4 py-4">
                            {row.associatedDates ? (
                              <>
                                <p className="text-sm leading-6 text-[var(--muted-strong)]">Edition {row.associatedDates.edition}</p>
                                <p className="mt-1 text-xs leading-5 text-[var(--muted)]">Coverage {row.associatedDates.coverage}</p>
                              </>
                            ) : (
                              <p className="text-sm leading-6 text-[var(--muted)]">General / no edition range</p>
                            )}
                          </td>
                          <td className="px-4 py-4">
                            <p className="text-sm leading-6 text-[var(--muted-strong)]">{formatUsdCost(row.totalCostUsd)}</p>
                            <p className="mt-1 text-xs leading-5 text-[var(--muted)]">
                              LLM {formatUsdCost(row.aiCostUsd)} · TTS {formatUsdCost(row.ttsCostUsd)}
                            </p>
                          </td>
                          <td className="px-4 py-4">
                            <SourceStatusChip status={row.status} />
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className="mt-6 rounded-3xl border border-dashed border-[var(--ink)]/12 bg-[rgba(255,255,255,0.42)] px-5 py-8 text-sm leading-7 text-[var(--muted)]">
                  No runs yet. Launch a job above and it will appear here.
                </div>
              )}
            </section>
          </div>
        )}
      </div>

      {isSourceEditorOpen ? (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-[rgba(17,19,18,0.38)] px-4 py-6 backdrop-blur-sm"
          onClick={resetSourceEditor}
        >
          <div
            aria-labelledby="source-editor-title"
            aria-modal="true"
            className="max-h-[90vh] w-full max-w-4xl overflow-hidden rounded-[2rem] border border-[var(--ink)]/10 bg-[rgba(247,240,224,0.96)] shadow-[0_32px_80px_rgba(17,19,18,0.22)]"
            onClick={(event) => event.stopPropagation()}
            role="dialog"
          >
            <div className="flex flex-wrap items-start justify-between gap-4 border-b border-[var(--ink)]/8 px-6 py-5">
              <div>
                <p className="section-kicker">{editingSourceId ? "Editing source" : "New source"}</p>
                <h4 className="section-title text-3xl" id="source-editor-title">
                  {editingSourceId ? "Adjust registry settings" : "Add another feed"}
                </h4>
                <p className="mt-3 text-sm leading-6 text-[var(--muted)]">
                  {editingSourceId
                    ? "Update the registry settings for this feed here without crowding the source list."
                    : "Open the source form when you need it, then return to a cleaner registry view."}
                </p>
              </div>
              <button className="secondary-button" disabled={sourceMutationBusy} onClick={resetSourceEditor} type="button">
                Close
              </button>
            </div>

            <div className="max-h-[calc(90vh-112px)] overflow-y-auto px-6 py-6">
              {sourceNotice ? (
                <div
                  className={`rounded-2xl border px-4 py-3 text-sm leading-6 ${
                    sourceNotice.tone === "error"
                      ? "border-[var(--danger)]/20 bg-[rgba(255,255,255,0.56)] text-[var(--danger)]"
                      : "border-[var(--ink)]/8 bg-[rgba(255,255,255,0.56)] text-[var(--muted-strong)]"
                  }`}
                >
                  {sourceNotice.message}
                </div>
              ) : null}

              <form className={`${sourceNotice ? "mt-5 " : ""}space-y-5`} onSubmit={handleSourceSubmit}>
                <div className="grid gap-3 sm:grid-cols-2">
                  <button
                    className={`rounded-[1.25rem] border px-4 py-3 text-left transition ${
                      sourceForm.type === "website"
                        ? "border-[var(--accent)]/28 bg-[rgba(154,52,18,0.08)]"
                        : "border-[var(--ink)]/8 bg-[rgba(255,255,255,0.58)]"
                    }`}
                    onClick={() => handleSourceTypeChange("website")}
                    type="button"
                  >
                    <p className="font-mono text-[11px] uppercase tracking-[0.16em] text-[var(--muted-strong)]">Website</p>
                    <p className="mt-2 text-sm leading-6 text-[var(--muted)]">RSS feeds or index pages from official sites and blogs.</p>
                  </button>
                  <button
                    className={`rounded-[1.25rem] border px-4 py-3 text-left transition ${
                      sourceForm.type === "gmail_newsletter"
                        ? "border-[var(--accent)]/28 bg-[rgba(154,52,18,0.08)]"
                        : "border-[var(--ink)]/8 bg-[rgba(255,255,255,0.58)]"
                    }`}
                    onClick={() => handleSourceTypeChange("gmail_newsletter")}
                    type="button"
                  >
                    <p className="font-mono text-[11px] uppercase tracking-[0.16em] text-[var(--muted-strong)]">Gmail newsletter</p>
                    <p className="mt-2 text-sm leading-6 text-[var(--muted)]">Sender-filtered inbox reads or advanced Gmail query rules.</p>
                  </button>
                </div>

                <div className="grid gap-4 sm:grid-cols-2">
                  <label>
                    <span className="field-label">Name</span>
                    <input
                      className="field-input"
                      onChange={(event) => setSourceForm((current) => ({ ...current, name: event.target.value }))}
                      placeholder={sourceForm.type === "gmail_newsletter" ? "TLDR AI" : "OpenAI News"}
                      value={sourceForm.name}
                    />
                  </label>
                  <label>
                    <span className="field-label">Max items per run</span>
                    <input
                      className="field-input"
                      inputMode="numeric"
                      onChange={(event) => setSourceForm((current) => ({ ...current, maxItems: event.target.value }))}
                      placeholder="20"
                      value={sourceForm.maxItems}
                    />
                  </label>
                  <label>
                    <span className="field-label">Raw document kind</span>
                    <select
                      className="field-input"
                      disabled={sourceForm.type === "gmail_newsletter"}
                      onChange={(event) => setSourceForm((current) => ({ ...current, rawKind: event.target.value as SourceRawKind }))}
                      value={sourceForm.type === "gmail_newsletter" ? "newsletter" : sourceForm.rawKind}
                    >
                      {sourceForm.type === "gmail_newsletter" ? (
                        <option value="newsletter">Newsletter</option>
                      ) : (
                        <>
                          <option value="blog-post">Blog post</option>
                          <option value="article">Article</option>
                          <option value="news">News</option>
                          <option value="paper">Paper</option>
                          <option value="thread">Thread</option>
                          <option value="signal">Signal</option>
                        </>
                      )}
                    </select>
                  </label>
                  <label>
                    <span className="field-label">Saved tags</span>
                    <input
                      className="field-input"
                      onChange={(event) => setSourceForm((current) => ({ ...current, tags: event.target.value }))}
                      placeholder="official, research, newsletter"
                      value={sourceForm.tags}
                    />
                    <p className="mt-2 text-xs leading-5 text-[var(--muted)]">
                      Optional custom labels for this source. These are different from the status, source type, and raw kind badges shown on the source card.
                    </p>
                  </label>
                </div>

                <div className="rounded-[1.25rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.62)] px-4 py-4">
                  <p className="field-label">Pipeline defaults</p>
                  <p className="mt-2 text-sm leading-6 text-[var(--muted)]">
                    {sourceForm.type === "gmail_newsletter"
                      ? "Gmail newsletter sources default to written-content auto classification plus newsletter-entry decomposition, so one issue can emit visible child docs for each extractable story."
                      : "Website sources default to fixed classification with no decomposition, so the fetched raw kind stays aligned with the source definition unless a dedicated custom pipeline says otherwise."}
                  </p>
                </div>

                {sourceForm.type === "website" ? (
                  <>
                    <div className="grid gap-3 sm:grid-cols-2">
                      <button
                        className={`rounded-[1.25rem] border px-4 py-3 text-left transition ${
                          sourceForm.discoveryMode === "rss_feed"
                            ? "border-[var(--accent)]/28 bg-[rgba(154,52,18,0.08)]"
                            : "border-[var(--ink)]/8 bg-[rgba(255,255,255,0.58)]"
                        }`}
                        onClick={() => setSourceForm((current) => ({ ...current, discoveryMode: "rss_feed" }))}
                        type="button"
                      >
                        <p className="font-mono text-[11px] uppercase tracking-[0.16em] text-[var(--muted-strong)]">RSS / feed</p>
                        <p className="mt-2 text-sm leading-6 text-[var(--muted)]">Read an Atom or RSS feed directly.</p>
                      </button>
                      <button
                        className={`rounded-[1.25rem] border px-4 py-3 text-left transition ${
                          sourceForm.discoveryMode === "website_index"
                            ? "border-[var(--accent)]/28 bg-[rgba(154,52,18,0.08)]"
                            : "border-[var(--ink)]/8 bg-[rgba(255,255,255,0.58)]"
                        }`}
                        onClick={() => setSourceForm((current) => ({ ...current, discoveryMode: "website_index" }))}
                        type="button"
                      >
                        <p className="font-mono text-[11px] uppercase tracking-[0.16em] text-[var(--muted-strong)]">Website index</p>
                        <p className="mt-2 text-sm leading-6 text-[var(--muted)]">Crawl an index page and extract matching article links.</p>
                      </button>
                    </div>

                    <div className="grid gap-4 sm:grid-cols-2">
                      <label>
                        <span className="field-label">{sourceForm.discoveryMode === "website_index" ? "Index URL" : "Feed URL"}</span>
                        <input
                          className="field-input"
                          onChange={(event) => setSourceForm((current) => ({ ...current, url: event.target.value }))}
                          placeholder={sourceForm.discoveryMode === "website_index" ? "https://www.anthropic.com/research" : "https://openai.com/news/rss.xml"}
                          value={sourceForm.url}
                        />
                      </label>
                      <label>
                        <span className="field-label">Website home (optional)</span>
                        <input
                          className="field-input"
                          onChange={(event) => setSourceForm((current) => ({ ...current, website: event.target.value }))}
                          placeholder="https://openai.com/news"
                          value={sourceForm.website}
                        />
                      </label>
                    </div>
                  </>
                ) : (
                  <label>
                    <span className="field-label">Sender email or Gmail query</span>
                    <input
                      className="field-input"
                      onChange={(event) => setSourceForm((current) => ({ ...current, query: event.target.value }))}
                      placeholder="newsletter@example.com or from:newsletter@example.com label:tldr-ai"
                      value={sourceForm.query}
                    />
                  </label>
                )}

                <label>
                  <span className="field-label">Description</span>
                  <textarea
                    className="field-input min-h-32"
                    onChange={(event) => setSourceForm((current) => ({ ...current, description: event.target.value }))}
                    placeholder="Describe what this source should capture and why it belongs in the queue."
                    value={sourceForm.description}
                  />
                </label>

                <div className="rounded-[1.25rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.62)] px-4 py-4">
                  <div className="flex items-center justify-between gap-3">
                    <div>
                      <p className="field-label">Status</p>
                      <p className="mt-1.5 text-sm leading-6 text-[var(--muted)]">
                        Paused sources stay visible here but stop participating in pipeline runs.
                      </p>
                    </div>
                    <button
                      className={`secondary-button px-3 py-2 text-[11px] ${sourceForm.active ? "filter-pill-active" : ""}`}
                      onClick={() => setSourceForm((current) => ({ ...current, active: !current.active }))}
                      type="button"
                    >
                      {sourceForm.active ? "Active" : "Paused"}
                    </button>
                  </div>
                </div>

                <div className="flex flex-wrap gap-2">
                  <button className="primary-button" disabled={sourceMutationBusy} type="submit">
                    {createSource.isPending
                      ? "Adding source..."
                      : updateSource.isPending
                        ? "Saving changes..."
                        : editingSourceId
                          ? "Save changes"
                          : "Add source"}
                  </button>
                  <button className="secondary-button" disabled={sourceMutationBusy} onClick={resetSourceEditor} type="button">
                    {editingSourceId ? "Cancel" : "Close"}
                  </button>
                </div>
              </form>
            </div>
          </div>
        </div>
      ) : null}

      {selectedRun ? (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-[rgba(17,19,18,0.38)] px-4 py-6 backdrop-blur-sm"
          onClick={() => navigateToPipeline({ runId: null }, { replace: true })}
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
                <p className="mt-3 text-sm leading-6 text-[var(--muted)]">{selectedRun.summary}</p>
              </div>
              <button className="secondary-button" onClick={() => navigateToPipeline({ runId: null }, { replace: true })} type="button">
                Close
              </button>
            </div>

            <div className="max-h-[calc(90vh-112px)] overflow-y-auto px-6 py-6">
              <div className="grid gap-4 lg:grid-cols-3">
                <article className="rounded-[1.4rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.52)] px-4 py-4">
                  <p className="field-label">Executed</p>
                  <p className="mt-2 text-sm leading-6 text-[var(--ink)]">{formatDateTimeLabel(selectedRun.started_at)}</p>
                  <p className="mt-1 text-xs leading-5 text-[var(--muted)]">
                    {formatRunDuration(selectedRun.started_at, selectedRun.finished_at)}
                  </p>
                </article>
                <article className="rounded-[1.4rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.52)] px-4 py-4">
                  <p className="field-label">Status</p>
                  <div className="mt-3">
                    <SourceStatusChip status={selectedRun.status} />
                  </div>
                  <p className="mt-2 text-xs leading-5 text-[var(--muted)]">
                    {selectedRun.trigger ? `Trigger: ${selectedRun.trigger.replace(/_/g, " ")}` : "Recorded operation"}
                  </p>
                </article>
                <article className="rounded-[1.4rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.52)] px-4 py-4">
                  <p className="field-label">Cost</p>
                  <p className="mt-2 text-sm leading-6 text-[var(--ink)]">{formatUsdCost(selectedRun.total_cost_usd)}</p>
                  <p className="mt-1 text-xs leading-5 text-[var(--muted)]">
                    LLM {formatUsdCost(selectedRun.ai_cost_usd)} · TTS {formatUsdCost(selectedRun.tts_cost_usd)}
                  </p>
                </article>
              </div>

              {selectedRunAssociatedDates ? (
                <div className="mt-6 rounded-[1.5rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.52)] px-4 py-4">
                  <p className="field-label">Edition / coverage</p>
                  <p className="mt-2 text-sm leading-6 text-[var(--ink)]">Edition {selectedRunAssociatedDates.edition}</p>
                  <p className="mt-1 text-sm leading-6 text-[var(--muted)]">Coverage {selectedRunAssociatedDates.coverage}</p>
                </div>
              ) : null}

              {selectedRun.basic_info.length ? (
                <div className="mt-6">
                  <p className="field-label">Basic info</p>
                  <div className="mt-3 grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
                    {selectedRun.basic_info.map((entry) => (
                      <article key={`${selectedRun.id}-${entry.label}`} className="rounded-[1.3rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.52)] px-4 py-4">
                        <p className="field-label">{entry.label}</p>
                        <p className="mt-2 break-words text-sm leading-6 text-[var(--ink)]">{entry.value}</p>
                      </article>
                    ))}
                  </div>
                </div>
              ) : null}

              {selectedRun.prompt_path || selectedRun.manifest_path || selectedRun.output_paths.length || selectedRun.final_summary ? (
                <div className="mt-6">
                  <p className="field-label">Run artifacts</p>
                  <div className="mt-3 grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
                    {selectedRun.prompt_path ? (
                      <article className="rounded-[1.3rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.52)] px-4 py-4">
                        <p className="field-label">Prompt</p>
                        <p className="mt-2 break-words text-sm leading-6 text-[var(--ink)]">{selectedRun.prompt_path}</p>
                      </article>
                    ) : null}
                    {selectedRun.manifest_path ? (
                      <article className="rounded-[1.3rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.52)] px-4 py-4">
                        <p className="field-label">Manifest</p>
                        <p className="mt-2 break-words text-sm leading-6 text-[var(--ink)]">{selectedRun.manifest_path}</p>
                      </article>
                    ) : null}
                    {selectedRun.output_paths.length ? (
                      <article className="rounded-[1.3rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.52)] px-4 py-4">
                        <p className="field-label">Outputs</p>
                        <p className="mt-2 break-words text-sm leading-6 text-[var(--ink)]">{selectedRun.output_paths.join(", ")}</p>
                      </article>
                    ) : null}
                  </div>
                  {selectedRun.final_summary ? (
                    <div className="mt-3 rounded-[1.3rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.52)] px-4 py-4">
                      <p className="field-label">Structured summary</p>
                      <p className="mt-2 text-sm leading-6 text-[var(--ink)]">{selectedRun.final_summary.summary}</p>
                      {selectedRun.final_summary.unresolved_questions.length ? (
                        <p className="mt-2 text-xs leading-5 text-[var(--muted)]">
                          Unresolved: {selectedRun.final_summary.unresolved_questions.join(" · ")}
                        </p>
                      ) : null}
                    </div>
                  ) : null}
                  {selectedRun.stderr_excerpt ? (
                    <div className="mt-3 rounded-[1.3rem] border border-[var(--danger)]/18 bg-[rgba(255,255,255,0.52)] px-4 py-4">
                      <p className="field-label text-[var(--danger)]">stderr excerpt</p>
                      <p className="mt-2 text-sm leading-6 text-[var(--danger)]">{selectedRun.stderr_excerpt}</p>
                    </div>
                  ) : null}
                </div>
              ) : null}

              {selectedRun.steps.length ? (
                <div className="mt-6">
                  <p className="field-label">Recorded steps</p>
                  <div className="mt-3 grid gap-3 sm:grid-cols-2">
                    {selectedRun.steps.map((step, index) => (
                      <article key={`${selectedRun.id}-step-${index}`} className="rounded-[1.3rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.52)] px-4 py-4">
                        <div className="flex flex-wrap items-center justify-between gap-3">
                          <p className="font-mono text-[11px] uppercase tracking-[0.18em] text-[var(--muted)]">{step.step_kind}</p>
                          <SourceStatusChip status={step.status} />
                        </div>
                        <p className="mt-3 text-sm leading-6 text-[var(--muted-strong)]">
                          {[
                            step.created_count ? `created ${step.created_count}` : null,
                            step.updated_count ? `updated ${step.updated_count}` : null,
                            step.skipped_count ? `skipped ${step.skipped_count}` : null,
                            Object.entries(step.counts_by_kind)
                              .filter(([, count]) => count > 0)
                              .map(([kind, count]) => `${formatRawKindLabel(kind)} ${count}`)
                              .join(" · "),
                          ]
                            .filter(Boolean)
                            .join(" · ") || "No counts recorded."}
                        </p>
                        {step.logs.length ? (
                          <div className="mt-3 space-y-2">
                            {step.logs.slice(-3).map((log, logIndex) => (
                              <p key={`${selectedRun.id}-step-${index}-log-${logIndex}`} className="text-xs leading-5 text-[var(--muted)]">
                                {`${formatLogTimeLabel(log.logged_at)} · ${log.level} · ${log.message}`}
                              </p>
                            ))}
                          </div>
                        ) : null}
                        {step.errors.length ? (
                          <ul className="mt-3 list-disc space-y-1 pl-5 text-xs leading-5 text-[var(--danger)]">
                            {step.errors.map((error) => (
                              <li key={`${selectedRun.id}-step-${index}-${error}`}>{error}</li>
                            ))}
                          </ul>
                        ) : null}
                      </article>
                    ))}
                  </div>
                </div>
              ) : null}

              {selectedRun.logs.length ? (
                <div className="mt-6">
                  <p className="field-label">Log</p>
                  <div className="mt-3 space-y-3">
                    {selectedRun.logs.map((log, index) => (
                      <article key={`${selectedRun.id}-log-${index}`} className="rounded-[1.3rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.52)] px-4 py-4">
                        <div className="flex flex-wrap items-center justify-between gap-2">
                          <p className="font-mono text-[11px] uppercase tracking-[0.18em] text-[var(--muted)]">{log.level}</p>
                          <p className="font-mono text-[11px] uppercase tracking-[0.18em] text-[var(--muted)]">{formatLogTimeLabel(log.logged_at)}</p>
                        </div>
                        <p className="mt-3 text-sm leading-6 text-[var(--ink)]">{log.message}</p>
                      </article>
                    ))}
                  </div>
                </div>
              ) : null}

              {selectedRun.errors.length ? (
                <div className="mt-6 rounded-[1.5rem] border border-[var(--danger)]/18 bg-[rgba(255,255,255,0.52)] px-4 py-4">
                  <p className="field-label text-[var(--danger)]">Errors</p>
                  <ul className="mt-3 list-disc space-y-2 pl-5 text-sm leading-6 text-[var(--danger)]">
                    {selectedRun.errors.map((error) => (
                      <li key={`${selectedRun.id}-${error}`}>{error}</li>
                    ))}
                  </ul>
                </div>
              ) : null}
            </div>
          </div>
        </div>
      ) : null}
    </>
  );
}

export function ConnectionsPage() {
  return <ConnectionsWorkspacePage mode="connections" />;
}

export function PipelinePage() {
  return <ConnectionsWorkspacePage mode="pipeline" />;
}
