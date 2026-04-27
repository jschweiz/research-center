import { useEffect, useMemo, useRef, useState, type CSSProperties, type ReactNode } from "react";
import { QueryClientProvider, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  BookOpen,
  Bot,
  Database,
  FileText,
  GitBranch,
  History,
  LoaderCircle,
  Mail,
  PanelLeftClose,
  PanelLeftOpen,
  RadioTower,
  RefreshCcw,
  Settings,
  Sparkles,
  Upload,
  Waves,
  Workflow,
  type LucideIcon,
} from "lucide-react";
import { BrowserRouter, Link, Navigate, NavLink, Outlet, Route, Routes, useLocation, useOutletContext, useSearchParams } from "react-router-dom";

import type { AlphaXivSort, IngestionRunHistoryEntry, RunStatus, Source, SourceType } from "../api/types";
import { hasSuccessfulEditionRun } from "../lib/edition-output-status";
import { queryClient } from "../lib/query-client";
import { useDefaultedDateInput } from "../lib/use-defaulted-date-input";
import {
  getStoredLocalControlToken,
  getStoredShellSidebarCollapsed,
  setStoredLocalControlToken,
  setStoredShellSidebarCollapsed,
} from "../runtime/storage";
import type { LocalControlStatus, RuntimeConfig } from "../runtime/types";
import { localControlClient, LocalControlError } from "./client";
import { LocalControlBriefTab } from "./LocalControlBriefTab";
import { LocalControlDocumentDetailPage } from "./LocalControlDocumentDetailPage";
import { LocalControlDocumentsTab } from "./LocalControlDocumentsTab";
import { LocalControlProfileTab } from "./LocalControlProfileTab";

type LocalControlSection = {
  path: string;
  label: string;
  title: string;
  description: string;
  kicker: string;
  icon: LucideIcon;
};

const LOCAL_CONTROL_SECTIONS: LocalControlSection[] = [
  {
    path: "/overview",
    label: "Overview",
    title: "Monitor the local research machine",
    description: "Inspect runtime health, vault readiness, and the latest local publication state.",
    kicker: "Mac-served app / Overview",
    icon: RadioTower,
  },
  {
    path: "/brief",
    label: "Brief",
    title: "Consult the generated brief",
    description: "Read the daily or weekly brief directly on the paired Mac surface, with links back into local document detail and audio when available.",
    kicker: "Mac-served app / Brief",
    icon: BookOpen,
  },
  {
    path: "/insights",
    label: "Insights",
    title: "Track the research graph",
    description: "Review rising topics, canonical topic coverage, and the materialized wiki map.",
    kicker: "Mac-served app / Insights",
    icon: Sparkles,
  },
  {
    path: "/documents",
    label: "Documents",
    title: "Browse the indexed vault documents",
    description: "Filter the full vault document table by source, content type, and triage status without leaving local control.",
    kicker: "Mac-served app / Documents",
    icon: FileText,
  },
  {
    path: "/pipeline",
    label: "Pipeline",
    title: "Run the staged vault pipeline",
    description: "Trigger fetch, lightweight enrichment, index rebuilds, publication refreshes, and scoped vault sync.",
    kicker: "Mac-served app / Pipeline",
    icon: Workflow,
  },
  {
    path: "/codex",
    label: "Codex jobs",
    title: "Drive synthesis and filing",
    description: "Run wiki compilation, health checks, ask vault-first questions, and persist durable outputs back into the wiki.",
    kicker: "Mac-served app / Codex jobs",
    icon: Bot,
  },
  {
    path: "/operations",
    label: "Operations",
    title: "Review recent Mac activity",
    description: "Inspect the execution history for pipeline runs, enrichment jobs, and publication work.",
    kicker: "Mac-served app / Operations",
    icon: History,
  },
  {
    path: "/profile",
    label: "Profile",
    title: "Tune the local briefing profile",
    description: "Adjust ranking, digest, audio, and prompt guidance settings that the Mac uses during briefing and enrichment.",
    kicker: "Mac-served app / Profile",
    icon: Settings,
  },
];

const PAIRING_SECTION: LocalControlSection = {
  path: "/pair",
  label: "Pairing",
  title: "Pair this device",
  description: "Redeem a pairing link from the Mac so this browser can control local jobs directly.",
  kicker: "Mac-served app / Pairing",
  icon: RadioTower,
};

const SOURCE_FETCH_MAX_ITEMS_LIMIT = 250;
const LIVE_DASHBOARD_POLL_MS = 1500;
const ACTION_RUN_RECONCILIATION_GRACE_MS = 2_000;
const ACTION_STALE_RECONCILIATION_DELAY_MS = 15_000;
const LIGHTWEIGHT_PHASE_PROGRESS_RE = /^(Metadata|Scoring) phase progress (\d+)\/(\d+): (.+)$/;
const LIGHTWEIGHT_PHASE_STEP_RE = /^(Metadata|Scoring) phase (\d+)\/(\d+) (completed|failed) for (.+)$/;

function getLocalControlSection(pathname: string) {
  const normalizedPath = pathname === "/" ? "/overview" : pathname.replace(/\/+$/, "");
  if (normalizedPath === "/pair" || normalizedPath.startsWith("/pair/")) {
    return PAIRING_SECTION;
  }
  return LOCAL_CONTROL_SECTIONS.find((section) => normalizedPath === section.path || normalizedPath.startsWith(`${section.path}/`)) ?? LOCAL_CONTROL_SECTIONS[0];
}

function formatTimestamp(value: string | null | undefined) {
  if (!value) return "Unknown";
  return new Date(value).toLocaleString();
}

function isSameLocalDay(value: string, reference = new Date()) {
  const candidate = new Date(value);
  if (Number.isNaN(candidate.getTime())) return false;
  return candidate.toDateString() === reference.toDateString();
}

function formatStepCounts(step: {
  created_count: number;
  updated_count: number;
  skipped_count: number;
  counts_by_kind: Record<string, number>;
}) {
  const counts = Object.entries(step.counts_by_kind)
    .filter(([, count]) => count > 0)
    .map(([kind, count]) => `${kind} ${count}`);
  const summary = [
    step.created_count ? `created ${step.created_count}` : null,
    step.updated_count ? `updated ${step.updated_count}` : null,
    step.skipped_count ? `skipped ${step.skipped_count}` : null,
    counts.length ? counts.join(" · ") : null,
  ]
    .filter(Boolean)
    .join(" · ");
  return summary || "No counts recorded.";
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

function describeStatusError(error: unknown) {
  if (error instanceof LocalControlError) {
    if (error.status === 401) {
      return `${error.message} Open a new pairing link from the Mac if this iPad should still be authorized.`;
    }
    return error.message;
  }
  if (error instanceof Error && error.message) {
    return error.message;
  }
  return "Local control could not load from the Mac.";
}

function canUseLocalMacWithoutPairing() {
  if (typeof window === "undefined") return false;
  return ["localhost", "127.0.0.1", "::1"].includes(window.location.hostname);
}

function StatusChip({ label, tone = "default" }: { label: string; tone?: "default" | "success" | "error" | "warning" }) {
  const toneClass =
    tone === "success"
      ? "border-[rgba(22,163,74,0.2)] bg-[rgba(22,163,74,0.08)] text-[#166534]"
      : tone === "error"
        ? "border-[var(--danger)]/18 bg-[rgba(159,18,57,0.08)] text-[var(--danger)]"
        : tone === "warning"
          ? "border-[var(--accent)]/18 bg-[rgba(154,52,18,0.08)] text-[var(--accent)]"
          : "border-[var(--ink)]/10 bg-[rgba(255,255,255,0.74)] text-[var(--muted-strong)]";

  return (
    <span className={`rounded-full border px-3 py-1 font-mono text-[10px] uppercase tracking-[0.16em] ${toneClass}`}>
      {label}
    </span>
  );
}

function SkeletonLine({ className }: { className: string }) {
  return <div aria-hidden className={`animate-pulse rounded-full bg-[var(--ink)]/8 ${className}`} />;
}

function SectionIntro({
  kicker,
  title,
  description,
  className,
}: {
  kicker: string;
  title: string;
  description?: string;
  className?: string;
}) {
  return (
    <div className={className}>
      <p className="section-kicker">{kicker}</p>
      <h3 className="section-title">{title}</h3>
      {description ? <p className="mt-3 max-w-4xl text-sm leading-6 text-[var(--muted)]">{description}</p> : null}
    </div>
  );
}

function LoadingHint({ label }: { label: string }) {
  return (
    <div className="flex items-center gap-2 text-sm leading-6 text-[var(--muted)]">
      <LoaderCircle className="h-4 w-4 animate-spin" />
      <span>{label}</span>
    </div>
  );
}

function ActionCardSkeleton({
  icon,
  title,
  subtitle,
  showControlRow = false,
  controlLabel,
  controlActionLabel,
  hideActionButton = false,
  buttonLabel,
}: {
  icon: ReactNode;
  title: string;
  subtitle: string;
  showControlRow?: boolean;
  controlLabel?: string;
  controlActionLabel?: string;
  hideActionButton?: boolean;
  buttonLabel?: string;
}) {
  return (
    <article className="editorial-panel flex h-full flex-col">
      <div className="flex items-center gap-3 text-[var(--accent)]">{icon}</div>
      <h3 className="mt-5 text-2xl font-semibold text-[var(--ink)]">{title}</h3>
      <p className="mt-3 text-sm leading-6 text-[var(--muted)]">{subtitle}</p>
      {showControlRow ? (
        <div className="mt-4 flex flex-col gap-3">
          <label className="rounded-[1.2rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.62)] px-3 py-3">
            <span className="field-label">{controlLabel ?? "Target"}</span>
            <SkeletonLine className="mt-3 h-10 w-full rounded-[0.95rem]" />
          </label>
          {controlActionLabel ? (
            <button className="secondary-button w-full justify-center" disabled type="button">
              {controlActionLabel}
            </button>
          ) : null}
        </div>
      ) : null}
      {hideActionButton ? null : (
        <div className="mt-6 flex flex-1 items-end justify-end">
          <button className="primary-button" disabled type="button">
            {buttonLabel ?? title}
          </button>
        </div>
      )}
    </article>
  );
}

function formatEndpointHost(value: string) {
  try {
    return new URL(value).host;
  } catch {
    return value.replace(/^https?:\/\//, "");
  }
}

function formatCommitHash(value: string | null | undefined) {
  return value ? value.slice(0, 7) : "Unknown";
}

function getOverviewCardToneClass(tone: "default" | "success" | "warning" | "error" = "default") {
  if (tone === "success") {
    return "border-[rgba(22,163,74,0.18)] bg-[linear-gradient(180deg,rgba(240,253,244,0.94),rgba(255,255,255,0.72))]";
  }
  if (tone === "warning") {
    return "border-[var(--accent)]/16 bg-[linear-gradient(180deg,rgba(255,237,213,0.92),rgba(255,255,255,0.72))]";
  }
  if (tone === "error") {
    return "border-[var(--danger)]/14 bg-[linear-gradient(180deg,rgba(255,228,230,0.9),rgba(255,255,255,0.72))]";
  }
  return "border-[var(--ink)]/8 bg-[rgba(255,255,255,0.52)]";
}

function OverviewMetricCard({
  label,
  value,
  detail,
  tone = "default",
}: {
  label: string;
  value: string;
  detail: string;
  tone?: "default" | "success" | "warning" | "error";
}) {
  return (
    <article className={`rounded-[1.55rem] border px-4 py-4 ${getOverviewCardToneClass(tone)}`}>
      <p className="font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">{label}</p>
      <p className="mt-3 font-display text-4xl leading-none text-[var(--ink)]">{value}</p>
      <p className="mt-2 text-sm leading-6 text-[var(--muted)]">{detail}</p>
    </article>
  );
}

function OverviewDetailCard({
  kicker,
  title,
  description,
  tone = "default",
  adornment,
  children,
}: {
  kicker: string;
  title: string;
  description?: string;
  tone?: "default" | "success" | "warning" | "error";
  adornment?: ReactNode;
  children: ReactNode;
}) {
  return (
    <article className={`rounded-[1.7rem] border px-5 py-5 ${getOverviewCardToneClass(tone)}`}>
      <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
        <div className="min-w-0">
          <p className="field-label">{kicker}</p>
          <h4 className="mt-3 font-display text-3xl leading-tight text-[var(--ink)]">{title}</h4>
          {description ? <p className="mt-3 text-sm leading-6 text-[var(--muted)]">{description}</p> : null}
        </div>
        {adornment ? <div className="shrink-0">{adornment}</div> : null}
      </div>
      {children}
    </article>
  );
}

function OverviewFactRows({
  items,
}: {
  items: Array<{ label: string; value: ReactNode; detail?: ReactNode }>;
}) {
  return (
    <dl className="mt-6 divide-y divide-[var(--ink)]/8">
      {items.map((item) => (
        <div className="grid gap-2 py-3 sm:grid-cols-[minmax(0,9.75rem)_minmax(0,1fr)] sm:gap-4" key={item.label}>
          <dt className="field-label pt-1">{item.label}</dt>
          <dd className="min-w-0">
            <div className="break-words text-sm leading-6 text-[var(--ink)]">{item.value}</div>
            {item.detail ? <div className="mt-1 text-xs leading-5 text-[var(--muted)]">{item.detail}</div> : null}
          </dd>
        </div>
      ))}
    </dl>
  );
}

function OverviewTabSkeleton() {
  return (
    <div className="space-y-8">
      <section className="editorial-panel relative overflow-hidden">
        <div aria-hidden className="pointer-events-none absolute -left-10 bottom-0 h-32 w-32 rounded-full bg-[var(--teal)]/10 blur-3xl" />
        <div aria-hidden className="pointer-events-none absolute -right-8 top-0 h-36 w-36 rounded-full bg-[var(--accent)]/10 blur-3xl" />
        <div className="relative flex flex-col gap-6 xl:flex-row xl:items-end xl:justify-between">
          <div className="min-w-0 flex-1">
            <p className="section-kicker">Device</p>
            <h3 className="section-title max-w-2xl">Connected research machine</h3>
            <p className="mt-3 max-w-2xl text-sm leading-6 text-[var(--muted)]">
              Core status, publication state, and vault endpoints appear here as soon as the Mac responds.
            </p>
            <SkeletonLine className="mt-6 h-10 w-80 max-w-full" />
            <SkeletonLine className="mt-4 h-4 w-full max-w-xl" />
          </div>
          <div className="w-full max-w-md rounded-[1.75rem] border border-[var(--accent)]/14 bg-[linear-gradient(180deg,rgba(255,247,237,0.96),rgba(255,255,255,0.78))] px-5 py-5 shadow-[0_20px_48px_rgba(154,52,18,0.08)]">
            <p className="field-label">Current brief</p>
            <SkeletonLine className="mt-4 h-10 w-44 max-w-full" />
            <p className="mt-3 text-sm leading-6 text-[var(--muted)]">
              Publication details will appear here once the latest machine snapshot is available.
            </p>
          </div>
        </div>
      </section>

      <section className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
        {["Raw documents", "Pending enrich", "Indexed items", "Stale in index", "Wiki pages", "Rising topics"].map((label) => (
          <article
            className="rounded-[1.55rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.52)] px-4 py-4"
            key={label}
          >
            <p className="font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">{label}</p>
            <SkeletonLine className="mt-4 h-10 w-24" />
            <SkeletonLine className="mt-3 h-4 w-full max-w-[15rem]" />
          </article>
        ))}
      </section>

      <section className="grid gap-4 xl:grid-cols-[minmax(0,1.08fr)_minmax(0,0.92fr)]">
        <article className="rounded-[1.7rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.52)] px-5 py-5">
          <p className="field-label">Access</p>
          <h4 className="mt-3 font-display text-3xl leading-tight text-[var(--ink)]">Paths and endpoints</h4>
          <dl className="mt-6 divide-y divide-[var(--ink)]/8">
            {["Paired local URL", "Vault root", "Viewer bundle"].map((label) => (
              <div className="grid gap-2 py-3 sm:grid-cols-[minmax(0,9.75rem)_minmax(0,1fr)] sm:gap-4" key={label}>
                <dt className="field-label pt-1">{label}</dt>
                <dd>
                  <SkeletonLine className="h-4 w-full max-w-[22rem]" />
                </dd>
              </div>
            ))}
          </dl>
        </article>

        <article className="rounded-[1.7rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.52)] px-5 py-5">
          <p className="field-label">Publishing state</p>
          <h4 className="mt-3 font-display text-3xl leading-tight text-[var(--ink)]">Current edition window</h4>
          <dl className="mt-6 divide-y divide-[var(--ink)]/8">
            <div className="grid gap-2 py-3 sm:grid-cols-[minmax(0,9.75rem)_minmax(0,1fr)] sm:gap-4">
              <dt className="field-label pt-1">Current brief</dt>
              <dd>
                <SkeletonLine className="h-4 w-32 max-w-full" />
              </dd>
            </div>
          </dl>
          <p className="mt-4 text-sm leading-6 text-[var(--muted)]">
            The latest publication block only appears if a viewer bundle has already been generated.
          </p>
        </article>
      </section>
    </div>
  );
}

function InsightsTabSkeleton() {
  return (
    <div className="space-y-8">
      <section className="editorial-panel">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <SectionIntro
            className="flex-1"
            description="The deterministic insight layer tracks canonical topics, rising clusters, and their supporting source-note coverage. Use this tab to decide where Codex should synthesize next."
            kicker="Insight radar"
            title="What is moving in the research graph"
          />
        </div>
        <div className="mt-6">
          <LoadingHint label="Loading insight radar…" />
        </div>
      </section>
    </div>
  );
}

function PipelineTabSkeleton() {
  return (
    <div className="space-y-8">
      <section className="space-y-4">
        <SectionIntro kicker="Staged pipeline" title="Run each step separately" />
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          <ActionCardSkeleton
            icon={<Mail className="h-5 w-5" />}
            subtitle="Discover website posts and newsletter issues, write canonical raw documents, and do deterministic newsletter decomposition only."
            title="Fetch sources"
          />
          <ActionCardSkeleton
            buttonLabel="Refresh metadata"
            icon={<Sparkles className="h-5 w-5" />}
            subtitle="Add or refresh tags, authors, and short summaries for raw documents."
            title="Enrichment"
          />
          <ActionCardSkeleton
            buttonLabel="Generate scores"
            icon={<Sparkles className="h-5 w-5" />}
            subtitle="Generate or refresh lightweight document scores after metadata is current."
            title="Scoring"
          />
          <ActionCardSkeleton
            icon={<Database className="h-5 w-5" />}
            subtitle="Refresh the inbox, filters, brief inputs, and viewer lookups from the current raw vault."
            title="Rebuild index"
          />
        </div>
      </section>
      <section className="space-y-4">
        <SectionIntro kicker="Convenience jobs" title="Refresh brief outputs" />
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
          <ActionCardSkeleton
            controlActionLabel="Regenerate"
            controlLabel="Edition"
            hideActionButton
            icon={<RefreshCcw className="h-5 w-5" />}
            showControlRow
            subtitle="Rebuild the current written brief from the current vault state."
            title="Regenerate brief"
          />
          <ActionCardSkeleton
            controlActionLabel="Generate"
            controlLabel="Edition"
            hideActionButton
            icon={<Waves className="h-5 w-5" />}
            showControlRow
            subtitle="Generate the audio script and audio file for the current brief."
            title="Generate audio"
          />
          <ActionCardSkeleton
            controlActionLabel="Publish"
            controlLabel="Edition"
            hideActionButton
            icon={<Upload className="h-5 w-5" />}
            showControlRow
            subtitle="Refresh the published viewer bundle from the current vault outputs."
            title="Publish viewer"
          />
        </div>
      </section>
      <section className="space-y-4">
        <SectionIntro kicker="Vault sync" title="Push only local-control artifacts" />
        <div className="grid gap-4 xl:max-w-[34rem]">
          <ActionCardSkeleton
            icon={<GitBranch className="h-5 w-5" />}
            subtitle="Commit and push only `raw/**`, `briefs/daily/**`, and `outputs/viewer/**`. This leaves `wiki/**` and other Codex-managed files untouched."
            title="Sync vault"
          />
        </div>
      </section>
      <section className="space-y-4">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
          <SectionIntro
            kicker="Per source fetch"
            title="Fetch one source at a time"
            description="Use this to backfill or retry one configured source without running the full fetch step. The per-run cap here overrides the source default for this run only, and paused sources can still be fetched manually."
          />
          <div className="flex flex-wrap gap-2">
            <span className="rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.56)] px-4 py-2 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">
              Configured sources
            </span>
            <span className="rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.56)] px-4 py-2 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">
              Active sources
            </span>
          </div>
        </div>
        <LoadingHint label="Loading configured sources…" />
      </section>
    </div>
  );
}

function CodexTabSkeleton() {
  return (
    <div className="space-y-8">
      <section className="space-y-4">
        <SectionIntro kicker="Advanced Codex jobs" title="Run wiki compilation, Q&A, and filing explicitly" />
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-2 2xl:grid-cols-4">
          <ActionCardSkeleton
            controlLabel="Compile batch size"
            icon={<BookOpen className="h-5 w-5" />}
            showControlRow
            subtitle="Use Codex to maintain `wiki/**` from changed raw documents, then rebuild the deterministic pages and graph indexes."
            title="Compile wiki with Codex"
          />
          <ActionCardSkeleton
            controlLabel="Scope / Topic"
            icon={<History className="h-5 w-5" />}
            showControlRow
            subtitle="Audit the vault for missing metadata, weak links, stale pages, duplicate concepts, and follow-up questions."
            title="Run health check"
          />
          <ActionCardSkeleton
            controlLabel="Question / Output kind"
            icon={<Bot className="h-5 w-5" />}
            showControlRow
            subtitle="Ask a vault-first Codex question and persist the answer as a report, slides, or chart bundle."
            title="Ask Codex"
          />
          <ActionCardSkeleton
            controlLabel="Vault-relative path"
            icon={<Upload className="h-5 w-5" />}
            showControlRow
            subtitle="Distill a prior durable output back into `wiki/**` so the knowledge base compounds over time."
            title="File output into wiki"
          />
        </div>
      </section>
    </div>
  );
}

function OperationsTabSkeleton() {
  return (
    <section className="editorial-panel">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
        <SectionIntro
          className="flex-1"
          kicker="Operational history"
          title="Recent job runs"
          description="Every local-control action records a run here when the Mac emits one. Open any row to inspect the detailed log, associated edition coverage, recorded artifacts, and the latest step-level messages."
        />
        <div className="flex flex-wrap gap-2">
          <button className="secondary-button" disabled type="button">
            Refresh
          </button>
        </div>
      </div>
      <div className="mt-6">
        <LoadingHint label="Loading operation history…" />
      </div>
    </section>
  );
}

function formatDateTimeLabel(value: string | null) {
  if (!value) return "Not finished yet";
  return new Date(value).toLocaleString();
}

function formatLogTimeLabel(value: string) {
  return new Date(value).toLocaleTimeString();
}

function formatSourceTypeLabel(type: SourceType) {
  return type === "gmail_newsletter" ? "Gmail" : "Website";
}

function formatRawKindLabel(value: string) {
  return value.replace(/[-_]/g, " ");
}

function formatEmittedKinds(kinds: string[]) {
  return kinds.length ? kinds.map((kind) => formatRawKindLabel(kind)).join(", ") : "No emitted kinds recorded yet";
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

function findRunBasicInfoValue(run: IngestionRunHistoryEntry, label: string) {
  return run.basic_info.find((entry) => entry.label === label)?.value ?? null;
}

function findRunSourceId(run: IngestionRunHistoryEntry) {
  return findRunBasicInfoValue(run, "Source ID") ?? run.source_stats.find((entry) => entry.source_id)?.source_id ?? null;
}

function latestRunMessage(run: IngestionRunHistoryEntry | null) {
  if (!run) return null;
  const stepWithLogs = [...run.steps].reverse().find((step) => step.logs.length > 0);
  if (stepWithLogs) {
    return stepWithLogs.logs[stepWithLogs.logs.length - 1]?.message ?? null;
  }
  return run.logs.length ? run.logs[run.logs.length - 1]?.message ?? null : null;
}

function findLatestLightweightPhaseProgress(run: IngestionRunHistoryEntry | null) {
  if (!run) return null;

  const allLogs = [
    ...run.logs,
    ...run.steps.flatMap((step) => step.logs),
  ].sort((left, right) => new Date(right.logged_at).getTime() - new Date(left.logged_at).getTime());

  for (const log of allLogs) {
    const progressMatch = log.message.match(LIGHTWEIGHT_PHASE_PROGRESS_RE);
    if (progressMatch) {
      const [, rawPhase, completedValue, totalValue, latest] = progressMatch;
      const completed = Number.parseInt(completedValue, 10);
      const total = Number.parseInt(totalValue, 10);
      if (!Number.isFinite(completed) || !Number.isFinite(total) || total <= 0) {
        continue;
      }
      return {
        phase: rawPhase.toLowerCase() as "metadata" | "scoring",
        completed,
        total,
        latest,
      };
    }

    const stepMatch = log.message.match(LIGHTWEIGHT_PHASE_STEP_RE);
    if (stepMatch) {
      const [, rawPhase, completedValue, totalValue, outcome, latest] = stepMatch;
      const completed = Number.parseInt(completedValue, 10);
      const total = Number.parseInt(totalValue, 10);
      if (!Number.isFinite(completed) || !Number.isFinite(total) || total <= 0) {
        continue;
      }
      return {
        phase: rawPhase.toLowerCase() as "metadata" | "scoring",
        completed,
        total,
        latest: `${outcome} ${latest}`,
      };
    }
  }

  return null;
}

function parsePositiveInteger(value: string | null) {
  if (!value) return null;
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

function parseNonNegativeInteger(value: string | null) {
  if (value === null || value === "") return null;
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : null;
}

function isLiveRunStatus(
  status: RunStatus | null | undefined,
): status is Extract<RunStatus, "pending" | "running"> {
  return status === "pending" || status === "running";
}

function hasLiveOperationRuns(runs: IngestionRunHistoryEntry[] | null | undefined) {
  return Boolean(runs?.some((run) => isLiveRunStatus(run.status)));
}

function triggerStartsWith(trigger: string | null | undefined, prefix: string) {
  return trigger === prefix || Boolean(trigger?.startsWith(`${prefix}:`));
}

function buildObservedSourceFetchAction({
  source,
  run,
  latestRun,
}: {
  source: Source;
  run: IngestionRunHistoryEntry | null;
  latestRun: Source["latest_extraction_run"];
}): ActiveLocalAction | null {
  const observedStatus = run?.status ?? latestRun?.status;
  if (observedStatus !== "running" && observedStatus !== "pending") {
    return null;
  }

  const startedAt = run?.started_at ?? latestRun?.started_at ?? new Date().toISOString();
  const requestedMaxItems =
    parsePositiveInteger(run ? findRunBasicInfoValue(run, "Max items") : null)
    ?? parsePositiveInteger(run ? findRunBasicInfoValue(run, "Configured max items") : null)
    ?? source.max_items;

  return {
    id: `observed-source-fetch-${source.id}`,
    operationRunId: run?.id ?? latestRun?.id ?? undefined,
    kind: "source_fetch",
    section: "pipeline_steps",
    label: `Fetch ${source.name}`,
    summary: run?.summary ?? latestRun?.summary ?? `${source.name} is fetching on the Mac.`,
    queuedAt: startedAt,
    startedAt: observedStatus === "running" ? startedAt : null,
    status: observedStatus,
    progressSnapshot: {
      activeSourceCount: null,
      rawDocumentCount: null,
      lightweightPendingCount: null,
      lightweightMetadataPendingCount: null,
      lightweightScoringPendingCount: null,
      staleDocumentCount: null,
    },
    sourceId: source.id,
    sourceName: source.name,
    maxItems: requestedMaxItems,
  };
}

function buildObservedDashboardAction(
  run: IngestionRunHistoryEntry,
): ActiveLocalAction | null {
  if (!isLiveRunStatus(run.status)) {
    return null;
  }

  const baseAction = {
    id: `observed-run-${run.id}`,
    operationRunId: run.id,
    queuedAt: run.started_at,
    startedAt: run.status === "running" ? run.started_at : null,
    status: run.status,
    progressSnapshot: {
      activeSourceCount: null,
      rawDocumentCount: null,
      lightweightPendingCount: null,
      lightweightMetadataPendingCount: null,
      lightweightScoringPendingCount: null,
      staleDocumentCount: null,
    },
    summary: run.summary,
  } satisfies Pick<
    ActiveLocalAction,
    "id" | "operationRunId" | "queuedAt" | "startedAt" | "status" | "progressSnapshot" | "summary"
  >;

  if (run.operation_kind === "raw_fetch" && run.trigger === "manual_fetch") {
    return {
      ...baseAction,
      kind: "fetch_sources",
      section: "pipeline_steps",
      label: "Fetch sources",
    };
  }

  if (run.operation_kind === "lightweight_enrichment") {
    const phase = findRunBasicInfoValue(run, "Phase")?.toLowerCase();
    const observedKind: Extract<LocalActionKind, "lightweight_metadata" | "lightweight_scoring"> =
      phase === "scoring" || (phase !== "metadata" && findLatestLightweightPhaseProgress(run)?.phase === "scoring")
        ? "lightweight_scoring"
        : "lightweight_metadata";
    return {
      ...baseAction,
      kind: observedKind,
      section: "pipeline_steps",
      label: observedKind === "lightweight_scoring" ? "Scoring" : "Enrichment",
    };
  }

  if (run.operation_kind === "vault_index" && run.trigger === "manual_index") {
    return {
      ...baseAction,
      kind: "rebuild_index",
      section: "pipeline_steps",
      label: "Rebuild index",
    };
  }

  if (run.operation_kind === "brief_generation" && run.trigger === "manual_digest") {
    return {
      ...baseAction,
      kind: "regenerate_brief",
      section: "convenience_jobs",
      label: "Regenerate brief",
      briefDate: run.affected_edition_days[0] ?? undefined,
    };
  }

  if (run.operation_kind === "audio_generation" && run.trigger === "manual_audio") {
    return {
      ...baseAction,
      kind: "generate_audio",
      section: "convenience_jobs",
      label: "Generate audio",
      briefDate: run.affected_edition_days[0] ?? undefined,
    };
  }

  if (run.operation_kind === "viewer_publish" && run.trigger === "manual_publish") {
    return {
      ...baseAction,
      kind: "publish",
      section: "convenience_jobs",
      label: "Publish viewer",
      briefDate: run.affected_edition_days[0] ?? undefined,
    };
  }

  if (run.operation_kind === "advanced_compile" && run.trigger === "manual_advanced_compile") {
    return {
      ...baseAction,
      kind: "compile_wiki",
      section: "codex_jobs",
      label: "Compile wiki with Codex",
    };
  }

  if (run.operation_kind === "health_check" && run.trigger === "manual_health_check") {
    return {
      ...baseAction,
      kind: "health_check",
      section: "codex_jobs",
      label: "Run health check",
    };
  }

  if (run.operation_kind === "answer_query" && run.trigger === "manual_answer_query") {
    return {
      ...baseAction,
      kind: "answer_query",
      section: "codex_jobs",
      label: "Ask Codex",
    };
  }

  if (run.operation_kind === "file_output" && run.trigger === "manual_file_output") {
    return {
      ...baseAction,
      kind: "file_output",
      section: "codex_jobs",
      label: "File output into wiki",
    };
  }

  if (run.operation_kind === "vault_sync" && run.trigger === "manual_local_control_sync") {
    return {
      ...baseAction,
      kind: "sync_vault",
      section: "vault_sync",
      label: "Sync vault",
    };
  }

  return null;
}

function findObservedDashboardAction(
  runs: IngestionRunHistoryEntry[],
  kind: Exclude<LocalActionKind, "source_fetch" | "ingest">,
) {
  for (const run of runs) {
    const action = buildObservedDashboardAction(run);
    if (action?.kind === kind) {
      return action;
    }
  }
  return null;
}

function preferLocalAction(
  actions: ActiveLocalAction[],
  observedAction: ActiveLocalAction | null,
) {
  return actions.find((action) => action.status === "running") ?? actions[0] ?? observedAction;
}

function RunStatusChip({ status }: { status: RunStatus | null }) {
  if (!status) {
    return <StatusChip label="No run yet" />;
  }

  const label = status === "pending" ? "Queued" : status.replace(/_/g, " ");

  return (
    <span
      className={`inline-flex items-center gap-1.5 rounded-full border px-3 py-1 font-mono text-[10px] uppercase tracking-[0.16em] ${
        status === "failed"
          ? "border-[var(--danger)]/18 bg-[rgba(159,18,57,0.08)] text-[var(--danger)]"
          : status === "running"
            ? "border-[rgba(14,77,100,0.18)] bg-[rgba(14,77,100,0.08)] text-[var(--teal)]"
            : status === "pending"
              ? "border-[var(--accent)]/18 bg-[rgba(154,52,18,0.08)] text-[var(--accent)]"
            : "border-[rgba(22,163,74,0.2)] bg-[rgba(22,163,74,0.08)] text-[#166534]"
      }`}
    >
      {status === "running" ? <LoaderCircle className="h-3 w-3 animate-spin" /> : null}
      {label}
    </span>
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

  return <RunStatusChip status={status} />;
}

function EditionTargetActionRow({
  buttonLabel,
  busyLabel,
  disabled,
  inputDisabled,
  loading,
  value,
  onBlur,
  onChange,
  onClick,
}: {
  buttonLabel: string;
  busyLabel: string;
  disabled: boolean;
  inputDisabled: boolean;
  loading: boolean;
  value: string;
  onBlur: () => void;
  onChange: (value: string) => void;
  onClick: () => void;
}) {
  return (
    <div className="mt-auto pt-4">
      <span className="field-label">Edition date</span>
      <div className="mt-2 flex flex-col gap-3 sm:flex-row sm:items-center">
        <input
          aria-label="Edition date"
          className="field-input mt-0 min-w-0 w-full sm:max-w-[11rem]"
          disabled={inputDisabled}
          onBlur={onBlur}
          onChange={(event) => onChange(event.target.value)}
          type="date"
          value={value}
        />
        <button className="primary-button w-full shrink-0 sm:w-auto" disabled={disabled} onClick={onClick} type="button">
          {loading ? <LoaderCircle className="h-4 w-4 animate-spin" /> : null}
          {loading ? busyLabel : buttonLabel}
        </button>
      </div>
      <p className="mt-2 text-xs leading-5 text-[var(--muted)]">
        Pick any single day. The latest brief day is used by default when it is available.
      </p>
    </div>
  );
}

function LocalShell() {
  const location = useLocation();
  const [sidebarCollapsed, setSidebarCollapsed] = useState(getStoredShellSidebarCollapsed);
  const activeSection = getLocalControlSection(location.pathname);

  useEffect(() => {
    setStoredShellSidebarCollapsed(sidebarCollapsed);
  }, [sidebarCollapsed]);

  const SidebarToggleIcon = sidebarCollapsed ? PanelLeftOpen : PanelLeftClose;

  return (
    <div className="min-h-screen bg-[var(--paper)] text-[var(--ink)]">
      <div
        className="app-grid mx-auto min-h-screen max-w-[1680px] px-4 py-4 sm:px-6 lg:px-8"
        data-sidebar-collapsed={sidebarCollapsed ? "true" : "false"}
      >
        <aside className="editorial-sidebar">
          <div className="editorial-sidebar-header">
            <div className="editorial-sidebar-heading">
              <p className="editorial-sidebar-kicker font-mono text-[11px] uppercase tracking-[0.35em] text-[var(--muted)]">Research Center</p>
              <span
                aria-hidden="true"
                className="editorial-sidebar-monogram font-mono text-[11px] uppercase tracking-[0.3em] text-[var(--muted)]"
              >
                RC
              </span>
              <button
                aria-expanded={!sidebarCollapsed}
                aria-label={sidebarCollapsed ? "Expand sidebar" : "Collapse sidebar"}
                className="secondary-button h-11 w-11 shrink-0 justify-center p-0"
                onClick={() => setSidebarCollapsed((current) => !current)}
                title={sidebarCollapsed ? "Expand sidebar" : "Collapse sidebar"}
                type="button"
              >
                <SidebarToggleIcon className="h-4 w-4" />
              </button>
            </div>

            <div className="editorial-sidebar-copy space-y-3">
              <h1 className="font-display text-5xl leading-none text-[var(--ink)]">Local control</h1>
              <p className="text-sm leading-6 text-[var(--muted)]">
                This surface talks directly to the Mac. The Mac is the only automated writer: it fetches source material, runs lightweight Ollama enrichment, rebuilds indexes, compiles the wiki, and syncs the vault back to GitHub.
              </p>
            </div>
          </div>

          <nav className="editorial-sidebar-nav mt-10 space-y-2">
            {LOCAL_CONTROL_SECTIONS.map((section) => {
              const Icon = section.icon;
              return (
                <NavLink
                  aria-label={section.label}
                  key={section.path}
                  className={({ isActive }) => `nav-link ${isActive ? "nav-link-active" : ""}`}
                  end
                  title={section.label}
                  to={section.path}
                >
                  <Icon className="h-4 w-4" />
                  <span className="nav-link-label">{section.label}</span>
                </NavLink>
              );
            })}
          </nav>
        </aside>

        <main className="editorial-main">
          <header className="editorial-topbar">
            <div className="min-w-0 flex-1">
              <p className="font-mono text-[11px] uppercase tracking-[0.26em] text-[var(--muted)]">{activeSection.kicker}</p>
              <h2 className="mt-3 max-w-full font-display text-4xl leading-[0.94] sm:text-5xl">{activeSection.title}</h2>
              <p className="mt-4 max-w-3xl text-sm leading-6 text-[var(--muted)]">{activeSection.description}</p>
            </div>
          </header>
          <Outlet />
        </main>
      </div>
    </div>
  );
}

function PairingPage() {
  const [searchParams] = useSearchParams();
  const pairingToken = searchParams.get("pairing_token");
  const redeem = useMutation({
    mutationFn: () => {
      if (!pairingToken) {
        throw new Error("Pairing token missing from this link.");
      }
      return localControlClient.redeemPairing(pairingToken);
    },
    onSuccess: (payload) => {
      setStoredLocalControlToken(payload.access_token);
    },
  });

  useEffect(() => {
    if (!pairingToken) return;
    if (redeem.isSuccess || redeem.isPending) return;
    redeem.mutate();
  }, [pairingToken, redeem]);

  const returnUrl = useMemo(() => {
    if (!redeem.data?.hosted_return_url) return null;
    try {
      const url = new URL(redeem.data.hosted_return_url);
      url.searchParams.set("pairedLocalUrl", redeem.data.paired_local_url);
      return url.toString();
    } catch {
      return redeem.data.hosted_return_url;
    }
  }, [redeem.data]);

  if (!pairingToken) {
    return <div className="page-empty">This pairing link is missing its token.</div>;
  }
  if (redeem.isPending) {
    return <div className="page-loading">Pairing this iPad with the Mac…</div>;
  }
  if (redeem.isError) {
    return (
      <div className="editorial-panel">
        <p className="section-kicker">Pairing failed</p>
        <p className="mt-3 text-sm leading-6 text-[var(--danger)]">
          {redeem.error instanceof LocalControlError ? redeem.error.message : "Pairing could not be completed."}
        </p>
      </div>
    );
  }

  return (
    <div className="editorial-panel">
      <p className="section-kicker">Paired</p>
      <h3 className="mt-3 font-display text-4xl leading-tight text-[var(--ink)]">This iPad is now authorized.</h3>
      <p className="mt-4 text-sm leading-6 text-[var(--muted)]">
        The local-control token is saved on this device. You can stay here to trigger staged jobs on the Mac, or return to the hosted viewer.
      </p>
      {returnUrl ? (
        <a className="primary-button mt-6 w-fit" href={returnUrl}>
          Return to viewer
        </a>
      ) : (
        <Link className="primary-button mt-6 w-fit" to="/overview">
          Open local control
        </Link>
      )}
    </div>
  );
}

type LocalActionKind =
  | "ingest"
  | "fetch_sources"
  | "source_fetch"
  | "lightweight_metadata"
  | "lightweight_scoring"
  | "rebuild_index"
  | "compile_wiki"
  | "health_check"
  | "answer_query"
  | "file_output"
  | "regenerate_brief"
  | "generate_audio"
  | "publish"
  | "sync_vault";

type LocalActionSection = "pipeline_steps" | "convenience_jobs" | "vault_sync" | "codex_jobs";

type ActiveLocalAction = {
  id: string;
  operationRunId?: string;
  kind: LocalActionKind;
  section: LocalActionSection;
  label: string;
  summary: string;
  queuedAt: string;
  startedAt: string | null;
  status: Extract<RunStatus, "pending" | "running">;
  progressSnapshot: {
    activeSourceCount: number | null;
    rawDocumentCount: number | null;
    lightweightPendingCount: number | null;
    lightweightMetadataPendingCount: number | null;
    lightweightScoringPendingCount: number | null;
    staleDocumentCount: number | null;
  };
  briefDate?: string;
  sourceId?: string;
  sourceName?: string;
  maxItems?: number;
};

type LocalActionQueueEntry = ActiveLocalAction & {
  execute: () => Promise<unknown>;
};

type SyntheticOperationRow = {
  id: string;
  title: string;
  summary: string;
  startedAt: string;
  associatedDates: { edition: string | null; coverage: string | null } | null;
  status: RunStatus;
  aiCostUsd: number;
  ttsCostUsd: number;
  totalCostUsd: number;
};

type LocalActionProgressState = {
  label: string;
  detail: string | null;
  fraction: number | null;
};

function formatCountLabel(count: number, noun: string) {
  return `${count} ${noun}${count === 1 ? "" : "s"}`;
}

function clampNumber(value: number, minimum: number, maximum: number) {
  return Math.min(maximum, Math.max(minimum, value));
}

function runMatchesAction(run: IngestionRunHistoryEntry, action: ActiveLocalAction) {
  if (action.operationRunId) {
    return run.id === action.operationRunId;
  }
  if (action.kind === "ingest") {
    return run.trigger === "default_ingest_pipeline";
  }
  if (action.kind === "fetch_sources") {
    return run.operation_kind === "raw_fetch" && run.trigger === "manual_fetch";
  }
  if (action.kind === "source_fetch") {
    if (run.operation_kind !== "raw_fetch" || !triggerStartsWith(run.trigger, "manual_source_fetch")) {
      return false;
    }
    if (!action.sourceId) {
      return true;
    }
    return findRunSourceId(run) === action.sourceId;
  }
  if (action.kind === "lightweight_metadata") {
    return run.operation_kind === "lightweight_enrichment" && run.trigger === "manual_lightweight_metadata";
  }
  if (action.kind === "lightweight_scoring") {
    return run.operation_kind === "lightweight_enrichment" && run.trigger === "manual_lightweight_scoring";
  }
  if (action.kind === "rebuild_index") {
    return run.operation_kind === "vault_index" && run.trigger === "manual_index";
  }
  if (action.kind === "compile_wiki") {
    return run.operation_kind === "advanced_compile" && run.trigger === "manual_advanced_compile";
  }
  if (action.kind === "regenerate_brief") {
    return run.operation_kind === "brief_generation" && run.trigger === "manual_digest";
  }
  if (action.kind === "generate_audio") {
    return run.operation_kind === "audio_generation" && run.trigger === "manual_audio";
  }
  if (action.kind === "publish") {
    return run.operation_kind === "viewer_publish" && run.trigger === "manual_publish";
  }
  if (action.kind === "health_check") {
    return run.operation_kind === "health_check" && run.trigger === "manual_health_check";
  }
  if (action.kind === "answer_query") {
    return run.operation_kind === "answer_query" && run.trigger === "manual_answer_query";
  }
  if (action.kind === "file_output") {
    return run.operation_kind === "file_output" && run.trigger === "manual_file_output";
  }
  if (action.kind === "sync_vault") {
    return run.operation_kind === "vault_sync" && run.trigger === "manual_local_control_sync";
  }
  return false;
}

function matchesActiveAction(run: IngestionRunHistoryEntry, action: ActiveLocalAction) {
  if (action.status !== "running" || !isLiveRunStatus(run.status)) {
    return false;
  }
  return runMatchesAction(run, action);
}

function findLatestMatchingActionRun(action: ActiveLocalAction, runs: IngestionRunHistoryEntry[]) {
  return runs.find((run) => runMatchesAction(run, action)) ?? null;
}

function runLikelyBelongsToAction(run: IngestionRunHistoryEntry, action: ActiveLocalAction) {
  const actionTimestamp = Date.parse(action.startedAt ?? action.queuedAt);
  const runTimestamp = Date.parse(run.started_at);
  if (!Number.isFinite(actionTimestamp) || !Number.isFinite(runTimestamp)) {
    return true;
  }
  return runTimestamp + ACTION_RUN_RECONCILIATION_GRACE_MS >= actionTimestamp;
}

function isStaleTrackedAction(action: ActiveLocalAction, runs: IngestionRunHistoryEntry[]) {
  if (action.status !== "running") {
    return false;
  }
  const actionTimestamp = Date.parse(action.startedAt ?? action.queuedAt);
  if (Number.isFinite(actionTimestamp) && Date.now() - actionTimestamp < ACTION_STALE_RECONCILIATION_DELAY_MS) {
    return false;
  }
  if (runs.some((run) => matchesActiveAction(run, action))) {
    return false;
  }
  const latestRun = findLatestMatchingActionRun(action, runs);
  if (!latestRun || isLiveRunStatus(latestRun.status)) {
    return false;
  }
  return runLikelyBelongsToAction(latestRun, action);
}

function getLocalActionSection(kind: LocalActionKind): LocalActionSection {
  if (
    kind === "fetch_sources"
    || kind === "source_fetch"
    || kind === "lightweight_metadata"
    || kind === "lightweight_scoring"
    || kind === "rebuild_index"
    || kind === "ingest"
  ) {
    return "pipeline_steps";
  }
  if (kind === "regenerate_brief" || kind === "generate_audio" || kind === "publish") {
    return "convenience_jobs";
  }
  if (kind === "sync_vault") {
    return "vault_sync";
  }
  return "codex_jobs";
}

function describeActiveActionDates(action: ActiveLocalAction) {
  if (!action.briefDate) return null;
  return {
    edition: formatBriefDayLabel(action.briefDate),
    coverage: formatBriefDayLabel(shiftIsoDate(action.briefDate, -1)),
  };
}

function buildSyntheticOperationRow(action: ActiveLocalAction): SyntheticOperationRow {
  return {
    id: `synthetic-${action.id}`,
    title: action.label,
    summary: action.summary,
    startedAt: action.startedAt ?? action.queuedAt,
    associatedDates: describeActiveActionDates(action),
    status: action.status,
    aiCostUsd: 0,
    ttsCostUsd: 0,
    totalCostUsd: 0,
  };
}

function findMatchingActionRun(action: ActiveLocalAction, runs: IngestionRunHistoryEntry[]) {
  if (action.operationRunId) {
    return runs.find((run) => run.id === action.operationRunId) ?? null;
  }
  return runs.find((run) => matchesActiveAction(run, action)) ?? null;
}

function describeLightweightStageProgress({
  action,
  stage,
  totalPending,
  remainingPending,
  matchingRun,
}: {
  action: ActiveLocalAction;
  stage: "metadata" | "scoring";
  totalPending: number | null;
  remainingPending: number | null;
  matchingRun: IngestionRunHistoryEntry | null;
}): LocalActionProgressState {
  const runTotal = matchingRun && matchingRun.total_titles > 0 ? matchingRun.total_titles : totalPending;
  const runCompleted = runTotal !== null ? clampNumber(matchingRun?.updated_count ?? 0, 0, runTotal) : 0;
  const lastRunMessage = latestRunMessage(matchingRun);
  const phaseProgress = findLatestLightweightPhaseProgress(matchingRun);
  const relevantPhaseProgress = phaseProgress?.phase === stage ? phaseProgress : null;
  const stageLabel = stage === "metadata" ? "metadata" : "scoring";
  const completionVerb = stage === "metadata" ? "refreshed" : "scored";

  if (
    action.status === "running"
    && matchingRun
    && runCompleted === 0
    && relevantPhaseProgress
  ) {
    const remaining = Math.max(0, relevantPhaseProgress.total - relevantPhaseProgress.completed);
    return {
      label: `${relevantPhaseProgress.completed} of ${relevantPhaseProgress.total} documents processed in ${stageLabel}`,
      detail:
        relevantPhaseProgress.latest
          ? remaining > 0
            ? `${formatCountLabel(remaining, "document")} remaining in ${stageLabel}. Latest: ${relevantPhaseProgress.latest}`
            : `Finalizing ${stageLabel}. Latest: ${relevantPhaseProgress.latest}`
          : remaining > 0
            ? `${formatCountLabel(remaining, "document")} remaining in ${stageLabel}.`
            : `Finalizing ${stageLabel}.`,
      fraction: relevantPhaseProgress.completed / relevantPhaseProgress.total,
    };
  }

  if (
    action.status === "running"
    && matchingRun
    && runTotal !== null
    && (matchingRun.updated_count > 0 || !relevantPhaseProgress)
  ) {
    const remaining = Math.max(0, runTotal - runCompleted);
    return {
      label: `${runCompleted} of ${runTotal} documents ${completionVerb}`,
      detail:
        lastRunMessage
          ? remaining > 0
            ? `${formatCountLabel(remaining, "document")} remaining in this run. Latest: ${lastRunMessage}`
            : `Finalizing the ${stageLabel} run. Latest: ${lastRunMessage}`
          : remaining > 0
            ? `${formatCountLabel(remaining, "document")} remaining in this run.`
            : `Finalizing the ${stageLabel} run.`,
      fraction: runTotal > 0 ? runCompleted / runTotal : null,
    };
  }

  if (totalPending === null || totalPending <= 0) {
    if (stage === "metadata") {
      return {
        label: action.status === "running" ? "Refreshing lightweight metadata" : "Queued metadata refresh",
        detail:
          action.status === "running"
            ? "The local Ollama pass is checking for raw documents that still need metadata."
            : "The metadata refresh will start as soon as the current job completes.",
        fraction: null,
      };
    }
    return {
      label: action.status === "running" ? "Refreshing lightweight scores" : "Queued score refresh",
      detail:
        action.status === "running"
          ? "The local Ollama pass is checking for documents that are ready for new scores."
          : "The scoring refresh will start as soon as the current job completes.",
      fraction: null,
    };
  }

  if (action.status === "pending") {
    return {
      label: `0 of ${totalPending} pending documents ${completionVerb}`,
      detail:
        stage === "metadata"
          ? `Queued to refresh ${formatCountLabel(totalPending, "document")} that still need metadata.`
          : `Queued to score ${formatCountLabel(totalPending, "document")} with current metadata.`,
      fraction: 0,
    };
  }

  const nextRemainingPending = remainingPending ?? totalPending;
  const completed = clampNumber(totalPending - nextRemainingPending, 0, totalPending);
  return {
    label: `${completed} of ${totalPending} pending documents ${completionVerb}`,
    detail:
      stage === "metadata"
        ? `${formatCountLabel(Math.max(0, nextRemainingPending), "document")} still need lightweight metadata.`
        : `${formatCountLabel(Math.max(0, nextRemainingPending), "document")} still need lightweight scoring.`,
    fraction: totalPending > 0 ? completed / totalPending : null,
  };
}

function describeActiveActionProgress(
  action: ActiveLocalAction,
  status: LocalControlStatus | undefined,
  runs: IngestionRunHistoryEntry[],
): LocalActionProgressState | null {
  const matchingRun = findMatchingActionRun(action, runs);

  if (action.kind === "lightweight_metadata") {
    return describeLightweightStageProgress({
      action,
      stage: "metadata",
      totalPending: action.progressSnapshot.lightweightMetadataPendingCount,
      remainingPending: status?.lightweight_metadata_pending_count ?? null,
      matchingRun,
    });
  }

  if (action.kind === "lightweight_scoring") {
    return describeLightweightStageProgress({
      action,
      stage: "scoring",
      totalPending: action.progressSnapshot.lightweightScoringPendingCount,
      remainingPending: status?.lightweight_scoring_pending_count ?? null,
      matchingRun,
    });
  }

  if (action.kind === "fetch_sources") {
    const totalSources = matchingRun?.source_count ?? action.progressSnapshot.activeSourceCount;
    const processedSources = action.status === "running" ? Math.min(totalSources ?? matchingRun?.logs.length ?? 0, matchingRun?.logs.length ?? 0) : 0;
    const newRawDocuments =
      action.status === "running" && action.progressSnapshot.rawDocumentCount !== null && status
        ? Math.max(0, status.raw_document_count - action.progressSnapshot.rawDocumentCount)
        : 0;
    const lastRunMessage = matchingRun?.logs.length ? matchingRun.logs[matchingRun.logs.length - 1]?.message ?? null : null;

    if (totalSources !== null && totalSources > 0) {
      return {
        label: `${processedSources} of ${totalSources} sources processed`,
        detail:
          action.status === "running"
            ? lastRunMessage
              ? `${formatCountLabel(newRawDocuments, "new raw document")} written so far. Latest: ${lastRunMessage}`
              : newRawDocuments > 0
                ? `${formatCountLabel(newRawDocuments, "new raw document")} written so far.`
                : "Waiting for the first source to complete."
            : `Queued to scan ${formatCountLabel(totalSources, "active source")}.`,
        fraction: action.status === "running" ? processedSources / totalSources : 0,
      };
    }

    return {
      label:
        action.status === "running"
          ? newRawDocuments > 0
            ? `${formatCountLabel(newRawDocuments, "new raw document")} written so far`
            : "Fetching active sources"
          : "Queued to fetch sources",
      detail:
        action.status === "running"
          ? lastRunMessage ?? "Waiting for the first source to complete."
          : "The source fetch will start as soon as the current job completes.",
      fraction: null,
    };
  }

  if (action.kind === "source_fetch") {
    const sourceLabel = action.sourceName ?? "This source";
    const requestedLimit = action.maxItems ?? null;
    const lastRunMessage = matchingRun?.logs.length ? matchingRun.logs[matchingRun.logs.length - 1]?.message ?? null : null;
    const touchedDocumentCount = matchingRun ? matchingRun.created_count + matchingRun.updated_count : 0;
    const plannedInputs = matchingRun ? parseNonNegativeInteger(findRunBasicInfoValue(matchingRun, "Inputs planned")) : null;
    const processedInputs = matchingRun ? parseNonNegativeInteger(findRunBasicInfoValue(matchingRun, "Inputs processed")) : null;
    const limitLabel = requestedLimit ? `${requestedLimit} document${requestedLimit === 1 ? "" : "s"}` : "the configured source limit";

    if (plannedInputs !== null && plannedInputs > 0) {
      const completedInputs = clampNumber(processedInputs ?? 0, 0, plannedInputs);
      if (action.status === "pending") {
        return {
          label: `0 of ${plannedInputs} source inputs processed`,
          detail: `Queued to fetch up to ${limitLabel} for ${sourceLabel}.`,
          fraction: 0,
        };
      }
      return {
        label: `${completedInputs} of ${plannedInputs} source inputs processed`,
        detail:
          lastRunMessage
            ? `${formatCountLabel(touchedDocumentCount, "document")} written or refreshed so far. Latest: ${lastRunMessage}`
            : `Fetching up to ${limitLabel}. Lightweight enrichment and index refresh stay manual until you run them from the controls above.`,
        fraction: completedInputs / plannedInputs,
      };
    }

    return {
      label: action.status === "running" ? `${sourceLabel} is fetching on the Mac` : `${sourceLabel} is queued`,
      detail:
        action.status === "running"
          ? lastRunMessage
            ? `${formatCountLabel(touchedDocumentCount, "document")} written or refreshed so far. Latest: ${lastRunMessage}`
            : `Fetching up to ${limitLabel}. Lightweight enrichment and index refresh stay manual until you run them from the controls above.`
          : `Queued to fetch up to ${limitLabel} for ${sourceLabel}.`,
      fraction: null,
    };
  }

  if (action.kind === "rebuild_index") {
    const totalStale = action.progressSnapshot.staleDocumentCount;
    const rawDocumentCount = action.progressSnapshot.rawDocumentCount ?? status?.raw_document_count ?? null;

    if (totalStale !== null && totalStale > 0) {
      if (action.status === "pending") {
        return {
          label: `0 of ${totalStale} stale documents resolved`,
          detail: `Queued to rebuild the index for ${formatCountLabel(totalStale, "stale document")}.`,
          fraction: 0,
        };
      }

      const remainingStale = status?.items_index.stale_document_count ?? totalStale;
      const resolved = clampNumber(totalStale - remainingStale, 0, totalStale);
      return {
        label: `${resolved} of ${totalStale} stale documents resolved`,
        detail:
          rawDocumentCount !== null
            ? `Rebuilding the index across ${formatCountLabel(rawDocumentCount, "raw document")}. Progress lands near the end when the new index is written.`
            : "Rebuilding the items index now.",
        fraction: totalStale > 0 ? resolved / totalStale : null,
      };
    }

    return {
      label:
        rawDocumentCount !== null
          ? `Refreshing the index across ${formatCountLabel(rawDocumentCount, "raw document")}`
          : "Refreshing the items index",
      detail:
        action.status === "running"
          ? "This stage writes the rebuilt index near the end, so progress updates appear late."
          : "Queued to refresh the current index snapshot.",
      fraction: null,
    };
  }

  return {
    label: action.status === "running" ? `${action.label} is running on the Mac` : `${action.label} is queued`,
    detail: action.summary,
    fraction: null,
  };
}

function InlineActionProgress({
  action,
  status,
  runs,
}: {
  action: ActiveLocalAction | null;
  status: LocalControlStatus | undefined;
  runs: IngestionRunHistoryEntry[];
}) {
  if (!action) return null;
  const progress = describeActiveActionProgress(action, status, runs);
  if (!progress) return null;

  const percentage = progress.fraction === null ? null : Math.round(progress.fraction * 100);
  const timeLabel =
    action.status === "running"
      ? `Started ${formatDateTimeLabel(action.startedAt ?? action.queuedAt)}`
      : `Queued ${formatDateTimeLabel(action.queuedAt)}`;

  return (
    <div className="mt-4 rounded-[1.15rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.46)] px-4 py-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex flex-wrap items-center gap-2">
          <RunStatusChip status={action.status} />
          <span className="rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.76)] px-3 py-1 font-mono text-[10px] uppercase tracking-[0.16em] text-[var(--muted)]">
            {timeLabel}
          </span>
        </div>
        <span className="font-mono text-[11px] uppercase tracking-[0.18em] text-[var(--muted)]">
          {percentage === null ? "Live" : `${percentage}%`}
        </span>
      </div>
      <p className="mt-3 text-sm leading-6 text-[var(--muted-strong)]">{progress.label}</p>
      {progress.detail ? <p className="mt-1 text-xs leading-5 text-[var(--muted)]">{progress.detail}</p> : null}
      <div
        aria-valuemax={100}
        aria-valuemin={0}
        aria-valuenow={percentage ?? undefined}
        aria-valuetext={progress.label}
        className="mt-3 h-2.5 overflow-hidden rounded-full bg-[var(--ink)]/10"
        role="progressbar"
      >
        <div
          className={`h-full rounded-full bg-[linear-gradient(90deg,#0e4d64,#2e7f95)] transition-[width] duration-500 ease-out ${
            progress.fraction === null ? "w-[38%] animate-pulse" : ""
          }`}
          style={progress.fraction === null ? undefined : { width: `${percentage}%` }}
        />
      </div>
    </div>
  );
}

function useLocalControlDashboardState() {
  const queryClient = useQueryClient();
  const location = useLocation();
  const localControlToken = getStoredLocalControlToken();
  const allowLocalMacAccess = canUseLocalMacWithoutPairing();
  const [activeActions, setActiveActions] = useState<LocalActionQueueEntry[]>([]);
  const nextActionIdRef = useRef(0);
  const runningActionIdRef = useRef<string | null>(null);
  const hadLiveRunsRef = useRef(false);
  const accessEnabled = Boolean(localControlToken) || allowLocalMacAccess;
  const activeSection = getLocalControlSection(location.pathname);
  const needsStatusQuery =
    accessEnabled
    && (activeSection.path === "/overview" || activeSection.path === "/pipeline" || activeSection.path === "/codex" || activeActions.length > 0);
  const needsInsightsQuery = accessEnabled && activeSection.path === "/insights";
  const needsSourcesQuery = accessEnabled && activeSection.path === "/pipeline";
  const needsBriefAvailabilityQuery = accessEnabled && activeSection.path === "/pipeline";
  const needsOperationsQuery =
    accessEnabled
    && (activeSection.path === "/pipeline" || activeSection.path === "/codex" || activeSection.path === "/operations" || activeActions.length > 0);
  const shouldPollLiveDashboardData = () => {
    if (activeActions.length) {
      return true;
    }
    const cachedRuns =
      (queryClient.getQueryData(["local-control", "operations"]) as
        | { runs: IngestionRunHistoryEntry[] }
        | undefined)?.runs ?? [];
    return hasLiveOperationRuns(cachedRuns);
  };

  const statusQuery = useQuery({
    queryKey: ["local-control", "status"],
    queryFn: localControlClient.getStatus,
    enabled: needsStatusQuery,
    retry: false,
    refetchInterval: () =>
      shouldPollLiveDashboardData() ? LIVE_DASHBOARD_POLL_MS : false,
    refetchIntervalInBackground: true,
  });
  const insightsQuery = useQuery({
    queryKey: ["local-control", "insights"],
    queryFn: localControlClient.getInsights,
    enabled: needsInsightsQuery,
    retry: false,
  });
  const sourcesQuery = useQuery({
    queryKey: ["local-control", "sources"],
    queryFn: localControlClient.getSources,
    enabled: needsSourcesQuery,
    retry: false,
    refetchInterval: () =>
      shouldPollLiveDashboardData() ? LIVE_DASHBOARD_POLL_MS : false,
    refetchIntervalInBackground: true,
  });
  const briefAvailabilityQuery = useQuery({
    queryKey: ["local-control", "briefs", "availability"],
    queryFn: localControlClient.getBriefAvailability,
    enabled: needsBriefAvailabilityQuery,
    retry: false,
    refetchInterval: () =>
      shouldPollLiveDashboardData() ? LIVE_DASHBOARD_POLL_MS : false,
    refetchIntervalInBackground: true,
  });
  const activeSourceCount = sourcesQuery.data?.filter((source) => source.active).length ?? null;

  const buildProgressSnapshot = () => ({
    activeSourceCount,
    rawDocumentCount: statusQuery.data?.raw_document_count ?? null,
    lightweightPendingCount: statusQuery.data?.lightweight_pending_count ?? null,
    lightweightMetadataPendingCount: statusQuery.data?.lightweight_metadata_pending_count ?? null,
    lightweightScoringPendingCount: statusQuery.data?.lightweight_scoring_pending_count ?? null,
    staleDocumentCount: statusQuery.data?.items_index.stale_document_count ?? null,
  });
  const operationsQuery = useQuery({
    queryKey: ["local-control", "operations"],
    queryFn: localControlClient.getOperations,
    enabled: needsOperationsQuery,
    retry: false,
    refetchInterval: (query) => {
      const runs = (query.state.data as { runs: IngestionRunHistoryEntry[] } | undefined)?.runs;
      return activeActions.length || hasLiveOperationRuns(runs)
        ? LIVE_DASHBOARD_POLL_MS
        : false;
    },
    refetchIntervalInBackground: true,
  });

  const refresh = async () => {
    await queryClient.invalidateQueries({ queryKey: ["local-control"] });
  };

  const executeTrackedAction = async (action: LocalActionQueueEntry) => {
    try {
      await queryClient.invalidateQueries({ queryKey: ["local-control", "operations"] });
      await action.execute();
    } catch {
      // Mutation state still captures the error for the feedback banner.
    } finally {
      try {
        await refresh();
      } finally {
        setActiveActions((current) => current.filter((entry) => entry.id !== action.id));
        if (runningActionIdRef.current === action.id) {
          runningActionIdRef.current = null;
        }
      }
    }
  };

  useEffect(() => {
    if (runningActionIdRef.current) return;

    const currentRunningAction = activeActions.find((action) => action.status === "running");
    if (currentRunningAction) {
      runningActionIdRef.current = currentRunningAction.id;
      void executeTrackedAction(currentRunningAction);
      return;
    }

    const nextQueuedAction = activeActions.find((action) => action.status === "pending");
    if (!nextQueuedAction) return;
    if (statusQuery.isFetching || sourcesQuery.isFetching) return;

    const startedAt = new Date().toISOString();
    const progressSnapshot = buildProgressSnapshot();
    const actionToRun: LocalActionQueueEntry = {
      ...nextQueuedAction,
      status: "running",
      startedAt,
      progressSnapshot,
    };

    runningActionIdRef.current = nextQueuedAction.id;
    setActiveActions((current) =>
      current.map((entry) =>
        entry.id === nextQueuedAction.id ? { ...entry, status: "running", startedAt, progressSnapshot } : entry,
      ),
    );
    void executeTrackedAction(actionToRun);
  }, [
    activeActions,
    queryClient,
    sourcesQuery.isFetching,
    statusQuery.isFetching,
    activeSourceCount,
    statusQuery.data,
  ]);

  useEffect(() => {
    const hasLiveRuns = hasLiveOperationRuns(operationsQuery.data?.runs);
    if (hadLiveRunsRef.current && !hasLiveRuns) {
      void queryClient.invalidateQueries({ queryKey: ["local-control"] });
    }
    hadLiveRunsRef.current = hasLiveRuns;
  }, [operationsQuery.data?.runs, queryClient]);

  useEffect(() => {
    const runs = operationsQuery.data?.runs ?? [];
    if (!activeActions.length || !runs.length) {
      return;
    }
    const staleActionIds = new Set(
      activeActions
        .filter((action) => isStaleTrackedAction(action, runs))
        .map((action) => action.id),
    );
    if (!staleActionIds.size) {
      return;
    }
    setActiveActions((current) => current.filter((entry) => !staleActionIds.has(entry.id)));
    if (runningActionIdRef.current && staleActionIds.has(runningActionIdRef.current)) {
      runningActionIdRef.current = null;
    }
    void queryClient.invalidateQueries({ queryKey: ["local-control"] });
  }, [activeActions, operationsQuery.data?.runs, queryClient]);

  const runTrackedAction = <T,>(
    action: Omit<ActiveLocalAction, "id" | "operationRunId" | "queuedAt" | "startedAt" | "status" | "section" | "progressSnapshot">,
    execute: () => Promise<T>,
  ) => {
    const queuedAt = new Date().toISOString();
    const actionId = `${action.kind}-${nextActionIdRef.current++}`;
    const progressSnapshot = buildProgressSnapshot();
    let shouldStartImmediately = false;

    setActiveActions((current) => {
      shouldStartImmediately = current.length === 0;
      return [
        ...current,
        {
          ...action,
          id: actionId,
          section: getLocalActionSection(action.kind),
          queuedAt,
          startedAt: shouldStartImmediately ? queuedAt : null,
          status: shouldStartImmediately ? "running" : "pending",
          progressSnapshot,
          execute,
        },
      ];
    });
  };

  const stopTrackedAction = (actionId: string) => {
    const action = activeActions.find((entry) => entry.id === actionId) ?? null;
    if (!action || action.status === "running") {
      return false;
    }
    setActiveActions((current) => current.filter((entry) => entry.id !== actionId));
    return true;
  };

  const fetchSources = useMutation({ mutationFn: localControlClient.runFetchSources, onSuccess: refresh });
  const sourceFetch = useMutation({
    mutationFn: (payload: { sourceId: string; max_items?: number; alphaxiv_sort?: AlphaXivSort }) =>
      localControlClient.runSourcePipeline(payload.sourceId, {
        max_items: payload.max_items,
        alphaxiv_sort: payload.alphaxiv_sort,
      }),
    onSuccess: refresh,
  });
  const stopSourceFetch = useMutation({
    mutationFn: (sourceId: string) => localControlClient.stopSourcePipeline(sourceId),
    onSuccess: refresh,
  });
  const lightweightMetadata = useMutation({ mutationFn: localControlClient.runLightweightMetadata, onSuccess: refresh });
  const lightweightScoring = useMutation({ mutationFn: localControlClient.runLightweightScoring, onSuccess: refresh });
  const stopLightweight = useMutation({ mutationFn: localControlClient.stopLightweightEnrich, onSuccess: refresh });
  const index = useMutation({ mutationFn: localControlClient.runRebuildItemsIndex, onSuccess: refresh });
  const compileWiki = useMutation({
    mutationFn: (payload?: { limit?: number }) => localControlClient.runAdvancedCompile(payload),
    onSuccess: refresh,
  });
  const healthCheck = useMutation({
    mutationFn: (payload: { scope?: "vault" | "wiki" | "raw"; topic?: string }) => localControlClient.runHealthCheck(payload),
    onSuccess: refresh,
  });
  const answerQuery = useMutation({
    mutationFn: (payload: { question: string; output_kind: "answer" | "slides" | "chart" }) => localControlClient.runAnswerQuery(payload),
    onSuccess: refresh,
  });
  const fileOutput = useMutation({
    mutationFn: (payload: { path: string }) => localControlClient.runFileOutput(payload),
    onSuccess: refresh,
  });
  const ingest = useMutation({ mutationFn: localControlClient.runIngest, onSuccess: refresh });
  const regenerate = useMutation({ mutationFn: (briefDate?: string) => localControlClient.runRegenerateBrief(briefDate), onSuccess: refresh });
  const audio = useMutation({ mutationFn: (briefDate?: string) => localControlClient.runGenerateAudio(briefDate), onSuccess: refresh });
  const publish = useMutation({ mutationFn: (briefDate?: string) => localControlClient.runPublish(briefDate), onSuccess: refresh });
  const syncVault = useMutation({ mutationFn: localControlClient.runSyncVault, onSuccess: refresh });

  const mutations = [
    fetchSources,
    sourceFetch,
    stopSourceFetch,
    lightweightMetadata,
    lightweightScoring,
    stopLightweight,
    index,
    compileWiki,
    healthCheck,
    answerQuery,
    fileOutput,
    ingest,
    regenerate,
    audio,
    publish,
    syncVault,
  ];
  const latestMutation = [...mutations].reverse().find((mutation) => mutation.isSuccess)?.data;
  const latestError = [...mutations].reverse().find((mutation) => mutation.isError)?.error;
  const anyBusy = mutations.some((mutation) => mutation.isPending);

  return {
    localControlToken,
    allowLocalMacAccess,
    statusQuery,
    insightsQuery,
    sourcesQuery,
    activeSourceCount,
    briefAvailabilityQuery,
    operationsQuery,
    activeActions,
    refresh,
    runTrackedAction,
    stopTrackedAction,
    fetchSources,
    sourceFetch,
    stopSourceFetch,
    lightweightMetadata,
    lightweightScoring,
    stopLightweight,
    index,
    compileWiki,
    healthCheck,
    answerQuery,
    fileOutput,
    ingest,
    regenerate,
    audio,
    publish,
    syncVault,
    latestMutation,
    latestError,
    anyBusy,
  };
}

type LocalDashboardContext = ReturnType<typeof useLocalControlDashboardState>;

function useLocalDashboard() {
  return useOutletContext<LocalDashboardContext>();
}

function LocalDashboardLayout() {
  const dashboard = useLocalControlDashboardState();
  const { localControlToken, allowLocalMacAccess, latestMutation, latestError } = dashboard;

  if (!localControlToken && !allowLocalMacAccess) {
    return <div className="page-empty">This iPad is not paired yet. Open a pairing link from the Mac.</div>;
  }

  return (
    <div className="space-y-8 pb-10">
      {latestMutation || latestError ? (
        <section className="editorial-panel">
          <p className="section-kicker">Run feedback</p>
          {latestMutation ? <p className="mt-3 text-sm leading-6 text-[#166534]">{latestMutation.detail}</p> : null}
          {latestError ? <p className="mt-3 text-sm leading-6 text-[var(--danger)]">{describeRequestError(latestError, "A local-control action failed.")}</p> : null}
        </section>
      ) : null}
      <Outlet context={dashboard} />
    </div>
  );
}

function OverviewTab() {
  const { statusQuery } = useLocalDashboard();

  if (statusQuery.isLoading && !statusQuery.data) {
    return <OverviewTabSkeleton />;
  }
  if (statusQuery.isError && !statusQuery.data) {
    return (
      <div className="editorial-panel">
        <p className="section-kicker">Local control unavailable</p>
        <p className="mt-3 text-sm leading-6 text-[var(--danger)]">{describeStatusError(statusQuery.error)}</p>
      </div>
    );
  }
  if (!statusQuery.data) {
    return <div className="page-empty">Local control status is unavailable.</div>;
  }

  const status = statusQuery.data;
  const lightweightMetadataPendingCount = status.lightweight_metadata_pending_count ?? 0;
  const lightweightScoringPendingCount = status.lightweight_scoring_pending_count ?? 0;
  const lightweightBacklogCount = lightweightMetadataPendingCount + lightweightScoringPendingCount;
  const indexUpToDate = status.items_index.up_to_date;
  const currentBriefLabel = formatBriefDayLabel(status.current_brief_date);
  const latestPublicationTime = status.latest_publication ? formatTimestamp(status.latest_publication.published_at) : "Not published yet";
  const latestPublicationTitle = status.latest_publication?.title ?? "Run Publish viewer to materialize the first bundle.";
  const pairedHost = formatEndpointHost(status.paired_local_url);
  const monoValueClassName =
    "inline-block max-w-full break-all rounded-[0.9rem] bg-white/72 px-2.5 py-1 font-mono text-[12px] text-[var(--muted-strong)]";
  const publicationTone = status.latest_publication ? "success" : "default";
  const ollamaTone = !status.ollama ? "default" : status.ollama.available ? "success" : "warning";
  const gitReady =
    Boolean(status.vault_sync?.repo_ready)
    && !status.vault_sync?.has_uncommitted_changes
    && (status.vault_sync?.ahead_count ?? 0) === 0
    && (status.vault_sync?.behind_count ?? 0) === 0;
  const gitTone =
    !status.vault_sync || status.vault_sync.enabled === false
      ? "default"
      : gitReady
        ? "success"
        : "warning";
  const codexTone = !status.codex ? "default" : status.codex.available && status.codex.authenticated ? "success" : "warning";
  const gitChipLabel =
    !status.vault_sync
      ? "Git not reported"
      : !status.vault_sync.enabled
        ? "Sync disabled"
        : gitReady
          ? "Sync ready"
          : status.vault_sync.has_uncommitted_changes
            ? "Changes pending"
            : "Needs attention";
  const gitSummary = !status.vault_sync
    ? "The Mac has not reported vault sync metadata yet."
    : !status.vault_sync.enabled
      ? "Local-control sync is configured off for this machine."
      : status.vault_sync.repo_ready
        ? status.vault_sync.current_summary ?? "Selective raw, brief, and viewer sync is ready from this repository."
        : "Repository metadata is present, but the sync target is not ready yet.";
  const remoteValue = status.vault_sync?.remote_url ? (
    <div className="space-y-2">
      <p>{status.vault_sync.remote_name ?? "Configured remote"}</p>
      <code className={monoValueClassName}>{status.vault_sync.remote_url}</code>
    </div>
  ) : (
    status.vault_sync?.remote_name ?? "Not configured"
  );
  const workingTreeValue = !status.vault_sync
    ? "Not reported"
    : status.vault_sync.has_uncommitted_changes
      ? `${status.vault_sync.changed_files.toLocaleString()} local file${status.vault_sync.changed_files === 1 ? "" : "s"} changed`
      : "Working tree clean";
  const ollamaSummary = !status.ollama
    ? "Lightweight enrichment status has not been reported yet."
    : status.ollama.available
      ? "Handles the fast local pass for tags, authors, short summaries, and lightweight scores."
      : status.ollama.detail ?? "Fetch and index can still run, but lightweight enrichment will fail fast until Ollama is reachable.";
  const codexSummary = !status.codex
    ? "Advanced synthesis status has not been reported yet."
    : status.codex.available
      ? status.codex.authenticated
        ? "Used for wiki compilation, health checks, question answering, and durable filing back into the vault."
        : status.codex.detail ?? "Codex is installed, but this machine still needs authentication."
      : status.codex.detail ?? "Codex CLI is unavailable on this Mac.";

  return (
    <div className="space-y-8">
      <section className="editorial-panel relative overflow-hidden">
        <div aria-hidden className="pointer-events-none absolute -left-10 bottom-0 h-32 w-32 rounded-full bg-[var(--teal)]/10 blur-3xl" />
        <div aria-hidden className="pointer-events-none absolute -right-8 top-0 h-36 w-36 rounded-full bg-[var(--accent)]/10 blur-3xl" />
        <div className="relative flex flex-col gap-6 xl:flex-row xl:items-end xl:justify-between">
          <div className="min-w-0 flex-1">
            <p className="section-kicker">Device</p>
            <h3 className="mt-3 max-w-3xl font-display text-4xl leading-[0.96] text-[var(--ink)] sm:text-5xl">
              {status.device_label}
            </h3>
            <p className="mt-4 max-w-2xl text-sm leading-6 text-[var(--muted)]">
              Fast readout for the paired machine, the active briefing date, and the local services behind fetch, indexing, and publication.
            </p>
            <div className="mt-5 flex flex-wrap gap-2">
              <StatusChip label={`Paired ${pairedHost}`} tone="default" />
              <StatusChip label={status.latest_publication ? "Publication ready" : "No publication yet"} tone={publicationTone} />
              <StatusChip
                label={indexUpToDate ? "Index current" : `${status.items_index.stale_document_count.toLocaleString()} stale`}
                tone={indexUpToDate ? "success" : "warning"}
              />
              <StatusChip
                label={lightweightBacklogCount ? `${lightweightBacklogCount.toLocaleString()} pending lightweight` : "No lightweight backlog"}
                tone={lightweightBacklogCount ? "warning" : "success"}
              />
            </div>
          </div>

          <div className="w-full max-w-md rounded-[1.75rem] border border-[var(--accent)]/14 bg-[linear-gradient(180deg,rgba(255,247,237,0.96),rgba(255,255,255,0.78))] px-5 py-5 shadow-[0_20px_48px_rgba(154,52,18,0.08)]">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <p className="field-label">Current brief</p>
              <StatusChip label={status.latest_publication ? "Published" : "Awaiting publish"} tone={publicationTone} />
            </div>
            <p className="mt-4 font-display text-4xl leading-[0.94] text-[var(--ink)]">{currentBriefLabel}</p>
            <p className="mt-3 text-sm leading-6 text-[var(--muted-strong)]">
              {status.latest_publication
                ? `Latest bundle published ${latestPublicationTime}.`
                : "The brief day is set, but a viewer bundle has not been published from this machine yet."}
            </p>
            <div className="mt-5 border-t border-[var(--ink)]/8 pt-4">
              <p className="field-label">Latest bundle</p>
              <p className="mt-2 text-lg font-semibold text-[var(--ink)]">{latestPublicationTitle}</p>
            </div>
          </div>
        </div>
      </section>

      <section className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
        <OverviewMetricCard
          detail="Canonical source captures already written to the vault."
          label="Raw documents"
          value={status.raw_document_count.toLocaleString()}
        />
        <OverviewMetricCard
          detail={
            lightweightBacklogCount
              ? `${lightweightMetadataPendingCount.toLocaleString()} waiting on metadata and ${lightweightScoringPendingCount.toLocaleString()} waiting on scoring.`
              : "Nothing is waiting on the lightweight pass right now."
          }
          label="Lightweight backlog"
          tone={lightweightBacklogCount ? "warning" : "success"}
          value={lightweightBacklogCount.toLocaleString()}
        />
        <OverviewMetricCard
          detail={
            indexUpToDate
              ? status.items_index.generated_at
                ? `Snapshot refreshed ${formatTimestamp(status.items_index.generated_at)}.`
                : "Viewer and filters are aligned with the current vault."
              : "Search, filters, and brief inputs need a rebuild."
          }
          label="Indexed items"
          tone={indexUpToDate ? "success" : "default"}
          value={status.items_index.indexed_item_count.toLocaleString()}
        />
        <OverviewMetricCard
          detail={indexUpToDate ? "Every known raw document is represented in the current index." : "Newer documents are not yet visible downstream."}
          label="Stale in index"
          tone={indexUpToDate ? "default" : "error"}
          value={status.items_index.stale_document_count.toLocaleString()}
        />
        <OverviewMetricCard
          detail={`${status.topic_count.toLocaleString()} canonical topics are currently tracked.`}
          label="Wiki pages"
          value={status.wiki_page_count.toLocaleString()}
        />
        <OverviewMetricCard
          detail={
            status.rising_topic_count
              ? "Momentum clusters are surfacing in the graph right now."
              : "No rising-topic spikes have been detected in the current snapshot."
          }
          label="Rising topics"
          tone={status.rising_topic_count ? "warning" : "default"}
          value={status.rising_topic_count.toLocaleString()}
        />
      </section>

      <section className="grid gap-4 xl:grid-cols-[minmax(0,1.08fr)_minmax(0,0.92fr)]">
        <OverviewDetailCard
          adornment={<StatusChip label={`Host ${pairedHost}`} tone="default" />}
          description="These are the concrete handoff points between the browser, the vault, and the published viewer bundle."
          kicker="Access"
          title="Paths and endpoints"
        >
          <OverviewFactRows
            items={[
              {
                label: "Paired local URL",
                value: (
                  <a className="inline-block max-w-full" href={status.paired_local_url} rel="noreferrer" target="_blank">
                    <code className={monoValueClassName}>{status.paired_local_url}</code>
                  </a>
                ),
              },
              {
                label: "Vault root",
                value: <code className={monoValueClassName}>{status.vault_root_dir}</code>,
              },
              {
                label: "Viewer bundle",
                value: <code className={monoValueClassName}>{status.viewer_bundle_dir}</code>,
              },
            ]}
          />
        </OverviewDetailCard>

        <OverviewDetailCard
          adornment={<StatusChip label={status.latest_publication ? "Bundle ready" : "Awaiting publish"} tone={publicationTone} />}
          description="The active brief day, its latest published bundle, and the most recent brief artifacts are surfaced here."
          kicker="Publishing state"
          title="Current edition window"
          tone={publicationTone}
        >
          <OverviewFactRows
            items={[
              {
                label: "Current brief",
                value: currentBriefLabel,
              },
              {
                label: "Latest publication",
                value: latestPublicationTime,
              },
              {
                label: "Bundle title",
                value: latestPublicationTitle,
              },
              {
                label: "Latest brief folder",
                value: status.latest_brief_dir ? <code className={monoValueClassName}>{status.latest_brief_dir}</code> : "Not materialized yet",
              },
            ]}
          />
        </OverviewDetailCard>
      </section>

      {status.vault_sync || status.ollama || status.codex ? (
        <section className="space-y-4">
          <SectionIntro
            description="Operational knobs and machine-reported runtime state, surfaced as compact facts instead of longer diagnostics."
            kicker="Runtime health"
            title="Services and sync posture"
          />
          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
            {status.vault_sync ? (
              <OverviewDetailCard
                adornment={<StatusChip label={gitChipLabel} tone={gitTone} />}
                description={gitSummary}
                kicker="Git transport"
                title={
                  status.vault_sync.enabled === false
                    ? "Sync disabled"
                    : status.vault_sync.repo_ready
                      ? "Repository ready"
                      : "Repository not ready"
                }
                tone={gitTone}
              >
                <OverviewFactRows
                  items={[
                    {
                      label: "Branch",
                      value: status.vault_sync.branch ?? "Not reported",
                    },
                    {
                      label: "Remote",
                      value: remoteValue,
                    },
                    {
                      label: "Commit",
                      value: status.vault_sync.current_commit
                        ? <code className={monoValueClassName}>{formatCommitHash(status.vault_sync.current_commit)}</code>
                        : "Unknown",
                    },
                    {
                      label: "Working tree",
                      detail: `Ahead ${status.vault_sync.ahead_count.toLocaleString()} · Behind ${status.vault_sync.behind_count.toLocaleString()}`,
                      value: workingTreeValue,
                    },
                  ]}
                />
              </OverviewDetailCard>
            ) : null}

            {status.ollama ? (
              <OverviewDetailCard
                adornment={<StatusChip label={status.ollama.available ? "Ready" : "Unavailable"} tone={ollamaTone} />}
                description={ollamaSummary}
                kicker="Lightweight runtime"
                title={status.ollama.available ? status.ollama.model ?? "Ollama ready" : "Ollama unavailable"}
                tone={ollamaTone}
              >
                <OverviewFactRows
                  items={[
                    {
                      label: "Model",
                      value: status.ollama.model ?? "Not reported",
                    },
                    {
                      label: "Pending queue",
                      detail: lightweightBacklogCount
                        ? `${lightweightMetadataPendingCount.toLocaleString()} need metadata and ${lightweightScoringPendingCount.toLocaleString()} are ready for scoring.`
                        : "No raw documents are queued for the lightweight pass.",
                      value: `${lightweightBacklogCount.toLocaleString()} document${lightweightBacklogCount === 1 ? "" : "s"}`,
                    },
                    {
                      label: "Role",
                      value: "Metadata and scoring",
                    },
                  ]}
                />
              </OverviewDetailCard>
            ) : null}

            {status.codex ? (
              <OverviewDetailCard
                adornment={
                  <StatusChip
                    label={status.codex.available ? (status.codex.authenticated ? "Authenticated" : "Needs auth") : "Unavailable"}
                    tone={codexTone}
                  />
                }
                description={codexSummary}
                kicker="Advanced runtime"
                title={
                  status.codex.available
                    ? status.codex.authenticated
                      ? status.codex.model ?? status.codex.profile ?? "Codex ready"
                      : "Authentication required"
                    : "Codex unavailable"
                }
                tone={codexTone}
              >
                <OverviewFactRows
                  items={[
                    {
                      label: "Profile / binary",
                      value: status.codex.profile
                        ? status.codex.profile
                        : status.codex.binary
                          ? <code className={monoValueClassName}>{status.codex.binary}</code>
                          : "Not reported",
                    },
                    {
                      label: "Web search",
                      value: status.codex.search_enabled ? "Enabled" : "Disabled",
                    },
                    {
                      label: "Timeout",
                      value: status.codex.timeout_minutes !== null ? `${status.codex.timeout_minutes} min` : "Not reported",
                    },
                    {
                      label: "Compile batch",
                      value: status.codex.compile_batch_size !== null ? status.codex.compile_batch_size.toLocaleString() : "Not reported",
                    },
                  ]}
                />
              </OverviewDetailCard>
            ) : null}
          </div>
        </section>
      ) : null}

      {statusQuery.isError ? (
        <div className="rounded-[1.5rem] border border-[var(--danger)]/18 bg-[rgba(255,255,255,0.52)] px-5 py-4 text-sm leading-6 text-[var(--danger)]">
          {describeStatusError(statusQuery.error)}
        </div>
      ) : null}
    </div>
  );
}

function InsightsTab() {
  const { insightsQuery } = useLocalDashboard();

  if (insightsQuery.isLoading && !insightsQuery.data) {
    return <InsightsTabSkeleton />;
  }
  if (insightsQuery.isError) {
    return (
      <div className="editorial-panel">
        <p className="section-kicker">Insight radar unavailable</p>
        <p className="mt-3 text-sm leading-6 text-[var(--danger)]">
          {describeRequestError(insightsQuery.error, "The insight radar could not be loaded.")}
        </p>
      </div>
    );
  }

  const insights = insightsQuery.data;
  const risingTopics = insights?.rising_topics.slice(0, 6) ?? [];

  return (
    <div className="space-y-8">
      <section className="editorial-panel">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <p className="section-kicker">Insight radar</p>
            <h3 className="section-title">What is moving in the research graph</h3>
            <p className="mt-3 max-w-3xl text-sm leading-6 text-[var(--muted)]">
              The deterministic insight layer tracks canonical topics, rising clusters, and their supporting source-note coverage. Use this tab to decide where Codex should synthesize next.
            </p>
          </div>
          {insights ? (
            <div className="rounded-[1.5rem] border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.52)] px-5 py-5">
              <p className="field-label">Vault pages</p>
              <p className="mt-2 text-sm leading-6 text-[var(--muted)]">Map: {insights.map_page ?? "Not materialized yet"}</p>
              <p className="text-sm leading-6 text-[var(--muted)]">Trend radar: {insights.trends_page ?? "Not materialized yet"}</p>
            </div>
          ) : null}
        </div>

        <div className="mt-6 grid gap-4 md:grid-cols-2 xl:grid-cols-3">
          {risingTopics.length ? (
            risingTopics.map((topic) => (
              <article
                className="rounded-[1.5rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.52)] px-5 py-5"
                key={topic.id}
              >
                <div className="flex flex-wrap items-center gap-2">
                  <StatusChip label={`7d ${topic.recent_item_count_7d}`} tone="warning" />
                  <StatusChip label={`sources ${topic.source_diversity}`} tone="default" />
                </div>
                <h4 className="mt-4 text-2xl font-semibold text-[var(--ink)]">{topic.label}</h4>
                <p className="mt-3 text-sm leading-6 text-[var(--muted)]">
                  Trend {topic.trend_score.toFixed(2)} · Novelty {topic.novelty_score.toFixed(2)} · 30d {topic.recent_item_count_30d} · Total {topic.total_item_count}
                </p>
                <p className="mt-3 text-xs leading-5 text-[var(--muted)]">
                  {topic.related_topic_ids.length ? `Related: ${topic.related_topic_ids.slice(0, 3).join(", ")}` : "Canonical topic page ready for Codex synthesis."}
                </p>
              </article>
            ))
          ) : (
            <p className="text-sm leading-6 text-[var(--muted)]">
              No insight topics have been materialized yet. Run fetch, lightweight enrich, and rebuild the index to generate the topic map.
            </p>
          )}
        </div>
      </section>
    </div>
  );
}

function PipelineTab() {
  const dashboard = useLocalDashboard();
  const { statusQuery } = dashboard;

  if (statusQuery.isLoading && !statusQuery.data) {
    return <PipelineTabSkeleton />;
  }
  if (statusQuery.isError) {
    return (
      <div className="editorial-panel">
        <p className="section-kicker">Pipeline status unavailable</p>
        <p className="mt-3 text-sm leading-6 text-[var(--danger)]">{describeStatusError(statusQuery.error)}</p>
      </div>
    );
  }
  if (!statusQuery.data) {
    return <div className="page-empty">Pipeline status is unavailable.</div>;
  }

  return <PipelineTabContent dashboard={dashboard} status={statusQuery.data} />;
}

function PipelineTabContent({
  dashboard,
  status,
}: {
  dashboard: LocalDashboardContext;
  status: LocalControlStatus;
}) {
  const {
    sourcesQuery,
    briefAvailabilityQuery,
    operationsQuery,
    activeActions,
    runTrackedAction,
    stopTrackedAction,
    fetchSources,
    sourceFetch,
    stopSourceFetch,
    lightweightMetadata,
    lightweightScoring,
    stopLightweight,
    index,
    regenerate,
    audio,
    publish,
    syncVault,
  } = dashboard;
  const runs = operationsQuery.data?.runs ?? [];
  const [sourceFetchOverrides, setSourceFetchOverrides] = useState<Record<string, string>>({});
  const [sourceFetchAlphaXivLikes, setSourceFetchAlphaXivLikes] = useState<Record<string, boolean>>({});
  const [sourceFetchNotice, setSourceFetchNotice] = useState<{ tone: "error" | "success"; message: string } | null>(null);
  const [stoppingSourceId, setStoppingSourceId] = useState<string | null>(null);
  const latestSuccessfulFetchRun =
    (operationsQuery.data?.runs ?? []).find(
      (run) => run.operation_kind === "raw_fetch" && run.trigger === "manual_fetch" && run.status === "succeeded",
    ) ?? null;
  const latestSuccessfulFetchAt = latestSuccessfulFetchRun?.finished_at ?? latestSuccessfulFetchRun?.started_at ?? null;
  const fetchUpdatedToday = latestSuccessfulFetchAt ? isSameLocalDay(latestSuccessfulFetchAt) : false;
  const fetchTone = fetchUpdatedToday ? "success" : "error";
  const fetchSubtitle = latestSuccessfulFetchAt
    ? fetchUpdatedToday
      ? `Discover website posts and newsletter issues, write canonical raw documents, and do deterministic newsletter decomposition only. Last successful fetch completed today at ${formatLogTimeLabel(latestSuccessfulFetchAt)}.`
      : `Discover website posts and newsletter issues, write canonical raw documents, and do deterministic newsletter decomposition only. Last successful fetch completed ${formatTimestamp(latestSuccessfulFetchAt)}.`
    : "Discover website posts and newsletter issues, write canonical raw documents, and do deterministic newsletter decomposition only. No successful fetch has been recorded yet.";
  const lightweightMetadataPendingCount = status.lightweight_metadata_pending_count ?? 0;
  const lightweightScoringPendingCount = status.lightweight_scoring_pending_count ?? 0;
  const lightweightReady = Boolean(status.ollama?.available);
  const metadataButtonLabel =
    lightweightMetadataPendingCount > 0
      ? `Refresh metadata (${lightweightMetadataPendingCount})`
      : "Refresh metadata";
  const metadataSubtitle =
    lightweightMetadataPendingCount > 0
      ? lightweightReady
        ? `${lightweightMetadataPendingCount} raw document${lightweightMetadataPendingCount === 1 ? "" : "s"} need tags, authors, or short summaries refreshed because their metadata hash is missing or stale.`
        : `${lightweightMetadataPendingCount} raw document${lightweightMetadataPendingCount === 1 ? "" : "s"} need metadata refresh, but Ollama is unavailable on this Mac right now.`
      : lightweightReady
        ? "All raw documents have current lightweight metadata. Run this only if you want to force a metadata refresh."
        : "No raw documents are waiting on metadata refresh. Ollama is currently unavailable for new work.";
  const scoringButtonLabel =
    lightweightScoringPendingCount > 0
      ? `Generate scores (${lightweightScoringPendingCount})`
      : "Generate scores";
  const scoringSubtitle =
    lightweightScoringPendingCount > 0
      ? lightweightReady
        ? `${lightweightScoringPendingCount} raw document${lightweightScoringPendingCount === 1 ? "" : "s"} have current metadata but still need lightweight scores refreshed because their scoring hash is missing or stale.`
        : `${lightweightScoringPendingCount} raw document${lightweightScoringPendingCount === 1 ? "" : "s"} are ready for scoring, but Ollama is unavailable on this Mac right now.`
      : lightweightReady
        ? "All metadata-current documents already have current lightweight scores."
        : "No documents are waiting on scoring right now. Ollama is currently unavailable for new work.";
  const indexUpToDate = status.items_index.up_to_date;
  const indexStaleCount = status.items_index.stale_document_count;
  const indexTone = indexUpToDate ? "success" : "error";
  const indexSubtitle = indexUpToDate
    ? `The items index is current${status.items_index.generated_at ? ` as of ${formatTimestamp(status.items_index.generated_at)}` : ""}. Rebuild only if you want to force a refresh.`
    : `The items index is stale for ${indexStaleCount} document${indexStaleCount === 1 ? "" : "s"}. Rebuild it to refresh the inbox, filters, brief inputs, and viewer lookups.`;
  const defaultBriefDate =
    briefAvailabilityQuery.data?.default_day
    ?? status.current_brief_date
    ?? briefAvailabilityQuery.data?.days.at(-1)?.brief_date
    ?? "";
  const briefDateInput = useDefaultedDateInput(defaultBriefDate);
  const activeBriefDate = briefDateInput.value;
  const currentEditionTarget = briefAvailabilityQuery.data?.default_day ?? status.current_brief_date ?? null;
  const currentEditionSelected = Boolean(currentEditionTarget && activeBriefDate === currentEditionTarget);
  const currentEditionOutputState = useMemo(
    () => ({
      brief: currentEditionSelected && hasSuccessfulEditionRun(runs, "brief_generation", currentEditionTarget),
      audio: currentEditionSelected && hasSuccessfulEditionRun(runs, "audio_generation", currentEditionTarget),
      viewer:
        currentEditionSelected
        && (
          hasSuccessfulEditionRun(runs, "viewer_publish", currentEditionTarget)
          || status.latest_publication?.brief_date === currentEditionTarget
        ),
    }),
    [currentEditionSelected, currentEditionTarget, runs, status.latest_publication?.brief_date],
  );
  const fetchActions = activeActions.filter((action) => action.kind === "fetch_sources");
  const activeFetchAction = preferLocalAction(
    fetchActions,
    findObservedDashboardAction(runs, "fetch_sources"),
  );
  const lightweightMetadataActions = activeActions.filter((action) => action.kind === "lightweight_metadata");
  const activeLightweightMetadataAction = preferLocalAction(
    lightweightMetadataActions,
    findObservedDashboardAction(runs, "lightweight_metadata"),
  );
  const lightweightScoringActions = activeActions.filter((action) => action.kind === "lightweight_scoring");
  const activeLightweightScoringAction = preferLocalAction(
    lightweightScoringActions,
    findObservedDashboardAction(runs, "lightweight_scoring"),
  );
  const metadataLoading = stopLightweight.isPending || Boolean(activeLightweightMetadataAction);
  const scoringLoading = stopLightweight.isPending || Boolean(activeLightweightScoringAction);
  const metadataBusyLabel = stopLightweight.isPending
    ? "Stopping..."
    : activeLightweightMetadataAction
      ? "Stop metadata"
      : "Running...";
  const scoringBusyLabel = stopLightweight.isPending
    ? "Stopping..."
    : activeLightweightScoringAction
      ? "Stop scoring"
      : "Running...";
  const indexActions = activeActions.filter((action) => action.kind === "rebuild_index");
  const activeIndexAction = preferLocalAction(
    indexActions,
    findObservedDashboardAction(runs, "rebuild_index"),
  );
  const regenerateActions = activeActions.filter((action) => action.kind === "regenerate_brief");
  const activeRegenerateAction = preferLocalAction(
    regenerateActions,
    findObservedDashboardAction(runs, "regenerate_brief"),
  );
  const regenerateLoading = Boolean(activeRegenerateAction);
  const regenerateBusyLabel = activeRegenerateAction?.status === "pending" ? "Queued..." : "Running...";
  const runRegenerateBrief = () =>
    void runTrackedAction(
      {
        kind: "regenerate_brief",
        label: "Regenerate brief",
        summary: "The written brief is being regenerated from the current local vault state.",
        briefDate: activeBriefDate,
      },
      () => regenerate.mutateAsync(activeBriefDate),
    );
  const audioActions = activeActions.filter((action) => action.kind === "generate_audio");
  const activeAudioAction = preferLocalAction(
    audioActions,
    findObservedDashboardAction(runs, "generate_audio"),
  );
  const audioLoading = Boolean(activeAudioAction);
  const audioBusyLabel = activeAudioAction?.status === "pending" ? "Queued..." : "Running...";
  const runGenerateAudio = () =>
    void runTrackedAction(
      {
        kind: "generate_audio",
        label: "Generate audio",
        summary: "The Mac is producing the spoken brief and audio bundle for the selected edition.",
        briefDate: activeBriefDate,
      },
      () => audio.mutateAsync(activeBriefDate),
    );
  const publishActions = activeActions.filter((action) => action.kind === "publish");
  const activePublishAction = preferLocalAction(
    publishActions,
    findObservedDashboardAction(runs, "publish"),
  );
  const publishLoading = Boolean(activePublishAction);
  const publishBusyLabel = activePublishAction?.status === "pending" ? "Queued..." : "Running...";
  const runPublishViewer = () =>
    void runTrackedAction(
      {
        kind: "publish",
        label: "Publish viewer",
        summary: "The viewer bundle is being rebuilt and published from the selected local edition.",
        briefDate: activeBriefDate,
      },
      () => publish.mutateAsync(activeBriefDate),
    );
  const syncActions = activeActions.filter((action) => action.kind === "sync_vault");
  const activeSyncAction = preferLocalAction(
    syncActions,
    findObservedDashboardAction(runs, "sync_vault"),
  );

  const sourceFetchMaxItemsValue = (source: Source) => sourceFetchOverrides[source.id] ?? String(source.max_items);
  const isAlphaXivSource = (source: Source) => source.custom_pipeline_id === "alphaxiv-paper";
  const sourceFetchUsesAlphaXivLikes = (source: Source) => Boolean(sourceFetchAlphaXivLikes[source.id]);

  const handleSourceFetchMaxItemsChange = (sourceId: string, value: string) => {
    setSourceFetchNotice(null);
    setSourceFetchOverrides((current) => {
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

  const handleSourceFetchAlphaXivLikesChange = (sourceId: string, checked: boolean) => {
    setSourceFetchNotice(null);
    setSourceFetchAlphaXivLikes((current) => {
      if (!checked) {
        if (!(sourceId in current)) return current;
        const next = { ...current };
        delete next[sourceId];
        return next;
      }
      if (current[sourceId]) return current;
      return { ...current, [sourceId]: true };
    });
  };

  const handleSourceFetch = (source: Source) => {
    setSourceFetchNotice(null);
    const requestedMaxItems = Number.parseInt(sourceFetchMaxItemsValue(source), 10);
    if (Number.isNaN(requestedMaxItems) || requestedMaxItems < 1 || requestedMaxItems > SOURCE_FETCH_MAX_ITEMS_LIMIT) {
      setSourceFetchNotice({
        tone: "error",
        message: `Docs this fetch must be a number between 1 and ${SOURCE_FETCH_MAX_ITEMS_LIMIT}.`,
      });
      return;
    }
    const requestedAlphaXivSort: AlphaXivSort | undefined =
      isAlphaXivSource(source) && sourceFetchUsesAlphaXivLikes(source)
        ? "Likes"
        : undefined;

    void runTrackedAction(
      {
        kind: "source_fetch",
        label: `Fetch ${source.name}`,
        summary:
          `${source.name} is being fetched on the Mac with a cap of ${requestedMaxItems} documents.`
          + (
            requestedAlphaXivSort
              ? ` This run overrides alphaXiv sort to ${requestedAlphaXivSort} so the fetch surfaces the platform’s most-liked papers before any manual enrichment or index refresh.`
              : " This fetch writes raw documents only. Run enrichment and index refresh manually from the controls above when you want them."
          ),
        sourceId: source.id,
        sourceName: source.name,
        maxItems: requestedMaxItems,
      },
      () =>
        sourceFetch.mutateAsync({
          sourceId: source.id,
          max_items: requestedMaxItems,
          alphaxiv_sort: requestedAlphaXivSort,
        }),
    );
  };

  const handleStopSourceFetch = async (source: Source, action: ActiveLocalAction | null) => {
    if (action && stopTrackedAction(action.id)) {
      setSourceFetchNotice({
        tone: "success",
        message: `Canceled the queued fetch for ${source.name} before it started on the Mac.`,
      });
      return;
    }

    setStoppingSourceId(source.id);
    try {
      const response = await stopSourceFetch.mutateAsync(source.id);
      setSourceFetchNotice({
        tone: "success",
        message: response.detail,
      });
    } catch (error) {
      setSourceFetchNotice({
        tone: "error",
        message: describeRequestError(error, `Could not stop ${source.name}.`),
      });
    } finally {
      setStoppingSourceId((current) => (current === source.id ? null : current));
    }
  };

  const runLightweightMetadata = () =>
    void runTrackedAction(
      {
        kind: "lightweight_metadata",
        label: "Enrichment",
        summary: "The local Ollama metadata pass is starting now. Watch operations for the run log and counts.",
      },
      () => lightweightMetadata.mutateAsync(),
    );

  const runLightweightScoring = () =>
    void runTrackedAction(
      {
        kind: "lightweight_scoring",
        label: "Scoring",
        summary: "The local Ollama scoring pass is starting now. Watch operations for the run log and counts.",
      },
      () => lightweightScoring.mutateAsync(),
    );

  const handleStopLightweight = async (action: ActiveLocalAction | null) => {
    if (action && stopTrackedAction(action.id)) {
      return;
    }
    try {
      await stopLightweight.mutateAsync();
    } catch {
      // Mutation state still captures the error for the feedback banner.
    }
  };

  const handleMetadataAction = () => {
    if (activeLightweightMetadataAction) {
      void handleStopLightweight(activeLightweightMetadataAction);
      return;
    }
    runLightweightMetadata();
  };

  const handleScoringAction = () => {
    if (activeLightweightScoringAction) {
      void handleStopLightweight(activeLightweightScoringAction);
      return;
    }
    runLightweightScoring();
  };

  return (
    <div className="space-y-8">
      <section className="space-y-4">
        <div>
          <p className="section-kicker">Staged pipeline</p>
          <h3 className="section-title">Run each step separately</h3>
        </div>
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          <ActionCard
            activeAction={activeFetchAction}
            icon={<Mail className="h-5 w-5" />}
            busyLabel={activeFetchAction?.status === "pending" ? "Queued..." : "Running..."}
            loading={Boolean(activeFetchAction)}
            onClick={() =>
              void runTrackedAction(
                {
                  kind: "fetch_sources",
                  label: "Fetch sources",
                  summary: "Source discovery has started on the Mac. The operations table will update as soon as the fetch run is recorded.",
                },
                () => fetchSources.mutateAsync(),
              )
            }
            subtitle={fetchSubtitle}
            title="Fetch sources"
            tone={fetchTone}
          >
            <InlineActionProgress action={activeFetchAction} runs={runs} status={status} />
          </ActionCard>
          <ActionCard
            activeAction={activeLightweightMetadataAction}
            buttonLabel={metadataButtonLabel}
            busyLabel={metadataBusyLabel}
            disabled={(!lightweightReady && !activeLightweightMetadataAction) || stopLightweight.isPending || Boolean(activeLightweightScoringAction)}
            allowBusyClick={Boolean(activeLightweightMetadataAction)}
            icon={<Sparkles className="h-5 w-5" />}
            loading={metadataLoading}
            onClick={handleMetadataAction}
            subtitle={metadataSubtitle}
            title="Enrichment"
            tone={lightweightMetadataPendingCount === 0 ? "success" : "error"}
          >
            <InlineActionProgress action={activeLightweightMetadataAction} runs={runs} status={status} />
          </ActionCard>
          <ActionCard
            activeAction={activeLightweightScoringAction}
            buttonLabel={scoringButtonLabel}
            busyLabel={scoringBusyLabel}
            disabled={(!lightweightReady && !activeLightweightScoringAction) || stopLightweight.isPending || Boolean(activeLightweightMetadataAction)}
            allowBusyClick={Boolean(activeLightweightScoringAction)}
            icon={<Sparkles className="h-5 w-5" />}
            loading={scoringLoading}
            onClick={handleScoringAction}
            subtitle={scoringSubtitle}
            title="Scoring"
            tone={lightweightScoringPendingCount === 0 ? "success" : "error"}
          >
            <InlineActionProgress action={activeLightweightScoringAction} runs={runs} status={status} />
          </ActionCard>
          <ActionCard
            activeAction={activeIndexAction}
            busyLabel={activeIndexAction?.status === "pending" ? "Queued..." : "Running..."}
            icon={<Database className="h-5 w-5" />}
            loading={Boolean(activeIndexAction)}
            onClick={() =>
              void runTrackedAction(
                {
                  kind: "rebuild_index",
                  label: "Rebuild index",
                  summary: "The Mac is rebuilding the items index and downstream deterministic views.",
                },
                () => index.mutateAsync(),
              )
            }
            subtitle={indexSubtitle}
            title="Rebuild index"
            tone={indexTone}
          >
            <InlineActionProgress action={activeIndexAction} runs={runs} status={status} />
          </ActionCard>
        </div>
      </section>

      <section className="space-y-4">
        <div>
          <p className="section-kicker">Convenience jobs</p>
          <h3 className="section-title">Refresh brief outputs</h3>
        </div>
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
          <ActionCard
            activeAction={activeRegenerateAction}
            busyLabel={regenerateBusyLabel}
            disabled={!activeBriefDate}
            hideActionButton
            icon={<RefreshCcw className="h-5 w-5" />}
            loading={regenerateLoading}
            onClick={runRegenerateBrief}
            subtitle="Rebuild the current written brief from the current vault state."
            title="Regenerate brief"
            tone={currentEditionOutputState.brief ? "success" : "default"}
          >
            <EditionTargetActionRow
              busyLabel={regenerateBusyLabel}
              buttonLabel="Regenerate"
              disabled={regenerateLoading || !activeBriefDate}
              inputDisabled={regenerateLoading}
              loading={regenerateLoading}
              onBlur={briefDateInput.onBlur}
              onChange={briefDateInput.onChange}
              onClick={runRegenerateBrief}
              value={briefDateInput.value}
            />
            <InlineActionProgress action={activeRegenerateAction} runs={runs} status={status} />
          </ActionCard>
          <ActionCard
            activeAction={activeAudioAction}
            busyLabel={audioBusyLabel}
            disabled={!activeBriefDate}
            hideActionButton
            icon={<Waves className="h-5 w-5" />}
            loading={audioLoading}
            onClick={runGenerateAudio}
            subtitle="Generate the audio script and audio file for the current brief."
            title="Generate audio"
            tone={currentEditionOutputState.audio ? "success" : "default"}
          >
            <EditionTargetActionRow
              busyLabel={audioBusyLabel}
              buttonLabel="Generate"
              disabled={audioLoading || !activeBriefDate}
              inputDisabled={audioLoading}
              loading={audioLoading}
              onBlur={briefDateInput.onBlur}
              onChange={briefDateInput.onChange}
              onClick={runGenerateAudio}
              value={briefDateInput.value}
            />
            <InlineActionProgress action={activeAudioAction} runs={runs} status={status} />
          </ActionCard>
          <ActionCard
            activeAction={activePublishAction}
            busyLabel={publishBusyLabel}
            disabled={!activeBriefDate}
            hideActionButton
            icon={<Upload className="h-5 w-5" />}
            loading={publishLoading}
            onClick={runPublishViewer}
            subtitle="Refresh the published viewer bundle from the current vault outputs."
            title="Publish viewer"
            tone={currentEditionOutputState.viewer ? "success" : "default"}
          >
            <EditionTargetActionRow
              busyLabel={publishBusyLabel}
              buttonLabel="Publish"
              disabled={publishLoading || !activeBriefDate}
              inputDisabled={publishLoading}
              loading={publishLoading}
              onBlur={briefDateInput.onBlur}
              onChange={briefDateInput.onChange}
              onClick={runPublishViewer}
              value={briefDateInput.value}
            />
            <InlineActionProgress action={activePublishAction} runs={runs} status={status} />
          </ActionCard>
        </div>
      </section>

      <section className="space-y-4">
        <div>
          <p className="section-kicker">Vault sync</p>
          <h3 className="section-title">Push only local-control artifacts</h3>
        </div>
        <div className="grid gap-4 xl:max-w-[34rem]">
          <ActionCard
            activeAction={activeSyncAction}
            busyLabel={activeSyncAction?.status === "pending" ? "Queued..." : "Running..."}
            icon={<GitBranch className="h-5 w-5" />}
            loading={Boolean(activeSyncAction)}
            onClick={() =>
              void runTrackedAction(
                {
                  kind: "sync_vault",
                  label: "Sync vault",
                  summary: "Git synchronization is running on the Mac for raw sources and local-control outputs only. The operations table now records the scoped sync run.",
                },
                () => syncVault.mutateAsync(),
              )
            }
            subtitle="Commit and push only `raw/**`, `briefs/daily/**`, and `outputs/viewer/**`. This leaves `wiki/**` and other Codex-managed files untouched."
            title="Sync vault"
          >
            <InlineActionProgress action={activeSyncAction} runs={runs} status={status} />
          </ActionCard>
        </div>
      </section>

      <section className="space-y-4">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
          <div>
            <p className="section-kicker">Per source fetch</p>
            <h3 className="section-title">Fetch one source at a time</h3>
            <p className="mt-3 max-w-4xl text-sm leading-6 text-[var(--muted)]">
              Use this to backfill or retry one configured source without running the full fetch step. The per-run cap here overrides the source default for this run only, and paused sources can still be fetched manually.
            </p>
          </div>
          <div className="flex flex-wrap gap-2">
            <span className="rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.56)] px-4 py-2 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">
              {sourcesQuery.data?.length ?? 0} configured
            </span>
            <span className="rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.56)] px-4 py-2 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">
              {sourcesQuery.data?.filter((source) => source.active).length ?? 0} active
            </span>
          </div>
        </div>

        {sourceFetchNotice ? (
          <div
            className={`rounded-[1.5rem] border px-4 py-4 text-sm leading-6 ${
              sourceFetchNotice.tone === "error"
                ? "border-[var(--danger)]/18 bg-[rgba(159,18,57,0.08)] text-[var(--danger)]"
                : "border-[rgba(22,163,74,0.2)] bg-[rgba(22,163,74,0.08)] text-[#166534]"
            }`}
          >
            {sourceFetchNotice.message}
          </div>
        ) : null}

        {sourcesQuery.isLoading && !sourcesQuery.data ? (
          <LoadingHint label="Loading configured sources…" />
        ) : sourcesQuery.isError ? (
          <div className="rounded-3xl border border-[var(--danger)]/18 bg-[rgba(159,18,57,0.08)] px-5 py-8 text-sm leading-7 text-[var(--danger)]">
            {describeRequestError(sourcesQuery.error, "Configured sources could not be loaded.")}
          </div>
        ) : sourcesQuery.data?.length ? (
          <div className="overflow-x-auto rounded-3xl border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.42)]">
            <table className="min-w-full border-collapse">
              <thead>
                <tr className="border-b border-[var(--ink)]/8 text-left">
                  <th className="px-4 py-3 font-mono text-[11px] uppercase tracking-[0.2em] text-[var(--muted)]">Source</th>
                  <th className="px-4 py-3 font-mono text-[11px] uppercase tracking-[0.2em] text-[var(--muted)]">Latest extraction</th>
                  <th className="px-4 py-3 font-mono text-[11px] uppercase tracking-[0.2em] text-[var(--muted)]">Actions</th>
                </tr>
              </thead>
              <tbody>
                {sourcesQuery.data.map((source) => {
                  const latestRun = source.latest_extraction_run;
                  const sourceActions = activeActions.filter((action) => action.kind === "source_fetch" && action.sourceId === source.id);
                  const activeSourceAction = sourceActions.find((action) => action.status === "running") ?? sourceActions[0] ?? null;
                  const liveSourceRun =
                    runs.find(
                      (run) =>
                        run.operation_kind === "raw_fetch"
                        && (run.status === "running" || run.status === "pending")
                        && findRunSourceId(run) === source.id,
                    ) ?? null;
                  const observedSourceAction = activeSourceAction
                    ? null
                    : buildObservedSourceFetchAction({
                        source,
                        run: liveSourceRun,
                        latestRun,
                      });
                  const sourceAction = activeSourceAction ?? observedSourceAction;
                  const canStopQueuedSourceAction = Boolean(activeSourceAction && activeSourceAction.status === "pending");
                  const canStopRunningSourceAction = Boolean(sourceAction && sourceAction.status === "running");
                  const isStoppingSource = stoppingSourceId === source.id;

                  return (
                    <tr key={`local-source-fetch-${source.id}`} className="border-b border-[var(--ink)]/6 align-top last:border-b-0">
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
                        </div>
                        {source.description ? (
                          <p className="mt-2 max-w-md text-sm leading-6 text-[var(--muted)]">{source.description}</p>
                        ) : (
                          <p className="mt-2 text-sm leading-6 text-[var(--muted)]">No description added.</p>
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
                              <p className="text-xs leading-5 text-[var(--muted)]">Emitted kinds: {formatEmittedKinds(latestRun.emitted_kinds)}</p>
                            </>
                          ) : (
                            <p className="text-sm leading-6 text-[var(--muted)]">No extraction has been recorded for this source yet.</p>
                          )}
                        </div>
                      </td>
                      <td className="px-4 py-4">
                        {sourceAction ? (
                          <div className="flex min-w-[260px] flex-col gap-3">
                            <InlineActionProgress action={sourceAction} runs={runs} status={status} />
                            <button
                              className="secondary-button w-full justify-center"
                              disabled={isStoppingSource || (!canStopQueuedSourceAction && !canStopRunningSourceAction)}
                              onClick={() => void handleStopSourceFetch(source, activeSourceAction)}
                              type="button"
                            >
                              {isStoppingSource ? "Stopping..." : "Stop"}
                            </button>
                            {canStopRunningSourceAction ? (
                              <p className="text-xs leading-5 text-[var(--muted)]">
                                Stop is cooperative. The current document or page request may finish before the fetch exits.
                              </p>
                            ) : null}
                            {!canStopQueuedSourceAction && !canStopRunningSourceAction ? (
                              <p className="text-xs leading-5 text-[var(--muted)]">
                                This queued fetch was not created from this page, so it cannot be canceled here yet.
                              </p>
                            ) : null}
                          </div>
                        ) : (
                          <div className="flex min-w-[260px] flex-col gap-3">
                            <label className="rounded-[1.2rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.62)] px-3 py-3">
                              <span className="field-label">Docs this fetch</span>
                              <input
                                className="field-input mt-2"
                                inputMode="numeric"
                                max={SOURCE_FETCH_MAX_ITEMS_LIMIT}
                                min={1}
                                onChange={(event) => handleSourceFetchMaxItemsChange(source.id, event.target.value)}
                                type="number"
                                value={sourceFetchMaxItemsValue(source)}
                              />
                              <span className="mt-2 block text-xs leading-5 text-[var(--muted)]">
                                Default {source.max_items} from source settings. Raise this for deeper backfills without changing the saved source config.
                              </span>
                            </label>
                            {isAlphaXivSource(source) ? (
                              <div className="rounded-[1.2rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.62)] px-3 py-3">
                                <span className="field-label">alphaXiv sort override</span>
                                <label className="mt-2 flex items-start gap-3 text-sm leading-6 text-[var(--muted-strong)]">
                                  <input
                                    checked={sourceFetchUsesAlphaXivLikes(source)}
                                    className="mt-1 h-4 w-4"
                                    onChange={(event) =>
                                      handleSourceFetchAlphaXivLikesChange(source.id, event.target.checked)
                                    }
                                    type="checkbox"
                                  />
                                  <span>
                                    Force Likes for this fetch
                                    <span className="mt-1 block text-xs leading-5 text-[var(--muted)]">
                                      Unchecked keeps the current profile alphaXiv sort. Checked overrides this run to Likes so the fetch surfaces the platform&apos;s most-liked papers.
                                    </span>
                                  </span>
                                </label>
                              </div>
                            ) : null}
                            <button className="secondary-button w-full justify-center" onClick={() => handleSourceFetch(source)} type="button">
                              Fetch source
                            </button>
                          </div>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="rounded-3xl border border-dashed border-[var(--ink)]/12 bg-[rgba(255,255,255,0.42)] px-5 py-8 text-sm leading-7 text-[var(--muted)]">
            No sources are configured yet. Add a source first, then use this section to fetch it directly with a per-run document cap.
          </div>
        )}
      </section>
    </div>
  );
}

function CodexTab() {
  const dashboard = useLocalDashboard();
  const { statusQuery } = dashboard;

  if (statusQuery.isLoading && !statusQuery.data) {
    return <CodexTabSkeleton />;
  }
  if (statusQuery.isError) {
    return (
      <div className="editorial-panel">
        <p className="section-kicker">Codex runtime unavailable</p>
        <p className="mt-3 text-sm leading-6 text-[var(--danger)]">{describeStatusError(statusQuery.error)}</p>
      </div>
    );
  }
  if (!statusQuery.data) {
    return <div className="page-empty">Codex runtime status is unavailable.</div>;
  }

  return <CodexTabContent dashboard={dashboard} status={statusQuery.data} />;
}

function CodexTabContent({
  dashboard,
  status,
}: {
  dashboard: LocalDashboardContext;
  status: LocalControlStatus;
}) {
  const { operationsQuery, activeActions, runTrackedAction, compileWiki, healthCheck, answerQuery, fileOutput } = dashboard;
  const runs = operationsQuery.data?.runs ?? [];
  const codexReady = Boolean(status.codex?.available && status.codex.authenticated);
  const compileBatchDefault = status.codex?.compile_batch_size ?? 8;
  const compileSubtitle = codexReady
    ? `Use Codex to maintain \`wiki/**\` from changed raw documents, then rebuild the deterministic pages and graph indexes. Default batch size on this Mac is ${compileBatchDefault}.`
    : status.codex?.detail ?? "Codex is not ready on this Mac yet. Authenticate it locally before running advanced compile.";
  const [advancedCompileLimit, setAdvancedCompileLimit] = useState(() => String(compileBatchDefault));
  const [healthCheckScope, setHealthCheckScope] = useState<"vault" | "wiki" | "raw">("vault");
  const [healthCheckTopic, setHealthCheckTopic] = useState("");
  const [answerQuestion, setAnswerQuestion] = useState("");
  const [answerOutputKind, setAnswerOutputKind] = useState<"answer" | "slides" | "chart">("answer");
  const [fileOutputPath, setFileOutputPath] = useState("");
  const compileActions = activeActions.filter((action) => action.kind === "compile_wiki");
  const activeCompileAction = preferLocalAction(
    compileActions,
    findObservedDashboardAction(runs, "compile_wiki"),
  );
  const healthCheckActions = activeActions.filter((action) => action.kind === "health_check");
  const activeHealthCheckAction = preferLocalAction(
    healthCheckActions,
    findObservedDashboardAction(runs, "health_check"),
  );
  const answerActions = activeActions.filter((action) => action.kind === "answer_query");
  const activeAnswerAction = preferLocalAction(
    answerActions,
    findObservedDashboardAction(runs, "answer_query"),
  );
  const fileOutputActions = activeActions.filter((action) => action.kind === "file_output");
  const activeFileOutputAction = preferLocalAction(
    fileOutputActions,
    findObservedDashboardAction(runs, "file_output"),
  );

  return (
    <div className="space-y-8">
      <section className="space-y-4">
        <div>
          <p className="section-kicker">Advanced Codex jobs</p>
          <h3 className="section-title">Run wiki compilation, Q&A, and filing explicitly</h3>
        </div>
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-2 2xl:grid-cols-4">
          <ActionCard
            activeAction={activeCompileAction}
            disabled={!advancedCompileLimit.trim() || !codexReady}
            busyLabel={activeCompileAction?.status === "pending" ? "Queued..." : "Running..."}
            icon={<BookOpen className="h-5 w-5" />}
            loading={Boolean(activeCompileAction)}
            onClick={() =>
              void runTrackedAction(
                {
                  kind: "compile_wiki",
                  label: "Compile wiki with Codex",
                  summary: "Codex compilation is running on the Mac. Open the operations table for the detailed run log once it lands.",
                },
                () => compileWiki.mutateAsync({ limit: Number(advancedCompileLimit) || compileBatchDefault }),
              )
            }
            subtitle={compileSubtitle}
            title="Compile wiki with Codex"
            tone={codexReady ? "default" : "warning"}
          >
            <label className="mt-4 block">
              <span className="field-label">Compile batch size</span>
              <input className="field-input mt-2" min={1} onChange={(event) => setAdvancedCompileLimit(event.target.value)} type="number" value={advancedCompileLimit} />
            </label>
            <InlineActionProgress action={activeCompileAction} runs={runs} status={status} />
          </ActionCard>
          <ActionCard
            activeAction={activeHealthCheckAction}
            disabled={!codexReady}
            busyLabel={activeHealthCheckAction?.status === "pending" ? "Queued..." : "Running..."}
            icon={<History className="h-5 w-5" />}
            loading={Boolean(activeHealthCheckAction)}
            onClick={() =>
              void runTrackedAction(
                {
                  kind: "health_check",
                  label: "Run health check",
                  summary: "Codex is auditing the vault for missing links, stale pages, and follow-up gaps.",
                },
                () => healthCheck.mutateAsync({ scope: healthCheckScope, topic: healthCheckTopic.trim() || undefined }),
              )
            }
            subtitle="Audit the vault for missing metadata, weak links, stale pages, duplicate concepts, and follow-up questions."
            title="Run health check"
            tone={codexReady ? "default" : "warning"}
          >
            <label className="mt-4 block">
              <span className="field-label">Scope</span>
              <select className="field-input mt-2" onChange={(event) => setHealthCheckScope(event.target.value as "vault" | "wiki" | "raw")} value={healthCheckScope}>
                <option value="vault">Vault</option>
                <option value="wiki">Wiki</option>
                <option value="raw">Raw</option>
              </select>
            </label>
            <label className="mt-4 block">
              <span className="field-label">Topic</span>
              <input className="field-input mt-2" onChange={(event) => setHealthCheckTopic(event.target.value)} placeholder="optional focus" type="text" value={healthCheckTopic} />
            </label>
            <InlineActionProgress action={activeHealthCheckAction} runs={runs} status={status} />
          </ActionCard>
          <ActionCard
            activeAction={activeAnswerAction}
            disabled={!codexReady || !answerQuestion.trim()}
            busyLabel={activeAnswerAction?.status === "pending" ? "Queued..." : "Running..."}
            icon={<Bot className="h-5 w-5" />}
            loading={Boolean(activeAnswerAction)}
            onClick={() =>
              void runTrackedAction(
                {
                  kind: "answer_query",
                  label: "Ask Codex",
                  summary: "A vault-first Codex answer is being generated on the Mac and written into the configured output bundle.",
                },
                () => answerQuery.mutateAsync({ question: answerQuestion.trim(), output_kind: answerOutputKind }),
              )
            }
            subtitle="Ask a vault-first Codex question and persist the answer as a report, slides, or chart bundle."
            title="Ask Codex"
            tone={codexReady ? "default" : "warning"}
          >
            <label className="mt-4 block">
              <span className="field-label">Question</span>
              <textarea className="field-input mt-2 min-h-28" onChange={(event) => setAnswerQuestion(event.target.value)} placeholder="What changed recently around eval agents?" value={answerQuestion} />
            </label>
            <label className="mt-4 block">
              <span className="field-label">Output kind</span>
              <select className="field-input mt-2" onChange={(event) => setAnswerOutputKind(event.target.value as "answer" | "slides" | "chart")} value={answerOutputKind}>
                <option value="answer">Answer</option>
                <option value="slides">Slides</option>
                <option value="chart">Chart bundle</option>
              </select>
            </label>
            <InlineActionProgress action={activeAnswerAction} runs={runs} status={status} />
          </ActionCard>
          <ActionCard
            activeAction={activeFileOutputAction}
            disabled={!codexReady || !fileOutputPath.trim()}
            busyLabel={activeFileOutputAction?.status === "pending" ? "Queued..." : "Running..."}
            icon={<Upload className="h-5 w-5" />}
            loading={Boolean(activeFileOutputAction)}
            onClick={() =>
              void runTrackedAction(
                {
                  kind: "file_output",
                  label: "File output into wiki",
                  summary: "Codex is distilling an existing durable output back into the wiki so the vault compounds over time.",
                },
                () => fileOutput.mutateAsync({ path: fileOutputPath.trim() }),
              )
            }
            subtitle="Distill a prior durable output back into `wiki/**` so the knowledge base compounds over time."
            title="File output into wiki"
            tone={codexReady ? "default" : "warning"}
          >
            <label className="mt-4 block">
              <span className="field-label">Vault-relative path</span>
              <input className="field-input mt-2" onChange={(event) => setFileOutputPath(event.target.value)} placeholder="outputs/answers/2026-04-07/report.md" type="text" value={fileOutputPath} />
            </label>
            <InlineActionProgress action={activeFileOutputAction} runs={runs} status={status} />
          </ActionCard>
        </div>
      </section>
    </div>
  );
}

function OperationsTab() {
  const { operationsQuery, activeActions, anyBusy, refresh } = useLocalDashboard();
  const [searchParams, setSearchParams] = useSearchParams();
  const selectedRunId = searchParams.get("run");
  const runs = operationsQuery.data?.runs ?? [];
  const selectedRun = runs.find((run) => run.id === selectedRunId) ?? null;
  const selectedRunAssociatedDates = selectedRun ? describeAssociatedDates(selectedRun) : null;
  const syntheticRows = useMemo(
    () =>
      activeActions
        .filter((action) => !runs.some((run) => matchesActiveAction(run, action)))
        .map((action) => buildSyntheticOperationRow(action)),
    [activeActions, runs],
  );
  const runningCount =
    runs.filter((run) => run.status === "running").length + syntheticRows.filter((row) => row.status === "running").length;
  const queuedCount =
    runs.filter((run) => run.status === "pending").length + syntheticRows.filter((row) => row.status === "pending").length;
  const totalRows = runs.length + syntheticRows.length;

  const openRun = (runId: string) => {
    const next = new URLSearchParams(searchParams);
    next.set("run", runId);
    setSearchParams(next, { replace: true });
  };

  const closeRun = () => {
    const next = new URLSearchParams(searchParams);
    next.delete("run");
    setSearchParams(next, { replace: true });
  };

  useEffect(() => {
    if (!selectedRunId) return;
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        closeRun();
      }
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [selectedRunId, searchParams]);

  if (operationsQuery.isLoading && !operationsQuery.data && !syntheticRows.length) {
    return <OperationsTabSkeleton />;
  }
  if (operationsQuery.isError) {
    return (
      <div className="editorial-panel">
        <p className="section-kicker">Recent operations unavailable</p>
        <p className="mt-3 text-sm leading-6 text-[var(--danger)]">
          {describeRequestError(operationsQuery.error, "The recent operations log could not be loaded.")}
        </p>
      </div>
    );
  }

  return (
    <>
      <section className="editorial-panel">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <p className="section-kicker">Operational history</p>
            <h3 className="section-title">Recent job runs</h3>
            <p className="mt-4 max-w-4xl text-sm leading-7 text-[var(--muted)]">
              Every local-control action records a run here when the Mac emits one. Open any row to inspect the detailed log, associated edition coverage, recorded artifacts, and the latest step-level messages.
            </p>
          </div>
          <div className="flex flex-wrap gap-2">
            <span className="rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.56)] px-4 py-2 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">
              {String(totalRows).padStart(2, "0")} runs
            </span>
            <span className="rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.56)] px-4 py-2 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">
              {runningCount ? `${runningCount} running` : "Idle"}
            </span>
            {queuedCount ? (
              <span className="rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.56)] px-4 py-2 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)]">
                {queuedCount} queued
              </span>
            ) : null}
            <button className="secondary-button" disabled={anyBusy} onClick={() => void refresh()} type="button">
              Refresh
            </button>
          </div>
        </div>

        {operationsQuery.isLoading && !runs.length && !syntheticRows.length ? (
          <div className="mt-6">
            <LoadingHint label="Loading operation history…" />
          </div>
        ) : totalRows ? (
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
                {syntheticRows.map((syntheticRow) => (
                  <tr key={syntheticRow.id} className="border-b border-[var(--ink)]/6 align-top bg-[rgba(154,52,18,0.05)]">
                    <td className="px-4 py-4 text-sm leading-6 text-[var(--muted-strong)]">{formatDateTimeLabel(syntheticRow.startedAt)}</td>
                    <td className="px-4 py-4">
                      <p className="text-sm font-medium text-[var(--ink)]">{syntheticRow.title}</p>
                      <p className="mt-1 text-xs leading-5 text-[var(--muted)]">
                        {syntheticRow.status === "pending" ? "Queued on this device" : "Starting locally"}
                      </p>
                      <p className="mt-2 text-xs leading-5 text-[var(--muted)]">{syntheticRow.summary}</p>
                    </td>
                    <td className="px-4 py-4">
                      {syntheticRow.associatedDates ? (
                        <>
                          <p className="text-sm leading-6 text-[var(--muted-strong)]">Edition {syntheticRow.associatedDates.edition}</p>
                          <p className="mt-1 text-xs leading-5 text-[var(--muted)]">Coverage {syntheticRow.associatedDates.coverage}</p>
                        </>
                      ) : (
                        <p className="text-sm leading-6 text-[var(--muted)]">General / no edition range</p>
                      )}
                    </td>
                    <td className="px-4 py-4">
                      <p className="text-sm leading-6 text-[var(--muted-strong)]">{formatUsdCost(syntheticRow.totalCostUsd)}</p>
                      <p className="mt-1 text-xs leading-5 text-[var(--muted)]">
                        LLM {formatUsdCost(syntheticRow.aiCostUsd)} · TTS {formatUsdCost(syntheticRow.ttsCostUsd)}
                      </p>
                    </td>
                    <td className="px-4 py-4">
                      <RunStatusChip status={syntheticRow.status} />
                    </td>
                  </tr>
                ))}

                {runs.map((run) => {
                  const associatedDates = describeAssociatedDates(run);
                  return (
                    <tr
                      key={run.id}
                      className="cursor-pointer border-b border-[var(--ink)]/6 transition hover:bg-[rgba(255,255,255,0.52)] last:border-b-0"
                      onClick={() => openRun(run.id)}
                      onKeyDown={(event) => {
                        if (event.key === "Enter" || event.key === " ") {
                          event.preventDefault();
                          openRun(run.id);
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
                        <p className="mt-2 text-xs leading-5 text-[var(--muted)]">{run.summary}</p>
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
                        <RunStatusChip status={run.status} />
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="mt-6 rounded-3xl border border-dashed border-[var(--ink)]/12 bg-[rgba(255,255,255,0.42)] px-5 py-8 text-sm leading-7 text-[var(--muted)]">
            No runs yet. Launch a job in Pipeline or Codex and it will appear here.
          </div>
        )}
      </section>

      {selectedRun ? (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-[rgba(17,19,18,0.38)] px-4 py-6 backdrop-blur-sm"
          onClick={closeRun}
        >
          <div
            aria-labelledby={`operation-run-${selectedRun.id}`}
            aria-modal="true"
            className="max-h-[90vh] w-full max-w-5xl overflow-hidden rounded-[2rem] border border-[var(--ink)]/10 bg-[rgba(247,240,224,0.96)] shadow-[0_32px_80px_rgba(17,19,18,0.22)]"
            onClick={(event) => event.stopPropagation()}
            role="dialog"
          >
            <div className="flex flex-wrap items-start justify-between gap-4 border-b border-[var(--ink)]/8 px-6 py-5">
              <div>
                <p className="section-kicker">Operation details</p>
                <h4 className="section-title text-3xl" id={`operation-run-${selectedRun.id}`}>
                  {selectedRun.title}
                </h4>
                <p className="mt-3 text-sm leading-6 text-[var(--muted)]">{selectedRun.summary}</p>
              </div>
              <button className="secondary-button" onClick={closeRun} type="button">
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
                    <RunStatusChip status={selectedRun.status} />
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

              {selectedRun.prompt_path || selectedRun.manifest_path || selectedRun.output_paths.length || selectedRun.codex_command?.length || selectedRun.final_summary ? (
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
                    {selectedRun.codex_command?.length ? (
                      <article className="rounded-[1.3rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.52)] px-4 py-4">
                        <p className="field-label">Codex command</p>
                        <p className="mt-2 break-words text-sm leading-6 text-[var(--ink)]">{selectedRun.codex_command.join(" ")}</p>
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
                          <RunStatusChip status={step.status} />
                        </div>
                        <p className="mt-3 text-sm leading-6 text-[var(--muted-strong)]">{formatStepCounts(step)}</p>
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

function ActionCard({
  activeAction,
  busyLabel,
  icon,
  title,
  buttonLabel,
  subtitle,
  onClick,
  loading,
  tone = "default",
  disabled = false,
  allowBusyClick = false,
  hideActionButton = false,
  children,
}: {
  activeAction?: ActiveLocalAction | null;
  busyLabel?: string;
  icon: ReactNode;
  title: string;
  buttonLabel?: string;
  subtitle: string;
  onClick: () => void;
  loading: boolean;
  tone?: "default" | "success" | "warning" | "error";
  disabled?: boolean;
  allowBusyClick?: boolean;
  hideActionButton?: boolean;
  children?: ReactNode;
}) {
  const cardStyle: CSSProperties | undefined =
    tone === "success"
      ? {
          borderColor: "#86efac",
          background: "linear-gradient(180deg, rgba(220,252,231,0.98), rgba(240,253,244,0.9))",
          boxShadow: "0 20px 48px rgba(22,163,74,0.12)",
        }
      : tone === "error"
        ? {
            borderColor: "rgba(244,63,94,0.34)",
            background: "linear-gradient(180deg, rgba(255,228,230,0.98), rgba(255,241,242,0.92))",
            boxShadow: "0 20px 48px rgba(159,18,57,0.12)",
          }
        : tone === "warning"
          ? {
              borderColor: "rgba(194,65,12,0.24)",
              background: "linear-gradient(180deg, rgba(255,237,213,0.98), rgba(255,247,237,0.92))",
              boxShadow: "0 20px 48px rgba(154,52,18,0.1)",
            }
          : undefined;
  const iconStyle: CSSProperties | undefined =
    tone === "success"
      ? { color: "#166534" }
      : tone === "error"
        ? { color: "var(--danger)" }
        : tone === "warning"
          ? { color: "var(--accent)" }
          : { color: "var(--accent)" };
  const resolvedBusyLabel =
    busyLabel ?? (activeAction?.status === "pending" ? "Queued..." : activeAction ? "Running..." : "Working...");

  return (
    <article className="editorial-panel flex h-full flex-col" style={cardStyle}>
      <div className="flex items-center gap-3" style={iconStyle}>
        {icon}
      </div>
      <h3 className="mt-5 text-2xl font-semibold text-[var(--ink)]">{title}</h3>
      <p className="mt-3 text-sm leading-6 text-[var(--muted)]">{subtitle}</p>
      {children}
      {hideActionButton ? null : (
        <div className="mt-6 flex flex-1 items-end justify-end">
          <button className="primary-button" disabled={disabled || (loading && !allowBusyClick)} onClick={onClick} type="button">
            {loading ? <LoaderCircle className="h-4 w-4 animate-spin" /> : null}
            {loading ? resolvedBusyLabel : buttonLabel ?? title}
          </button>
        </div>
      )}
    </article>
  );
}

export function LocalControlApp({ config: _config }: { config: RuntimeConfig }) {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <Routes>
          <Route element={<LocalShell />}>
            <Route element={<PairingPage />} path="pair" />
            <Route element={<LocalDashboardLayout />}>
              <Route element={<Navigate replace to="/overview" />} index />
              <Route element={<OverviewTab />} path="overview" />
              <Route element={<LocalControlBriefTab />} path="brief" />
              <Route element={<InsightsTab />} path="insights" />
              <Route element={<LocalControlDocumentsTab />} path="documents" />
              <Route element={<LocalControlDocumentDetailPage />} path="documents/:itemId" />
              <Route element={<PipelineTab />} path="pipeline" />
              <Route element={<CodexTab />} path="codex" />
              <Route element={<OperationsTab />} path="operations" />
              <Route element={<LocalControlProfileTab />} path="profile" />
            </Route>
          </Route>
        </Routes>
      </BrowserRouter>
    </QueryClientProvider>
  );
}
