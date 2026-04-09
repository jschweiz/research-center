import { createContext, useContext, useEffect, useMemo, useState } from "react";
import { QueryClientProvider, useQuery } from "@tanstack/react-query";
import { BrowserRouter, Link, NavLink, Outlet, Route, Routes, useLocation, useParams, useSearchParams } from "react-router-dom";
import { ArrowLeft, ExternalLink, FolderTree, PanelLeftClose, PanelLeftOpen, RadioTower, TabletSmartphone } from "lucide-react";

import type { AudioBrief } from "../api/types";
import { AudioBriefPlayer } from "../components/AudioBriefPlayer";
import { MarkdownText } from "../components/MarkdownText";
import { PaperAudioPlayer } from "../components/PaperAudioPlayer";
import { SkimmableText } from "../components/SkimmableText";
import { SimilarPapersPanel } from "../components/SimilarPapersPanel";
import { resolvePaperAudioUrl, resolvePaperFiledText, resolvePaperSummary, resolveSimilarPapers } from "../lib/paper-details";
import { queryClient } from "../lib/query-client";
import {
  getStoredPairedLocalUrl,
  getStoredShellSidebarCollapsed,
  setStoredPairedLocalUrl,
  setStoredShellSidebarCollapsed,
} from "../runtime/storage";
import type { PublishedDigestEntry, PublishedEditionManifest, PublishedEditionSummary, PublishedItemDetail, RuntimeConfig } from "../runtime/types";
import { PublishedDataClient } from "./cloudkit-client";
import { PublishedItemCard } from "./PublishedItemCard";

const PublishedDataContext = createContext<PublishedDataClient | null>(null);

function usePublishedDataClient() {
  const client = useContext(PublishedDataContext);
  if (!client) {
    throw new Error("Published data client is not available.");
  }
  return client;
}

function formatDate(value: string | null) {
  if (!value) return "Undated";
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
    year: "numeric",
  }).format(new Date(value));
}

function formatEditionLabel(summary: PublishedEditionSummary) {
  if (summary.period_type === "week" && summary.week_start) {
    return `${summary.title} · Week of ${formatDate(summary.week_start)}`;
  }
  return `${summary.title} · ${formatDate(summary.brief_date)}`;
}

function formatDateRange(start: string, end: string) {
  if (start === end) return formatDate(start);
  return `${formatDate(start)} to ${formatDate(end)}`;
}

function formatEnrichmentStatus(status: string) {
  if (status === "succeeded") return "Ready";
  if (status === "failed") return "Failed";
  if (status === "interrupted") return "Interrupted";
  if (status === "running") return "Running";
  return "Pending";
}

function HostedShell({ pairedLocalUrl }: { pairedLocalUrl: string | null }) {
  const location = useLocation();
  const [sidebarCollapsed, setSidebarCollapsed] = useState(getStoredShellSidebarCollapsed);
  const title = location.pathname.startsWith("/items/") ? "Published item detail" : "Published brief";

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
              <h1 className="font-display text-5xl leading-none text-[var(--ink)]">Published briefing</h1>
              <SkimmableText className="text-sm leading-6 text-[var(--muted)]">
                The iPad viewer reads the curated CloudKit snapshot and opens the local Mac only when you want to trigger work.
              </SkimmableText>
            </div>
          </div>

          <nav className="editorial-sidebar-nav mt-10 space-y-2">
            <NavLink aria-label="Brief" className={({ isActive }) => `nav-link ${isActive ? "nav-link-active" : ""}`} title="Brief" to="/">
              <RadioTower className="h-4 w-4" />
              <span className="nav-link-label">Brief</span>
            </NavLink>
          </nav>

          <section className="editorial-panel editorial-sidebar-panel mt-10 bg-[rgba(255,255,255,0.58)]">
            <p className="font-mono text-[11px] uppercase tracking-[0.24em] text-[var(--muted)]">Local handoff</p>
            <SkimmableText className="mt-3 text-sm leading-6 text-[var(--muted)]">
              The hosted viewer stays read-only. Opening the local Mac uses a normal top-level navigation so Safari does not hit mixed-content restrictions.
            </SkimmableText>
            {pairedLocalUrl ? (
              <a className="secondary-button mt-5 w-full justify-center" href={pairedLocalUrl}>
                <TabletSmartphone className="h-4 w-4" />
                Open Local Mac
              </a>
            ) : (
              <p className="mt-5 text-sm leading-6 text-[var(--muted)]">Pair this iPad with the Mac to enable local control.</p>
            )}
          </section>
        </aside>

        <main className="editorial-main">
          <header className="editorial-topbar">
            <div className="min-w-0 flex-1">
              <p className="font-mono text-[11px] uppercase tracking-[0.26em] text-[var(--muted)]">Hosted viewer / CloudKit snapshot</p>
              <h2 className="mt-3 max-w-full font-display text-4xl leading-[0.94] sm:text-5xl">{title}</h2>
            </div>
          </header>
          <Outlet />
        </main>
      </div>
    </div>
  );
}

function HostedBriefPage() {
  const client = usePublishedDataClient();
  const [searchParams, setSearchParams] = useSearchParams();
  const latestManifestQuery = useQuery({
    queryKey: ["published", "latest"],
    queryFn: () => client.fetchLatestManifest(),
  });

  const selectedRecordName = searchParams.get("record");
  const selectedManifestQuery = useQuery({
    queryKey: ["published", "record", selectedRecordName],
    queryFn: () => client.fetchManifest(selectedRecordName ?? ""),
    enabled: Boolean(selectedRecordName) && selectedRecordName !== latestManifestQuery.data?.edition.record_name,
  });

  const manifest =
    selectedRecordName && selectedRecordName !== latestManifestQuery.data?.edition.record_name
      ? selectedManifestQuery.data
      : latestManifestQuery.data;

  useEffect(() => {
    if (!latestManifestQuery.data) return;
    if (selectedRecordName) return;
    const nextSearch = new URLSearchParams(searchParams);
    nextSearch.set("record", latestManifestQuery.data.edition.record_name);
    setSearchParams(nextSearch, { replace: true });
  }, [latestManifestQuery.data, searchParams, selectedRecordName, setSearchParams]);

  if (latestManifestQuery.isLoading || (selectedRecordName && selectedManifestQuery.isLoading)) {
    return <div className="page-loading">Loading published brief…</div>;
  }
  if (latestManifestQuery.error || selectedManifestQuery.error || !manifest) {
    return <div className="page-empty">The published brief could not be loaded.</div>;
  }

  return (
    <div className="space-y-8 pb-10">
      <section className="editorial-panel">
        <div className="flex flex-col gap-6 lg:flex-row lg:items-start lg:justify-between">
          <div className="min-w-0">
            <p className="section-kicker">Edition</p>
            <h3 className="mt-3 font-display text-4xl leading-tight text-[var(--ink)]">{manifest.digest.title}</h3>
            <SkimmableText
              className="mt-4 max-w-3xl text-base leading-7 text-[var(--muted)]"
              children={`Coverage window: ${formatDateRange(manifest.digest.coverage_start, manifest.digest.coverage_end)}`}
            />
            {manifest.digest.editorial_note ? (
              <SkimmableText className="mt-5 text-lg leading-8 text-[var(--muted-strong)]">
                {manifest.digest.editorial_note}
              </SkimmableText>
            ) : null}
          </div>

          <label className="block w-full max-w-md">
            <span className="field-label">Published edition</span>
            <select
              className="field-input mt-2"
              onChange={(event) => {
                const nextSearch = new URLSearchParams(searchParams);
                nextSearch.set("record", event.target.value);
                setSearchParams(nextSearch);
              }}
              value={manifest.edition.record_name}
            >
              {manifest.available_editions.map((edition) => (
                <option key={edition.record_name} value={edition.record_name}>
                  {formatEditionLabel(edition)}
                </option>
              ))}
            </select>
          </label>
        </div>

        {manifest.digest.audio_brief ? (
          <div className="mt-6 border-t border-[var(--ink)]/8 pt-6">
            <AudioBriefPlayer
              audioBrief={manifest.digest.audio_brief as AudioBrief}
              briefDate={manifest.digest.brief_date ?? ""}
            />
          </div>
        ) : null}
      </section>

      <PublishedSection entries={manifest.digest.editorial_shortlist} recordName={manifest.edition.record_name} title="Editorial shortlist" />
      <PublishedSection entries={manifest.digest.headlines} recordName={manifest.edition.record_name} title="Headlines" />
      <PublishedSection entries={manifest.digest.interesting_side_signals} recordName={manifest.edition.record_name} title="Interesting side signals" />
      <PublishedSection entries={manifest.digest.remaining_reads} recordName={manifest.edition.record_name} title="Remaining reads" />
      <PublishedPapersTable items={manifest.digest.papers_table} recordName={manifest.edition.record_name} />
    </div>
  );
}

function PublishedSection({
  entries,
  recordName,
  title,
}: {
  entries: PublishedDigestEntry[];
  recordName: string;
  title: string;
}) {
  if (!entries.length) return null;
  return (
    <section className="space-y-4">
      <div>
        <p className="section-kicker">{title}</p>
        <h3 className="section-title">{title}</h3>
      </div>
      <div className="grid gap-4 lg:grid-cols-2 2xl:grid-cols-3">
        {entries.map((entry) => (
          <PublishedItemCard key={entry.item.id} item={entry.item} note={entry.note} recordName={recordName} />
        ))}
      </div>
    </section>
  );
}

function PublishedPapersTable({
  items,
  recordName,
}: {
  items: PublishedEditionManifest["digest"]["papers_table"];
  recordName: string;
}) {
  if (!items.length) return null;
  return (
    <section className="space-y-4">
      <div>
        <p className="section-kicker">Papers</p>
        <h3 className="section-title">Top papers</h3>
      </div>
      <div className="grid gap-4 lg:grid-cols-2 2xl:grid-cols-3">
        {items.map((entry) => (
          <PublishedItemCard key={entry.item.id} item={entry.item} note={entry.zotero_tags.join(", ")} recordName={recordName} />
        ))}
      </div>
    </section>
  );
}

function HostedItemDetailPage() {
  const client = usePublishedDataClient();
  const { itemId = "" } = useParams();
  const [searchParams] = useSearchParams();
  const recordName = searchParams.get("record");
  const latestManifestQuery = useQuery({
    queryKey: ["published", "detail-latest"],
    queryFn: () => client.fetchLatestManifest(),
    enabled: !recordName,
  });
  const manifestQuery = useQuery({
    queryKey: ["published", "detail-manifest", recordName],
    queryFn: () => client.fetchManifest(recordName ?? ""),
    enabled: Boolean(recordName),
  });

  const manifest = recordName ? manifestQuery.data : latestManifestQuery.data;

  if ((recordName && manifestQuery.isLoading) || (!recordName && latestManifestQuery.isLoading)) {
    return <div className="page-loading">Loading published item…</div>;
  }
  if (manifestQuery.error || latestManifestQuery.error || !manifest) {
    return <div className="page-empty">Published item not found.</div>;
  }

  const item = manifest.items[itemId];
  if (!item) {
    return <div className="page-empty">Published item not found.</div>;
  }

  const summary = resolvePaperSummary(item);
  const filedText = resolvePaperFiledText(item);
  const audioUrl = resolvePaperAudioUrl(item);
  const similarPapers = resolveSimilarPapers(item);

  return (
    <div className="grid gap-6 pb-10 xl:grid-cols-[minmax(0,1.3fr)_380px]">
      <section className="editorial-panel">
        <Link
          className="inline-flex items-center gap-2 font-mono text-xs uppercase tracking-[0.24em] text-[var(--muted)]"
          to={`/?record=${encodeURIComponent(manifest.edition.record_name)}`}
        >
          <ArrowLeft className="h-4 w-4" />
          Back to brief
        </Link>

        <p className="section-kicker mt-6">{item.source_name}</p>
        <h3 className="mt-3 font-display text-5xl leading-tight text-[var(--ink)]">{item.title}</h3>
        <div className="mt-5 flex flex-wrap gap-3 font-mono text-xs uppercase tracking-[0.18em] text-[var(--muted)]">
          {item.organization_name ? <span>{item.organization_name}</span> : null}
          {item.authors.length ? <span>{item.authors.join(", ")}</span> : null}
          <span>{item.content_type}</span>
          {item.kind && item.kind !== item.content_type ? <span>{item.kind}</span> : null}
          <span>{formatDate(item.published_at)}</span>
        </div>

        {audioUrl ? (
          <div className="mt-8">
            <PaperAudioPlayer audioUrl={audioUrl} />
          </div>
        ) : null}

        <div className="mt-8 space-y-5">
          <ContentBlock label="Short summary" value={summary ?? "Summary pending."} />
          <ContentBlock label="Filed text" value={filedText ?? "No normalized text is available in the published snapshot."} />
        </div>
      </section>

      <aside className="space-y-4">
        <section className="editorial-panel">
          <p className="section-kicker">Published metadata</p>
          <div className="mt-4 space-y-3 text-sm leading-6 text-[var(--muted-strong)]">
            <MetadataRow label="Enrichment" value={formatEnrichmentStatus(item.lightweight_enrichment_status)} />
            <MetadataRow label="Doc role" value={item.doc_role} />
            <MetadataRow label="Parent doc" value={item.parent_id} />
            <MetadataRow label="Source ID" value={item.source_id ?? null} />
            <MetadataRow label="Raw path" value={item.raw_doc_path} />
          </div>
        </section>

        {item.asset_paths.length ? (
          <section className="editorial-panel">
            <p className="section-kicker">Attached files</p>
            <div className="mt-4 space-y-3">
              {item.asset_paths.map((assetPath) => (
                <div
                  key={assetPath}
                  className="flex items-start gap-3 rounded-2xl border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.56)] px-4 py-4"
                >
                  <FolderTree className="mt-0.5 h-4 w-4 text-[var(--accent)]" />
                  <div>
                    <p className="text-sm font-medium text-[var(--ink)]">{assetPath}</p>
                    <p className="mt-1 text-xs leading-5 text-[var(--muted)]">Stored in the source raw folder on the Mac and mirrored into the published bundle when needed.</p>
                  </div>
                </div>
              ))}
            </div>
          </section>
        ) : null}

        <section className="editorial-panel">
          <p className="section-kicker">Links</p>
          <div className="mt-4 space-y-3">
            <a className="secondary-button w-full justify-center" href={item.canonical_url} rel="noreferrer" target="_blank">
              <ExternalLink className="h-4 w-4" />
              Open source
            </a>
            {item.outbound_links.slice(0, 5).map((link) => (
              <a className="block text-sm leading-6 text-[var(--muted)] underline-offset-4 hover:underline" href={link} key={link} rel="noreferrer" target="_blank">
                {link}
              </a>
            ))}
          </div>
        </section>

        <SimilarPapersPanel
          papers={similarPapers}
          hrefForItemId={(id) => (manifest.items[id] ? `/items/${id}?record=${encodeURIComponent(manifest.edition.record_name)}` : null)}
        />
      </aside>
    </div>
  );
}

function ContentBlock({ label, value }: { label: string; value: string }) {
  return (
    <div className="content-block">
      <p className="content-label">{label}</p>
      <MarkdownText className="mt-3">{value}</MarkdownText>
    </div>
  );
}

function MetadataRow({ label, value }: { label: string; value: string | null | undefined }) {
  if (!value) return null;
  return (
    <div className="flex items-start justify-between gap-4 border-t border-[var(--ink)]/8 pt-3 first:border-t-0 first:pt-0">
      <span className="font-mono text-[11px] uppercase tracking-[0.16em] text-[var(--muted)]">{label}</span>
      <span className="text-right text-sm leading-6 text-[var(--ink)]">{value}</span>
    </div>
  );
}

export function HostedViewerApp({ config }: { config: RuntimeConfig }) {
  const cloudKitConfig = config.cloudKit;
  const client = useMemo(() => {
    if (!cloudKitConfig) return null;
    return new PublishedDataClient(cloudKitConfig);
  }, [cloudKitConfig]);
  const [pairedLocalUrl, setPairedLocalUrlState] = useState<string | null>(() => getStoredPairedLocalUrl() ?? config.pairedLocalUrl ?? null);

  useEffect(() => {
    const url = new URL(window.location.href);
    const nextPairedLocalUrl = url.searchParams.get("pairedLocalUrl");
    if (!nextPairedLocalUrl) return;
    setStoredPairedLocalUrl(nextPairedLocalUrl);
    setPairedLocalUrlState(nextPairedLocalUrl);
    url.searchParams.delete("pairedLocalUrl");
    window.history.replaceState({}, "", `${url.pathname}${url.search}${url.hash}`);
  }, []);

  if (!client) {
    return <div className="loading-screen">CloudKit is not configured for the hosted viewer.</div>;
  }

  return (
    <QueryClientProvider client={queryClient}>
      <PublishedDataContext.Provider value={client}>
        <BrowserRouter>
          <Routes>
            <Route element={<HostedShell pairedLocalUrl={pairedLocalUrl} />}>
              <Route element={<HostedBriefPage />} index />
              <Route element={<HostedItemDetailPage />} path="items/:itemId" />
            </Route>
          </Routes>
        </BrowserRouter>
      </PublishedDataContext.Provider>
    </QueryClientProvider>
  );
}
