import { createContext, useContext, useEffect, useMemo, useState } from "react";
import { QueryClientProvider, useQuery } from "@tanstack/react-query";
import { BrowserRouter, HashRouter, Link, NavLink, Outlet, Route, Routes, useNavigate, useParams, useSearchParams } from "react-router-dom";
import { ArrowLeft, ChevronDown, ExternalLink, FolderTree, LibraryBig, Smartphone, TabletSmartphone } from "lucide-react";
import clsx from "clsx";

import { MarkdownText } from "../components/MarkdownText";
import { PaperAudioPlayer } from "../components/PaperAudioPlayer";
import { SimilarPapersPanel } from "../components/SimilarPapersPanel";
import { SkimmableText } from "../components/SkimmableText";
import { resolveExternalUrl } from "../lib/external-links";
import { resolvePaperAudioUrl, resolvePaperFiledText, resolvePaperSummary, resolveSimilarPapers } from "../lib/paper-details";
import { queryClient } from "../lib/query-client";
import {
  getInstallHintDismissed,
  getStoredPairedLocalUrl,
  setInstallHintDismissed,
  setStoredPairedLocalUrl,
} from "../runtime/storage";
import type {
  PublishedDigestEntry,
  PublishedEditionManifest,
  PublishedEditionSummary,
  PublishedPaperTableEntry,
  RuntimeConfig,
} from "../runtime/types";
import {
  CloudKitPublishedDataClient,
  type PublishedDataClient,
  StaticPublishedDataClient,
} from "./cloudkit-client";
import {
  HostedAudioMiniPlayer,
  HostedAudioProvider,
  HostedAudioSheet,
  HostedAudioTeaser,
  HostedAudioUtilityCard,
  HostedEditionAudioBridge,
  useHostedAudio,
} from "./HostedAudioController";
import { PublishedLeadStory, PublishedStoryRow } from "./PublishedItemCard";

type PublishedChromeContextValue = {
  isStaticPublished: boolean;
  pairedLocalUrl: string | null;
};

type FeedBlock =
  | { kind: "divider"; count: number; id: string; title: string }
  | { kind: "leadStory"; entry: PublishedDigestEntry; id: string }
  | { kind: "paperRow"; entry: PublishedPaperTableEntry; id: string }
  | { kind: "storyRow"; entry: PublishedDigestEntry; id: string };

const PublishedDataContext = createContext<PublishedDataClient | null>(null);
const PublishedChromeContext = createContext<PublishedChromeContextValue | null>(null);

function usePublishedDataClient() {
  const client = useContext(PublishedDataContext);
  if (!client) {
    throw new Error("Published data client is not available.");
  }
  return client;
}

function usePublishedChrome() {
  const value = useContext(PublishedChromeContext);
  if (!value) {
    throw new Error("Published chrome context is not available.");
  }
  return value;
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

function buildFeedBlocks(manifest: PublishedEditionManifest): FeedBlock[] {
  const blocks: FeedBlock[] = [];
  const shortlist = manifest.digest.editorial_shortlist;

  if (shortlist[0]) {
    blocks.push({
      kind: "leadStory",
      entry: shortlist[0],
      id: `lead-${shortlist[0].item.id}`,
    });
  }

  for (const entry of shortlist.slice(1)) {
    blocks.push({
      kind: "storyRow",
      entry,
      id: `shortlist-${entry.item.id}`,
    });
  }

  const sections: Array<{ id: string; title: string; entries: PublishedDigestEntry[] }> = [
    {
      id: "headlines",
      title: "Headlines",
      entries: manifest.digest.headlines,
    },
    {
      id: "side-signals",
      title: "Interesting side signals",
      entries: manifest.digest.interesting_side_signals,
    },
    {
      id: "remaining-reads",
      title: "Remaining reads",
      entries: manifest.digest.remaining_reads,
    },
  ];

  for (const section of sections) {
    if (!section.entries.length) continue;
    blocks.push({
      kind: "divider",
      count: section.entries.length,
      id: section.id,
      title: section.title,
    });
    for (const entry of section.entries) {
      blocks.push({
        kind: "storyRow",
        entry,
        id: `${section.id}-${entry.item.id}`,
      });
    }
  }

  if (manifest.digest.papers_table.length) {
    blocks.push({
      kind: "divider",
      count: manifest.digest.papers_table.length,
      id: "papers",
      title: "Top papers",
    });
    for (const entry of manifest.digest.papers_table) {
      blocks.push({
        kind: "paperRow",
        entry,
        id: `paper-${entry.item.id}`,
      });
    }
  }

  return blocks;
}

function isStandaloneDisplayMode() {
  if (typeof window === "undefined") return false;
  const navigatorWithStandalone = navigator as Navigator & { standalone?: boolean };
  return window.matchMedia("(display-mode: standalone)").matches || navigatorWithStandalone.standalone === true;
}

function isIosSafari() {
  if (typeof navigator === "undefined") return false;
  const userAgent = navigator.userAgent;
  const isIos = /iPad|iPhone|iPod/i.test(userAgent);
  const isWebKit = /WebKit/i.test(userAgent);
  const isOtherBrowser = /CriOS|FxiOS|EdgiOS|OPiOS/i.test(userAgent);
  return isIos && isWebKit && !isOtherBrowser;
}

function PublishedTopbar({
  homeHref,
  onOpenArchive,
}: {
  homeHref: string;
  onOpenArchive: () => void;
}) {
  return (
    <header className="pv-topbar">
      <Link className="pv-topbar-brand" to={homeHref}>
        <span className="pv-topbar-kicker">Research Center</span>
        <span className="pv-topbar-title">Published brief</span>
      </Link>

      <button className="ghost-button pv-topbar-button" onClick={onOpenArchive} type="button">
        <LibraryBig className="h-4 w-4" />
        <span>Editions</span>
        <ChevronDown className="h-4 w-4" />
      </button>
    </header>
  );
}

function PublishedMasthead({
  manifest,
}: {
  manifest: PublishedEditionManifest;
}) {
  return (
    <section className="pv-masthead">
      <p className="pv-eyebrow">Morning edition</p>
      <h1 className="pv-masthead-title">{manifest.digest.title}</h1>
      <p className="pv-masthead-meta">
        <span>{formatDateRange(manifest.digest.coverage_start, manifest.digest.coverage_end)}</span>
        <span>{manifest.available_editions.length} archived editions</span>
        <span>Published {formatDate(manifest.edition.published_at)}</span>
      </p>
      {manifest.digest.editorial_note ? (
        <SkimmableText className="pv-masthead-note">{manifest.digest.editorial_note}</SkimmableText>
      ) : null}
    </section>
  );
}

function PublishedSectionDivider({
  count,
  id,
  title,
}: {
  count: number;
  id: string;
  title: string;
}) {
  return (
    <div className="pv-section-divider" id={id}>
      <span className="pv-section-divider-title">{title}</span>
      <span className="pv-section-divider-count">{count}</span>
    </div>
  );
}

function EditionPicker({
  currentRecordName,
  editions,
  isOpen,
  onClose,
  onSelect,
  pairedLocalUrl,
}: {
  currentRecordName: string;
  editions: PublishedEditionSummary[];
  isOpen: boolean;
  onClose: () => void;
  onSelect: (recordName: string) => void;
  pairedLocalUrl: string | null;
}) {
  if (!isOpen) return null;

  return (
    <div className="pv-edition-backdrop" onClick={onClose} role="presentation">
      <div
        aria-label="Published editions"
        className="pv-edition-sheet"
        onClick={(event) => event.stopPropagation()}
        role="dialog"
      >
        <div className="pv-edition-sheet-header">
          <div>
            <p className="pv-eyebrow">Archive</p>
            <h3 className="pv-edition-sheet-title">Choose an edition</h3>
          </div>
          <button className="ghost-button" onClick={onClose} type="button">
            Close
          </button>
        </div>

        {pairedLocalUrl ? (
          <div className="pv-edition-sheet-actions">
            <a className="secondary-button" href={pairedLocalUrl}>
              <TabletSmartphone className="h-4 w-4" />
              Open Mac
            </a>
          </div>
        ) : null}

        <div className="pv-edition-list">
          {editions.map((edition) => {
            const isActive = edition.record_name === currentRecordName;
            return (
              <button
                className={clsx("pv-edition-option", isActive && "pv-edition-option-active")}
                key={edition.record_name}
                onClick={() => {
                  onSelect(edition.record_name);
                  onClose();
                }}
                type="button"
              >
                <span className="pv-edition-option-title">{edition.title}</span>
                <span className="pv-edition-option-meta">{formatEditionLabel(edition)}</span>
              </button>
            );
          })}
        </div>
      </div>
    </div>
  );
}

function InstallHintBanner() {
  const [dismissed, setDismissed] = useState(getInstallHintDismissed);

  if (dismissed || !isIosSafari() || isStandaloneDisplayMode()) return null;

  return (
    <section className="pv-install-banner">
      <div>
        <p className="pv-eyebrow">Install</p>
        <p className="pv-install-copy">
          In Safari, tap Share, then <strong>Add to Home Screen</strong> to keep this briefing beside your other apps.
        </p>
      </div>
      <div className="pv-install-actions">
        <span className="pv-install-pill">
          <Smartphone className="h-4 w-4" />
          Safari only
        </span>
        <button
          className="ghost-button"
          onClick={() => {
            setInstallHintDismissed(true);
            setDismissed(true);
          }}
          type="button"
        >
          Dismiss
        </button>
      </div>
    </section>
  );
}

function PublishedRail() {
  return (
    <aside className="pv-rail">
      <div className="pv-rail-brand">
        <span className="pv-rail-monogram">RC</span>
        <span className="pv-rail-label">Research Center</span>
      </div>

      <nav className="pv-rail-nav">
        <NavLink className={({ isActive }) => clsx("pv-rail-link", isActive && "pv-rail-link-active")} to="/">
          Brief
        </NavLink>
      </nav>
    </aside>
  );
}

function PublishedUtilityPanel({
  onOpenArchive,
  pairedLocalUrl,
}: {
  onOpenArchive: () => void;
  pairedLocalUrl: string | null;
}) {
  return (
    <aside className="pv-utility-column">
      <HostedAudioUtilityCard />

      <section className="pv-utility-card">
        <p className="pv-eyebrow">Edition tools</p>
        <h3 className="pv-utility-card-title">Browse the archive or reopen your paired Mac.</h3>
        <div className="pv-utility-actions">
          <button className="secondary-button" onClick={onOpenArchive} type="button">
            <LibraryBig className="h-4 w-4" />
            Editions
          </button>
          {pairedLocalUrl ? (
            <a className="secondary-button" href={pairedLocalUrl}>
              <TabletSmartphone className="h-4 w-4" />
              Open Mac
            </a>
          ) : null}
        </div>
      </section>
    </aside>
  );
}

function HostedShell({
  isStaticPublished,
  pairedLocalUrl,
}: {
  isStaticPublished: boolean;
  pairedLocalUrl: string | null;
}) {
  const { hasPlayableAudio } = useHostedAudio();

  return (
    <PublishedChromeContext.Provider value={{ isStaticPublished, pairedLocalUrl }}>
      <div className={clsx("pv-shell", hasPlayableAudio && "pv-shell-with-player")}>
        <div className="pv-frame">
          <PublishedRail />
          <main className="pv-main">
            <Outlet />
          </main>
        </div>
        <HostedAudioMiniPlayer pairedLocalUrl={pairedLocalUrl} />
        <HostedAudioSheet pairedLocalUrl={pairedLocalUrl} />
      </div>
    </PublishedChromeContext.Provider>
  );
}

function HostedBriefPage() {
  const client = usePublishedDataClient();
  const { isStaticPublished, pairedLocalUrl } = usePublishedChrome();
  const [searchParams, setSearchParams] = useSearchParams();
  const [isEditionPickerOpen, setEditionPickerOpen] = useState(false);
  const selectedRecordName = searchParams.get("record");

  const latestManifestQuery = useQuery({
    queryKey: ["published", "latest"],
    queryFn: () => client.fetchLatestManifest(),
    initialData: client.getCachedLatestManifest() ?? undefined,
  });

  const selectedManifestQuery = useQuery({
    queryKey: ["published", "record", selectedRecordName],
    queryFn: () => client.fetchManifest(selectedRecordName ?? ""),
    enabled: Boolean(selectedRecordName) && selectedRecordName !== latestManifestQuery.data?.edition.record_name,
    initialData: selectedRecordName ? client.getCachedManifest(selectedRecordName) ?? undefined : undefined,
  });

  const manifest =
    selectedRecordName && selectedRecordName !== latestManifestQuery.data?.edition.record_name
      ? selectedManifestQuery.data
      : latestManifestQuery.data;

  useEffect(() => {
    if (!latestManifestQuery.data || selectedRecordName) return;
    const nextSearch = new URLSearchParams(searchParams);
    nextSearch.set("record", latestManifestQuery.data.edition.record_name);
    setSearchParams(nextSearch, { replace: true });
  }, [latestManifestQuery.data, searchParams, selectedRecordName, setSearchParams]);

  useEffect(() => {
    if (!manifest) return;
    document.title = manifest.digest.title;
  }, [manifest]);

  if (latestManifestQuery.isLoading || (selectedRecordName && selectedManifestQuery.isLoading)) {
    return <div className="page-loading">Loading published brief…</div>;
  }
  if (latestManifestQuery.error || selectedManifestQuery.error || !manifest) {
    return <div className="page-empty">The published brief could not be loaded.</div>;
  }

  const feedBlocks = buildFeedBlocks(manifest);
  const briefHref = `/?record=${encodeURIComponent(manifest.edition.record_name)}`;

  const selectEdition = (recordName: string) => {
    const nextSearch = new URLSearchParams(searchParams);
    nextSearch.set("record", recordName);
    setSearchParams(nextSearch);
  };

  return (
    <div className="pv-page">
      <HostedEditionAudioBridge manifest={manifest} />
      <PublishedTopbar homeHref={briefHref} onOpenArchive={() => setEditionPickerOpen(true)} />

      <div className="pv-brief-layout">
        <div className="pv-feed-column">
          <PublishedMasthead manifest={manifest} />
          {isStaticPublished ? <InstallHintBanner /> : null}

          <div className="pv-feed">
            {manifest.digest.audio_brief?.status === "succeeded" ? <HostedAudioTeaser /> : null}

            {feedBlocks.map((block) => {
              if (block.kind === "divider") {
                return <PublishedSectionDivider count={block.count} id={block.id} key={block.id} title={block.title} />;
              }

              if (block.kind === "leadStory") {
                return (
                  <PublishedLeadStory
                    item={block.entry.item}
                    key={block.id}
                    note={block.entry.note}
                    rank={block.entry.rank}
                    recordName={manifest.edition.record_name}
                  />
                );
              }

              if (block.kind === "paperRow") {
                return (
                  <PublishedStoryRow
                    item={block.entry.item}
                    key={block.id}
                    note={block.entry.zotero_tags.join(" · ") || "Paper brief"}
                    rank={block.entry.rank}
                    recordName={manifest.edition.record_name}
                  />
                );
              }

              return (
                <PublishedStoryRow
                  item={block.entry.item}
                  key={block.id}
                  note={block.entry.note}
                  rank={block.entry.rank}
                  recordName={manifest.edition.record_name}
                />
              );
            })}
          </div>
        </div>

        <PublishedUtilityPanel onOpenArchive={() => setEditionPickerOpen(true)} pairedLocalUrl={pairedLocalUrl} />
      </div>

      <EditionPicker
        currentRecordName={manifest.edition.record_name}
        editions={manifest.available_editions}
        isOpen={isEditionPickerOpen}
        onClose={() => setEditionPickerOpen(false)}
        onSelect={selectEdition}
        pairedLocalUrl={pairedLocalUrl}
      />
    </div>
  );
}

function HostedItemDetailPage() {
  const client = usePublishedDataClient();
  const navigate = useNavigate();
  const { pairedLocalUrl } = usePublishedChrome();
  const { pause } = useHostedAudio();
  const { itemId = "" } = useParams();
  const [searchParams] = useSearchParams();
  const [isEditionPickerOpen, setEditionPickerOpen] = useState(false);
  const recordName = searchParams.get("record");

  const latestManifestQuery = useQuery({
    queryKey: ["published", "detail-latest"],
    queryFn: () => client.fetchLatestManifest(),
    enabled: !recordName,
    initialData: !recordName ? client.getCachedLatestManifest() ?? undefined : undefined,
  });
  const manifestQuery = useQuery({
    queryKey: ["published", "detail-manifest", recordName],
    queryFn: () => client.fetchManifest(recordName ?? ""),
    enabled: Boolean(recordName),
    initialData: recordName ? client.getCachedManifest(recordName) ?? undefined : undefined,
  });

  const manifest = recordName ? manifestQuery.data : latestManifestQuery.data;

  useEffect(() => {
    if (!manifest) return;
    const item = manifest.items[itemId];
    if (item) document.title = item.title;
  }, [itemId, manifest]);

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
  const canonicalUrl = resolveExternalUrl(item.canonical_url);
  const briefHref = `/?record=${encodeURIComponent(manifest.edition.record_name)}`;

  return (
    <div className="pv-page pv-detail-page">
      <HostedEditionAudioBridge manifest={manifest} />
      <PublishedTopbar homeHref={briefHref} onOpenArchive={() => setEditionPickerOpen(true)} />

      <div className="pv-detail-layout">
        <section className="editorial-panel pv-detail-main">
          <Link
            className="pv-back-link"
            to={briefHref}
          >
            <ArrowLeft className="h-4 w-4" />
            Back to brief
          </Link>

          <p className="pv-eyebrow pv-detail-source">{item.source_name}</p>
          <h1 className="pv-detail-title">{item.title}</h1>
          <div className="pv-detail-meta">
            {item.organization_name ? <span>{item.organization_name}</span> : null}
            {item.authors.length ? <span>{item.authors.join(", ")}</span> : null}
            <span>{item.content_type}</span>
            {item.kind && item.kind !== item.content_type ? <span>{item.kind}</span> : null}
            <span>{formatDate(item.published_at)}</span>
          </div>

          {audioUrl ? (
            <div className="pv-detail-audio">
              <PaperAudioPlayer audioUrl={audioUrl} onPlay={pause} />
            </div>
          ) : null}

          <div className="pv-detail-content">
            <ContentBlock label="Short summary" value={summary ?? "Summary pending."} />
            <ContentBlock label="Filed text" value={filedText ?? "No normalized text is available in the published snapshot."} />
          </div>
        </section>

        <aside className="pv-detail-side">
          <section className="editorial-panel">
            <p className="pv-eyebrow">Published metadata</p>
            <div className="pv-detail-metadata">
              <MetadataRow label="Enrichment" value={formatEnrichmentStatus(item.lightweight_enrichment_status)} />
              <MetadataRow label="Doc role" value={item.doc_role} />
              <MetadataRow label="Parent doc" value={item.parent_id} />
              <MetadataRow label="Source ID" value={item.source_id ?? null} />
              <MetadataRow label="Raw path" value={item.raw_doc_path} />
            </div>
          </section>

          {item.asset_paths.length ? (
            <section className="editorial-panel">
              <p className="pv-eyebrow">Attached files</p>
              <div className="pv-asset-list">
                {item.asset_paths.map((assetPath) => (
                  <div key={assetPath} className="pv-asset-row">
                    <FolderTree className="mt-0.5 h-4 w-4 text-[var(--accent)]" />
                    <div>
                      <p className="pv-asset-title">{assetPath}</p>
                      <p className="pv-asset-copy">
                        Stored in the source raw folder on the Mac and mirrored into the published bundle when needed.
                      </p>
                    </div>
                  </div>
                ))}
              </div>
            </section>
          ) : null}

          <section className="editorial-panel">
            <p className="pv-eyebrow">Links</p>
            <div className="pv-link-list">
              <a className="secondary-button w-full justify-center" href={canonicalUrl} rel="noreferrer" target="_blank">
                <ExternalLink className="h-4 w-4" />
                Open source
              </a>
              {item.outbound_links.slice(0, 5).map((link) => (
                <a
                  className="pv-link-row"
                  href={resolveExternalUrl(link)}
                  key={link}
                  rel="noreferrer"
                  target="_blank"
                >
                  {resolveExternalUrl(link)}
                </a>
              ))}
            </div>
          </section>

          <SimilarPapersPanel
            hrefForItemId={(id) => (manifest.items[id] ? `/items/${id}?record=${encodeURIComponent(manifest.edition.record_name)}` : null)}
            papers={similarPapers}
          />
        </aside>
      </div>

      <EditionPicker
        currentRecordName={manifest.edition.record_name}
        editions={manifest.available_editions}
        isOpen={isEditionPickerOpen}
        onClose={() => setEditionPickerOpen(false)}
        onSelect={(nextRecordName) => navigate(`/?record=${encodeURIComponent(nextRecordName)}`)}
        pairedLocalUrl={pairedLocalUrl}
      />
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
    <div className="pv-metadata-row">
      <span className="pv-metadata-label">{label}</span>
      <span className="pv-metadata-value">{value}</span>
    </div>
  );
}

export function HostedViewerApp({ config }: { config: RuntimeConfig }) {
  const isStaticPublished = Boolean(config.staticPublishedBasePath);
  const client = useMemo(() => {
    if (config.staticPublishedBasePath) {
      return new StaticPublishedDataClient(config.staticPublishedBasePath);
    }
    if (config.cloudKit) {
      return new CloudKitPublishedDataClient(config.cloudKit);
    }
    return null;
  }, [config.cloudKit, config.staticPublishedBasePath]);
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
    return <div className="loading-screen">The published viewer is not configured with a data source.</div>;
  }

  const RouterComponent = isStaticPublished ? HashRouter : BrowserRouter;

  return (
    <QueryClientProvider client={queryClient}>
      <PublishedDataContext.Provider value={client}>
        <RouterComponent>
          <HostedAudioProvider>
            <Routes>
              <Route element={<HostedShell isStaticPublished={isStaticPublished} pairedLocalUrl={pairedLocalUrl} />}>
                <Route element={<HostedBriefPage />} index />
                <Route element={<HostedItemDetailPage />} path="items/:itemId" />
              </Route>
            </Routes>
          </HostedAudioProvider>
        </RouterComponent>
      </PublishedDataContext.Provider>
    </QueryClientProvider>
  );
}
