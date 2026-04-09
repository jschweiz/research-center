import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { ChevronLeft, FolderTree, Link2, SquareArrowOutUpRight } from "lucide-react";
import { Link, useLocation, useParams } from "react-router-dom";

import { MarkdownText } from "../components/MarkdownText";
import { PaperAudioPlayer } from "../components/PaperAudioPlayer";
import { SimilarPapersPanel } from "../components/SimilarPapersPanel";
import { resolvePaperAudioUrl, resolvePaperFiledText, resolvePaperSummary, resolveSimilarPapers } from "../lib/paper-details";
import { LocalControlError, localControlClient } from "./client";

const CONTENT_TYPE_LABEL: Record<string, string> = {
  article: "Article",
  news: "News",
  newsletter: "Newsletter",
  paper: "Paper",
  post: "Post",
  signal: "Signal",
  thread: "Thread",
};

function formatItemDate(value: string | null) {
  if (!value) return "Undated";

  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
    year: "numeric",
  }).format(new Date(value));
}

function formatEnrichmentStatus(status: string) {
  if (status === "succeeded") return "Ready";
  if (status === "failed") return "Failed";
  if (status === "interrupted") return "Interrupted";
  if (status === "running") return "Running";
  return "Pending";
}

function formatLabel(value: string) {
  return value.replace(/_/g, " ");
}

function readErrorMessage(error: unknown) {
  if (error instanceof LocalControlError) return error.message;
  return error instanceof Error && error.message ? error.message : "The document could not be loaded.";
}

function formatDocumentsPath(search: string) {
  return search ? `/documents${search}` : "/documents";
}

function formatDocumentDetailPath(itemId: string, search: string) {
  return search ? `/documents/${itemId}${search}` : `/documents/${itemId}`;
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

function EnrichmentBlock({
  label,
  body,
}: {
  label: string;
  body: string | null | undefined;
}) {
  if (!body?.trim()) return null;

  return (
    <article className="content-block">
      <p className="content-label">{label}</p>
      <MarkdownText className="mt-3">{body}</MarkdownText>
    </article>
  );
}

export function LocalControlDocumentDetailPage() {
  const location = useLocation();
  const { itemId = "" } = useParams();
  const itemQuery = useQuery({
    queryKey: ["local-control", "document", itemId],
    queryFn: () => localControlClient.getDocument(itemId),
    enabled: Boolean(itemId),
    retry: false,
  });

  const sections = useMemo(() => {
    if (!itemQuery.data) return [];
    const { item } = { item: itemQuery.data };
    const summary = resolvePaperSummary(item);
    return [
      { label: "Short summary", body: summary },
      { label: "Why it matters", body: item.insight.why_it_matters },
      { label: "What is new", body: item.insight.whats_new },
      { label: "Caveats", body: item.insight.caveats },
      { label: "Contribution", body: item.insight.contribution },
      { label: "Method", body: item.insight.method },
      { label: "Result", body: item.insight.result },
      { label: "Limitation", body: item.insight.limitation },
      { label: "Possible extension", body: item.insight.possible_extension },
      { label: "Deeper summary", body: item.insight.deeper_summary },
    ].filter((section) => section.body?.trim());
  }, [itemQuery.data]);

  if (itemQuery.isLoading) return <div className="page-loading">Loading document…</div>;
  if (itemQuery.isError) {
    return (
      <div className="editorial-panel">
        <p className="section-kicker">Document unavailable</p>
        <p className="mt-3 text-sm leading-6 text-[var(--danger)]">{readErrorMessage(itemQuery.error)}</p>
        <Link className="secondary-button mt-5 w-fit" to={formatDocumentsPath(location.search)}>
          <ChevronLeft className="h-4 w-4" />
          Back to documents
        </Link>
      </div>
    );
  }
  if (!itemQuery.data) return <div className="page-empty">Document not found.</div>;

  const item = itemQuery.data;
  const authorLabel = item.authors.join(", ").trim();
  const followUps = item.insight.follow_up_questions.filter((entry) => entry.trim());
  const experimentIdeas = item.insight.experiment_ideas.filter((entry) => entry.trim());
  const filedText = resolvePaperFiledText(item);
  const audioUrl = resolvePaperAudioUrl(item);
  const similarPapers = resolveSimilarPapers(item);

  return (
    <div className="grid gap-6 pb-10 xl:grid-cols-[minmax(0,1.25fr)_360px]">
      <section className="editorial-panel">
        <Link className="secondary-button w-fit" to={formatDocumentsPath(location.search)}>
          <ChevronLeft className="h-4 w-4" />
          Back to documents
        </Link>

        <p className="mt-6 section-kicker">{item.source_name}</p>
        <h3 className="mt-3 font-display text-5xl leading-tight text-[var(--ink)]">{item.title}</h3>
        <div className="mt-5 flex flex-wrap gap-3 font-mono text-xs uppercase tracking-[0.18em] text-[var(--muted)]">
          {authorLabel ? <span>{authorLabel}</span> : null}
          <span>{CONTENT_TYPE_LABEL[item.content_type] ?? item.content_type}</span>
          {item.kind && item.kind !== item.content_type ? <span>{item.kind}</span> : null}
          <span>{formatItemDate(item.published_at)}</span>
        </div>

        <div className="mt-8 space-y-5">
          {sections.length ? sections.map((section) => <EnrichmentBlock key={section.label} body={section.body} label={section.label} />) : null}

          {followUps.length ? (
            <article className="content-block">
              <p className="content-label">Follow-up questions</p>
              <ul className="mt-3 list-disc space-y-2 pl-5 text-sm leading-6 text-[var(--ink)]">
                {followUps.map((question) => (
                  <li key={question}>{question}</li>
                ))}
              </ul>
            </article>
          ) : null}

          {experimentIdeas.length ? (
            <article className="content-block">
              <p className="content-label">Experiment ideas</p>
              <ul className="mt-3 list-disc space-y-2 pl-5 text-sm leading-6 text-[var(--ink)]">
                {experimentIdeas.map((idea) => (
                  <li key={idea}>{idea}</li>
                ))}
              </ul>
            </article>
          ) : null}

          <article className="content-block">
            <p className="content-label">Filed text</p>
            <MarkdownText className="mt-3">
              {filedText?.trim() || "No normalized text is available for this raw document yet."}
            </MarkdownText>
          </article>
        </div>
      </section>

      <aside className="space-y-4">
        {audioUrl ? <PaperAudioPlayer audioUrl={audioUrl} variant="compact" /> : null}

        <section className="editorial-panel">
          <p className="section-kicker">Enrichment status</p>
          <div className="mt-4 space-y-3 text-sm leading-6 text-[var(--muted-strong)]">
            <MetadataRow label="Status" value={formatEnrichmentStatus(item.lightweight_enrichment_status)} />
            <MetadataRow label="Enriched at" value={item.lightweight_enriched_at ? formatItemDate(item.lightweight_enriched_at) : null} />
            <MetadataRow label="Score bucket" value={formatLabel(item.score.bucket)} />
            <MetadataRow label="Total score" value={item.score.total_score.toFixed(2)} />
            <MetadataRow label="Extraction confidence" value={item.extraction_confidence.toFixed(2)} />
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
                    <p className="mt-1 text-xs leading-5 text-[var(--muted)]">Stored alongside the canonical raw document folder.</p>
                  </div>
                </div>
              ))}
            </div>
          </section>
        ) : null}

        {item.also_mentioned_in.length ? (
          <section className="editorial-panel">
            <p className="section-kicker">Related mentions</p>
            <div className="mt-4 space-y-3">
              {item.also_mentioned_in.slice(0, 6).map((mention) => (
                <Link
                  key={mention.item_id}
                  className="block rounded-2xl border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.56)] px-4 py-4 transition hover:border-[var(--ink)]/16"
                  to={formatDocumentDetailPath(mention.item_id, location.search)}
                >
                  <p className="text-sm font-medium leading-6 text-[var(--ink)]">{mention.title}</p>
                  <p className="mt-1 text-xs leading-5 text-[var(--muted)]">{mention.source_name}</p>
                </Link>
              ))}
            </div>
          </section>
        ) : null}

        <section className="editorial-panel">
          <p className="section-kicker">Links</p>
          <div className="mt-4 space-y-3">
            <a className="secondary-button w-full justify-center" href={item.canonical_url} rel="noreferrer" target="_blank">
              <SquareArrowOutUpRight className="h-4 w-4" />
              Open source
            </a>
            {item.outbound_links.slice(0, 8).map((link) => (
              <a key={link} className="block text-sm leading-6 text-[var(--muted)] underline-offset-4 hover:underline" href={link} rel="noreferrer" target="_blank">
                <Link2 className="mr-2 inline h-4 w-4" />
                {link}
              </a>
            ))}
            {!item.outbound_links.length ? (
              <p className="text-sm leading-6 text-[var(--muted)]">No outbound links were indexed for this document.</p>
            ) : null}
          </div>
        </section>

        <SimilarPapersPanel papers={similarPapers} hrefForItemId={(id) => `/documents/${id}`} />
      </aside>
    </div>
  );
}
