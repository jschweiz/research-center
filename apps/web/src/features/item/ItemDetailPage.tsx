import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Clock3, FolderTree, Link2, SquareArrowOutUpRight } from "lucide-react";
import { useParams } from "react-router-dom";

import { api } from "../../api/client";
import { MarkdownText } from "../../components/MarkdownText";
import type { MediumDigestActionState } from "../../components/MediumDigestTable";
import { MediumDigestTable } from "../../components/MediumDigestTable";
import { PaperAudioPlayer } from "../../components/PaperAudioPlayer";
import { QuickActions } from "../../components/QuickActions";
import { SimilarPapersPanel } from "../../components/SimilarPapersPanel";
import { resolveExternalUrl } from "../../lib/external-links";
import { isMediumDigestItem, parseMediumDigestArticles } from "../../lib/medium-newsletter";
import { resolvePaperAudioUrl, resolvePaperFiledText, resolvePaperSummary, resolveSimilarPapers } from "../../lib/paper-details";

const contentTypeLabel: Record<string, string> = {
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

  return new Intl.DateTimeFormat("en-GB", {
    day: "2-digit",
    month: "2-digit",
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

function MetadataRow({ label, value }: { label: string; value: string | null | undefined }) {
  if (!value) return null;
  return (
    <div className="flex items-start justify-between gap-4 border-t border-[var(--ink)]/8 pt-3 first:border-t-0 first:pt-0">
      <span className="font-mono text-[11px] uppercase tracking-[0.16em] text-[var(--muted)]">{label}</span>
      <span className="text-right text-sm leading-6 text-[var(--ink)]">{value}</span>
    </div>
  );
}

export function ItemDetailPage() {
  const { itemId = "" } = useParams();
  const queryClient = useQueryClient();
  const [mediumActionState, setMediumActionState] = useState<Record<string, MediumDigestActionState>>({});
  const itemQuery = useQuery({
    queryKey: ["item", itemId],
    queryFn: () => api.getItem(itemId),
    enabled: Boolean(itemId),
  });
  const mediumArticles = useMemo(
    () => (itemQuery.data && isMediumDigestItem(itemQuery.data) ? parseMediumDigestArticles(itemQuery.data.cleaned_text) : []),
    [itemQuery.data],
  );
  const addMediumArticle = useMutation({
    mutationFn: (url: string) => api.importUrlWithSummary(url),
    onMutate: (url) => {
      setMediumActionState((current) => ({
        ...current,
        [url]: { status: "pending" },
      }));
    },
    onError: (error, url) => {
      setMediumActionState((current) => ({
        ...current,
        [url]: {
          status: "failed",
          message: error instanceof Error && error.message ? error.message : "The article could not be added right now.",
        },
      }));
    },
    onSuccess: async (createdItem, url) => {
      setMediumActionState((current) => ({
        ...current,
        [url]: {
          status: "succeeded",
          itemId: createdItem.id,
        },
      }));
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["items"] }),
        queryClient.invalidateQueries({ queryKey: ["item", itemId] }),
      ]);
    },
  });

  if (itemQuery.isLoading) return <div className="page-loading">Loading item…</div>;
  if (!itemQuery.data) return <div className="page-empty">Item not found.</div>;

  const item = itemQuery.data;
  const authorLabel = item.authors.join(", ").trim();
  const summary = resolvePaperSummary(item);
  const filedText = resolvePaperFiledText(item);
  const audioUrl = resolvePaperAudioUrl(item);
  const similarPapers = resolveSimilarPapers(item);
  const canonicalUrl = resolveExternalUrl(item.canonical_url);

  return (
    <div className="grid gap-6 pb-10 xl:grid-cols-[minmax(0,1.3fr)_380px]">
      <section className="editorial-panel">
        <p className="section-kicker">{item.source_name}</p>
        <h3 className="mt-3 font-display text-5xl leading-tight text-[var(--ink)]">{item.title}</h3>
        <div className="mt-5 flex flex-wrap gap-3 font-mono text-xs uppercase tracking-[0.18em] text-[var(--muted)]">
          {authorLabel ? <span>{authorLabel}</span> : null}
          <span>{contentTypeLabel[item.content_type] ?? item.content_type}</span>
          {item.kind && item.kind !== item.content_type ? <span>{item.kind}</span> : null}
          <span>{formatItemDate(item.published_at)}</span>
        </div>

        {audioUrl ? (
          <div className="mt-8">
            <PaperAudioPlayer audioUrl={audioUrl} />
          </div>
        ) : null}

        <div className="mt-8 space-y-5">
          <div className="content-block">
            <p className="content-label">Short summary</p>
            <MarkdownText className="mt-3">
              {summary ?? "No summary has been written for this raw document yet."}
            </MarkdownText>
          </div>

          <MediumDigestTable
            articles={mediumArticles}
            onAddToVault={(url) => addMediumArticle.mutate(url)}
            stateByUrl={mediumActionState}
          />

          <div className="content-block">
            <p className="content-label">Filed text</p>
            <MarkdownText className="mt-3">
              {filedText?.trim() || "No normalized text is available for this raw document yet."}
            </MarkdownText>
          </div>
        </div>

        <div className="mt-8 rounded-[1.7rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.46)] px-5 py-5">
          <p className="content-label">Actions</p>
          <div className="mt-4">
            <QuickActions itemId={item.id} starred={item.starred} triageStatus={item.triage_status} url={item.canonical_url} />
          </div>
        </div>
      </section>

      <aside className="space-y-4">
        <section className="editorial-panel">
          <p className="section-kicker">Vault metadata</p>
          <div className="mt-4 space-y-3 text-sm leading-6 text-[var(--muted-strong)]">
            <MetadataRow label="Enrichment" value={formatEnrichmentStatus(item.lightweight_enrichment_status)} />
            <MetadataRow label="Enriched at" value={item.lightweight_enriched_at ? formatItemDate(item.lightweight_enriched_at) : null} />
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

        <section className="editorial-panel">
          <p className="section-kicker">Links</p>
          <div className="mt-4 space-y-3">
            <a className="secondary-button w-full justify-center" href={canonicalUrl} rel="noreferrer" target="_blank">
              <SquareArrowOutUpRight className="h-4 w-4" />
              Open source
            </a>
            {item.outbound_links.slice(0, 8).map((link) => (
              <a
                key={link}
                className="block text-sm leading-6 text-[var(--muted)] underline-offset-4 hover:underline"
                href={resolveExternalUrl(link)}
                rel="noreferrer"
                target="_blank"
              >
                <Link2 className="mr-2 inline h-4 w-4" />
                {resolveExternalUrl(link)}
              </a>
            ))}
            {!item.outbound_links.length ? (
              <p className="text-sm leading-6 text-[var(--muted)]">No outbound links were indexed for this document.</p>
            ) : null}
          </div>
        </section>

        <SimilarPapersPanel papers={similarPapers} hrefForItemId={(id) => `/items/${id}`} />

        <section className="editorial-panel">
          <p className="section-kicker">Pipeline</p>
          <div className="mt-4 space-y-3 text-sm leading-6 text-[var(--muted-strong)]">
            <div className="flex items-start gap-3 rounded-2xl border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.56)] px-4 py-4">
              <Clock3 className="mt-0.5 h-4 w-4 text-[var(--accent)]" />
              <div>
                <p className="font-medium text-[var(--ink)]">Fetch</p>
                <p className="mt-1 text-[var(--muted)]">The raw document was discovered and normalized without any LLM generation.</p>
              </div>
            </div>
            <div className="flex items-start gap-3 rounded-2xl border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.56)] px-4 py-4">
              <Clock3 className="mt-0.5 h-4 w-4 text-[var(--accent)]" />
              <div>
                <p className="font-medium text-[var(--ink)]">Lightweight enrichment</p>
                <p className="mt-1 text-[var(--muted)]">Only small metadata such as authors, tags, and a short summary should appear here in v1.</p>
              </div>
            </div>
          </div>
        </section>
      </aside>
    </div>
  );
}
