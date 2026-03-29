import * as Tabs from "@radix-ui/react-tabs";
import { useQuery } from "@tanstack/react-query";
import { useParams } from "react-router-dom";
import { Link2, SquareArrowOutUpRight } from "lucide-react";

import { api } from "../../api/client";
import { QuickActions } from "../../components/QuickActions";
import { SkimmableText } from "../../components/SkimmableText";

function formatItemDate(value: string | null) {
  if (!value) return "Undated";

  return new Intl.DateTimeFormat("en-GB", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
  }).format(new Date(value));
}

export function ItemDetailPage() {
  const { itemId = "" } = useParams();
  const itemQuery = useQuery({
    queryKey: ["item", itemId],
    queryFn: () => api.getItem(itemId),
    enabled: Boolean(itemId),
  });

  if (itemQuery.isLoading) return <div className="page-loading">Loading item…</div>;
  if (!itemQuery.data) return <div className="page-empty">Item not found.</div>;

  const item = itemQuery.data;
  const authorLabel = item.authors.join(", ").trim();

  return (
    <div className="grid gap-6 pb-10 xl:grid-cols-[minmax(0,1.3fr)_380px]">
      <section className="editorial-panel">
        <p className="section-kicker">{item.source_name}</p>
        <h3 className="mt-3 font-display text-5xl leading-tight text-[var(--ink)]">{item.title}</h3>
        <div className="mt-5 flex flex-wrap gap-3 font-mono text-xs uppercase tracking-[0.18em] text-[var(--muted)]">
          {authorLabel ? <span>{authorLabel}</span> : null}
          <span>{item.content_type}</span>
          <span>{formatItemDate(item.published_at)}</span>
          <span>{Math.round(item.score.total_score * 100)} total score</span>
        </div>

        <div className="mt-8 grid gap-6 lg:grid-cols-[minmax(0,1fr)_minmax(0,0.9fr)]">
          <div className="space-y-5">
            <div className="content-block">
              <p className="content-label">Summary</p>
              <SkimmableText>{item.insight.short_summary ?? "Summary pending."}</SkimmableText>
            </div>
            <div className="content-block">
              <p className="content-label">Why this matters</p>
              <SkimmableText>{item.insight.why_it_matters ?? "Signal rationale pending."}</SkimmableText>
            </div>
            <div className="content-block">
              <p className="content-label">What’s actually new</p>
              <SkimmableText>{item.insight.whats_new ?? "Novelty framing pending."}</SkimmableText>
            </div>
            <div className="content-block">
              <p className="content-label">Caveats</p>
              <SkimmableText>{item.insight.caveats ?? "No caveats captured yet."}</SkimmableText>
            </div>
          </div>

          <div className="editorial-panel bg-[rgba(255,255,255,0.45)]">
            <p className="content-label">Follow-up prompts</p>
            <ul className="mt-4 space-y-3 text-sm leading-6 text-[var(--muted-strong)]">
              {item.insight.follow_up_questions.map((question) => (
                <SkimmableText key={question} as="li">{question}</SkimmableText>
              ))}
            </ul>
            <div className="mt-6">
              <QuickActions itemId={item.id} starred={item.starred} url={item.canonical_url} />
            </div>
          </div>
        </div>

        {item.content_type === "paper" ? (
          <Tabs.Root className="mt-10" defaultValue="contribution">
            <Tabs.List className="flex flex-wrap gap-2">
              {[
                ["contribution", "Contribution"],
                ["method", "Method"],
                ["result", "Result"],
                ["limitation", "Limitation"],
              ].map(([value, label]) => (
                <Tabs.Trigger key={value} className="filter-pill" value={value}>
                  {label}
                </Tabs.Trigger>
              ))}
            </Tabs.List>
            <Tabs.Content className="content-block mt-5" value="contribution">
              <SkimmableText>{item.insight.contribution ?? item.insight.short_summary ?? "No contribution summary yet."}</SkimmableText>
            </Tabs.Content>
            <Tabs.Content className="content-block mt-5" value="method">
              <SkimmableText>{item.insight.method ?? "Method notes not generated yet."}</SkimmableText>
            </Tabs.Content>
            <Tabs.Content className="content-block mt-5" value="result">
              <SkimmableText>{item.insight.result ?? "Result framing not generated yet."}</SkimmableText>
            </Tabs.Content>
            <Tabs.Content className="content-block mt-5" value="limitation">
              <SkimmableText>{item.insight.limitation ?? "Limitation notes not generated yet."}</SkimmableText>
            </Tabs.Content>
          </Tabs.Root>
        ) : null}

        {item.insight.deeper_summary ? (
          <div className="content-block mt-8">
            <p className="content-label">Deeper analysis</p>
            <SkimmableText>{item.insight.deeper_summary}</SkimmableText>
            <ul className="mt-4 space-y-3 text-sm leading-6 text-[var(--muted-strong)]">
              {item.insight.experiment_ideas.map((idea) => (
                <SkimmableText key={idea} as="li">{idea}</SkimmableText>
              ))}
            </ul>
          </div>
        ) : null}
      </section>

      <aside className="space-y-4">
        <section className="editorial-panel">
          <p className="section-kicker">Reason trace</p>
          <ul className="mt-4 space-y-3 text-sm leading-6 text-[var(--muted-strong)]">
            {Object.entries(item.score.reason_trace).map(([key, value]) => (
              <li key={key} className="flex items-start justify-between gap-4 border-t border-[var(--ink)]/8 pt-3 first:border-t-0 first:pt-0">
                <span className="font-mono text-[11px] uppercase tracking-[0.16em] text-[var(--muted)]">{key}</span>
                <span className="text-right">{String(value)}</span>
              </li>
            ))}
          </ul>
        </section>

        <section className="editorial-panel">
          <p className="section-kicker">Also mentioned in</p>
          <div className="mt-4 space-y-4">
            {item.also_mentioned_in.length ? (
              item.also_mentioned_in.map((related) => (
                <a
                  key={related.item_id}
                  className="block rounded-2xl border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.56)] px-4 py-4 transition hover:-translate-y-0.5"
                  href={related.canonical_url}
                  rel="noreferrer"
                  target="_blank"
                >
                  <p className="font-medium text-[var(--ink)]">{related.title}</p>
                  <p className="mt-2 text-sm text-[var(--muted)]">{related.source_name}</p>
                </a>
              ))
            ) : (
              <p className="text-sm leading-6 text-[var(--muted)]">No clustered mentions yet.</p>
            )}
          </div>
        </section>

        <section className="editorial-panel">
          <p className="section-kicker">Links</p>
          <div className="mt-4 space-y-3">
            <a className="secondary-button w-full justify-center" href={item.canonical_url} rel="noreferrer" target="_blank">
              <SquareArrowOutUpRight className="h-4 w-4" />
              Open source
            </a>
            {item.outbound_links.slice(0, 5).map((link) => (
              <a key={link} className="block text-sm leading-6 text-[var(--muted)] underline-offset-4 hover:underline" href={link} rel="noreferrer" target="_blank">
                <Link2 className="mr-2 inline h-4 w-4" />
                {link}
              </a>
            ))}
          </div>
        </section>
      </aside>
    </div>
  );
}
