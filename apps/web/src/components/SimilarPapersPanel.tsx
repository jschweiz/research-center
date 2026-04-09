import { Link } from "react-router-dom";
import { ArrowRight, SquareArrowOutUpRight } from "lucide-react";

import { SkimmableText } from "./SkimmableText";

type SimilarPaper = {
  title: string;
  canonical_url: string;
  app_item_id: string | null;
  authors: string[];
  short_summary: string | null;
};

export function SimilarPapersPanel({
  papers,
  hrefForItemId,
}: {
  papers: SimilarPaper[];
  hrefForItemId?: (itemId: string) => string | null;
}) {
  if (!papers.length) return null;

  return (
    <section className="editorial-panel">
      <p className="section-kicker">Similar papers</p>
      <div className="mt-4 space-y-3">
        {papers.map((paper) => {
          const appHref = paper.app_item_id ? hrefForItemId?.(paper.app_item_id) ?? null : null;
          const byline = paper.authors.join(", ").trim();
          const body = (
            <>
              <div className="flex items-start justify-between gap-3">
                <div className="min-w-0">
                  <p className="text-sm font-medium leading-6 text-[var(--ink)]">{paper.title}</p>
                  {byline ? <p className="mt-1 text-xs leading-5 text-[var(--muted)]">{byline}</p> : null}
                </div>
                {appHref ? <ArrowRight className="mt-0.5 h-4 w-4 text-[var(--accent)]" /> : <SquareArrowOutUpRight className="mt-0.5 h-4 w-4 text-[var(--accent)]" />}
              </div>
              {paper.short_summary ? (
                <SkimmableText className="mt-2 text-sm leading-6 text-[var(--muted-strong)]">{paper.short_summary}</SkimmableText>
              ) : null}
            </>
          );

          if (appHref) {
            return (
              <Link
                key={`${paper.title}-${paper.canonical_url}`}
                className="block rounded-2xl border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.56)] px-4 py-4 transition hover:border-[var(--ink)]/16"
                to={appHref}
              >
                {body}
              </Link>
            );
          }

          return (
            <a
              key={`${paper.title}-${paper.canonical_url}`}
              className="block rounded-2xl border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.56)] px-4 py-4 transition hover:border-[var(--ink)]/16"
              href={paper.canonical_url}
              rel="noreferrer"
              target="_blank"
            >
              {body}
            </a>
          );
        })}
      </div>
    </section>
  );
}
