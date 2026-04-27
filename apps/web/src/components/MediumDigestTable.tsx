import { resolveExternalUrl } from "../lib/external-links";
import type { MediumDigestArticle } from "../lib/medium-newsletter";

export type MediumDigestActionState =
  | {
      status: "pending" | "succeeded";
      itemId?: string;
      message?: string;
    }
  | {
      status: "failed";
      itemId?: string;
      message: string;
    }
  | undefined;

interface MediumDigestTableProps {
  articles: MediumDigestArticle[];
  onAddToVault: (url: string) => void;
  stateByUrl: Record<string, MediumDigestActionState>;
}

export function MediumDigestTable({
  articles,
  onAddToVault,
  stateByUrl,
}: MediumDigestTableProps) {
  if (!articles.length) return null;

  return (
    <article className="content-block">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <p className="content-label">Medium articles</p>
          <p className="mt-2 max-w-2xl text-sm leading-6 text-[var(--muted)]">
            Parsed directly from the digest so you can skim the issue and file the full article into the vault when it is worth a closer read.
          </p>
        </div>
        <p className="font-mono text-[11px] uppercase tracking-[0.16em] text-[var(--muted)]">
          {articles.length} article{articles.length === 1 ? "" : "s"}
        </p>
      </div>

      <div className="mt-4 overflow-hidden rounded-[1.5rem] border border-[var(--ink)]/8 bg-[rgba(255,255,255,0.56)]">
        <div className="overflow-x-auto">
          <table className="min-w-full border-collapse text-left">
            <thead className="bg-[rgba(17,19,18,0.03)]">
              <tr>
                <th className="px-4 py-3 font-mono text-[11px] uppercase tracking-[0.16em] text-[var(--muted)]">Title</th>
                <th className="px-4 py-3 font-mono text-[11px] uppercase tracking-[0.16em] text-[var(--muted)]">Claps</th>
                <th className="px-4 py-3 font-mono text-[11px] uppercase tracking-[0.16em] text-[var(--muted)]">Time to read</th>
                <th className="px-4 py-3 font-mono text-[11px] uppercase tracking-[0.16em] text-[var(--muted)]">Action</th>
              </tr>
            </thead>
            <tbody>
              {articles.map((article) => {
                const actionState = stateByUrl[article.url];
                const buttonLabel =
                  actionState?.status === "pending"
                    ? "Adding..."
                    : actionState?.status === "succeeded"
                      ? "Added"
                      : "Add to vault";

                return (
                  <tr className="border-t border-[var(--ink)]/8 align-top first:border-t-0" key={article.url}>
                    <td className="px-4 py-4">
                      <a
                        className="text-sm font-medium leading-6 text-[var(--ink)] underline decoration-[var(--ink)]/18 underline-offset-4 transition hover:decoration-[var(--accent)]"
                        href={resolveExternalUrl(article.url)}
                        rel="noreferrer"
                        target="_blank"
                      >
                        {article.title}
                      </a>
                    </td>
                    <td className="px-4 py-4 text-sm leading-6 text-[var(--muted-strong)]">{article.claps ?? "Unknown"}</td>
                    <td className="px-4 py-4 text-sm leading-6 text-[var(--muted-strong)]">{article.readTime ?? "Unknown"}</td>
                    <td className="px-4 py-4">
                      <button
                        className="secondary-button whitespace-nowrap"
                        disabled={actionState?.status === "pending" || actionState?.status === "succeeded"}
                        onClick={() => onAddToVault(article.url)}
                        type="button"
                      >
                        {buttonLabel}
                      </button>
                      {actionState?.status === "failed" ? (
                        <p className="mt-2 max-w-xs text-xs leading-5 text-[var(--danger)]">{actionState.message}</p>
                      ) : null}
                      {actionState?.status === "succeeded" ? (
                        <p className="mt-2 text-xs leading-5 text-[var(--muted)]">Summary written into the vault.</p>
                      ) : null}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
    </article>
  );
}
