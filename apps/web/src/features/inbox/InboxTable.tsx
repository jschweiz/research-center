import { Link } from "react-router-dom";

import type { ItemListEntry } from "../../api/types";
import { QuickActions } from "../../components/QuickActions";

const contentTypeLabel: Record<ItemListEntry["content_type"], string> = {
  article: "Article",
  newsletter: "Newsletter",
  paper: "Paper",
  post: "Post",
  signal: "Signal",
  thread: "Thread",
};

function formatInboxDate(value: string | null) {
  if (!value) return "Undated";

  return new Intl.DateTimeFormat("en-GB", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
  }).format(new Date(value));
}

export function InboxTable({ items }: { items: ItemListEntry[] }) {
  return (
    <div className="editorial-panel overflow-hidden px-0 py-0">
      <div className="overflow-x-auto">
        <table className="min-w-[940px] w-full table-fixed border-collapse">
          <thead>
            <tr className="border-b border-[var(--ink)]/8 bg-[rgba(255,255,255,0.48)] text-left">
              {["Item", "Source", "Type", "Date", "Actions"].map((label) => (
                <th
                  key={label}
                  className={`px-5 py-4 font-mono text-[11px] uppercase tracking-[0.22em] text-[var(--muted)] ${
                    label === "Source"
                      ? "w-[170px]"
                      : label === "Type"
                        ? "w-[120px]"
                        : label === "Date"
                          ? "w-[140px]"
                          : label === "Actions"
                            ? "w-[240px]"
                            : ""
                  }`}
                  scope="col"
                >
                  {label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {items.map((item) => {
              const authorLabel = item.authors.join(", ").trim();
              return (
              <tr key={item.id} className="border-b border-[var(--ink)]/8 align-top last:border-b-0 hover:bg-[rgba(255,255,255,0.36)]">
                <td className="px-4 py-3">
                  <Link
                    className="block max-w-[42rem] font-display text-[0.9rem] leading-[1.32] text-[var(--ink)] transition hover:text-[var(--accent)] md:text-[0.98rem]"
                    to={`/items/${item.id}`}
                  >
                    {item.title}
                  </Link>
                  {item.short_summary ? (
                    <p className="mt-1.5 max-w-[38rem] truncate text-[14px] leading-6 text-[var(--muted-strong)]">{item.short_summary}</p>
                  ) : null}
                  <div className="mt-2.5 flex flex-wrap items-center gap-2.5 font-mono text-[11px] uppercase tracking-[0.16em] text-[var(--muted)]">
                    {authorLabel ? <span>{authorLabel}</span> : null}
                    {item.also_mentioned_in_count ? <span>{item.also_mentioned_in_count} also mentioned</span> : null}
                  </div>
                </td>
                <td className="px-4 py-4 text-[15px] leading-6 text-[var(--muted-strong)]">{item.source_name}</td>
                <td className="px-4 py-4">
                  <span className="inline-flex rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.68)] px-3 py-1.5 font-mono text-[10px] uppercase tracking-[0.15em] text-[var(--muted-strong)]">
                    {contentTypeLabel[item.content_type]}
                  </span>
                </td>
                <td className="px-4 py-4 font-mono text-[12px] uppercase tracking-[0.14em] text-[var(--muted-strong)]">
                  {formatInboxDate(item.published_at)}
                </td>
                <td className="w-[240px] px-4 py-3">
                  <QuickActions compact inlineNotice={false} itemId={item.id} showDeeper={false} starred={item.starred} url={item.canonical_url} />
                </td>
              </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
