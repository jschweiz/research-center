import { FormEvent, useDeferredValue, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { LayoutGrid, Rows3 } from "lucide-react";
import { useNavigate } from "react-router-dom";

import { api } from "../../api/client";
import { ItemCard } from "../../components/ItemCard";
import { SkimmableText } from "../../components/SkimmableText";
import { InboxTable } from "./InboxTable";

const filters = [
  { label: "All", status: undefined, type: undefined },
  { label: "Papers", status: undefined, type: "paper" },
  { label: "Newsletters", status: undefined, type: "newsletter" },
  { label: "Saved", status: "saved", type: undefined },
  { label: "Needs Review", status: "needs_review", type: undefined },
  { label: "Archived", status: "archived", type: undefined },
];

export function InboxPage() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [selected, setSelected] = useState(filters[0]);
  const [sort, setSort] = useState("newest");
  const [viewMode, setViewMode] = useState<"cards" | "table">("cards");
  const [query, setQuery] = useState("");
  const [sourceId, setSourceId] = useState("");
  const [importUrl, setImportUrl] = useState("");
  const deferredQuery = useDeferredValue(query.trim());
  const sourcesQuery = useQuery({
    queryKey: ["sources", "inbox", "include_manual"],
    queryFn: () => api.getSources({ includeManual: true }),
  });

  const itemsQuery = useQuery({
    queryKey: ["items", selected.status, selected.type, sourceId, sort, deferredQuery],
    queryFn: () =>
      api.getItems({
        status: selected.status,
        content_type: selected.type,
        source_id: sourceId || undefined,
        sort,
        q: deferredQuery || undefined,
      }),
  });
  const importMutation = useMutation({
    mutationFn: (url: string) => api.importUrl(url),
    onSuccess: async (item) => {
      setImportUrl("");
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["items"] }),
        queryClient.invalidateQueries({ queryKey: ["briefs"] }),
      ]);
      navigate(`/items/${item.id}`);
    },
  });

  const heading = useMemo(() => {
    if (selected.label === "Needs Review") return "Items that need metadata review or a retry before they should hit Zotero.";
    if (selected.label === "Saved") return "Items already exported or marked as keepers.";
    return "Browse beyond the brief, filter the stream, and keep the queue under control.";
  }, [selected.label]);

  const emptyMessage = useMemo(() => {
    if (query.trim()) return `No items match “${query.trim()}”.`;
    if (sourceId) {
      const sourceName = sourcesQuery.data?.find((source) => source.id === sourceId)?.name ?? "that source";
      return `No items from ${sourceName} are available for this view yet.`;
    }
    if (selected.label === "Newsletters") return "No newsletter items have been ingested yet.";
    if (selected.label === "Saved") return "Nothing has been saved yet.";
    if (selected.label === "Needs Review") return "Nothing needs review right now.";
    if (selected.label === "Archived") return "No archived items yet.";
    return "No items are available for this view yet.";
  }, [query, selected.label, sourceId, sourcesQuery.data]);

  return (
    <div className="space-y-6 pb-10">
      <section className="editorial-panel">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
          <div>
            <p className="section-kicker">Triage lane</p>
            <h3 className="section-title">Inbox</h3>
            <SkimmableText className="mt-4 max-w-3xl text-base leading-7 text-[var(--muted)]">{heading}</SkimmableText>
          </div>
          <div className="grid gap-3 md:grid-cols-[minmax(0,1fr)_220px_180px_auto]">
            <input
              className="field-input"
              onChange={(event) => setQuery(event.target.value)}
              placeholder="Search titles"
              value={query}
            />
            <select className="field-input" onChange={(event) => setSourceId(event.target.value)} value={sourceId}>
              <option value="">{sourcesQuery.isLoading ? "Loading sources..." : "All sources"}</option>
              {sourcesQuery.data?.map((source) => (
                <option key={source.id} value={source.id}>
                  {source.name}
                </option>
              ))}
            </select>
            <select className="field-input" onChange={(event) => setSort(event.target.value)} value={sort}>
              <option value="newest">Newest</option>
              <option value="importance">Importance</option>
              <option value="source">Source</option>
            </select>
            <div className="flex items-end">
              <div className="inline-flex rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.62)] p-1">
                <button
                  aria-label="Card view"
                  className={`filter-pill border-transparent bg-transparent px-3 py-2 ${viewMode === "cards" ? "filter-pill-active" : ""}`}
                  onClick={() => setViewMode("cards")}
                  type="button"
                >
                  <LayoutGrid className="h-4 w-4" />
                  Cards
                </button>
                <button
                  aria-label="Table view"
                  className={`filter-pill border-transparent bg-transparent px-3 py-2 ${viewMode === "table" ? "filter-pill-active" : ""}`}
                  onClick={() => setViewMode("table")}
                  type="button"
                >
                  <Rows3 className="h-4 w-4" />
                  Table
                </button>
              </div>
            </div>
          </div>
        </div>
        <form
          className="mt-5 grid gap-3 border-t border-[var(--ink)]/8 pt-5 sm:grid-cols-[minmax(0,1fr)_160px]"
          onSubmit={(event: FormEvent) => {
            event.preventDefault();
            if (!importUrl.trim()) return;
            importMutation.mutate(importUrl.trim());
          }}
        >
          <input
            className="field-input"
            onChange={(event) => {
              if (importMutation.isError) importMutation.reset();
              setImportUrl(event.target.value);
            }}
            placeholder="Paste an article or paper URL to import"
            value={importUrl}
          />
          <button className="primary-button" disabled={importMutation.isPending} type="submit">
            {importMutation.isPending ? "Importing..." : "Import URL"}
          </button>
        </form>
        {importMutation.isError ? (
          <div className="rounded-2xl border border-[var(--danger)]/20 bg-[rgba(255,255,255,0.56)] px-4 py-3 text-sm leading-6 text-[var(--danger)]">
            {importMutation.error.message}
          </div>
        ) : null}
        <div className="mt-5 flex flex-wrap gap-2">
          {filters.map((filter) => (
            <button
              key={filter.label}
              className={`filter-pill ${filter.label === selected.label ? "filter-pill-active" : ""}`}
              onClick={() => setSelected(filter)}
              type="button"
            >
              {filter.label}
            </button>
          ))}
        </div>
      </section>

      <div className="page-breakout space-y-6">
        {itemsQuery.isLoading ? <div className="page-loading">Loading items…</div> : null}
        {itemsQuery.isError ? <div className="page-empty">Inbox unavailable right now. Refresh or check the backend.</div> : null}
        {!itemsQuery.isLoading && !itemsQuery.isError && (itemsQuery.data?.length ?? 0) === 0 ? (
          <div className="page-empty">{emptyMessage}</div>
        ) : null}
        {viewMode === "table" ? (
          <InboxTable items={itemsQuery.data ?? []} />
        ) : (
          <div className="grid gap-4 xl:grid-cols-2">
            {itemsQuery.data?.map((item) => (
              <ItemCard
                compactActions
                key={item.id}
                item={item}
                publishedAtFormat="date"
                showScore={false}
              />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
