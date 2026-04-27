import { useMutation, useQueryClient } from "@tanstack/react-query";
import { Archive, ArrowUpRight } from "lucide-react";

import { api } from "../api/client";
import { resolveExternalUrl } from "../lib/external-links";
import { ImportantButton } from "./ImportantButton";

interface QuickActionsProps {
  itemId: string;
  url: string;
  starred?: boolean;
  compact?: boolean;
  triageStatus?: "unread" | "needs_review" | "saved" | "archived";
}

export function QuickActions({
  itemId,
  url,
  starred = false,
  compact = false,
  triageStatus = "unread",
}: QuickActionsProps) {
  const queryClient = useQueryClient();
  const resolvedUrl = resolveExternalUrl(url);
  const refresh = async () => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: ["briefs"] }),
      queryClient.invalidateQueries({ queryKey: ["items"] }),
      queryClient.invalidateQueries({ queryKey: ["item", itemId] }),
    ]);
  };

  const archive = useMutation({
    mutationFn: () => api.archiveItem(itemId),
    onSuccess: refresh,
  });
  const interactionBusy = archive.isPending;
  const archived = triageStatus === "archived";
  const archiveLabel = archived ? "Archived" : archive.isPending ? "Archiving..." : "Archive";
  const compactActionClass =
    "flex h-8 w-8 shrink-0 items-center justify-center rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.78)] text-[var(--muted)] transition hover:-translate-y-0.5 hover:border-[var(--accent)]/26 hover:text-[var(--accent)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--accent)]/18 disabled:cursor-not-allowed disabled:opacity-60";
  const compactActionIconClass = "h-[13px] w-[13px]";

  if (compact) {
    return (
      <div className="flex flex-nowrap items-center gap-1">
        <ImportantButton disabled={interactionBusy} iconOnly iconOnlySize="compact" itemId={itemId} starred={starred} />
        <button
          aria-label={archiveLabel}
          className={compactActionClass}
          disabled={interactionBusy || archived}
          onClick={() => archive.mutate()}
          title={archiveLabel}
          type="button"
        >
          <Archive className={compactActionIconClass} />
          <span className="sr-only">{archiveLabel}</span>
        </button>
        <a aria-label="Open source" className={compactActionClass} href={resolvedUrl} rel="noreferrer" target="_blank" title="Open source">
          <ArrowUpRight className={compactActionIconClass} />
          <span className="sr-only">Open source</span>
        </a>
      </div>
    );
  }

  return (
    <div className="pt-2">
      <div className="flex flex-wrap gap-2">
        <ImportantButton disabled={interactionBusy} itemId={itemId} starred={starred} />
        <button className="secondary-button" disabled={interactionBusy || archived} onClick={() => archive.mutate()} type="button">
          <Archive className="h-4 w-4" />
          {archiveLabel}
        </button>
        <a className="secondary-button" href={resolvedUrl} rel="noreferrer" target="_blank">
          <ArrowUpRight className="h-4 w-4" />
          Open source
        </a>
      </div>
    </div>
  );
}
