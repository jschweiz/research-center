import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Archive, ArrowUpRight, Ban, BrainCircuit, BookmarkPlus } from "lucide-react";
import { useNavigate } from "react-router-dom";
import clsx from "clsx";

import { api } from "../api/client";
import { ImportantButton } from "./ImportantButton";

interface QuickActionsProps {
  itemId: string;
  url: string;
  starred?: boolean;
  compact?: boolean;
  inlineNotice?: boolean;
  showDeeper?: boolean;
}

function readStringMetadata(value: unknown) {
  return typeof value === "string" && value.trim() ? value : null;
}

export function QuickActions({
  itemId,
  url,
  starred = false,
  compact = false,
  inlineNotice = !compact,
  showDeeper = true,
}: QuickActionsProps) {
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const zoteroQuery = useQuery({
    queryKey: ["connections", "zotero"],
    queryFn: api.getZoteroConnection,
    staleTime: 60_000,
  });
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
  const save = useMutation({
    mutationFn: () => api.saveToZotero(itemId, ["research-center"]),
    onSuccess: refresh,
  });
  const ignoreSimilar = useMutation({
    mutationFn: () => api.ignoreSimilar(itemId),
    onSuccess: refresh,
  });
  const deeper = useMutation({
    mutationFn: () => api.generateDeeperSummary(itemId),
    onSuccess: refresh,
  });
  const interactionBusy = archive.isPending || ignoreSimilar.isPending || (showDeeper && deeper.isPending);
  const zoteroChecking = zoteroQuery.isPending && zoteroQuery.data === undefined;
  const zoteroReady = zoteroQuery.data?.status === "connected";
  const zoteroError = readStringMetadata(zoteroQuery.data?.metadata_json.last_error);
  const saveLabel = save.isPending ? "Saving..." : zoteroChecking ? "Checking Zotero" : zoteroReady ? "Add to library" : "Connect Zotero";
  let saveNotice: string | null = null;
  if (save.isError) {
    saveNotice = save.error.message;
  } else if (save.isSuccess) {
    saveNotice = save.data.detail;
  } else if (!zoteroChecking && !zoteroReady) {
    saveNotice = zoteroError ?? "Connect Zotero in Settings to enable library saves.";
  }
  const saveNoticeTone = save.isError || (!zoteroReady && zoteroError) ? "text-[var(--danger)]" : "text-[var(--muted)]";
  const showSaveNotice = inlineNotice && Boolean(saveNotice) && (!compact || save.isSuccess || save.isError || Boolean(zoteroError));
  const compactActionClass =
    "flex h-8 w-8 shrink-0 items-center justify-center rounded-full border border-[var(--ink)]/10 bg-[rgba(255,255,255,0.78)] text-[var(--muted)] transition hover:-translate-y-0.5 hover:border-[var(--accent)]/26 hover:text-[var(--accent)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--accent)]/18 disabled:cursor-not-allowed disabled:opacity-60";
  const compactActionIconClass = "h-[13px] w-[13px]";

  if (compact) {
    return (
      <div className="space-y-1.5">
        <div className="flex flex-nowrap items-center gap-1">
          <button
            aria-label={saveLabel}
            className={compactActionClass}
            disabled={zoteroChecking || save.isPending || interactionBusy}
            onClick={() => {
              if (!zoteroReady) {
                navigate("/connections");
                return;
              }
              save.mutate();
            }}
            title={saveLabel}
            type="button"
          >
            <BookmarkPlus className={compactActionIconClass} />
            <span className="sr-only">{saveLabel}</span>
          </button>
          <ImportantButton disabled={interactionBusy || save.isPending} iconOnly iconOnlySize="compact" itemId={itemId} starred={starred} />
          <button
            aria-label={archive.isPending ? "Archiving..." : "Archive"}
            className={compactActionClass}
            disabled={interactionBusy || save.isPending}
            onClick={() => archive.mutate()}
            title={archive.isPending ? "Archiving..." : "Archive"}
            type="button"
          >
            <Archive className={compactActionIconClass} />
            <span className="sr-only">{archive.isPending ? "Archiving..." : "Archive"}</span>
          </button>
          <button
            aria-label={ignoreSimilar.isPending ? "Ignoring similar items..." : "Ignore similar"}
            className={compactActionClass}
            disabled={interactionBusy || save.isPending}
            onClick={() => ignoreSimilar.mutate()}
            title={ignoreSimilar.isPending ? "Ignoring similar items..." : "Ignore similar"}
            type="button"
          >
            <Ban className={compactActionIconClass} />
            <span className="sr-only">{ignoreSimilar.isPending ? "Ignoring similar items..." : "Ignore similar"}</span>
          </button>
          {showDeeper ? (
            <button
              aria-label={deeper.isPending ? "Generating deeper analysis..." : "Ask deeper"}
              className={compactActionClass}
              disabled={interactionBusy || save.isPending}
              onClick={() => deeper.mutate()}
              title={deeper.isPending ? "Generating deeper analysis..." : "Ask deeper"}
              type="button"
            >
              <BrainCircuit className={compactActionIconClass} />
              <span className="sr-only">{deeper.isPending ? "Generating deeper analysis..." : "Ask deeper"}</span>
            </button>
          ) : null}
          <a aria-label="Open source" className={compactActionClass} href={url} rel="noreferrer" target="_blank" title="Open source">
            <ArrowUpRight className={compactActionIconClass} />
            <span className="sr-only">Open source</span>
          </a>
        </div>
        {showDeeper && deeper.isPending ? <p className="text-xs leading-5 text-[var(--muted)]">Generating deeper analysis…</p> : null}
        {showSaveNotice ? <p className={`text-xs leading-5 ${saveNoticeTone}`}>{saveNotice}</p> : null}
      </div>
    );
  }

  return (
    <div className="space-y-3 pt-2">
      <div className="flex flex-wrap gap-2">
        <button
          className="secondary-button"
          disabled={zoteroChecking || save.isPending || interactionBusy}
          onClick={() => {
            if (!zoteroReady) {
              navigate("/connections");
              return;
            }
            save.mutate();
          }}
          type="button"
        >
          <BookmarkPlus className="h-4 w-4" />
          {saveLabel}
        </button>
        <ImportantButton disabled={interactionBusy || save.isPending} itemId={itemId} starred={starred} />
        <button className="secondary-button" disabled={interactionBusy || save.isPending} onClick={() => archive.mutate()} type="button">
          <Archive className="h-4 w-4" />
          {archive.isPending ? "Archiving..." : "Archive"}
        </button>
        <button className="secondary-button" disabled={interactionBusy || save.isPending} onClick={() => ignoreSimilar.mutate()} type="button">
          <Ban className="h-4 w-4" />
          {ignoreSimilar.isPending ? "Updating..." : "Ignore similar"}
        </button>
        {showDeeper ? (
          <button className="secondary-button" disabled={interactionBusy || save.isPending} onClick={() => deeper.mutate()} type="button">
            <BrainCircuit className="h-4 w-4" />
            {deeper.isPending ? "Generating..." : "Ask deeper"}
          </button>
        ) : null}
        <a className="secondary-button" href={url} rel="noreferrer" target="_blank">
          <ArrowUpRight className="h-4 w-4" />
          Open source
        </a>
      </div>
      {showDeeper && deeper.isPending ? <p className="text-sm leading-6 text-[var(--muted)]">Generating deeper analysis…</p> : null}
      {showSaveNotice ? <p className={clsx("text-sm leading-6", saveNoticeTone)}>{saveNotice}</p> : null}
    </div>
  );
}
