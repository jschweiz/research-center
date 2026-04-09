import type { IngestionRunHistoryEntry } from "../api/types";

export type EditionOutputOperationKind =
  | "brief_generation"
  | "audio_generation"
  | "viewer_publish";

export function hasSuccessfulEditionRun(
  runs: IngestionRunHistoryEntry[],
  operationKind: EditionOutputOperationKind,
  editionDay: string | null | undefined,
) {
  if (!editionDay) return false;
  return runs.some((run) =>
    run.status === "succeeded"
    && run.operation_kind === operationKind
    && run.affected_edition_days.includes(editionDay)
  );
}
