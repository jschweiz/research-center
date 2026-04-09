import type { BriefAvailability, IngestionRunHistoryEntry, ItemDetail, ItemListEntry, Profile, Source } from "../api/types";
import { getConfiguredApiUrl } from "../api/client";
import { getStoredLocalControlToken } from "../runtime/storage";
import type {
  LocalControlJobResponse,
  LocalControlInsights,
  LocalControlStatus,
  PairRedeemResponse,
} from "../runtime/types";

export class LocalControlError extends Error {
  status: number;

  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

async function request<T>(path: string, init?: RequestInit, includeToken = true): Promise<T> {
  const headers = new Headers(init?.headers ?? {});
  if (!headers.has("Accept")) {
    headers.set("Accept", "application/json");
  }
  if (typeof init?.body === "string" && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }
  if (includeToken) {
    const token = getStoredLocalControlToken();
    if (token) {
      headers.set("Authorization", `Bearer ${token}`);
    }
  }

  let response: Response;
  try {
    response = await fetch(`${getConfiguredApiUrl()}/local-control${path}`, {
      cache: "no-store",
      headers,
      ...init,
    });
  } catch {
    throw new LocalControlError(
      0,
      "Could not reach the paired Mac. Make sure this iPad is on the same Wi-Fi, then reopen the Mac pairing link if needed.",
    );
  }

  const text = await response.text();
  let payload: unknown = {};
  if (text) {
    try {
      payload = JSON.parse(text);
    } catch {
      payload = { detail: text };
    }
  }
  if (!response.ok) {
    const detail =
      payload && typeof payload === "object" && "detail" in payload && typeof payload.detail === "string"
        ? payload.detail
        : "Request failed.";
    throw new LocalControlError(response.status, detail);
  }
  return payload as T;
}

export const localControlClient = {
  redeemPairing: (pairingToken: string, deviceLabel?: string) =>
    request<PairRedeemResponse>(
      "/pair/redeem",
      {
        method: "POST",
        body: JSON.stringify({
          pairing_token: pairingToken,
          device_label: deviceLabel,
        }),
      },
      false,
    ),
  getStatus: () => request<LocalControlStatus>("/status"),
  getInsights: () => request<LocalControlInsights>("/insights"),
  getOperations: () => request<{ runs: IngestionRunHistoryEntry[] }>("/operations"),
  getDocuments: (params: {
    q?: string;
    status?: string;
    content_type?: string;
    source_id?: string;
    from?: string;
    to?: string;
    sort?: string;
  }) => {
    const search = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value) search.set(key, value);
    });
    const suffix = search.toString() ? `?${search.toString()}` : "";
    return request<ItemListEntry[]>(`/documents${suffix}`);
  },
  getDocument: (id: string) => request<ItemDetail>(`/documents/${id}`),
  getSources: () => request<Source[]>("/sources"),
  getBriefAvailability: () => request<BriefAvailability>("/briefs/availability"),
  getProfile: () => request<Profile>("/profile"),
  updateProfile: (payload: Record<string, unknown>) =>
    request<Profile>("/profile", {
      method: "PATCH",
      body: JSON.stringify(payload),
    }),
  runIngest: () => request<LocalControlJobResponse>("/jobs/ingest", { method: "POST" }),
  runFetchSources: () => request<LocalControlJobResponse>("/jobs/fetch-sources", { method: "POST" }),
  runSourcePipeline: (sourceId: string, payload?: { max_items?: number }) =>
    request<LocalControlJobResponse>(`/jobs/sources/${sourceId}/inject`, {
      method: "POST",
      body: JSON.stringify(payload ?? {}),
    }),
  stopSourcePipeline: (sourceId: string) =>
    request<LocalControlJobResponse>(`/jobs/sources/${sourceId}/stop`, {
      method: "POST",
    }),
  runLightweightEnrich: () => request<LocalControlJobResponse>("/jobs/lightweight-enrich", { method: "POST" }),
  stopLightweightEnrich: () =>
    request<LocalControlJobResponse>("/jobs/lightweight-enrich/stop", {
      method: "POST",
    }),
  runRebuildItemsIndex: () => request<LocalControlJobResponse>("/jobs/rebuild-items-index", { method: "POST" }),
  runCompileWiki: (payload?: { source_id?: string; doc_id?: string; limit?: number }) =>
    request<LocalControlJobResponse>("/jobs/compile-wiki", {
      method: "POST",
      body: JSON.stringify(payload ?? {}),
    }),
  runAdvancedCompile: (payload?: { source_id?: string; doc_id?: string; limit?: number }) =>
    request<LocalControlJobResponse>("/jobs/advanced-compile", {
      method: "POST",
      body: JSON.stringify(payload ?? {}),
    }),
  runHealthCheck: (payload?: { scope?: "vault" | "wiki" | "raw"; topic?: string }) =>
    request<LocalControlJobResponse>("/jobs/health-check", {
      method: "POST",
      body: JSON.stringify(payload ?? {}),
    }),
  runAnswerQuery: (payload: { question: string; output_kind: "answer" | "slides" | "chart" }) =>
    request<LocalControlJobResponse>("/jobs/answer-query", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  runFileOutput: (payload: { path: string }) =>
    request<LocalControlJobResponse>("/jobs/file-output", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  runRegenerateBrief: (briefDate?: string) =>
    request<LocalControlJobResponse>("/jobs/regenerate-brief", {
      method: "POST",
      body: JSON.stringify(briefDate ? { brief_date: briefDate } : {}),
    }),
  runGenerateAudio: (briefDate?: string) =>
    request<LocalControlJobResponse>("/jobs/generate-audio", {
      method: "POST",
      body: JSON.stringify(briefDate ? { brief_date: briefDate } : {}),
    }),
  runPublish: (briefDate?: string) =>
    request<LocalControlJobResponse>("/jobs/publish", {
      method: "POST",
      body: JSON.stringify(briefDate ? { brief_date: briefDate } : {}),
    }),
  runSyncVault: () => request<LocalControlJobResponse>("/jobs/sync-vault", { method: "POST" }),
};
