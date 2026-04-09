import type {
  ActionResponse,
  AdvancedOutputKind,
  AudioBrief,
  BriefAvailability,
  Connection,
  ConnectionCapabilities,
  CodexRuntimeStatus,
  Digest,
  HealthCheckScope,
  IngestionRunHistoryEntry,
  ItemDetail,
  ItemListEntry,
  JobResponse,
  MeResponse,
  PipelineStatus,
  Profile,
  Source,
  SourceLatestLogResult,
  SourceProbeResult,
} from "./types";

let apiUrl = import.meta.env.VITE_API_URL ?? "http://localhost:8000/api";

function normalizeApiUrl(value: string) {
  return value.endsWith("/") ? value.slice(0, -1) : value;
}

export function configureApiClient(nextApiUrl: string) {
  apiUrl = normalizeApiUrl(nextApiUrl);
}

export function getConfiguredApiUrl() {
  return apiUrl;
}

export class ApiError extends Error {
  status: number;

  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

function describeValidationIssue(issue: unknown): string | null {
  if (typeof issue === "string" && issue.trim()) return issue;
  if (!issue || typeof issue !== "object") return null;

  const payload = issue as Record<string, unknown>;
  const message = typeof payload.msg === "string" ? payload.msg : null;
  const location = Array.isArray(payload.loc)
    ? payload.loc
        .filter((part) => part !== "body")
        .map((part) => String(part))
        .join(".")
    : "";

  if (location && message) return `${location}: ${message}`;
  return message;
}

function extractErrorMessage(payload: unknown): string {
  if (!payload || typeof payload !== "object") return "Request failed.";

  const errorPayload = payload as Record<string, unknown>;
  if (typeof errorPayload.detail === "string" && errorPayload.detail.trim()) {
    return errorPayload.detail;
  }
  if (Array.isArray(errorPayload.detail)) {
    const issues = errorPayload.detail
      .map(describeValidationIssue)
      .filter((value): value is string => Boolean(value));
    if (issues.length) return issues.join(" ");
  }
  if (typeof errorPayload.message === "string" && errorPayload.message.trim()) {
    return errorPayload.message;
  }

  return "Request failed.";
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const headers = new Headers(init?.headers ?? {});
  if (!headers.has("Accept")) {
    headers.set("Accept", "application/json");
  }
  if (typeof init?.body === "string" && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }

  const response = await fetch(`${apiUrl}${path}`, {
    cache: init?.cache ?? "no-store",
    credentials: "include",
    headers,
    ...init,
  });

  if (response.status === 204) {
    return {} as T;
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
    throw new ApiError(response.status, extractErrorMessage(payload));
  }
  return payload as T;
}

export const api = {
  me: () => request<MeResponse>("/me"),
  login: (email: string, password: string) =>
    request<MeResponse>("/auth/login", {
      method: "POST",
      body: JSON.stringify({ email, password }),
    }),
  logout: () => request<{ status: string }>("/auth/logout", { method: "POST" }),

  getTodayBrief: () => request<Digest>("/briefs/today"),
  getBriefAvailability: () => request<BriefAvailability>("/briefs/availability"),
  getBrief: (date: string) => request<Digest>(`/briefs/${date}`),
  getWeekBrief: (weekStart: string) => request<Digest>(`/briefs/weeks/${weekStart}`),
  generateAudioSummary: (date: string) =>
    request<AudioBrief>(`/briefs/${date}/generate-audio-summary`, { method: "POST" }),
  getAudioSummaryUrl: (date: string) => `${apiUrl}/briefs/${date}/audio`,

  getItems: (params: { q?: string; status?: string; content_type?: string; source_id?: string; sort?: string }) => {
    const search = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value) search.set(key, value);
    });
    const suffix = search.toString() ? `?${search.toString()}` : "";
    return request<ItemListEntry[]>(`/items${suffix}`);
  },
  getItem: (id: string) => request<ItemDetail>(`/items/${id}`),
  importUrl: (url: string) =>
    request<ItemDetail>("/items/import-url", {
      method: "POST",
      body: JSON.stringify({ url }),
    }),
  archiveItem: (id: string) => request<ActionResponse>(`/items/${id}/archive`, { method: "POST" }),
  starItem: (id: string) => request<ActionResponse>(`/items/${id}/star`, { method: "POST" }),

  getSources: (params?: { includeManual?: boolean }) => {
    const search = new URLSearchParams();
    if (params?.includeManual) search.set("include_manual", "true");
    const suffix = search.toString() ? `?${search.toString()}` : "";
    return request<Source[]>(`/sources${suffix}`);
  },
  createSource: (payload: Record<string, unknown>) =>
    request<Source>("/sources", { method: "POST", body: JSON.stringify(payload) }),
  probeSource: (id: string) => request<SourceProbeResult>(`/sources/${id}/probe`, { method: "POST" }),
  injectSource: (id: string, payload?: { max_items?: number }) =>
    request<JobResponse>(`/sources/${id}/inject`, {
      method: "POST",
      body: JSON.stringify(payload ?? {}),
    }),
  getSourceLatestLog: (id: string) => request<SourceLatestLogResult>(`/sources/${id}/latest-log`),
  updateSource: (id: string, payload: Record<string, unknown>) =>
    request<Source>(`/sources/${id}`, { method: "PATCH", body: JSON.stringify(payload) }),
  deleteSource: (id: string) => request<void>(`/sources/${id}`, { method: "DELETE" }),

  getConnectionCapabilities: () => request<ConnectionCapabilities>("/connections/capabilities"),
  getGmailConnection: () => request<Connection | null>("/connections/gmail"),
  saveGmailConnection: (payload: Record<string, unknown>) =>
    request<Connection>("/connections/gmail", { method: "POST", body: JSON.stringify(payload) }),
  getZoteroConnection: () => request<Connection | null>("/connections/zotero"),
  saveZoteroConnection: (payload: Record<string, unknown>) =>
    request<Connection>("/connections/zotero", { method: "POST", body: JSON.stringify(payload) }),

  getProfile: () => request<Profile>("/profile"),
  updateProfile: (payload: Record<string, unknown>) =>
    request<Profile>("/profile", { method: "PATCH", body: JSON.stringify(payload) }),

  runIngest: () => request<JobResponse>("/ops/run-ingest", { method: "POST" }),
  fetchSources: () => request<JobResponse>("/ops/fetch-sources", { method: "POST" }),
  lightweightEnrich: () => request<JobResponse>("/ops/lightweight-enrich", { method: "POST" }),
  rebuildItemsIndex: () => request<JobResponse>("/ops/rebuild-items-index", { method: "POST" }),
  getPipelineStatus: () => request<PipelineStatus>("/ops/pipeline-status"),
  ingestNow: () => request<JobResponse>("/ops/ingest-now", { method: "POST" }),
  syncSources: () => request<JobResponse>("/ops/sync-sources", { method: "POST" }),
  rebuildIndex: () => request<JobResponse>("/ops/rebuild-index", { method: "POST" }),
  enrichAll: () => request<JobResponse>("/ops/lightweight-enrich", { method: "POST" }),
  compileWiki: (payload?: { source_id?: string; doc_id?: string; limit?: number }) =>
    request<JobResponse>("/ops/compile-wiki", {
      method: "POST",
      body: JSON.stringify(payload ?? {}),
    }),
  advancedCompile: (payload?: { source_id?: string; doc_id?: string; limit?: number }) =>
    request<JobResponse>("/ops/advanced-compile", {
      method: "POST",
      body: JSON.stringify(payload ?? {}),
    }),
  healthCheck: (payload?: { scope?: HealthCheckScope; topic?: string }) =>
    request<JobResponse>("/ops/health-check", {
      method: "POST",
      body: JSON.stringify(payload ?? {}),
    }),
  answerQuery: (payload: { question: string; output_kind: AdvancedOutputKind }) =>
    request<JobResponse>("/ops/answer-query", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  fileOutput: (payload: { path: string }) =>
    request<JobResponse>("/ops/file-output", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  getAdvancedRuntime: () => request<CodexRuntimeStatus>("/ops/advanced-runtime"),
  regenerateBrief: (briefDate?: string) =>
    request<JobResponse>("/ops/regenerate-brief", {
      method: "POST",
      body: JSON.stringify(briefDate ? { brief_date: briefDate } : {}),
    }),
  generateAudio: (briefDate?: string) =>
    request<JobResponse>("/ops/generate-audio", {
      method: "POST",
      body: JSON.stringify(briefDate ? { brief_date: briefDate } : {}),
    }),
  publishLatest: (briefDate?: string) =>
    request<JobResponse>("/ops/publish-latest", {
      method: "POST",
      body: JSON.stringify(briefDate ? { brief_date: briefDate } : {}),
    }),
  deepEnrichment: () => request<JobResponse>("/ops/deep-enrichment", { method: "POST" }),
  retryFailedJobs: () => request<JobResponse>("/ops/retry-failed-jobs", { method: "POST" }),
  clearContent: () => request<JobResponse>("/ops/clear-content", { method: "POST" }),
  getIngestionRuns: () => request<IngestionRunHistoryEntry[]>("/ops/ingestion-runs"),
  oauthUrl: (path: string) => `${apiUrl}${path}`,
};
