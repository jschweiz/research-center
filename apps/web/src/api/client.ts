import type {
  ActionResponse,
  AudioBrief,
  BriefAvailability,
  Connection,
  ConnectionCapabilities,
  Digest,
  IngestionRunHistoryEntry,
  ItemDetail,
  ItemListEntry,
  JobResponse,
  MeResponse,
  Profile,
  Source,
  SourceProbeResult,
} from "./types";

const API_URL = import.meta.env.VITE_API_URL ?? "http://localhost:8000/api";

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

  const response = await fetch(`${API_URL}${path}`, {
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
  getAudioSummaryUrl: (date: string) => `${API_URL}/briefs/${date}/audio`,

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
  ignoreSimilar: (id: string) => request<ActionResponse>(`/items/${id}/ignore-similar`, { method: "POST" }),
  saveToZotero: (id: string, tags: string[]) =>
    request<ActionResponse>(`/items/${id}/save-to-zotero`, {
      method: "POST",
      body: JSON.stringify({ tags }),
    }),
  generateDeeperSummary: (id: string) =>
    request<JobResponse>(`/items/${id}/generate-deeper-summary`, { method: "POST" }),

  getSources: (params?: { includeManual?: boolean }) => {
    const search = new URLSearchParams();
    if (params?.includeManual) search.set("include_manual", "true");
    const suffix = search.toString() ? `?${search.toString()}` : "";
    return request<Source[]>(`/sources${suffix}`);
  },
  createSource: (payload: Record<string, unknown>) =>
    request<Source>("/sources", { method: "POST", body: JSON.stringify(payload) }),
  probeSource: (id: string) => request<SourceProbeResult>(`/sources/${id}/probe`, { method: "POST" }),
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

  ingestNow: () => request<JobResponse>("/ops/ingest-now", { method: "POST" }),
  enrichAll: () => request<JobResponse>("/ops/enrich-all", { method: "POST" }),
  regenerateBrief: (briefDate?: string) =>
    request<JobResponse>("/ops/regenerate-brief", {
      method: "POST",
      body: JSON.stringify(briefDate ? { brief_date: briefDate } : {}),
    }),
  retryFailedJobs: () => request<JobResponse>("/ops/retry-failed-jobs", { method: "POST" }),
  clearContent: () => request<JobResponse>("/ops/clear-content", { method: "POST" }),
  getIngestionRuns: () => request<IngestionRunHistoryEntry[]>("/ops/ingestion-runs"),
  oauthUrl: (path: string) => `${API_URL}${path}`,
};
