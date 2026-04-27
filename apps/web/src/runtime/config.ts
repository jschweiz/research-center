import type { RuntimeConfig } from "./types";

const FALLBACK_API_BASE_URL = import.meta.env.VITE_API_URL ?? "http://localhost:8000/api";
const FALLBACK_MODE: RuntimeConfig["mode"] = __PUBLISHED_ONLY__ ? "hosted" : "local";

function fallbackRuntimeConfig(): RuntimeConfig {
  return {
    mode: FALLBACK_MODE,
    apiBaseUrl: FALLBACK_API_BASE_URL,
    pairedLocalUrl: null,
    hostedViewerUrl: null,
    cloudKit: null,
    staticPublishedBasePath: __PUBLISHED_ONLY__ ? "." : null,
  };
}

export async function loadRuntimeConfig(): Promise<RuntimeConfig> {
  try {
    const response = await fetch("./app-config.json", {
      cache: "no-store",
    });
    if (!response.ok) {
      return fallbackRuntimeConfig();
    }

    const payload = (await response.json()) as RuntimeConfig;
    return {
      mode: payload.mode ?? FALLBACK_MODE,
      apiBaseUrl: payload.apiBaseUrl ?? FALLBACK_API_BASE_URL,
      pairedLocalUrl: payload.pairedLocalUrl ?? null,
      hostedViewerUrl: payload.hostedViewerUrl ?? null,
      cloudKit: payload.cloudKit ?? null,
      staticPublishedBasePath: payload.staticPublishedBasePath ?? (__PUBLISHED_ONLY__ ? "." : null),
    };
  } catch {
    return fallbackRuntimeConfig();
  }
}
