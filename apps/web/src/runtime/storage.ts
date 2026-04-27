import type { PublishedArchive, PublishedEditionManifest } from "./types";

const PAIRED_LOCAL_URL_KEY = "research-center.paired-local-url";
const LOCAL_CONTROL_TOKEN_KEY = "research-center.local-control-token";
const SHELL_SIDEBAR_COLLAPSED_KEY = "research-center.shell-sidebar-collapsed";
const STATIC_ARCHIVE_CACHE_KEY = "research-center.published-archive";
const STATIC_MANIFEST_CACHE_KEY = "research-center.published-manifests";
const INSTALL_HINT_DISMISSED_KEY = "research-center.install-hint-dismissed";

const MAX_MANIFEST_CACHE_ENTRIES = 5;

function canUseStorage() {
  return typeof window !== "undefined" && typeof window.localStorage !== "undefined";
}

export function getStoredPairedLocalUrl() {
  if (!canUseStorage()) return null;
  return window.localStorage.getItem(PAIRED_LOCAL_URL_KEY);
}

export function setStoredPairedLocalUrl(value: string) {
  if (!canUseStorage()) return;
  window.localStorage.setItem(PAIRED_LOCAL_URL_KEY, value);
}

export function getStoredLocalControlToken() {
  if (!canUseStorage()) return null;
  return window.localStorage.getItem(LOCAL_CONTROL_TOKEN_KEY);
}

export function setStoredLocalControlToken(value: string) {
  if (!canUseStorage()) return;
  window.localStorage.setItem(LOCAL_CONTROL_TOKEN_KEY, value);
}

export function clearStoredLocalControlToken() {
  if (!canUseStorage()) return;
  window.localStorage.removeItem(LOCAL_CONTROL_TOKEN_KEY);
}

export function getStoredShellSidebarCollapsed() {
  if (!canUseStorage()) return false;
  return window.localStorage.getItem(SHELL_SIDEBAR_COLLAPSED_KEY) === "true";
}

export function setStoredShellSidebarCollapsed(value: boolean) {
  if (!canUseStorage()) return;
  window.localStorage.setItem(SHELL_SIDEBAR_COLLAPSED_KEY, value ? "true" : "false");
}

type StoredManifestEntry = {
  cachedAt: string;
  manifest: PublishedEditionManifest;
  recordName: string;
};

type StoredManifestPayload = {
  entries: StoredManifestEntry[];
};

function readJson<T>(key: string): T | null {
  if (!canUseStorage()) return null;
  try {
    const raw = window.localStorage.getItem(key);
    if (!raw) return null;
    return JSON.parse(raw) as T;
  } catch {
    return null;
  }
}

function writeJson<T>(key: string, value: T) {
  if (!canUseStorage()) return;
  try {
    window.localStorage.setItem(key, JSON.stringify(value));
  } catch {
    // Ignore quota/storage failures; cache is opportunistic.
  }
}

export function getCachedPublishedArchive() {
  return readJson<PublishedArchive>(STATIC_ARCHIVE_CACHE_KEY);
}

export function setCachedPublishedArchive(archive: PublishedArchive) {
  writeJson(STATIC_ARCHIVE_CACHE_KEY, archive);
}

export function getCachedPublishedManifest(recordName: string) {
  const payload = readJson<StoredManifestPayload>(STATIC_MANIFEST_CACHE_KEY);
  return payload?.entries.find((entry) => entry.recordName === recordName)?.manifest ?? null;
}

export function setCachedPublishedManifest(manifest: PublishedEditionManifest) {
  const payload = readJson<StoredManifestPayload>(STATIC_MANIFEST_CACHE_KEY);
  const nextEntry: StoredManifestEntry = {
    cachedAt: new Date().toISOString(),
    manifest,
    recordName: manifest.edition.record_name,
  };
  const nextEntries = [nextEntry].concat((payload?.entries ?? []).filter((entry) => entry.recordName !== nextEntry.recordName));
  writeJson(STATIC_MANIFEST_CACHE_KEY, {
    entries: nextEntries.slice(0, MAX_MANIFEST_CACHE_ENTRIES),
  });
}

export function getCachedLatestPublishedManifest() {
  const archive = getCachedPublishedArchive();
  if (!archive?.latest.record_name) return null;
  return getCachedPublishedManifest(archive.latest.record_name);
}

export function getInstallHintDismissed() {
  if (!canUseStorage()) return false;
  return window.localStorage.getItem(INSTALL_HINT_DISMISSED_KEY) === "true";
}

export function setInstallHintDismissed(value: boolean) {
  if (!canUseStorage()) return;
  window.localStorage.setItem(INSTALL_HINT_DISMISSED_KEY, value ? "true" : "false");
}
