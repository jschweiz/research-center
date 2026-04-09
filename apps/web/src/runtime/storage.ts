const PAIRED_LOCAL_URL_KEY = "research-center.paired-local-url";
const LOCAL_CONTROL_TOKEN_KEY = "research-center.local-control-token";
const SHELL_SIDEBAR_COLLAPSED_KEY = "research-center.shell-sidebar-collapsed";

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
