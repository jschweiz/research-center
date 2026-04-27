const BACKEND_ORIGINS = ["http://localhost:8000", "http://127.0.0.1:8000"];
const SAVE_ROUTE = "/api/local-control/documents/import-page";
const OPEN_PATH_PREFIX = "/documents/";

const state = {
  activeTab: null,
  capturePreview: null,
  canSave: false,
  busy: false,
};

const ui = {
  pageTitle: document.getElementById("pageTitle"),
  pageMeta: document.getElementById("pageMeta"),
  pageUrl: document.getElementById("pageUrl"),
  modeValue: document.getElementById("modeValue"),
  textValue: document.getElementById("textValue"),
  authorsValue: document.getElementById("authorsValue"),
  statusPanel: document.getElementById("statusPanel"),
  statusTitle: document.getElementById("statusTitle"),
  statusDetail: document.getElementById("statusDetail"),
  saveButton: document.getElementById("saveButton"),
};

function isSupportedUrl(value) {
  if (!value) {
    return false;
  }
  try {
    const parsed = new URL(value);
    return parsed.protocol === "http:" || parsed.protocol === "https:";
  } catch (_error) {
    return false;
  }
}

function truncate(value, maxChars = 120) {
  const text = String(value || "").trim();
  if (text.length <= maxChars) {
    return text;
  }
  return `${text.slice(0, maxChars - 1).trimEnd()}…`;
}

function formatMode(value) {
  return value ? value.replace(/[-_]/g, " ") : "-";
}

function formatTextCount(text) {
  const length = String(text || "").trim().length;
  if (!length) {
    return "empty";
  }
  return `${length.toLocaleString()} ch`;
}

function describeCapturePreview(capture, tab) {
  const url = capture?.url || tab?.url || "";
  const host = isSupportedUrl(url) ? new URL(url).hostname.replace(/^www\./, "") : "unsupported";
  const mode = formatMode(capture?.extraction_mode);
  return `${host} • ${mode}`;
}

function setStatus(tone, title, detail) {
  ui.statusPanel.dataset.tone = tone;
  ui.statusTitle.textContent = title;
  ui.statusDetail.textContent = detail;
}

function refreshSaveButton() {
  ui.saveButton.disabled = !state.canSave || state.busy;
  ui.saveButton.textContent = state.busy ? "Saving…" : "Save Page";
}

function renderUnsupported(tab) {
  state.canSave = false;
  refreshSaveButton();
  ui.pageTitle.textContent = tab?.title || "Unsupported page";
  ui.pageMeta.textContent = "Only normal http/https tabs are supported.";
  ui.pageUrl.textContent = tab?.url || "chrome:// and browser-internal pages are out of scope.";
  ui.modeValue.textContent = "-";
  ui.textValue.textContent = "-";
  ui.authorsValue.textContent = "-";
  setStatus(
    "error",
    "Cannot capture this tab",
    "Use the extension on a normal web page. Browser-internal, file, and extension pages stay disabled.",
  );
}

function renderPreview(capture, tab) {
  const title = capture?.page_title || tab?.title || "Untitled page";
  const canonical = capture?.canonical_url ? `Canonical hint: ${capture.canonical_url}` : "No canonical hint detected.";
  const authors = Array.isArray(capture?.author_hints) ? capture.author_hints : [];

  ui.pageTitle.textContent = truncate(title, 96) || "Untitled page";
  ui.pageMeta.textContent = `${describeCapturePreview(capture, tab)} • ${canonical}`;
  ui.pageUrl.textContent = capture?.url || tab?.url || "-";
  ui.modeValue.textContent = formatMode(capture?.extraction_mode);
  ui.textValue.textContent = formatTextCount(capture?.content_text);
  ui.authorsValue.textContent = authors.length ? String(authors.length) : "-";
}

async function getActiveTab() {
  const tabs = await chrome.tabs.query({ active: true, currentWindow: true });
  return tabs[0] ?? null;
}

async function ensureCaptureScripts(tabId) {
  await chrome.scripting.executeScript({
    target: { tabId },
    files: ["vendor/readability/Readability.js", "capture-page.js"],
  });
}

async function capturePage(tabId) {
  await ensureCaptureScripts(tabId);
  const results = await chrome.scripting.executeScript({
    target: { tabId },
    func: () => {
      if (typeof globalThis.__researchCenterCapturePage__ !== "function") {
        throw new Error("Capture bridge was not initialized.");
      }
      return globalThis.__researchCenterCapturePage__();
    },
  });
  const payload = results[0]?.result ?? null;
  if (!payload || typeof payload !== "object") {
    throw new Error("The page capture returned no payload.");
  }
  return payload;
}

async function fetchWithTimeout(url, options, timeoutMs = 15000) {
  const controller = new AbortController();
  const timeoutId = window.setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    window.clearTimeout(timeoutId);
  }
}

async function parseJsonSafely(response) {
  const raw = await response.text();
  if (!raw) {
    return {};
  }
  try {
    return JSON.parse(raw);
  } catch (_error) {
    return { detail: raw };
  }
}

function describeRemoteError(payload, status) {
  if (payload && typeof payload.detail === "string" && payload.detail.trim()) {
    return payload.detail.trim();
  }
  return `Request failed with status ${status}.`;
}

function describeError(error) {
  if (error && typeof error.message === "string" && error.message.trim()) {
    return error.message.trim();
  }
  return "The extension could not complete the request.";
}

async function importCapturedPage(payload) {
  const failures = [];

  for (const origin of BACKEND_ORIGINS) {
    try {
      const response = await fetchWithTimeout(`${origin}${SAVE_ROUTE}`, {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
      });
      const responsePayload = await parseJsonSafely(response);
      if (!response.ok) {
        throw new Error(describeRemoteError(responsePayload, response.status));
      }
      return { origin, item: responsePayload };
    } catch (error) {
      failures.push(`${origin}: ${describeError(error)}`);
    }
  }

  throw new Error(
    failures.length
      ? failures.join(" ")
      : "No local backend responded on localhost:8000 or 127.0.0.1:8000.",
  );
}

async function openImportedDocument(origin, itemId) {
  const url = new URL(`${OPEN_PATH_PREFIX}${itemId}`, origin).href;
  await chrome.tabs.create({ url });
}

async function loadPreview() {
  const tab = state.activeTab;
  if (!tab || typeof tab.id !== "number") {
    throw new Error("No active tab is available.");
  }

  setStatus(
    "working",
    "Inspecting the page",
    "Running Readability first, then selection and DOM fallbacks for a capture preview.",
  );
  const capture = await capturePage(tab.id);
  state.capturePreview = capture;
  renderPreview(capture, tab);
  state.canSave = true;
  refreshSaveButton();
  setStatus(
    "idle",
    "Ready to save",
    "The backend will write source.md from this captured body and then run lightweight metadata enrichment.",
  );
}

async function onSaveClick() {
  if (!state.activeTab || typeof state.activeTab.id !== "number") {
    return;
  }

  state.busy = true;
  refreshSaveButton();
  setStatus(
    "working",
    "Saving to the local backend",
    "Posting the captured page to /api/local-control/documents/import-page.",
  );

  try {
    const capture = await capturePage(state.activeTab.id);
    state.capturePreview = capture;
    renderPreview(capture, state.activeTab);

    const { origin, item } = await importCapturedPage(capture);
    setStatus(
      "success",
      "Saved successfully",
      "Opening the imported document in the local Research Center app.",
    );
    await openImportedDocument(origin, item.id);
    window.setTimeout(() => window.close(), 250);
  } catch (error) {
    setStatus(
      "error",
      "Save failed",
      describeError(error),
    );
  } finally {
    state.busy = false;
    refreshSaveButton();
  }
}

async function init() {
  ui.saveButton.addEventListener("click", () => {
    void onSaveClick();
  });

  try {
    state.activeTab = await getActiveTab();
    const tab = state.activeTab;

    if (!tab) {
      throw new Error("No active browser tab was found.");
    }

    if (!isSupportedUrl(tab.url)) {
      renderUnsupported(tab);
      return;
    }

    ui.pageTitle.textContent = truncate(tab.title || "Current page", 96);
    ui.pageMeta.textContent = `${new URL(tab.url).hostname.replace(/^www\./, "")} • awaiting capture`;
    ui.pageUrl.textContent = tab.url;
    await loadPreview();
  } catch (error) {
    state.canSave = Boolean(state.activeTab && isSupportedUrl(state.activeTab.url));
    refreshSaveButton();
    setStatus(
      "error",
      "Preview failed",
      `${describeError(error)} You can still try a direct save on this tab.`,
    );
    if (state.activeTab) {
      ui.pageTitle.textContent = truncate(state.activeTab.title || "Current page", 96);
      ui.pageMeta.textContent = "The popup could not inject the capture helper.";
      ui.pageUrl.textContent = state.activeTab.url || "-";
    }
  }
}

document.addEventListener("DOMContentLoaded", () => {
  void init();
});
