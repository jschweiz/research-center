# Research Center Save Page

Local-only Chrome extension for saving the active tab into the Research Center backend with captured page content.

## What it does

- Captures visible page content in the browser.
- Uses Mozilla Readability first, then falls back to selected text, `article/main/[role=main]`, and finally `document.body.innerText`.
- Sends the capture to `POST /api/local-control/documents/import-page`.
- Opens the imported document at `/documents/{id}` after the backend responds.
- Lets the backend's existing lightweight Ollama/Gemma pass enrich authors, summary, tags, and score.

## Load it unpacked

1. Open `chrome://extensions`.
2. Enable **Developer mode**.
3. Choose **Load unpacked**.
4. Select this folder: `/Users/jschweiz/github/research-center/extensions/save-page`

## Backend assumptions

- The local backend is running on `http://localhost:8000` or `http://127.0.0.1:8000`.
- The backend already trusts loopback for local-control requests.
- The local app is available from the same origin so `/documents/{id}` resolves after import.

## Scope

- Supported: normal `http` and `https` tabs.
- Unsupported: `chrome://`, `file://`, browser-internal pages, and extension pages.

## Vendored dependency

- `vendor/readability/Readability.js` is copied from `@mozilla/readability@0.6.0`.
- Upstream license text is included in `vendor/readability/LICENSE.md`.
