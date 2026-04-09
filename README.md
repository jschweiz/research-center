# Research Center

Research Center now runs as a file-native Mac-hosted system with a Git-backed vault.

- The Mac is the only automated writer and compute node.
- Canonical documents live in the vault, while runtime state and indexes live in local SQLite.
- The default vault lives in the `vault/` submodule and syncs through `https://github.com/jschweiz/research-vault`.
- Obsidian, Working Copy, or any Git-aware client can open the vault away from the Mac.
- The Mac-served webapp is for local control and status, not full corpus editing.

## Vault Layout

The backend expects this structure under `VAULT_ROOT_DIR`:

- `raw/<kind>/<doc-id>/source.md`
- `wiki/<namespace>/<slug>.md`
- `briefs/daily/YYYY-MM-DD/{brief.md,brief.json,audio.mp3,slides.md}`
- `outputs/viewer/{latest/,history/}`

Source config, runs, leases, stop requests, pairing state, stars, AI budgets, and all materialized indexes now live in the local SQLite runtime at `DATABASE_URL`.
Secrets and AI trace artifacts are stored outside the vault in `LOCAL_STATE_DIR`.
LLM prompt bundles and response traces are written under `LOCAL_STATE_DIR/ai-traces/` and retained according to `AI_TRACE_RETENTION_DAYS`.

The vault repo should track:

- `raw/`
- `wiki/`
- `briefs/`
- `outputs/viewer/`

The vault repo should ignore `system/` and any other local runtime scratch space.

## Start The App

Backend:

```bash
python3 --version
make backend-install
make vault-submodule-init
cp apps/backend/.env.example apps/backend/.env
make web-build
make backend-run
```

There is no migration step anymore. `make backend-migrate` is a no-op kept only for compatibility.
The app bootstraps the remaining auth/profile/connection SQLite tables automatically on startup unless `AUTO_CREATE_SCHEMA=false`.

If `apps/web/dist/index.html` exists, the backend serves the local-control app and emits `/app-config.json` dynamically.

## Key Settings

Set these in `apps/backend/.env`:

```bash
APP_ENV=development
AUTO_CREATE_SCHEMA=true
FRONTEND_ORIGIN=http://localhost:8000
LOCAL_SERVER_BASE_URL=http://localhost:8000
HOSTED_VIEWER_URL=
VAULT_ROOT_DIR=vault
VAULT_GIT_ENABLED=true
VAULT_GIT_REMOTE_URL=https://github.com/jschweiz/research-vault
VAULT_GIT_BRANCH=main
LOCAL_STATE_DIR=apps/backend/.local-state
AI_TRACE_RETENTION_DAYS=30
```

Notes:

- If `VAULT_ROOT_DIR` is omitted, the app defaults to the repo-local `vault/` path.
- `AUTO_CREATE_SCHEMA=true` keeps the small remaining SQLite tables ready for auth, profile settings, and saved connections without running migrations by hand.
- `VAULT_GIT_ENABLED=true` makes the Mac pull before write pipelines when it can fast-forward cleanly, then commit and push vault changes back to GitHub after successful work.
- The first sync can bootstrap an empty `research-vault` repo with the baseline vault files if the local checkout has GitHub push access.
- `DATABASE_URL` points at the local runtime SQLite database used for source config, runs, leases, pairing, stars, AI budgets, and search/index projections.
- `HOSTED_VIEWER_URL` is optional and lets the pairing flow offer a “return to viewer” link after a device redeems a local-control token.
- Dedicated source pipelines are configured in the local DB and can be managed from the source CRUD API / local-control UI.
- Gmail-backed newsletter ingestion can use either `GMAIL_INGEST_EMAIL` plus `GMAIL_INGEST_APP_PASSWORD`, or Gmail OAuth.
- Gmail OAuth requires `GMAIL_OAUTH_CLIENT_ID` and `GMAIL_OAUTH_CLIENT_SECRET` in `apps/backend/.env`.
- In Google Cloud, add the backend callback URL as an authorized redirect URI: `http://localhost:8000/api/connections/gmail/oauth/callback` for local development, or the equivalent callback on your deployed backend origin.

## Dedicated Sources

`run-ingest-inline` now starts by syncing these dedicated sources into the vault:

- `openai-website`: website posts into `raw/blog-post/`
- `anthropic-research`: research pages into `raw/blog-post/`
- `mistral-research`: Mistral research news into `raw/blog-post/`
- `tldr-email`: Gmail newsletters into `raw/newsletter/`
- `medium-email`: Gmail digests into `raw/newsletter/`

Edit sources from the Connections / Sources UI or the source CRUD API if you need to tune limits or disable one of these dedicated pipelines.

The ingest/index path also supports `raw_kind=paper`. If you add a source such as alphaXiv, use `paper` so the rebuilt vault index classifies it as `ContentType.PAPER`.

## One-Time Export From SQLite

If you already have data in the old SQLite database:

```bash
cd apps/backend
.venv/bin/python -m app.tasks.jobs export-sqlite-to-vault-inline
```

That exports stored items into `raw/` and rebuilds the vault indexes.

## Pipeline Commands

All runtime commands are vault-centric:

```bash
cd apps/backend
.venv/bin/python -m app.tasks.jobs run-ingest-inline
.venv/bin/python -m app.tasks.jobs compile-wiki-inline
.venv/bin/python -m app.tasks.jobs generate-brief-inline --brief-date 2026-04-07
.venv/bin/python -m app.tasks.jobs generate-audio-inline --brief-date 2026-04-07
.venv/bin/python -m app.tasks.jobs publish-latest-inline
.venv/bin/python -m app.tasks.jobs sync-vault-inline
.venv/bin/python -m app.tasks.jobs audit-vault-inline
```

What each phase does:

- `run-ingest-inline`: syncs dedicated sources from the local DB, writes raw documents into `raw/`, normalizes `source.md`, and rebuilds local DB indexes from vault files
- `compile-wiki-inline`: rebuilds managed wiki pages and their local DB page/graph projections
- `generate-brief-inline`: writes `brief.md`, `brief.json`, and `slides.md`
- `generate-audio-inline`: writes `audio-script.md` and `audio.mp3` when TTS is configured
- `publish-latest-inline`: rewrites the read-only viewer bundle under `outputs/viewer/`
- `sync-vault-inline`: fast-forwards the local vault from GitHub when possible, then commits and pushes local vault changes

## Pair An iPad

Create a pairing link:

```bash
cd apps/backend
.venv/bin/python -m app.tasks.jobs pair-device-code --label "Office iPad"
```

Open the returned `pairing_url` on the iPad while it is on the same Wi‑Fi as the Mac. After redemption, the iPad can call `/api/local-control/*` on the Mac origin.

## What The iPad Uses

Away from the Mac:

- open the synced vault in Obsidian or Working Copy
- or open `outputs/viewer/latest/index.html` from Files / Safari

On the same Wi‑Fi:

- open the paired Mac URL
- trigger ingest, brief regeneration, audio generation, viewer publish, or an explicit vault sync
- pull the `research-vault` repo on the iPad after the Mac pushes changes

## Tests

Backend:

```bash
make backend-test
```

Frontend:

```bash
make web-typecheck
make web-build
```
