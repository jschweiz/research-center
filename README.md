# Research Center

Research Center is a backend-first research briefing MVP. It ingests feeds and newsletters, deduplicates noisy coverage, ranks important items, generates an editorial morning brief, and lets you save the best items into Zotero from an iPad-first web app.

## Stack

- Backend: FastAPI, SQLAlchemy 2, Alembic, Celery, Postgres/pgvector
- Frontend: React, TypeScript, Vite, React Router, TanStack Query, Tailwind CSS, Radix UI, PWA support
- Infra: Render Static Site, Web Service, Background Worker, Cron Jobs, Postgres, Key Value

## Monorepo layout

- `apps/backend`: API, worker, domain services, migrations, tests
- `apps/web`: iPad-first PWA client
- `render.yaml`: Render blueprint for the full hosted MVP

## Quick start

### Backend

```bash
python3 --version  # 3.12+ required
make backend-install
cp apps/backend/.env.example apps/backend/.env
make backend-migrate
make backend-run
```

To apply or refresh the curated default source catalog in an existing database without resetting data:

```bash
make backend-upsert-sources
```

If you prefer running `uvicorn` directly, use the backend venv and app dir explicitly:

```bash
apps/backend/.venv/bin/python -m uvicorn app.main:app --app-dir apps/backend --reload
```

### Gmail OAuth for local development

The `Connect Gmail` button stays disabled until the backend sees both of these values in `apps/backend/.env`:

```bash
GMAIL_OAUTH_CLIENT_ID=your-google-oauth-client-id
GMAIL_OAUTH_CLIENT_SECRET=your-google-oauth-client-secret
```

Create a Google OAuth client for a web app and register this local callback URI:

```text
http://localhost:8000/api/connections/gmail/oauth/callback
```

Then restart the backend and refresh the app. The `Connections` page should report Gmail OAuth as configured and enable `Connect Gmail`.

If you do not want to configure Google Cloud locally, the `Connections` page also supports direct Gmail access with a Gmail address plus an app password. That path does not need `GMAIL_OAUTH_CLIENT_ID` or `GMAIL_OAUTH_CLIENT_SECRET`.

### Google Cloud TTS for free local voice summaries

The voice-summary path now uses Google Cloud Text-to-Speech. For local development, the simplest free-tier setup is:

```bash
gcloud auth application-default login
```

The backend will automatically pick up the local ADC credentials file that command creates. If you prefer explicit configuration, you can also set either:

```bash
GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/application_default_credentials.json
```

or:

```bash
GOOGLE_CLOUD_TTS_CREDENTIALS_JSON='{"type":"service_account",...}'
```

The default voice config is pinned to `en-US-Studio-O` with `MP3` output for a higher-quality narration voice by default. Override `GOOGLE_TTS_VOICE_NAME` in `apps/backend/.env` if you want a different speaker.

### Web

```bash
cd apps/web
npm install
cp .env.example .env
npm run dev
```

The backend seeds demo data by default in local development so the app has a usable brief, inbox, and profile immediately.
That demo path now also upserts the curated default source catalog so newly added feeds appear in local dev without recreating the database.
Semantic embeddings are now an optional install-time extra rather than a baseline runtime dependency; the default MVP path falls back to lexical clustering unless you explicitly install `.[embeddings]` and enable them.

## Production on Render

`render.yaml` is now set up as the deployment source of truth for the hosted MVP:

- Render Postgres in Frankfurt with `pgvector` enabled by Alembic migrations
- Render Key Value as the Celery broker/result backend with `noeviction`
- Render Web Service for FastAPI with `preDeployCommand: alembic upgrade head`
- Render Background Worker for Celery jobs, plus a persistent disk mounted at `/var/data`
- Render Static Site for the React PWA with an SPA rewrite to `index.html`
- Render Cron Jobs for ingest, digest enqueue, Zotero sync enqueue, database backup enqueue, and raw-email cleanup

### Production secrets

Set these in Render before the first production deploy:

- `ADMIN_EMAIL`
- `ADMIN_PASSWORD`
- `GEMINI_API_KEY`
- `METRICS_TOKEN` if you want the API metrics endpoint enabled in production
- `GOOGLE_CLOUD_TTS_CREDENTIALS_JSON` if you want provider-backed voice briefs
- `GMAIL_OAUTH_CLIENT_ID`
- `GMAIL_OAUTH_CLIENT_SECRET`
- `SENTRY_DSN` if you want error reporting
- `WORKER_METRICS_PORT` if you want a worker-local metrics listener for Prometheus scraping

The Blueprint generates `SECRET_KEY` and `ENCRYPTION_KEY` automatically.

Zotero credentials are intentionally **not** long-lived Render env vars. Configure Zotero once from the deployed app's `Connections` screen; the API key and library metadata are then encrypted and stored in Postgres.

### Deployment behavior

- The API runs Alembic migrations before each deploy, so schema changes are applied before the new API version starts.
- The static site builds against `VITE_API_URL=https://research-center-api.onrender.com/api`.
- React Router deep links such as `/inbox`, `/connections`, and `/items/:id` work in production because the static site rewrites unmatched paths to `index.html`.

### Scheduled jobs

Render cron schedules are UTC, so local-time jobs are implemented as frequent enqueue jobs plus due checks in the worker:

- ingest enqueue: every 30 minutes
- digest enqueue: every 15 minutes, then the worker checks the profile's local digest time (default `07:00 Europe/Zurich`)
- Zotero sync enqueue: every 30 minutes, then the worker runs it once after `02:00 Europe/Zurich`
- database backup enqueue: daily at `01:30 UTC`, then the worker writes a compressed snapshot to `DATABASE_BACKUP_DIR`
- raw email payload purge: daily cleanup

### Database backups

- Admins can trigger an on-demand backup with `POST /api/ops/backup-now`.
- The worker writes compressed full-database snapshots named `research-center-db-backup-<timestamp>.json.gz`.
- Render production is configured to store those snapshots on the worker disk at `/var/data/db_backups`.
- Retention is count-based through `DATABASE_BACKUP_RETENTION_COUNT`, with a default of `14`.
- Backup runs appear in the same operation history feed as other admin jobs, including file, size, row/table counts, and prune activity.
- Hosted restore validation is still pending; only the local backup/create/prune path is verified in-repo.

### AI cost guardrails

- Paid AI calls are now hard-capped by `AI_DAILY_COST_LIMIT_USD`, which defaults to `$10.00` per app day.
- The cap applies to both Gemini LLM requests and Google Cloud TTS synthesis, not just text generation.
- The backend reserves budget before provider calls and releases or consumes that reservation after the call finishes, so concurrent workers do not overspend the daily limit.
- Gemini paths fall back to existing heuristic behavior when the cap is exhausted; TTS synthesis is blocked once the budget is gone.
- Stale in-flight reservations expire automatically after `AI_BUDGET_RESERVATION_TTL_MINUTES` to avoid a crashed worker permanently locking the budget.

### Auth hardening

- Production cookie-authenticated login and side-effecting routes require the configured frontend `Origin` or `Referer`.
- Repeated failed admin logins are throttled through `LOGIN_RATE_LIMIT_MAX_ATTEMPTS`, `LOGIN_RATE_LIMIT_WINDOW_MINUTES`, and `LOGIN_RATE_LIMIT_LOCKOUT_MINUTES`.
- The current throttle is intentionally in-process, which matches the app's single-web-service MVP deployment shape on Render.
- Optional semantic clustering now requires installing the backend with `.[embeddings]`; the default production build keeps embeddings disabled and uses lexical clustering instead.

### Logging and traceability

- The backend now emits structured application logs for API requests, startup/shutdown, auth events, queued operations, and worker task lifecycles.
- Every API response includes an `X-Request-ID` header, and the same request ID is attached to the corresponding log lines.
- Production defaults to JSON logs. Override `LOG_FORMAT=text` locally if you want human-readable console logs, or change `LOG_LEVEL` to increase/decrease verbosity.

### Metrics

- The API now exposes Prometheus-style metrics at `/metrics` when `METRICS_ENABLED=true`.
- In production, the API metrics endpoint stays disabled unless `METRICS_TOKEN` is configured; scrape it with either `Authorization: Bearer <token>` or `X-Metrics-Token: <token>`.
- The metrics include API request counts/latency, auth event counters, admin operation counters, and worker task counts/latency.
- The Celery worker can expose the same metrics format on its own HTTP listener when `WORKER_METRICS_PORT` is set. It uses the same `METRICS_PATH` and `METRICS_TOKEN` settings.
- Worker metrics binding defaults to `127.0.0.1`; only move `WORKER_METRICS_HOST` off localhost if you intend to scrape it from a trusted network path.

## Background jobs

- `celery -A app.tasks.celery_app.celery_app worker --loglevel=info`
- `python -m app.tasks.jobs`

Use the cron endpoints in `render.yaml` to enqueue ingest, Zotero sync, and daily brief generation in hosted environments.
The same helper also supports `enqueue-database-backup` and `run-database-backup-inline` for scheduled and local backup execution.

## Tests

```bash
make backend-test
```

Repository CI is also configured in `.github/workflows/ci.yml` to run backend lint/tests, frontend typecheck/build, and runtime dependency audits on pushes and pull requests.

For a fuller pre-release pass, run:

```bash
make release-check
```

## Current implementation scope

This repo implements the full MVP skeleton and working core flows:

- managed single-user auth with signed session cookies
- source management, manual URL import, digest and item APIs
- deterministic ranking and brief generation
- demo ingest, clustering, insights, and follow-up prompts
- Gmail, Zotero, LLM, and paper adapters with concrete service interfaces
- Celery tasks for ingest, digest, sync, and cleanup
- responsive editorial PWA for Brief, Inbox, Item Detail, Connections, and Profile

External integrations depend on credentials and provider tokens in environment variables or connection settings. The default hosted LLM path uses `GEMINI_API_KEY`, while provider-backed voice briefs use Google Cloud ADC or `GOOGLE_CLOUD_TTS_CREDENTIALS_JSON`.
