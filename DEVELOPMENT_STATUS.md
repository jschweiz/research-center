# Research Center v1 Development Page

Updated: 2026-03-29

## Legend

| Marker | Meaning |
| --- | --- |
| `[x]` | Done and verified locally |
| `[~]` | Implemented, but still needs production confirmation |
| `[ ]` | Not done yet |

## Current Goal

Ship the single-user, Render-hosted web MVP described in the v1 design:

- backend-first ingestion and processing
- iPad-first PWA client
- morning brief, inbox triage, Zotero save flow
- resilient scheduled jobs and production deployment on Render

## Current Verdict

- [x] Core repository flows work locally after the 2026-03-28 audit fixes: backend lint/tests, runtime dependency audits, frontend typecheck/build/audit, and targeted browser QA all pass.
- [x] The 2026-03-28 security hardening pass closed the highest-risk repo findings: production origin checks for cookie-authenticated writes and side-effecting brief `GET`s, SSRF/local-network fetch guardrails, and fail-fast rejection of default production secrets.
- [x] Backend observability is now materially better: centralized application/worker logs, request IDs, auth/operation events, Prometheus-style metrics, and task lifecycle visibility are implemented and verified locally.
- [x] Database backup coverage is now implemented locally: daily scheduled enqueue, worker-side compressed snapshots, count-based retention pruning, and a manual admin backup path all work in repository verification.
- [x] Daily AI spend is now hard-capped locally: Gemini and Google Cloud TTS are gated behind a `$10/day` app budget with DB-backed reservations to avoid concurrent overspend, and the SQLite local path now tolerates transient budget-ledger lock contention during audio generation.
- [x] The repo-side release candidate now clears a fresh `make release-check` pass on 2026-03-29.
- [ ] The repo is not yet ready to call production-shippable from repository evidence alone.
- [ ] Production deploy, secrets, cron health, real Gmail OAuth, real Zotero export, and the first hosted brief remain unverified.
- [ ] Hosted backup verification is still missing: no production snapshot/restore drill has been observed yet.
- [ ] Hosted AI-budget verification is still missing: the spend cap has not yet been observed against real production provider credentials.
- [~] Release automation is now materially stronger: repository CI runs backend lint/tests, frontend typecheck/build, and runtime dependency audits, but hosted deployment smoke tests still remain manual.
- [~] Security is improved at the app layer, but the production bar still needs hosted-environment verification.

## Scope Checklist

### Product scope

- [x] Unified ingestion for RSS feeds
- [x] Unified ingestion for Gmail newsletters
- [x] Unified ingestion for arXiv feeds
- [x] Manual URL import
- [x] Deduplication by canonical URL and normalized content hash
- [x] Near-duplicate clustering with representative item and "also mentioned in"
- [x] Transparent ranking with persisted `reason_trace`
- [x] Smart summaries for ranked items
- [x] Paper-specific fields: contribution, method, result, limitation, possible extension
- [x] Follow-up prompts and on-demand deeper analysis
- [x] One-tap Zotero save flow
- [x] `Needs Review` fallback when Zotero confidence or export fails
- [x] Daily briefing generation
- [x] Behavior logging through item/user actions
- [x] Admin actions: ingest now, retry failed jobs, regenerate brief, backup now
- [x] Connections screen for Gmail, Zotero, and sources
- [x] Profile screen for interest/ranking controls
- [x] Inbox filters for all, papers, newsletters, saved, needs review, archived
- [x] Morning Brief screen
- [x] Item Detail screen
- [x] Responsive iPad-first web/PWA layout

### Explicitly deferred for v1

- [~] Dedicated audio brief experience is not a v1 ship requirement, but a minimal voice-summary path is now present in the API and UI
- [x] Weekly review remains out of scope
- [x] General website crawling remains out of scope
- [x] PDF-required parsing remains out of scope
- [x] NotebookLM automation remains out of scope
- [x] Social features remain out of scope
- [x] Automatic model retraining remains out of scope

## Deployment Checklist

### Render and infra

- [x] Monorepo structure in place: `apps/backend`, `apps/web`, root `render.yaml`
- [x] Render Blueprint file created and updated
- [x] Render Postgres declared in Frankfurt
- [x] Render Key Value declared for Celery broker/result backend
- [x] Render API web service declared
- [x] Render background worker declared
- [x] Render worker persistent disk declared for backup snapshot storage
- [x] Render static site declared for the PWA
- [x] Render cron jobs declared for ingest, digest enqueue, Zotero sync enqueue, database backup enqueue, and raw payload cleanup
- [x] API deploy flow runs `alembic upgrade head` before startup
- [x] Static site rewrite added for SPA routes
- [x] Production env wiring documented
- [x] Zurich-local scheduling handled in application logic rather than hard-coded UTC cron assumptions

### Build / release pipeline

- [x] Repository CI workflow configured
- [x] Automated lint gate configured
- [x] Automated runtime dependency audits configured
- [x] Manual local verification commands remain runnable

### Production rollout

- [ ] Render Blueprint created in the actual Render account
- [ ] First production deploy completed
- [ ] Production Postgres initialized from the Blueprint
- [ ] Production Key Value instance attached
- [ ] Production API and worker observed healthy after deploy
- [ ] Production static site reachable
- [ ] Production cron jobs observed running on schedule
- [ ] First hosted database backup snapshot observed on the worker disk
- [ ] Backup restore drill completed from a hosted snapshot

### Secrets and credentials

- [x] `GEMINI_API_KEY` used as the active LLM path
- [x] Production secret list documented in the repo
- [ ] Production `ADMIN_EMAIL` configured in Render
- [ ] Production `ADMIN_PASSWORD` configured in Render
- [ ] Production `GEMINI_API_KEY` configured in Render
- [ ] Production `GMAIL_OAUTH_CLIENT_ID` configured in Render
- [ ] Production `GMAIL_OAUTH_CLIENT_SECRET` configured in Render
- [ ] Production `SENTRY_DSN` configured in Render
- [ ] Real Gmail OAuth completed against the production app
- [ ] Real Zotero connection configured in the production app

## Successive Test Log

### Stage 0: clean boot / seed smoke test

- [x] Fresh isolated QA backend now boots with `SEED_DEMO_DATA=true`
- [x] Demo seeding no longer depends on the removed `The Batch` source name
- [x] Demo seeding now keys off existing items instead of existing sources, so a partial source-only boot does not suppress later demo item creation

### Stage 1: automated backend verification

- [x] Backend tests pass locally
- [x] Current result: `152 passed in 16.79s`
- [x] Covers auth, briefs, items, ingestion, logging/observability, metrics, manual import fallback, profile, sources, connections, migrations, LLM adapter behavior, and voice/deeper-summary paths

Command:

```bash
cd apps/backend
.venv/bin/pytest app/tests -q
```

### Stage 1b: backend lint posture

- [x] Logging-related backend Ruff checks pass locally
- [x] Ruff lint passes locally
- [x] Current result: `ruff check app` passes after reducing FastAPI-specific lint noise and autofixing the remaining backend issues
- [x] Lint is now part of the repository CI workflow

### Stage 1c: logging, tracing, and metrics verification

- [x] Centralized backend logging is configured through the standard library and initialized in both the API process and Celery worker process
- [x] API request lifecycle logging is present for start, completion, and exception paths
- [x] Every API response includes `X-Request-ID`, and the same request ID is attached to request log records
- [x] Successful health-check requests are intentionally suppressed from normal request logs to reduce noise
- [x] Auth login success, login failure, and logout events now emit logs without recording credentials
- [x] Manual operation enqueue/destructive actions emit logs for ingest, digest, enrich-all, retry, Zotero sync, and clear-content flows
- [x] Worker tasks emit start/completion/failure logs and attach task context where available
- [x] Production defaults to JSON logs, while local development can stay on text logs with `LOG_FORMAT=text`
- [x] The API exposes Prometheus-style metrics for request volume/latency, auth events, operation events, and task execution outcomes
- [x] The production API metrics endpoint is token-gated instead of being left anonymously exposed
- [x] The worker can optionally expose its own metrics listener when `WORKER_METRICS_PORT` is configured

### Stage 1d: backup verification

- [x] Backup snapshots are created through backend code instead of external database-dump binaries
- [x] Backup files are gzip-compressed and include manifest metadata plus per-table rows
- [x] Old snapshots are pruned automatically according to `DATABASE_BACKUP_RETENTION_COUNT`
- [x] Manual `POST /api/ops/backup-now` creates a snapshot and a succeeded operation-history entry
- [x] Scheduled enqueue now records `scheduled_backup` instead of incorrectly appearing as a manual trigger
- [~] Hosted worker-disk persistence and restore behavior still need production confirmation

### Stage 1e: AI budget verification

- [x] Paid Gemini and Google Cloud TTS requests are now gated by a shared daily cost cap
- [x] The default cap is `AI_DAILY_COST_LIMIT_USD=10.0`
- [x] Budget is reserved before provider calls and then consumed or released after the request completes
- [x] Existing current-day operation history is used to seed the first budget ledger row after deploy
- [x] Gemini paths fall back to zero-cost heuristics when the cap is exhausted instead of continuing to spend
- [x] Google Cloud TTS synthesis is blocked before any provider call when the cap is exhausted
- [x] SQLite-backed local development now enables WAL plus busy-timeout handling and retries transient lock-specific budget-reservation errors so audio generation does not fail on `ai_budget_days` ledger contention
- [~] Hosted provider-backed verification of the cap still needs production confirmation

### Stage 1f: dependency audit posture

- [x] Frontend runtime dependency audit passes locally: `npm audit --omit=dev` reported `0` vulnerabilities
- [x] Backend runtime dependency audit now passes locally against exported non-dev requirements
- [x] The backend audit path now runs `pip-audit` directly against the frozen runtime export with `--no-deps --disable-pip`, avoiding environment-specific resolver hangs while still checking the shipped pinned package set
- [x] Repository CI runs both runtime dependency audits automatically

### Stage 2: frontend verification

- [x] Frontend type checking passes
- [x] Production build passes
- [x] PWA assets generated successfully
- [x] Login screen no longer pre-fills development credentials

Commands:

```bash
cd apps/web
npm run typecheck
npm run build
```

### Stage 3: live local data and extraction verification

- [~] This stage was not rerun in the 2026-03-28 audit; the checks below remain the latest known live-data verification from 2026-03-27

- [x] Backend started against a fresh live QA database
- [x] Frontend served against the live backend
- [x] Gemini-backed extraction/summarization path verified with `GEMINI_API_KEY`
- [x] Recent March 2026 items ingested from live sources
- [x] Digest generated from live data

Verified sources and behavior:

- [x] arXiv ingestion
- [x] OpenAI RSS ingestion
- [x] Hugging Face RSS ingestion
- [x] Gmail missing-token failure path stays isolated and does not block digest generation

### Stage 4: browser end-to-end QA (rerun on 2026-03-28 with an isolated seeded QA stack)

- [x] Login
- [x] Logout
- [x] Brief page render
- [x] Inbox page render
- [x] Item Detail page
- [x] Connections page render
- [x] Profile page save flow
- [x] Screenshot review caught and fixed a low-contrast profile posture card
- [x] Manual URL import with graceful fallback into item detail
- [x] `Ask deeper`
- [x] Source pause flow
- [x] Data-mode switch from `seed` to `live`
- [x] Empty live-data brief state
- [x] Audio button surfaces a clear configuration error when Google Cloud TTS credentials are absent
- [~] `Mark important`, `Archive`, `Ignore similar`, `Retry failed jobs`, and add-source creation were not re-clicked in this browser pass; rely on automated backend coverage plus earlier QA history
- [~] Real Gmail OAuth and real Zotero save/export still need hosted-environment verification

### Stage 5: release-fix regression

- [x] Fixed polluted ignored-topic hints caused by arXiv title prefixes
- [x] Added regression test for arXiv title sanitization
- [x] Added Zotero save success-path test coverage
- [x] Fixed clean-start demo seeding so a fresh local boot no longer crashes on a missing `The Batch` source name
- [x] Replaced calendar-bound manual-import test fixtures with timezone-relative expectations

## Implementation Notes

### Scheduling

- Render cron jobs run in UTC.
- The app should keep deciding "is this due now?" in Europe/Zurich through `ScheduleService`.
- Digest enqueue is intentionally frequent so a user-configured local digest time can be respected without hard-coding DST-sensitive cron expressions.
- Zotero sync also relies on due checks, not on a single fixed UTC cron slot.

### Secrets and integrations

- The hosted LLM path uses `GEMINI_API_KEY`.
- Gmail OAuth client ID and secret belong in Render env vars.
- Voice summaries use Google Cloud TTS through ADC or `GOOGLE_CLOUD_TTS_CREDENTIALS_JSON`.
- Zotero credentials should be configured in-app and stored encrypted in Postgres, not as long-lived Render env vars.
- `SECRET_KEY` and `ENCRYPTION_KEY` can be generated by Render from the Blueprint.
- Production startup now refuses default `SECRET_KEY`, `ENCRYPTION_KEY`, `ADMIN_PASSWORD`, and insecure `http://` frontend origins.
- Production defaults to lexical clustering unless the backend image is explicitly built with the optional `embeddings` extra and `ENABLE_EMBEDDINGS=true`.

### Security posture

- [x] Cookie-authenticated `POST`, `PUT`, `PATCH`, and `DELETE` API requests now require a matching frontend `Origin` or `Referer` in production
- [x] The Gmail OAuth start redirect is also origin-protected in production, even though it is a `GET`
- [x] Side-effecting brief `GET` routes that can backfill digests or synthesize cached audio now require the frontend `Origin` or `Referer` in production
- [x] Manual import and source-fetch paths now reject non-HTTP(S), localhost, single-label/internal hostnames, and private-network targets
- [x] Manual URL import now fails closed on blocked outbound URLs instead of silently creating placeholder items for those targets
- [x] Production startup now fails fast if default auth or encryption secrets are still in place
- [x] Daily paid AI usage is now constrained by an application-enforced `$10/day` cap across Gemini and Google Cloud TTS
- [x] Runtime production frontend dependency audit is clean: `npm audit --omit=dev` reported `0` vulnerabilities
- [x] Runtime backend dependency audit is clean against exported non-dev requirements
- [x] Repeated failed admin logins are now throttled per IP/email in the API process for the current single-service MVP deployment model

### Observability

- Backend runtime logs are now centralized through `app.core.logging` instead of being left to ad hoc module-level logging
- Logs are emitted to stdout so Render can collect them, with text output locally and JSON output by default in production
- Request logs carry `request_id`, while worker logs carry `task_id` and `task_name` when Celery provides them
- Startup, shutdown, request handling, auth events, queueing actions, and task execution are all now represented in logs
- Prometheus-style metrics now cover API request counts/latency, auth events, admin operation events, and worker task counts/latency
- The API metrics endpoint is guarded by `METRICS_TOKEN` in production, and the worker exporter stays off unless explicitly configured
- The implementation intentionally avoids logging secrets, full broker/backend URLs, or user credentials
- Sentry remains complementary error monitoring, but hosted Sentry ingestion still needs production verification

### Database and migrations

- Production should run Alembic migrations, not `AUTO_CREATE_SCHEMA`.
- Postgres should have the `vector` extension enabled through the migration path.
- Demo seeding must stay off in production.
- Demo seeding now checks for existing items rather than existing sources so a partial source-catalog commit does not wedge future local demo boots.

### Backups

- Daily database backups are now scheduled through Render cron and executed by the Celery worker.
- The worker stores compressed snapshots under `DATABASE_BACKUP_DIR`, which is configured as `/var/data/db_backups` in the Render worker.
- Retention is count-based via `DATABASE_BACKUP_RETENTION_COUNT`, defaulting to `14`.
- Backup runs are visible in operation history with file, size, row/table count, and prune details.
- Repository verification covers create/prune/history behavior locally; hosted snapshot retention and restore drills are still pending.

### AI cost controls

- The backend now enforces a shared paid-AI budget through `AI_DAILY_COST_LIMIT_USD`, defaulting to `$10.00` per day.
- Gemini text generation and Google Cloud TTS both draw from that same cap.
- The implementation uses DB-backed reservations so concurrent workers cannot spend the same remaining budget twice.
- Reservation rows expire after `AI_BUDGET_RESERVATION_TTL_MINUTES` so a crashed worker does not hold the budget forever.
- Local SQLite now enables WAL mode, sets a busy timeout, and retries transient lock-specific reservation writes so audio generation can reserve budget without failing on a parallel request session.
- Local verification covers budget seeding, reservation consume/release, Gemini preflight blocking, and TTS preflight blocking; hosted verification is still pending.

### UI and QA notes

- The PWA uses SPA rewrites and therefore needs the static-site catch-all rewrite in Render.
- A real issue was fixed in `Ignore similar`: arXiv prefixes were leaking into ignored-topic hints.
- Screenshot QA on 2026-03-28 also caught a profile-side-card contrast regression; it was fixed by switching that panel to the existing dark treatment used elsewhere in the design system.
- Semantic embeddings are no longer a default runtime dependency; the ship path now uses lexical clustering by default and only loads `sentence-transformers` when the optional extra is intentionally installed.
- Browser automation was sometimes unreliable for React-controlled inputs after validation errors; native setter dispatch was the stable workaround during QA.
- The local QA database now contains test data and should not be treated as a pristine demo environment.
- The current audio button is visible even when Google Cloud TTS is not configured; today it degrades to a clear inline error instead of hiding the feature.

## Open Risks / Pending Production Confirmation

- [ ] Releases still depend partly on manual verification because hosted smoke tests still require a real deploy
- [ ] Login throttling is in-process only today; move it to a shared store if the API is scaled beyond a single web instance
- [ ] Production API metrics scraping has not yet been verified against the hosted deploy
- [ ] Worker metrics exposure/scraping has not yet been verified in the hosted environment
- [ ] Production structured logs have not yet been inspected on the hosted Render services after a real deploy
- [ ] Real Gmail OAuth flow has not yet been verified against the deployed production site
- [ ] Real successful Zotero export has not yet been verified against the deployed production site
- [ ] Hosted Google Cloud TTS / audio generation has not yet been verified end to end with real credentials
- [ ] Production Sentry ingestion has not yet been verified
- [ ] Production cron execution history has not yet been verified
- [ ] First production morning brief has not yet been observed end to end

## Post-v1 Roadmap

### Audio brief

- [~] Minimal voice-summary generation is already implemented and exposed on the current `Brief` page
- [~] Generate a TTS-ready voice summary script from the written digest
- [x] Google Cloud TTS integrated with cached digest audio output
- [~] Store cached audio artifacts and delivery metadata
- [~] Add chapter markers tied to digest entries
- [ ] Build the dedicated `Audio` page
- [~] Add minimal playback controls on the current `Brief` page
- [ ] Validate commute/walk usage on iPad and mobile widths

Implementation notes:

- Audio is no longer absent from the repo; it is better described as a partial, config-dependent feature that is still outside the v1 ship bar.
- A digest-level scaffold now stores generated narration script text, chapter metadata, generation status, provider metadata, and cached provider-generated audio files.
- The current UI entry point lives on the `Brief` page and now uses a compact seekable player backed by provider-generated audio.
- This still needs durable object storage, an audio retention policy, and a dedicated `Audio` surface before the feature is complete.

### Weekly summary / weekly review

- [ ] Add a weekly aggregation job
- [ ] Generate weekly themes, recurring authors/labs, surprises, and save-vs-ignore patterns
- [ ] Build the `Weekly Review` page
- [ ] Add suggested new feeds/topics based on weekly behavior
- [ ] Decide whether weekly output should be purely in-app or also delivered by email

Implementation notes:

- Weekly review was intentionally deferred from v1 because daily usefulness comes first.
- This should be built from persisted digests, scores, and user actions rather than from fresh reprocessing.
- Weekly synthesis should stay editorial and sparse, not become another dump page.

### NotebookLM handoff / upload

- [ ] Define the first supported NotebookLM handoff format
- [ ] Decide whether the first step is manual export, shareable package generation, or direct upload
- [ ] Add a lightweight NotebookLM handoff action for selected items or a digest
- [ ] Include summaries, source URLs, and follow-up prompts in the exported package
- [ ] Validate privacy boundaries before exporting newsletter-derived content

Implementation notes:

- Full NotebookLM automation is still intentionally deferred.
- The next step should be a controlled handoff or export flow, not browser automation hacks.
- Start with a narrow export path for curated items before considering deeper integration.

## Next Steps

### Immediate launch steps

- [ ] Decide whether to add CI plus a lint-clean baseline before the first production deploy
- [ ] Push the current branch/changes
- [ ] Create the Render Blueprint in the real Render account
- [ ] Fill all required production env vars and secrets
- [ ] Run the first production deploy
- [ ] Confirm API health, worker startup, static site reachability, and cron registration

### First production smoke test

- [ ] Log into the production app
- [ ] Connect Gmail with real OAuth
- [ ] Connect Zotero with real credentials
- [ ] Add the real feed/source set
- [ ] Trigger an ingest run in production
- [ ] Confirm recent items arrive
- [ ] Generate or wait for a real morning brief
- [ ] Save one item successfully to Zotero
- [ ] Force or observe one item landing in `Needs Review`

### Early post-launch follow-up

- [ ] Monitor the first 24 hours of worker logs and cron executions
- [ ] Tune source priorities and profile weights from real usage
- [ ] Decide whether `Weekly Review` is the next feature
- [ ] Decide whether `Audio Brief` is the next feature
- [ ] Decide whether the next NotebookLM step is manual export or a more direct handoff

### Post-v1 feature track

- [ ] Design and implement the `Audio` page and TTS pipeline
- [ ] Design and implement the `Weekly Review` page and weekly synthesis pipeline
- [ ] Design and implement the first NotebookLM upload/handoff flow
