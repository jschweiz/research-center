# launchd Templates

These templates are for the Mac-hosted Research Center deployment shape.

## Placeholders

Replace these before loading the plists:

- `__REPO_ROOT__`: absolute path to this repository
- `__LOG_DIR__`: absolute path to a writable log directory, for example `~/Library/Logs/research-center`

## Install

1. Build the web bundle with `make web-build`.
2. Create the log directory.
3. Copy the templates into `~/Library/LaunchAgents/` and remove the `.template` suffix.
4. Replace placeholders in the copied files.
5. Validate each plist with `plutil -lint`.
6. Load with `launchctl bootstrap gui/$UID ~/Library/LaunchAgents/<name>.plist`.

## Included jobs

- `com.researchcenter.api.plist.template`: FastAPI service on port 8000
- `com.researchcenter.jobs.ingest.plist.template`: inline ingest every 30 minutes
- `com.researchcenter.jobs.digest.plist.template`: inline digest due-check every 15 minutes
- `com.researchcenter.jobs.publish.plist.template`: snapshot export every 15 minutes
- `com.researchcenter.jobs.zotero-sync.plist.template`: Zotero sync due-check every 30 minutes
- `com.researchcenter.jobs.cleanup.plist.template`: raw-email cleanup daily at 03:15 local time
- `com.researchcenter.jobs.backup.plist.template`: database backup daily at 03:45 local time

The templates use `LaunchAgent` semantics. If you need the jobs to continue without a logged-in user session, convert them to `LaunchDaemon`s and adjust ownership and filesystem paths accordingly.
