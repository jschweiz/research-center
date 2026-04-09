from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path

from app.core.config import get_settings
from app.services.vault_runtime import utcnow
from app.vault.store import VaultStore

DEFAULT_GITIGNORE = """# Legacy/bootstrap scratch space
system/

# Local-only machine state
.local-state/
.cache/
.tmp/

# Obsidian local noise
.obsidian/cache/
.obsidian/workspace.json
.obsidian/workspace-mobile.json
.trash/

# OS noise
.DS_Store
"""

DEFAULT_GITATTRIBUTES = """*.pdf  filter=lfs diff=lfs merge=lfs -text
*.mp3  filter=lfs diff=lfs merge=lfs -text
*.m4a  filter=lfs diff=lfs merge=lfs -text
*.wav  filter=lfs diff=lfs merge=lfs -text
*.epub filter=lfs diff=lfs merge=lfs -text
*.png  filter=lfs diff=lfs merge=lfs -text
*.jpg  filter=lfs diff=lfs merge=lfs -text
*.jpeg filter=lfs diff=lfs merge=lfs -text
*.webp filter=lfs diff=lfs merge=lfs -text
"""

DEFAULT_VAULT_README = """# Research Vault

This repository is the durable vault for Research Center.

- Markdown files under `raw/` and `wiki/` are the canonical content.
- Daily outputs live under `briefs/` and `outputs/viewer/`.
- Runtime state, indexes, source config, and local-control state live in the local SQLite runtime, not in this repo.

The Mac-hosted app writes to this repository and syncs it with GitHub.
"""

LOCAL_CONTROL_SYNC_PATHS = (
    "raw",
    "briefs/daily",
    "outputs/viewer",
)


class VaultGitSyncError(RuntimeError):
    pass


@dataclass(frozen=True)
class VaultGitStatus:
    enabled: bool
    repo_ready: bool
    branch: str | None
    remote_name: str | None
    remote_url: str | None
    current_commit: str | None
    current_summary: str | None
    has_uncommitted_changes: bool
    changed_files: int
    ahead_count: int
    behind_count: int
    git_lfs_available: bool


class VaultGitSyncService:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.store = VaultStore()
        self.store.ensure_layout()

    def status(self) -> VaultGitStatus:
        repo_ready = self._is_git_repo()
        branch = self.settings.vault_git_branch
        remote_name = self.settings.vault_git_remote_name
        remote_url = self.settings.vault_git_remote_url or None
        current_commit: str | None = None
        current_summary: str | None = None
        has_uncommitted_changes = False
        changed_files = 0
        ahead_count = 0
        behind_count = 0

        if repo_ready:
            branch_line, status_lines = self._status_snapshot()
            branch, ahead_count, behind_count = self._parse_branch_line(branch_line, default_branch=branch)
            remote_url = self._git_stdout(["remote", "get-url", remote_name], check=False) or remote_url
            current_commit = self._git_stdout(["rev-parse", "--short", "HEAD"], check=False) or None
            current_summary = self._git_stdout(["log", "-1", "--pretty=%s"], check=False) or None
            changed_files = len(status_lines)
            has_uncommitted_changes = changed_files > 0

        return VaultGitStatus(
            enabled=self.settings.vault_git_enabled,
            repo_ready=repo_ready,
            branch=branch,
            remote_name=remote_name,
            remote_url=remote_url,
            current_commit=current_commit,
            current_summary=current_summary,
            has_uncommitted_changes=has_uncommitted_changes,
            changed_files=changed_files,
            ahead_count=ahead_count,
            behind_count=behind_count,
            git_lfs_available=self._git_lfs_available(),
        )

    def prepare_for_mutation(self) -> VaultGitStatus:
        if not self.settings.vault_git_enabled:
            return self.status()

        self.ensure_repository()
        if not self._has_remote():
            return self.status()

        if not self._remote_branch_exists():
            return self.status()

        self._fetch_remote()
        status = self.status()
        if status.behind_count <= 0:
            return status
        if status.has_uncommitted_changes:
            raise VaultGitSyncError(
                "The vault repo has remote updates and local uncommitted changes. "
                "Sync or commit the vault manually before running new work."
            )

        self._run_git(["pull", "--ff-only", status.remote_name or "origin", status.branch or self.settings.vault_git_branch])
        return self.status()

    def push_local_changes(self, *, message: str) -> VaultGitStatus:
        return self._commit_and_push(message=message)

    def push_local_control_changes(self, *, message: str) -> VaultGitStatus:
        return self._commit_and_push(message=message, pathspecs=list(LOCAL_CONTROL_SYNC_PATHS))

    def synchronize(self, *, message: str) -> VaultGitStatus:
        self.prepare_for_mutation()
        return self.push_local_changes(message=message)

    def synchronize_local_control(self, *, message: str) -> VaultGitStatus:
        self.prepare_for_mutation()
        return self.push_local_control_changes(message=message)

    def _commit_and_push(
        self,
        *,
        message: str,
        pathspecs: list[str] | None = None,
    ) -> VaultGitStatus:
        if not self.settings.vault_git_enabled:
            return self.status()

        self.ensure_repository()
        if self._has_pending_changes(pathspecs=pathspecs):
            self._stage_changes(pathspecs=pathspecs)
            if self._has_staged_changes(pathspecs=pathspecs):
                self._commit_changes(message=message, pathspecs=pathspecs)

        if not self._has_remote():
            return self.status()

        remote_name = self.settings.vault_git_remote_name
        branch = self.settings.vault_git_branch
        if self._remote_branch_exists():
            self._fetch_remote()
            status = self.status()
            if status.behind_count > 0:
                raise VaultGitSyncError(
                    "The vault repo moved ahead on GitHub while local work was running. "
                    "Pull and resolve that divergence before pushing again."
                )
            self._run_git(["push", remote_name, branch])
            return self.status()

        self._run_git(["push", "-u", remote_name, branch])
        return self.status()

    def ensure_repository(self) -> VaultGitStatus:
        if not self.settings.vault_git_enabled:
            return self.status()

        if self._is_git_repo():
            self._ensure_remote()
            self._ensure_housekeeping_files()
            self._ensure_lfs_local_config()
            return self.status()

        self._initialize_repository()
        return self.status()

    def _initialize_repository(self) -> None:
        self.store.root.mkdir(parents=True, exist_ok=True)
        self._run_git(["init"])
        self._run_git(["branch", "-M", self.settings.vault_git_branch])
        self._ensure_remote()

        if self._remote_branch_exists():
            try:
                self._fetch_remote()
                self._run_git(
                    [
                        "checkout",
                        "-B",
                        self.settings.vault_git_branch,
                        "--track",
                        f"{self.settings.vault_git_remote_name}/{self.settings.vault_git_branch}",
                    ]
                )
            except VaultGitSyncError as exc:
                raise VaultGitSyncError(
                    f"Could not attach {self.store.root} to the existing vault repo on GitHub. "
                    "Initialize the submodule or clear the local vault directory first."
                ) from exc

        self._ensure_housekeeping_files()
        self._ensure_lfs_local_config()

        if self._has_pending_changes():
            self._run_git(["add", "-A"])
            if self._has_staged_changes():
                self._run_git(["commit", "-m", self._commit_message("Initialize vault repository")])

        if self._has_remote() and not self._remote_branch_exists() and self._has_commits():
            self._run_git(["push", "-u", self.settings.vault_git_remote_name, self.settings.vault_git_branch])

    def _ensure_housekeeping_files(self) -> None:
        self._write_if_missing(self.store.root / ".gitignore", DEFAULT_GITIGNORE)
        self._write_if_missing(self.store.root / "README.md", DEFAULT_VAULT_README)
        if self._git_lfs_available():
            self._write_if_missing(self.store.root / ".gitattributes", DEFAULT_GITATTRIBUTES)

    def _ensure_remote(self) -> None:
        remote_url = self.settings.vault_git_remote_url.strip() if self.settings.vault_git_remote_url else ""
        if not remote_url:
            return
        remote_name = self.settings.vault_git_remote_name
        if self._git_stdout(["remote", "get-url", remote_name], check=False):
            return
        self._run_git(["remote", "add", remote_name, remote_url])

    def _fetch_remote(self) -> None:
        if not self._has_remote() or not self._remote_branch_exists():
            return
        self._run_git(["fetch", self.settings.vault_git_remote_name, self.settings.vault_git_branch])

    def _has_remote(self) -> bool:
        return bool(self._git_stdout(["remote", "get-url", self.settings.vault_git_remote_name], check=False))

    def _remote_branch_exists(self) -> bool:
        remote_url = self._git_stdout(["remote", "get-url", self.settings.vault_git_remote_name], check=False)
        if not remote_url:
            remote_url = self.settings.vault_git_remote_url or ""
        if not remote_url:
            return False
        return (
            self._run_git(
                ["ls-remote", "--exit-code", "--heads", remote_url, self.settings.vault_git_branch],
                cwd=self.store.root,
                check=False,
            ).returncode
            == 0
        )

    def _status_snapshot(self, *, pathspecs: list[str] | None = None) -> tuple[str, list[str]]:
        args = ["status", "--short", "--branch"]
        if pathspecs:
            args.extend(["--", *pathspecs])
        output = self._git_stdout(args, check=False)
        lines = output.splitlines()
        if not lines:
            return ("", [])
        if lines[0].startswith("## "):
            return (lines[0], lines[1:])
        return ("", lines)

    def _parse_branch_line(self, line: str, *, default_branch: str | None) -> tuple[str | None, int, int]:
        branch = default_branch
        ahead_count = 0
        behind_count = 0
        if not line.startswith("## "):
            return branch, ahead_count, behind_count

        payload = line[3:]
        if payload.startswith("No commits yet on "):
            return payload.removeprefix("No commits yet on ").strip(), 0, 0

        branch_part, _, tracking = payload.partition("...")
        branch = branch_part.strip() or branch
        if "[" not in tracking or "]" not in tracking:
            return branch, ahead_count, behind_count

        detail = tracking[tracking.index("[") + 1 : tracking.index("]")]
        for segment in detail.split(","):
            cleaned = segment.strip()
            if cleaned.startswith("ahead "):
                ahead_count = int(cleaned.removeprefix("ahead ").strip())
            if cleaned.startswith("behind "):
                behind_count = int(cleaned.removeprefix("behind ").strip())
        return branch, ahead_count, behind_count

    def _has_pending_changes(self, *, pathspecs: list[str] | None = None) -> bool:
        _, status_lines = self._status_snapshot(pathspecs=pathspecs)
        return bool(status_lines)

    def _has_staged_changes(self, *, pathspecs: list[str] | None = None) -> bool:
        args = ["diff", "--cached", "--quiet"]
        if pathspecs:
            args.extend(["--", *pathspecs])
        return self._run_git(args, check=False).returncode == 1

    def _stage_changes(self, *, pathspecs: list[str] | None = None) -> None:
        if pathspecs:
            self._run_git(["add", "--all", "--", *pathspecs])
            return
        self._run_git(["add", "-A"])

    def _commit_changes(self, *, message: str, pathspecs: list[str] | None = None) -> None:
        args = ["commit", "-m", self._commit_message(message)]
        if pathspecs:
            staged_paths = self._staged_paths(pathspecs=pathspecs)
            if not staged_paths:
                return
            # Commit only the staged local-control files, even if the user has other staged work.
            args.extend(["--only", "--", *staged_paths])
        self._run_git(args)

    def _staged_paths(self, *, pathspecs: list[str]) -> list[str]:
        output = self._git_stdout(["diff", "--cached", "--name-only", "--", *pathspecs], check=False)
        return [line.strip() for line in output.splitlines() if line.strip()]

    def _has_commits(self) -> bool:
        return self._run_git(["rev-parse", "--verify", "HEAD"], check=False).returncode == 0

    def _is_git_repo(self) -> bool:
        return self._run_git(["rev-parse", "--is-inside-work-tree"], check=False).returncode == 0

    def _git_lfs_available(self) -> bool:
        return self._run_git(["lfs", "version"], cwd=self.store.root, check=False).returncode == 0

    def _ensure_lfs_local_config(self) -> None:
        if not self._git_lfs_available():
            return
        self._run_git(["lfs", "install", "--local"], check=False)

    def _commit_message(self, message: str) -> str:
        prefix = self.settings.vault_git_commit_prefix.strip()
        timestamp = utcnow().replace(microsecond=0).isoformat()
        base = message.strip() or "Update vault"
        return f"{prefix}: {base} ({timestamp})"

    def _git_stdout(self, args: list[str], *, check: bool) -> str:
        completed = self._run_git(args, check=check)
        return completed.stdout.strip()

    def _run_git(
        self,
        args: list[str],
        *,
        cwd: Path | None = None,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        completed = subprocess.run(
            ["git", *args],
            cwd=cwd or self.store.root,
            capture_output=True,
            text=True,
            check=False,
        )
        if check and completed.returncode != 0:
            stderr = completed.stderr.strip()
            stdout = completed.stdout.strip()
            detail = stderr or stdout or "Unknown git error."
            raise VaultGitSyncError(detail)
        return completed

    def _write_if_missing(self, path: Path, content: str) -> None:
        if path.exists():
            return
        self.store.write_text(path, content)
