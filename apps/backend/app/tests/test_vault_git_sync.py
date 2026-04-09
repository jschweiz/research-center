from __future__ import annotations

import subprocess
from pathlib import Path

from app.core.config import get_settings
from app.services.vault_git_sync import VaultGitSyncService


def _run_git(cwd: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=True,
    )
    return completed.stdout.strip()


def test_push_local_control_changes_only_commits_local_control_paths(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("VAULT_ROOT_DIR", str(tmp_path / "vault"))
    monkeypatch.setenv("LOCAL_STATE_DIR", str(tmp_path / "local-state"))
    monkeypatch.setenv("VAULT_GIT_ENABLED", "true")
    monkeypatch.setenv("VAULT_GIT_REMOTE_URL", "")
    monkeypatch.setenv("GIT_AUTHOR_NAME", "Test User")
    monkeypatch.setenv("GIT_AUTHOR_EMAIL", "test@example.com")
    monkeypatch.setenv("GIT_COMMITTER_NAME", "Test User")
    monkeypatch.setenv("GIT_COMMITTER_EMAIL", "test@example.com")
    get_settings.cache_clear()

    try:
        service = VaultGitSyncService()
        service.ensure_repository()
        vault_root = service.store.root

        raw_path = vault_root / "raw" / "article" / "seed" / "source.md"
        wiki_path = vault_root / "wiki" / "topics" / "seed.md"

        service.store.write_text(raw_path, "raw v1\n")
        service.store.write_text(wiki_path, "wiki v1\n")
        _run_git(vault_root, "add", "-A")
        _run_git(vault_root, "commit", "-m", "Seed tracked files")

        service.store.write_text(raw_path, "raw v2\n")
        service.store.write_text(wiki_path, "wiki v2\n")
        before_head = _run_git(vault_root, "rev-parse", "HEAD")

        service.push_local_control_changes(message="Sync local-control outputs")

        after_head = _run_git(vault_root, "rev-parse", "HEAD")
        changed_files = _run_git(vault_root, "show", "--pretty=format:", "--name-only", "HEAD").splitlines()
        status_lines = _run_git(vault_root, "status", "--short").splitlines()

        assert after_head != before_head
        assert "raw/article/seed/source.md" in changed_files
        assert "wiki/topics/seed.md" not in changed_files
        assert any(line.endswith("wiki/topics/seed.md") for line in status_lines)
        assert not any(line.endswith("raw/article/seed/source.md") for line in status_lines)
    finally:
        get_settings.cache_clear()
