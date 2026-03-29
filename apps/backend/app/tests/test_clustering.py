import builtins
import logging

from app.core.config import get_settings
from app.services.clustering import EmbeddingBackend


def _reset_embedding_backend() -> None:
    EmbeddingBackend._model = None
    EmbeddingBackend._load_failed = False


def test_embedding_backend_returns_none_when_disabled(monkeypatch) -> None:
    monkeypatch.setenv("ENABLE_EMBEDDINGS", "false")
    get_settings.cache_clear()
    _reset_embedding_backend()

    assert EmbeddingBackend.encode("ranking transparency") is None

    _reset_embedding_backend()
    get_settings.cache_clear()


def test_embedding_backend_logs_and_falls_back_when_dependency_is_missing(
    monkeypatch,
    caplog,
) -> None:
    monkeypatch.setenv("ENABLE_EMBEDDINGS", "true")
    get_settings.cache_clear()
    _reset_embedding_backend()
    caplog.set_level(logging.WARNING)
    original_import = builtins.__import__

    def _patched_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "sentence_transformers":
            raise ModuleNotFoundError("sentence_transformers not installed")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _patched_import)

    assert EmbeddingBackend.encode("verifier routing") is None
    assert EmbeddingBackend._load_failed is True
    assert "clustering.embedding_backend_unavailable" in caplog.text

    _reset_embedding_backend()
    get_settings.cache_clear()
