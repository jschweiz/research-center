from __future__ import annotations

import httpx

from app.integrations.extractors import ContentExtractor


def _response(url: str, body: str) -> httpx.Response:
    return httpx.Response(
        200,
        request=httpx.Request("GET", url),
        text=body,
        headers={"content-type": "text/html; charset=utf-8"},
    )


def test_content_extractor_retries_after_transient_fetch_failure(monkeypatch) -> None:
    calls: list[bool] = []
    url = "https://example.com/post"
    html = """
<html>
  <head><title>Example Post</title></head>
  <body>
    <article><p>Recovered body text.</p></article>
  </body>
</html>
""".strip()

    def fake_fetch(
        candidate_url: str,
        *,
        timeout: float,
        headers=None,
        max_redirects: int = 5,
        allow_insecure_tls: bool = False,
    ) -> httpx.Response:
        assert candidate_url == url
        assert timeout > 0
        assert max_redirects >= 1
        assert headers is None or isinstance(headers, dict)
        calls.append(allow_insecure_tls)
        if len(calls) == 1:
            raise httpx.ReadTimeout("timed out", request=httpx.Request("GET", candidate_url))
        return _response(candidate_url, html)

    monkeypatch.setattr("app.integrations.extractors.fetch_safe_response", fake_fetch)
    monkeypatch.setattr("app.integrations.extractors.trafilatura.extract", lambda *args, **kwargs: "Recovered body text.")

    result = ContentExtractor().extract_from_url(url)

    assert result.title == "Example Post"
    assert result.cleaned_text == "Recovered body text."
    assert calls == [False, True]


def test_content_extractor_recovers_from_title_only_extraction_using_article_html(
    monkeypatch,
) -> None:
    url = "https://example.com/post"
    html = """
<html>
  <head>
    <title>Example Post</title>
    <meta name="description" content="Short summary that should only be used as a last resort." />
  </head>
  <body>
    <article>
      <h1>Example Post</h1>
      <p>First body paragraph.</p>
      <p>Second body paragraph with more detail.</p>
    </article>
  </body>
</html>
""".strip()

    def fake_fetch(
        candidate_url: str,
        *,
        timeout: float,
        headers=None,
        max_redirects: int = 5,
        allow_insecure_tls: bool = False,
    ) -> httpx.Response:
        assert candidate_url == url
        assert timeout > 0
        assert max_redirects >= 1
        assert headers is None or isinstance(headers, dict)
        return _response(candidate_url, html)

    def fake_extract(payload: str, **kwargs) -> str:
        if "<html" in payload:
            return "Example Post"
        return ""

    monkeypatch.setattr("app.integrations.extractors.fetch_safe_response", fake_fetch)
    monkeypatch.setattr("app.integrations.extractors.trafilatura.extract", fake_extract)

    result = ContentExtractor().extract_from_url(url)

    assert result.title == "Example Post"
    assert "First body paragraph." in result.cleaned_text
    assert "Second body paragraph with more detail." in result.cleaned_text
    assert result.cleaned_text != "Example Post"

