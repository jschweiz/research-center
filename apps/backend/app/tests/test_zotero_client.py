import httpx

from app.integrations.zotero import ZoteroClient


def _json_response(method: str, url: str, payload, status_code: int = 200) -> httpx.Response:
    return httpx.Response(
        status_code,
        request=httpx.Request(method, url),
        json=payload,
    )


def test_save_item_adds_existing_collection_key_to_payload(monkeypatch) -> None:
    recorded_posts: list[tuple[str, dict, list[dict]]] = []

    def _get(url: str, headers: dict, params: dict, timeout: int) -> httpx.Response:
        assert url.endswith("/collections/top")
        assert params["format"] == "json"
        return _json_response(
            "GET",
            url,
            [{"key": "COLL1234", "data": {"key": "COLL1234", "name": "Research Center"}}],
        )

    def _post(url: str, headers: dict, json: list[dict], timeout: int) -> httpx.Response:
        recorded_posts.append((url, headers, json))
        return _json_response("POST", url, {"success": {"0": "ITEM1234"}})

    monkeypatch.setattr("app.integrations.zotero.httpx.get", _get)
    monkeypatch.setattr("app.integrations.zotero.httpx.post", _post)

    result = ZoteroClient(api_key="secret-token", library_id="12345").save_item(
        item={
            "title": "Verifier Routing for Research Agents",
            "canonical_url": "https://example.com/paper",
            "authors": ["Analyst"],
            "content_type": "paper",
            "published_at": "2026-03-25T17:54:10+00:00",
        },
        insight={
            "short_summary": "Summarizes verification-first routing.",
            "why_it_matters": "Improves confidence in agent outputs.",
            "follow_up_questions": [],
        },
        tags=["paper"],
        collection_name="Research Center",
    )

    assert result.success is True
    assert len(recorded_posts) == 1
    item_request = recorded_posts[0]
    assert item_request[0].endswith("/items")
    assert item_request[2][0]["collections"] == ["COLL1234"]
    assert "Zotero-Write-Token" in item_request[1]


def test_save_item_creates_missing_nested_collection_path(monkeypatch) -> None:
    recorded_posts: list[tuple[str, dict, list[dict]]] = []

    def _get(url: str, headers: dict, params: dict, timeout: int) -> httpx.Response:
        if url.endswith("/collections/top"):
            return _json_response("GET", url, [])
        if url.endswith("/collections/COLLROOT1/collections"):
            return _json_response("GET", url, [])
        raise AssertionError(f"Unexpected GET url: {url}")

    def _post(url: str, headers: dict, json: list[dict], timeout: int) -> httpx.Response:
        recorded_posts.append((url, headers, json))
        if url.endswith("/collections"):
            payload = json[0]
            if payload["name"] == "Research Center":
                return _json_response(
                    "POST",
                    url,
                    {"successful": {"0": {"key": "COLLROOT1", "data": {"key": "COLLROOT1"}}}},
                )
            if payload["name"] == "Papers":
                assert payload["parentCollection"] == "COLLROOT1"
                return _json_response(
                    "POST",
                    url,
                    {"successful": {"0": {"key": "COLLSUB2", "data": {"key": "COLLSUB2"}}}},
                )
            raise AssertionError(f"Unexpected collection payload: {payload}")
        if url.endswith("/items"):
            return _json_response("POST", url, {"success": {"0": "ITEM1234"}})
        raise AssertionError(f"Unexpected POST url: {url}")

    monkeypatch.setattr("app.integrations.zotero.httpx.get", _get)
    monkeypatch.setattr("app.integrations.zotero.httpx.post", _post)

    result = ZoteroClient(api_key="secret-token", library_id="12345").save_item(
        item={
            "title": "Verifier Routing for Research Agents",
            "canonical_url": "https://example.com/paper",
            "authors": ["Analyst"],
            "content_type": "paper",
            "published_at": "2026-03-25T17:54:10+00:00",
        },
        insight={
            "short_summary": "Summarizes verification-first routing.",
            "why_it_matters": "Improves confidence in agent outputs.",
            "follow_up_questions": [],
        },
        tags=["paper"],
        collection_name="Research Center / Papers",
    )

    assert result.success is True
    assert len(recorded_posts) == 3
    assert recorded_posts[0][0].endswith("/collections")
    assert recorded_posts[1][0].endswith("/collections")
    assert recorded_posts[2][0].endswith("/items")
    assert recorded_posts[2][2][0]["collections"] == ["COLLSUB2"]
    assert result.response_payload["collection_key"] == "COLLSUB2"


def test_save_item_returns_failure_when_collection_creation_reports_error(monkeypatch) -> None:
    recorded_posts: list[str] = []

    def _get(url: str, headers: dict, params: dict, timeout: int) -> httpx.Response:
        return _json_response("GET", url, [])

    def _post(url: str, headers: dict, json: list[dict], timeout: int) -> httpx.Response:
        recorded_posts.append(url)
        if url.endswith("/collections"):
            return _json_response(
                "POST",
                url,
                {"successful": {}, "failed": {"0": {"message": "Collection already exists in trash."}}},
            )
        raise AssertionError("Item creation should not be attempted when collection setup fails.")

    monkeypatch.setattr("app.integrations.zotero.httpx.get", _get)
    monkeypatch.setattr("app.integrations.zotero.httpx.post", _post)

    result = ZoteroClient(api_key="secret-token", library_id="12345").save_item(
        item={
            "title": "Verifier Routing for Research Agents",
            "canonical_url": "https://example.com/paper",
            "authors": ["Analyst"],
            "content_type": "paper",
            "published_at": "2026-03-25T17:54:10+00:00",
        },
        insight={
            "short_summary": "Summarizes verification-first routing.",
            "why_it_matters": "Improves confidence in agent outputs.",
            "follow_up_questions": [],
        },
        tags=["paper"],
        collection_name="Research Center",
    )

    assert result.success is False
    assert result.detail == "Zotero collection creation failed: Collection already exists in trash."
    assert recorded_posts == ["https://api.zotero.org/users/12345/collections"]
