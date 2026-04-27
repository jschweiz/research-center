from app.core.external_urls import resolve_external_url


def test_resolve_external_url_routes_medium_through_freedium() -> None:
    url = "https://medium.com/personal-growth/"

    assert resolve_external_url(url) == "https://freedium-mirror.cfd/https://medium.com/personal-growth/"


def test_resolve_external_url_leaves_non_medium_urls_unchanged() -> None:
    url = "https://example.com/story"

    assert resolve_external_url(url) == url


def test_resolve_external_url_is_idempotent_for_existing_freedium_links() -> None:
    url = "https://freedium-mirror.cfd/https://medium.com/personal-growth/"

    assert resolve_external_url(url) == url
