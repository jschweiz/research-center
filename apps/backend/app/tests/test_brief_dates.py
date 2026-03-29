from datetime import UTC, date, datetime

from app.services.brief_dates import coverage_day_for_datetimes, edition_day_for_datetimes


def test_edition_day_shifts_source_date_to_next_day() -> None:
    published_at = datetime(2026, 3, 27, 8, 0, tzinfo=UTC)

    assert coverage_day_for_datetimes(
        published_at=published_at,
        first_seen_at=None,
        timezone_name="Europe/Zurich",
    ) == date(2026, 3, 27)
    assert edition_day_for_datetimes(
        published_at=published_at,
        first_seen_at=None,
        timezone_name="Europe/Zurich",
    ) == date(2026, 3, 28)


def test_edition_day_falls_back_to_first_seen_at_when_published_at_is_missing() -> None:
    first_seen_at = datetime(2026, 3, 26, 14, 30, tzinfo=UTC)

    assert coverage_day_for_datetimes(
        published_at=None,
        first_seen_at=first_seen_at,
        timezone_name="Europe/Zurich",
    ) == date(2026, 3, 26)
    assert edition_day_for_datetimes(
        published_at=None,
        first_seen_at=first_seen_at,
        timezone_name="Europe/Zurich",
    ) == date(2026, 3, 27)


def test_edition_day_respects_timezone_for_near_midnight_utc_items() -> None:
    published_at = datetime(2026, 3, 26, 23, 30, tzinfo=UTC)

    assert coverage_day_for_datetimes(
        published_at=published_at,
        first_seen_at=None,
        timezone_name="Europe/Zurich",
    ) == date(2026, 3, 27)
    assert edition_day_for_datetimes(
        published_at=published_at,
        first_seen_at=None,
        timezone_name="Europe/Zurich",
    ) == date(2026, 3, 28)
