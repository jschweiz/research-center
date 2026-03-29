from __future__ import annotations

from app.db.models import DataMode, Item


def data_modes_for_metadata(
    metadata_json: dict | None,
    *,
    assume_unflagged_live: bool = False,
) -> set[DataMode]:
    payload = metadata_json or {}
    modes: set[DataMode] = set()

    raw_modes = payload.get("data_modes")
    if isinstance(raw_modes, list):
        for raw_mode in raw_modes:
            try:
                modes.add(DataMode(str(raw_mode)))
            except ValueError:
                continue

    if payload.get("seeded"):
        modes.add(DataMode.SEED)

    if assume_unflagged_live and not modes:
        modes.add(DataMode.LIVE)

    return modes


def merge_metadata_for_data_mode(
    existing_metadata: dict | None,
    incoming_metadata: dict | None,
    *,
    incoming_mode: DataMode,
) -> dict:
    merged = (existing_metadata or {}) | (incoming_metadata or {})
    existing_modes = data_modes_for_metadata(
        existing_metadata,
        assume_unflagged_live=existing_metadata is not None and not bool((existing_metadata or {}).get("seeded")),
    )
    incoming_modes = data_modes_for_metadata(
        incoming_metadata,
        assume_unflagged_live=incoming_mode == DataMode.LIVE and not bool((incoming_metadata or {}).get("seeded")),
    )
    merged["data_modes"] = sorted(mode.value for mode in existing_modes | incoming_modes | {incoming_mode})
    return merged


def is_seeded_item(item: Item) -> bool:
    return DataMode.SEED in data_modes_for_metadata(item.metadata_json)


def item_matches_data_mode(item: Item, data_mode: DataMode) -> bool:
    modes = data_modes_for_metadata(
        item.metadata_json,
        assume_unflagged_live=not bool((item.metadata_json or {}).get("seeded")),
    )
    return data_mode in modes


def filter_items_for_data_mode(items: list[Item], data_mode: DataMode) -> list[Item]:
    return [item for item in items if item_matches_data_mode(item, data_mode)]
