from __future__ import annotations

from app.db.session import get_session_factory
from app.services.default_sources import upsert_default_sources


def main() -> None:
    session_factory = get_session_factory()
    db = session_factory()
    try:
        summary = upsert_default_sources(db)
        db.commit()
    finally:
        db.close()

    for name, action in summary:
        print(f"{action}: {name}")


if __name__ == "__main__":
    main()
