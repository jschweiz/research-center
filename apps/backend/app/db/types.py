from collections.abc import Sequence
from typing import Any

from sqlalchemy import JSON, TypeDecorator


class EmbeddingVector(TypeDecorator[list[float] | None]):
    impl = JSON
    cache_ok = True

    def __init__(self, dimensions: int = 384) -> None:
        super().__init__()
        self.dimensions = dimensions

    def load_dialect_impl(self, dialect):  # type: ignore[override]
        if dialect.name == "postgresql":
            from pgvector.sqlalchemy import Vector

            return dialect.type_descriptor(Vector(self.dimensions))
        return dialect.type_descriptor(JSON())

    def process_bind_param(self, value: Sequence[float] | None, dialect) -> Any:  # type: ignore[override]
        if value is None:
            return None
        return list(value)

    def process_result_value(self, value: Any, dialect) -> list[float] | None:  # type: ignore[override]
        if value is None:
            return None
        return list(value)
