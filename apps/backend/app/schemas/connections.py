from datetime import datetime

from pydantic import BaseModel, Field

from app.db.models import ConnectionProvider, ConnectionStatus
from app.schemas.common import ORMModel


class ConnectionPayload(BaseModel):
    label: str
    payload: dict = Field(default_factory=dict)
    metadata_json: dict = Field(default_factory=dict)


class ConnectionRead(ORMModel):
    id: str
    provider: ConnectionProvider
    label: str
    metadata_json: dict
    status: ConnectionStatus
    last_synced_at: datetime | None
    created_at: datetime
    updated_at: datetime


class ConnectionCapabilitiesRead(BaseModel):
    gmail_oauth_configured: bool = False
