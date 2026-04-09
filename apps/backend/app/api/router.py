from fastapi import APIRouter

from app.api.routes import (
    auth,
    briefs,
    connections,
    health,
    items,
    local_control,
    me,
    ops,
    profile,
    sources,
)

api_router = APIRouter()
api_router.include_router(health.router, tags=["health"])
api_router.include_router(me.router, tags=["auth"])
api_router.include_router(auth.router, prefix="/auth", tags=["auth"])
api_router.include_router(briefs.router, prefix="/briefs", tags=["briefs"])
api_router.include_router(connections.router, prefix="/connections", tags=["connections"])
api_router.include_router(items.router, prefix="/items", tags=["items"])
api_router.include_router(ops.router, prefix="/ops", tags=["ops"])
api_router.include_router(profile.router, prefix="/profile", tags=["profile"])
api_router.include_router(sources.router, prefix="/sources", tags=["sources"])
api_router.include_router(local_control.router)
