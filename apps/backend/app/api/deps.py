from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.security import get_session_manager
from app.db.session import get_db


def get_current_email(request: Request) -> str:
    settings = get_settings()
    token = request.cookies.get(settings.session_cookie_name)
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required.")

    email = get_session_manager().load_token(token)
    if not email:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Session expired.")
    return email


def get_current_user(email: str = Depends(get_current_email)) -> dict:
    return {"email": email}


DBSession = Session


def get_db_session(db: Session = Depends(get_db)) -> Session:
    return db
