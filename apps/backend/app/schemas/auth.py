from pydantic import BaseModel, EmailStr


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class MeResponse(BaseModel):
    email: EmailStr
    authenticated: bool
