from __future__ import annotations

import hashlib
import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user
from app.core.database import get_db
from app.models.models import PlanEnum, User

router = APIRouter(prefix="/auth", tags=["auth"])


class RegisterRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8, max_length=256)
    name: str = Field(min_length=1, max_length=200)


class LoginRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=1, max_length=256)


def _hash_password(password: str) -> str:
    digest = hashlib.sha256(password.encode("utf-8")).hexdigest()
    return f"sha256${digest}"


def _verify_password(password: str, stored: str) -> bool:
    if not stored:
        return False
    if stored.startswith("sha256$"):
        return _hash_password(password) == stored
    # Backward compatibility for existing dev rows that used plain/mock values.
    return stored == password


def _token_for_user(user: User) -> str:
    # Dev token format: raw UUID string (works with deps parser).
    return str(user.id)


def _serialize_user(user: User) -> dict:
    return {
        "id": str(user.id),
        "email": user.email,
        "name": user.name,
        "plan": user.plan.value if hasattr(user.plan, "value") else str(user.plan),
    }


@router.post("/register", status_code=status.HTTP_201_CREATED)
async def register(payload: RegisterRequest, db: AsyncSession = Depends(get_db)):
    existing = await db.execute(select(User).where(User.email == payload.email))
    if existing.scalars().first():
        raise HTTPException(status_code=400, detail="Email already registered")

    user = User(
        id=uuid.uuid4(),
        email=payload.email,
        hashed_password=_hash_password(payload.password),
        name=payload.name.strip(),
        plan=PlanEnum.free,
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)

    return {
        "access_token": _token_for_user(user),
        "token_type": "bearer",
        "user": _serialize_user(user),
    }


@router.post("/login")
async def login(payload: LoginRequest, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(User).where(User.email == payload.email))
    user = result.scalars().first()

    if not user or not _verify_password(payload.password, user.hashed_password):
        raise HTTPException(status_code=401, detail="Invalid email or password")

    return {
        "access_token": _token_for_user(user),
        "token_type": "bearer",
        "user": _serialize_user(user),
    }


@router.get("/me")
async def get_me(current_user: User = Depends(get_current_user)):
    return _serialize_user(current_user)
