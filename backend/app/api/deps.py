import uuid
from typing import Optional

from fastapi import Depends, Header
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.models.models import PlanEnum, User


async def get_or_create_default_user(db: AsyncSession) -> User:
    result = await db.execute(select(User).order_by(User.created_at.asc()).limit(1))
    user = result.scalars().first()
    if user:
        return user

    user = User(
        id=uuid.uuid4(),
        email="test@datasage.local",
        hashed_password="mock",
        name="Test User",
        plan=PlanEnum.pro,
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)
    return user


async def get_current_user(
    db: AsyncSession = Depends(get_db),
    x_user_id: Optional[str] = Header(default=None, alias="X-User-Id"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> User:
    """
    Development auth shim.
    - If X-User-Id is provided and exists, that user is returned.
    - Otherwise, the first existing user is used or a default user is created.
    """
    # Priority 1: explicit user id header (legacy dev shim).
    if x_user_id:
        try:
            parsed_id = uuid.UUID(x_user_id)
            user = await db.get(User, parsed_id)
            if user:
                return user
        except ValueError:
            pass

    # Priority 2: bearer token carrying user UUID.
    if authorization and authorization.lower().startswith("bearer "):
        token = authorization.split(" ", 1)[1].strip()
        try:
            parsed_id = uuid.UUID(token)
            user = await db.get(User, parsed_id)
            if user:
                return user
        except ValueError:
            pass

    return await get_or_create_default_user(db)
