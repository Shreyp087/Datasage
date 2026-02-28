from __future__ import annotations

import uuid
from copy import deepcopy
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.models import PlanEnum, User
from app.models.notebook import Notebook
from app.notebooks.templates.aiid_template import AIID_TEMPLATE


async def get_or_create_system_user(db: AsyncSession) -> uuid.UUID:
    result = await db.execute(select(User).where(User.email == "test@datasage.local").limit(1))
    user = result.scalars().first()
    if user:
        return user.id

    user = User(
        id=uuid4(),
        email="test@datasage.local",
        hashed_password="mock",
        name="Test User",
        plan=PlanEnum.pro,
    )
    db.add(user)
    await db.flush()
    return user.id


async def seed_aiid_template(db: AsyncSession) -> None:
    """
    Run on startup: ensure the AIID template notebook exists in DB.
    Idempotent - only creates if not already present.
    """
    existing = await db.execute(
        select(Notebook).where(
            Notebook.title == AIID_TEMPLATE["title"],
            Notebook.is_template.is_(True),
        ).limit(1)
    )
    if existing.scalar_one_or_none():
        return

    system_user_id = await get_or_create_system_user(db)

    notebook = Notebook(
        user_id=system_user_id,
        dataset_id=None,
        title=AIID_TEMPLATE["title"],
        description=AIID_TEMPLATE["description"],
        domain=AIID_TEMPLATE["domain"],
        cells=deepcopy(AIID_TEMPLATE["cells"]),
        results={},
        is_template=True,
        is_public=True,
        tags=AIID_TEMPLATE["tags"],
    )
    db.add(notebook)
    await db.commit()
    print("AIID notebook template seeded")


async def seed_default_notebook_templates(db: AsyncSession) -> int:
    """
    Backward-compatible wrapper for existing startup calls.
    """
    before = await db.execute(
        select(Notebook.id).where(
            Notebook.title == AIID_TEMPLATE["title"],
            Notebook.is_template.is_(True),
        ).limit(1)
    )
    existed = before.scalar_one_or_none() is not None
    await seed_aiid_template(db)
    return 0 if existed else 1
