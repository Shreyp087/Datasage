from fastapi import APIRouter, Depends

from app.api.deps import get_current_user
from app.models.models import User

router = APIRouter(prefix="/auth", tags=["auth"])


@router.get("/me")
async def get_me(current_user: User = Depends(get_current_user)):
    return {
        "id": str(current_user.id),
        "email": current_user.email,
        "name": current_user.name,
        "plan": current_user.plan.value if hasattr(current_user.plan, "value") else str(current_user.plan),
    }
