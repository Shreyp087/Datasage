import os
import json
import redis
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
redis_client = redis.from_url(REDIS_URL)

def update_job_progress(job_id: str, pct: float, step: str, message: str):
    """
    Store progress in Redis key: f"job:progress:{job_id}"
    Format: {"pct": float, "step": str, "message": str, "updated_at": timestamp}
    TTL: 24 hours
    """
    key = f"job:progress:{job_id}"
    data = {
        "pct": pct,
        "step": step,
        "message": message,
        "updated_at": datetime.utcnow().isoformat()
    }
    try:
        redis_client.setex(key, 86400, json.dumps(data))
        
        # Also publish to a pubsub channel for WebSocket immediate push
        redis_client.publish(f"job:pubsub:{job_id}", json.dumps(data))
    except Exception as e:
        logger.error(f"Failed to update progress in Redis for {job_id}: {e}")

def get_job_progress(job_id: str) -> dict:
    key = f"job:progress:{job_id}"
    try:
        data = redis_client.get(key)
        if data:
            return json.loads(data)
    except Exception as e:
        logger.error(f"Failed to fetch progress from Redis for {job_id}: {e}")
    return None
