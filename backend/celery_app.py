import os
from celery import Celery
from kombu import Queue
from dotenv import load_dotenv

# Load root .env so worker processes pick up DATABASE_URL/REDIS_URL consistently.
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")

celery_app = Celery(
    "datasage_worker",
    broker=redis_url,
    backend=redis_url,
    include=["workers.tasks"]
)

# Critical Celery configs for Data processing
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    worker_prefetch_multiplier=1, # Never prefetch heavy datasets
    task_acks_late=True, # Ack only after completion
    worker_max_tasks_per_child=10, # Prevent memory leaks
    task_routes={
        "workers.tasks.process_dataset": {"queue": "fast"},
    },
    task_default_queue="fast",
    task_default_exchange="datasage",
    task_default_exchange_type="direct",
    task_default_routing_key="fast",
    task_queues=(
        Queue("fast", routing_key="fast"),
        Queue("heavy", routing_key="heavy"),
        Queue("agents", routing_key="agents"),
    ),
)
