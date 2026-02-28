import time

from minio import Minio

from app.core.config import settings

_client: Minio | None = None


def get_minio_client() -> Minio:
    global _client
    if _client is None:
        _client = Minio(
            settings.normalized_minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=settings.minio_secure,
        )
    return _client


def ensure_bucket_exists(retries: int = 10, delay_seconds: int = 1) -> None:
    client = get_minio_client()
    bucket = settings.normalized_minio_bucket
    last_error: Exception | None = None
    for _ in range(retries):
        try:
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
            last_error = None
            break
        except Exception as exc:
            last_error = exc
            time.sleep(delay_seconds)
    if last_error is not None:
        raise last_error


minio_client = get_minio_client()
