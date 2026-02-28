from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Core infrastructure
    database_url: str = Field(
        default="postgresql+asyncpg://datasage:datasage_secret@localhost:5432/datasage_db"
    )
    redis_url: str = Field(default="redis://localhost:6379/0")

    # MinIO / object storage
    minio_endpoint: str = Field(default="localhost:9000")
    minio_access_key: str = Field(default="minioadmin")
    minio_secret_key: str = Field(default="minioadmin")
    minio_bucket: str = Field(default="datasage")
    minio_secure: bool = Field(default=False)

    # Optional compatibility input used by older env files
    minio_url: str = Field(default="")
    minio_bucket_name: str = Field(default="")

    # LLM config
    llm_provider: str = Field(default="openai")
    openai_api_key: str = Field(default="")
    openai_model: str = Field(default="gpt-4o")
    anthropic_api_key: str = Field(default="")
    anthropic_model: str = Field(default="claude-sonnet-4-6")

    # CORS
    cors_allow_origins: str = Field(default="")

    @field_validator("llm_provider")
    @classmethod
    def validate_provider(cls, value: str) -> str:
        allowed = {"openai", "anthropic"}
        if value not in allowed:
            raise ValueError(f"LLM_PROVIDER must be one of {sorted(allowed)}")
        return value

    @property
    def normalized_minio_endpoint(self) -> str:
        """Return endpoint in host:port form expected by the MinIO SDK."""
        if self.minio_url:
            parsed = urlparse(self.minio_url)
            if parsed.netloc:
                return parsed.netloc
            if parsed.path:
                return parsed.path
        return self.minio_endpoint

    @property
    def normalized_minio_bucket(self) -> str:
        return self.minio_bucket_name or self.minio_bucket

    @property
    def async_database_url(self) -> str:
        """Normalize DATABASE_URL into an async SQLAlchemy URL."""
        url = self.database_url
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql://", 1)
        if url.startswith("postgresql://"):
            url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
        if url.startswith("postgresql+psycopg2://"):
            url = url.replace("postgresql+psycopg2://", "postgresql+asyncpg://", 1)

        parsed = urlparse(url)
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        local_hosts = {"localhost", "127.0.0.1", "postgres"}
        if parsed.hostname and parsed.hostname not in local_hosts:
            if "ssl" not in query and "sslmode" not in query:
                query["ssl"] = "require"
            url = urlunparse(parsed._replace(query=urlencode(query)))
        return url

    @property
    def sync_database_url(self) -> str:
        """Convert asyncpg URL to psycopg2 URL for sync contexts (Celery)."""
        url = self.async_database_url.replace(
            "postgresql+asyncpg://",
            "postgresql+psycopg2://",
            1,
        )
        parsed = urlparse(url)
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        if "ssl" in query and "sslmode" not in query:
            query["sslmode"] = query.pop("ssl")
        return urlunparse(parsed._replace(query=urlencode(query)))


settings = Settings()
