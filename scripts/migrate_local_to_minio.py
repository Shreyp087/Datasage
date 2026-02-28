import os
import re
import sys
import uuid
from pathlib import Path

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

ROOT_DIR = Path(__file__).resolve().parents[1]
BACKEND_DIR = ROOT_DIR / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.core.config import settings  # noqa: E402
from app.core.minio_client import minio_client  # noqa: E402
from app.models.models import (  # noqa: E402
    Dataset,
    DatasetDomainEnum,
    DatasetStatusEnum,
    FileFormatEnum,
    PlanEnum,
    User,
)

UPLOADS_DIR = BACKEND_DIR / "uploads"
FILE_PATTERN = re.compile(r"^(?P<dataset_id>[0-9a-fA-F-]{36})_(?P<filename>.+)$")


def resolve_file_format(filename: str) -> FileFormatEnum:
    ext = Path(filename).suffix.lower().lstrip(".")
    mapping = {
        "csv": FileFormatEnum.csv,
        "json": FileFormatEnum.json,
        "parquet": FileFormatEnum.parquet,
        "xlsx": FileFormatEnum.excel,
        "xls": FileFormatEnum.excel,
        "tsv": FileFormatEnum.tsv,
        "zip": FileFormatEnum.zip,
    }
    return mapping.get(ext, FileFormatEnum.csv)


def get_or_create_system_user(session) -> User:
    user = session.execute(select(User).order_by(User.created_at.asc()).limit(1)).scalar_one_or_none()
    if user:
        return user
    user = User(
        id=uuid.uuid4(),
        email="system@datasage.local",
        hashed_password="migrated",
        name="System Migration User",
        plan=PlanEnum.pro,
    )
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def main() -> None:
    if not UPLOADS_DIR.exists():
        print(f"Uploads directory not found: {UPLOADS_DIR}")
        return

    engine = create_engine(settings.sync_database_url, pool_pre_ping=True)
    Session = sessionmaker(bind=engine, autocommit=False, autoflush=False)

    migrated = 0
    skipped = 0
    failures: list[tuple[str, str]] = []

    with Session() as session:
        user = get_or_create_system_user(session)
        bucket = settings.normalized_minio_bucket

        for file_path in sorted(UPLOADS_DIR.iterdir()):
            if not file_path.is_file():
                continue

            match = FILE_PATTERN.match(file_path.name)
            if not match:
                skipped += 1
                continue

            dataset_id_str = match.group("dataset_id")
            original_filename = match.group("filename")
            storage_key = f"raw/system/{dataset_id_str}/{original_filename}"

            try:
                minio_client.fput_object(
                    bucket,
                    storage_key,
                    str(file_path),
                    content_type="application/octet-stream",
                )

                dataset_uuid = uuid.UUID(dataset_id_str)
                dataset = session.get(Dataset, dataset_uuid)
                if not dataset:
                    dataset = Dataset(
                        id=dataset_uuid,
                        user_id=user.id,
                        name=Path(original_filename).stem,
                        domain=DatasetDomainEnum.general,
                        original_filename=original_filename,
                        storage_path=storage_key,
                        file_format=resolve_file_format(original_filename),
                        file_size_bytes=file_path.stat().st_size,
                        status=DatasetStatusEnum.uploaded,
                    )
                    session.add(dataset)
                else:
                    dataset.storage_path = storage_key
                    dataset.file_size_bytes = file_path.stat().st_size

                session.commit()
                migrated += 1
            except Exception as exc:
                session.rollback()
                failures.append((file_path.name, str(exc)))

    print("Migration complete")
    print(f"Migrated: {migrated}")
    print(f"Skipped (pattern mismatch): {skipped}")
    print(f"Failures: {len(failures)}")
    if failures:
        for name, error in failures:
            print(f" - {name}: {error}")


if __name__ == "__main__":
    main()
