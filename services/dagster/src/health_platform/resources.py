import os
from pathlib import Path

import boto3
from dagster import resource
from sqlalchemy import create_engine


def _metadata_db_url() -> str:
    direct_url = os.getenv("METADATA_DB_URL")
    if direct_url:
        return direct_url

    return (
        f"postgresql+psycopg2://{os.getenv('METADATA_PG_USER')}:{os.getenv('METADATA_PG_PASSWORD')}"
        f"@{os.getenv('METADATA_PG_HOST')}:{os.getenv('METADATA_PG_PORT')}/{os.getenv('METADATA_PG_DB')}"
    )


class LocalObjectStoreClient:
    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)

    def put_object(self, Bucket: str, Key: str, Body: bytes, ContentType: str | None = None):  # noqa: N803
        del ContentType
        target_path = self.base_dir / Bucket / Key
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_bytes(Body)
        return {"path": str(target_path)}


@resource
def metadata_db_resource(_context):
    engine = create_engine(_metadata_db_url(), future=True)
    try:
        yield engine
    finally:
        engine.dispose()


@resource
def minio_resource(_context):
    mode = os.getenv("OBJECT_STORE_MODE", "s3").lower()
    if mode == "local":
        local_dir = os.getenv("LOCAL_OBJECT_STORE_DIR", ".local_object_store")
        yield LocalObjectStoreClient(local_dir)
        return

    secure = os.getenv("MINIO_SECURE", "false").lower() == "true"
    scheme = "https" if secure else "http"
    client = boto3.client(
        "s3",
        endpoint_url=f"{scheme}://{os.getenv('MINIO_ENDPOINT')}",
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
        region_name=os.getenv("MINIO_REGION", "us-east-1"),
    )
    yield client
