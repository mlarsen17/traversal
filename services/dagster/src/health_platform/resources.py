import os

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


@resource
def metadata_db_resource(_context):
    engine = create_engine(_metadata_db_url(), future=True)
    try:
        yield engine
    finally:
        engine.dispose()


@resource
def minio_resource(_context):
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
