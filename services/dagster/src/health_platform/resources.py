import os

import boto3
from dagster import resource
from sqlalchemy import create_engine

from health_platform.intake.object_store import S3ObjectStore


def _metadata_db_url() -> str:
    direct_url = os.getenv("METADATA_DB_URL")
    if direct_url:
        return direct_url

    return (
        f"postgresql+psycopg2://{os.getenv('METADATA_PG_USER')}:{os.getenv('METADATA_PG_PASSWORD')}"
        f"@{os.getenv('METADATA_PG_HOST')}:{os.getenv('METADATA_PG_PORT')}/{os.getenv('METADATA_PG_DB')}"
    )


def _build_s3_client():
    endpoint_url = os.getenv("S3_ENDPOINT_URL") or None
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=os.getenv("S3_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("S3_SECRET_ACCESS_KEY"),
        region_name=os.getenv("S3_REGION", "us-east-1"),
    )


@resource
def metadata_db_resource(_context):
    engine = create_engine(_metadata_db_url(), future=True)
    try:
        yield engine
    finally:
        engine.dispose()


@resource
def object_store_resource(_context):
    client = _build_s3_client()
    yield S3ObjectStore(client, os.getenv("S3_BUCKET", "health-raw"))
