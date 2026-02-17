import os

import boto3
from botocore.config import Config
from dagster import resource
from sqlalchemy import create_engine

from health_platform.intake.object_store import S3ObjectStore
from health_platform.utils.s3 import resolve_s3_settings


def _metadata_db_url() -> str:
    direct_url = os.getenv("METADATA_DB_URL")
    if direct_url:
        return direct_url

    return (
        f"postgresql+psycopg2://{os.getenv('METADATA_PG_USER')}:{os.getenv('METADATA_PG_PASSWORD')}"
        f"@{os.getenv('METADATA_PG_HOST')}:{os.getenv('METADATA_PG_PORT')}/{os.getenv('METADATA_PG_DB')}"
    )


def _build_s3_client():
    settings = resolve_s3_settings()
    return boto3.client(
        "s3",
        endpoint_url=settings.endpoint_url,
        aws_access_key_id=settings.access_key_id,
        aws_secret_access_key=settings.secret_access_key,
        region_name=settings.region,
        verify=settings.verify_ssl,
        config=Config(s3={"addressing_style": settings.addressing_style}),
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
    yield S3ObjectStore(client, resolve_s3_settings().bucket)
