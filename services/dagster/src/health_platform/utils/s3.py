from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class S3Settings:
    endpoint_url: str | None
    access_key_id: str | None
    secret_access_key: str | None
    region: str
    bucket: str
    verify_ssl: bool
    addressing_style: str


def _pick(*values: str | None) -> str | None:
    for value in values:
        if value:
            return value
    return None


def resolve_s3_settings() -> S3Settings:
    endpoint_url = _pick(
        os.getenv("S3_ENDPOINT_URL"),
        os.getenv("MINIO_ENDPOINT_URL"),
        os.getenv("MINIO_ENDPOINT"),
    )
    access_key_id = _pick(os.getenv("S3_ACCESS_KEY_ID"), os.getenv("MINIO_ROOT_USER"))
    secret_access_key = _pick(os.getenv("S3_SECRET_ACCESS_KEY"), os.getenv("MINIO_ROOT_PASSWORD"))
    bucket = _pick(os.getenv("S3_BUCKET"), os.getenv("RAW_BUCKET_NAME"), "health-raw") or "health-raw"
    region = os.getenv("S3_REGION", "us-east-1")

    if endpoint_url and not endpoint_url.startswith(("http://", "https://")):
        endpoint_url = f"http://{endpoint_url}"

    is_minio = bool(
        endpoint_url
        and ("minio" in endpoint_url or "127.0.0.1" in endpoint_url or "localhost" in endpoint_url)
    )
    verify_ssl = False if is_minio else True
    addressing_style = "path" if is_minio else "auto"

    return S3Settings(
        endpoint_url=endpoint_url,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        region=region,
        bucket=bucket,
        verify_ssl=verify_ssl,
        addressing_style=addressing_style,
    )


def upload_text_object(s3_client, bucket_name: str, key: str, payload):
    s3_client.put_object(Bucket=bucket_name, Key=key, Body=payload.read(), ContentType="text/plain")
