from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime

import boto3
from botocore.exceptions import ClientError


@dataclass
class ObjectMeta:
    key: str
    size_bytes: int
    last_modified: datetime
    etag: str | None = None


class BaseObjectStore:
    def list_objects(self, prefix: str) -> list[ObjectMeta]:
        raise NotImplementedError

    def stat_object(self, key: str) -> ObjectMeta | None:
        raise NotImplementedError

    def get_bytes(self, key: str, max_bytes: int | None = None) -> bytes:
        raise NotImplementedError

    def put_bytes(self, key: str, data: bytes) -> None:
        raise NotImplementedError

    def copy_object(self, src: str, dst: str) -> None:
        raise NotImplementedError

    def delete_object(self, key: str) -> None:
        raise NotImplementedError

    def ensure_prefix(self, prefix: str) -> None:
        raise NotImplementedError

    # compatibility for existing phase-0 utility
    def put_object(self, Bucket: str, Key: str, Body: bytes, ContentType: str | None = None):  # noqa: N803
        del Bucket, ContentType
        self.put_bytes(Key, Body)


class S3ObjectStore(BaseObjectStore):
    def __init__(self, bucket: str):
        secure = os.getenv("MINIO_SECURE", "false").lower() == "true"
        scheme = "https" if secure else "http"
        endpoint = os.getenv("MINIO_ENDPOINT")
        endpoint_url = f"{scheme}://{endpoint}" if endpoint else None

        self.bucket = bucket
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
            region_name=os.getenv("MINIO_REGION", "us-east-1"),
        )

        try:
            self.client.head_bucket(Bucket=self.bucket)
        except ClientError:
            self.client.create_bucket(Bucket=self.bucket)

    def list_objects(self, prefix: str) -> list[ObjectMeta]:
        paginator = self.client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.bucket, Prefix=prefix)
        results: list[ObjectMeta] = []
        for page in pages:
            for obj in page.get("Contents", []):
                results.append(
                    ObjectMeta(
                        key=obj["Key"],
                        size_bytes=obj["Size"],
                        last_modified=obj["LastModified"],
                        etag=(obj.get("ETag") or "").strip('"') or None,
                    )
                )
        return sorted(results, key=lambda item: item.key)

    def stat_object(self, key: str) -> ObjectMeta | None:
        try:
            response = self.client.head_object(Bucket=self.bucket, Key=key)
        except ClientError:
            return None
        return ObjectMeta(
            key=key,
            size_bytes=response["ContentLength"],
            last_modified=response["LastModified"],
            etag=(response.get("ETag") or "").strip('"') or None,
        )

    def get_bytes(self, key: str, max_bytes: int | None = None) -> bytes:
        kwargs = {"Bucket": self.bucket, "Key": key}
        if max_bytes:
            kwargs["Range"] = f"bytes=0-{max_bytes - 1}"
        return self.client.get_object(**kwargs)["Body"].read()

    def put_bytes(self, key: str, data: bytes) -> None:
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data)

    def copy_object(self, src: str, dst: str) -> None:
        self.client.copy_object(Bucket=self.bucket, CopySource={"Bucket": self.bucket, "Key": src}, Key=dst)

    def delete_object(self, key: str) -> None:
        self.client.delete_object(Bucket=self.bucket, Key=key)

    def ensure_prefix(self, prefix: str) -> None:
        self.put_bytes(prefix.rstrip("/") + "/.keep", b"")


def create_object_store() -> BaseObjectStore:
    return S3ObjectStore(os.getenv("RAW_BUCKET_NAME", "health-raw"))
