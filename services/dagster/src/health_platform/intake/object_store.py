from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol


@dataclass(frozen=True)
class ObjectMetadata:
    key: str
    size: int
    etag: str | None
    last_modified: datetime | None


class ObjectStore(Protocol):
    def list_objects(self, prefix: str) -> list[ObjectMetadata]: ...

    def stat_object(self, key: str) -> ObjectMetadata | None: ...

    def copy_object(self, source_key: str, dest_key: str) -> None: ...

    def delete_object(self, key: str) -> None: ...

    def put_bytes(self, key: str, payload: bytes, content_type: str = "application/octet-stream") -> None: ...

    def get_bytes(self, key: str) -> bytes: ...


class S3ObjectStore:
    def __init__(self, client, bucket: str):
        self.client = client
        self.bucket = bucket

    def list_objects(self, prefix: str) -> list[ObjectMetadata]:
        token = None
        out: list[ObjectMetadata] = []
        while True:
            kwargs = {"Bucket": self.bucket, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            resp = self.client.list_objects_v2(**kwargs)
            for obj in resp.get("Contents", []):
                out.append(
                    ObjectMetadata(
                        key=obj["Key"],
                        size=int(obj["Size"]),
                        etag=str(obj.get("ETag", "")).strip('"') or None,
                        last_modified=obj.get("LastModified"),
                    )
                )
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        return out

    def stat_object(self, key: str) -> ObjectMetadata | None:
        try:
            resp = self.client.head_object(Bucket=self.bucket, Key=key)
        except Exception:
            return None
        return ObjectMetadata(
            key=key,
            size=int(resp["ContentLength"]),
            etag=str(resp.get("ETag", "")).strip('"') or None,
            last_modified=resp.get("LastModified"),
        )

    def copy_object(self, source_key: str, dest_key: str) -> None:
        self.client.copy_object(Bucket=self.bucket, CopySource={"Bucket": self.bucket, "Key": source_key}, Key=dest_key)

    def delete_object(self, key: str) -> None:
        self.client.delete_object(Bucket=self.bucket, Key=key)

    def put_bytes(self, key: str, payload: bytes, content_type: str = "application/octet-stream") -> None:
        self.client.put_object(Bucket=self.bucket, Key=key, Body=payload, ContentType=content_type)

    def get_bytes(self, key: str) -> bytes:
        obj = self.client.get_object(Bucket=self.bucket, Key=key)
        return obj["Body"].read()
