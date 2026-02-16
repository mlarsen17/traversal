import io
import os
from datetime import datetime, timezone

from dagster import MaterializeResult, asset

from health_platform.utils.db import insert_bootstrap_heartbeat


@asset(group_name="bootstrap", required_resource_keys={"metadata_db", "object_store"})
def bootstrap_heartbeat_asset(context) -> MaterializeResult:
    message = "phase0_ok"
    created_at = datetime.now(timezone.utc)

    insert_bootstrap_heartbeat(context.resources.metadata_db, message, created_at)
    context.log.info("Inserted bootstrap heartbeat row into metadata database")

    raw_bucket_name = os.getenv("S3_BUCKET", "health-raw")
    payload = io.BytesIO(b"hello from phase 0\n")
    context.resources.object_store.put_bytes(
        "bootstrap/hello.txt", payload.read(), content_type="text/plain"
    )
    context.log.info("Uploaded bootstrap/hello.txt to object store bucket '%s'", raw_bucket_name)

    return MaterializeResult(
        metadata={
            "message": message,
            "created_at": created_at.isoformat(),
            "bucket": raw_bucket_name,
            "key": "bootstrap/hello.txt",
        }
    )
