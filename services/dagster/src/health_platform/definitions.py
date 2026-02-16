from dagster import Definitions

from health_platform.assets import bootstrap_heartbeat_asset, sync_layout_registry
from health_platform.intake.jobs import register_submission_job
from health_platform.intake.sensors import inbox_discovery_sensor, inbox_grouping_sensor
from health_platform.resources import metadata_db_resource, minio_resource, object_store_resource


defs = Definitions(
    assets=[bootstrap_heartbeat_asset, sync_layout_registry],
    jobs=[register_submission_job],
    sensors=[inbox_discovery_sensor, inbox_grouping_sensor],
    resources={
        "metadata_db": metadata_db_resource,
        "minio": minio_resource,
        "object_store": object_store_resource,
    },
)
