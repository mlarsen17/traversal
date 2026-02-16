from dagster import Definitions

from health_platform.assets.bootstrap_assets import bootstrap_heartbeat_asset
from health_platform.assets.intake_assets import register_submission_from_group_job, sync_layout_registry
from health_platform.resources import metadata_db_resource, minio_resource
from health_platform.sensors.inbox_sensors import inbox_discovery_sensor


defs = Definitions(
    assets=[bootstrap_heartbeat_asset, sync_layout_registry],
    jobs=[register_submission_from_group_job],
    sensors=[inbox_discovery_sensor],
    resources={
        "metadata_db": metadata_db_resource,
        "minio": minio_resource,
    },
)
