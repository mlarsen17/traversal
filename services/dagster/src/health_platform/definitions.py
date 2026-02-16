from dagster import Definitions

from health_platform.assets.bootstrap_assets import bootstrap_heartbeat_asset
from health_platform.resources import metadata_db_resource, minio_resource


defs = Definitions(
    assets=[bootstrap_heartbeat_asset],
    resources={
        "metadata_db": metadata_db_resource,
        "minio": minio_resource,
    },
)
