from dagster import Definitions

from health_platform.assets import (
    bootstrap_heartbeat_asset,
    seed_validation_rule_sets,
    sync_canonical_registry,
    sync_layout_registry,
)
from health_platform.gold_canonical.jobs import build_canonical_month_job
from health_platform.gold_canonical.sensors import canonical_rebuild_sensor
from health_platform.gold_submitter.jobs import build_submitter_gold_job
from health_platform.gold_submitter.sensors import ready_for_submitter_gold_sensor
from health_platform.intake.jobs import discover_inbox_objects_job, register_submission_job
from health_platform.intake.sensors import inbox_discovery_sensor, inbox_grouping_sensor
from health_platform.parse.jobs import parse_submission_job
from health_platform.parse.sensors import ready_for_parse_sensor
from health_platform.resources import metadata_db_resource, object_store_resource
from health_platform.validation.jobs import validate_submission_job
from health_platform.validation.sensors import ready_for_validate_sensor

defs = Definitions(
    assets=[
        bootstrap_heartbeat_asset,
        sync_layout_registry,
        sync_canonical_registry,
        seed_validation_rule_sets,
    ],
    jobs=[
        discover_inbox_objects_job,
        register_submission_job,
        parse_submission_job,
        validate_submission_job,
        build_submitter_gold_job,
        build_canonical_month_job,
    ],
    sensors=[
        inbox_discovery_sensor,
        inbox_grouping_sensor,
        ready_for_parse_sensor,
        ready_for_validate_sensor,
        ready_for_submitter_gold_sensor,
        canonical_rebuild_sensor,
    ],
    resources={
        "metadata_db": metadata_db_resource,
        "object_store": object_store_resource,
    },
)
