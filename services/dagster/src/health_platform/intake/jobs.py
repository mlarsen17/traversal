from dagster import Field, OpExecutionContext, Out, graph, op

from health_platform.intake.filename_conventions import default_registry
from health_platform.intake.processing import GroupCandidate, process_group


@op(
    required_resource_keys={"metadata_db", "object_store"},
    config_schema={
        "submitter_id": Field(str),
        "inbox_prefix": Field(str),
        "grouping_method": Field(str),
        "object_keys": Field([str]),
    },
    out=Out(str),
)
def register_submission_op(context: OpExecutionContext) -> str:
    cfg = context.op_config
    candidate = GroupCandidate(
        submitter_id=cfg["submitter_id"],
        inbox_prefix=cfg["inbox_prefix"],
        grouping_method=cfg["grouping_method"],
        object_keys=cfg["object_keys"],
    )
    submission_id = process_group(
        context.resources.metadata_db,
        context.resources.object_store,
        default_registry(),
        candidate,
    )
    context.log.info("Processed candidate %s -> %s", candidate.inbox_prefix, submission_id)
    return submission_id


@graph
def register_submission_graph():
    register_submission_op()


register_submission_job = register_submission_graph.to_job(name="register_submission_job")
