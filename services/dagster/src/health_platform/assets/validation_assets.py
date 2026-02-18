from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

from dagster import MaterializeResult, asset
from sqlalchemy import text


@asset(group_name="validation", required_resource_keys={"metadata_db"})
def seed_validation_rule_sets(context) -> MaterializeResult:
    now = datetime.now(timezone.utc)
    default_set_name = "medical-default-v1"

    with context.resources.metadata_db.begin() as conn:
        existing = conn.execute(
            text("SELECT rule_set_id FROM validation_rule_set WHERE name=:name"),
            {"name": default_set_name},
        ).fetchone()
        if existing:
            return MaterializeResult(metadata={"rule_sets_seeded": 0, "rules_seeded": 0})

        rule_set_id = str(uuid.uuid4())
        conn.execute(
            text(
                """
                INSERT INTO validation_rule_set(
                    rule_set_id, name, file_type, layout_version, effective_start, effective_end, status, created_by, created_at
                ) VALUES (
                    :rule_set_id, :name, 'medical', 'v1', NULL, NULL, 'ACTIVE', 'seed', :created_at
                )
                """
            ),
            {"rule_set_id": rule_set_id, "name": default_set_name, "created_at": now},
        )

        rules = [
            {
                "name": "patient_not_null",
                "rule_kind": "NOT_NULL",
                "default_severity": "HARD",
                "default_threshold_type": "COUNT",
                "default_threshold_value": 0.0,
                "definition_json": {"column": "PATIENT"},
            },
            {
                "name": "code_in_range",
                "rule_kind": "RANGE",
                "default_severity": "SOFT",
                "default_threshold_type": "RATE",
                "default_threshold_value": 0.01,
                "definition_json": {"column": "CODE", "min": 1, "max": 99999},
            },
            {
                "name": "encounter_unique",
                "rule_kind": "UNIQUE_KEY",
                "default_severity": "HARD",
                "default_threshold_type": "COUNT",
                "default_threshold_value": 0.0,
                "definition_json": {"columns": ["Id", "START"]},
            },
        ]

        for rule in rules:
            rule_id = str(uuid.uuid4())
            conn.execute(
                text(
                    """
                    INSERT INTO validation_rule(
                        rule_id, file_type, name, description, rule_kind, default_severity,
                        default_threshold_type, default_threshold_value, definition_json, sql_template,
                        created_at, updated_at
                    ) VALUES (
                        :rule_id, 'medical', :name, NULL, :rule_kind, :default_severity,
                        :default_threshold_type, :default_threshold_value, :definition_json, NULL,
                        :created_at, :updated_at
                    )
                    """
                ),
                {
                    "rule_id": rule_id,
                    "name": rule["name"],
                    "rule_kind": rule["rule_kind"],
                    "default_severity": rule["default_severity"],
                    "default_threshold_type": rule["default_threshold_type"],
                    "default_threshold_value": rule["default_threshold_value"],
                    "definition_json": json.dumps(rule["definition_json"]),
                    "created_at": now,
                    "updated_at": now,
                },
            )
            conn.execute(
                text(
                    """
                    INSERT INTO validation_rule_set_rule(
                        rule_set_id, rule_id, enabled, severity_override,
                        threshold_type_override, threshold_value_override, params_override_json
                    ) VALUES (
                        :rule_set_id, :rule_id, 1, NULL, NULL, NULL, NULL
                    )
                    """
                ),
                {"rule_set_id": rule_set_id, "rule_id": rule_id},
            )

    return MaterializeResult(metadata={"rule_sets_seeded": 1, "rules_seeded": len(rules)})
