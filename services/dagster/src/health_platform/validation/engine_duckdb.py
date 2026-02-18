from __future__ import annotations

import json
import os
import re
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
from sqlalchemy import text

from health_platform.intake.object_store import ObjectStore

BLOCKED_SQL_KEYWORDS = {
    "DROP",
    "DELETE",
    "UPDATE",
    "INSERT",
    "CREATE",
    "ALTER",
    "COPY",
    "ATTACH",
    "DETACH",
    "EXPORT",
    "IMPORT",
    "PRAGMA",
}


@dataclass(frozen=True)
class ResolvedRule:
    rule_id: str
    name: str
    description: str | None
    rule_kind: str
    severity: str
    threshold_type: str
    threshold_value: float
    params: dict
    sql_template: str | None


@dataclass(frozen=True)
class RuleResult:
    rule_id: str
    name: str
    rule_kind: str
    severity: str
    threshold_type: str
    threshold_value: float
    violations_count: int
    denominator_count: int
    violations_rate: float
    passed: bool
    sample_object_key: str | None


@dataclass(frozen=True)
class ValidationReport:
    submission_id: str
    validation_run_id: str
    rule_set_id: str
    outcome: str
    submission_status: str
    total_rows: int
    rows_by_month: dict[str, int]
    report_object_key: str
    findings: list[RuleResult]


def _strip_sql_comments(sql: str) -> str:
    no_line = re.sub(r"--.*?$", "", sql, flags=re.MULTILINE)
    return re.sub(r"/\*.*?\*/", "", no_line, flags=re.DOTALL)


def _validate_custom_sql(sql: str) -> None:
    cleaned = _strip_sql_comments(sql).strip()
    if not cleaned.upper().startswith("SELECT"):
        raise ValueError("CUSTOM_SQL must start with SELECT")

    upper = cleaned.upper()
    for keyword in BLOCKED_SQL_KEYWORDS:
        if re.search(rf"\b{keyword}\b", upper):
            raise ValueError(f"CUSTOM_SQL contains blocked keyword: {keyword}")


def _configure_s3(con: duckdb.DuckDBPyConnection) -> bool:
    try:
        con.execute("LOAD httpfs")
    except Exception:
        try:
            con.execute("INSTALL httpfs")
            con.execute("LOAD httpfs")
        except Exception:
            return False

    con.execute(f"SET s3_region='{os.getenv('S3_REGION', 'us-east-1')}'")
    con.execute(f"SET s3_access_key_id='{os.getenv('S3_ACCESS_KEY_ID', '')}'")
    con.execute(f"SET s3_secret_access_key='{os.getenv('S3_SECRET_ACCESS_KEY', '')}'")
    endpoint = os.getenv("S3_ENDPOINT_URL")
    if endpoint:
        cleaned = endpoint.replace("http://", "").replace("https://", "")
        use_ssl = endpoint.startswith("https://")
        con.execute(f"SET s3_endpoint='{cleaned}'")
        con.execute(f"SET s3_use_ssl={'true' if use_ssl else 'false'}")
        con.execute("SET s3_url_style='path'")
    return True


def _month_to_date(month: str | None, end: bool = False) -> date | None:
    if not month:
        return None
    normalized = month.replace("-", "")
    base = datetime.strptime(normalized, "%Y%m")
    if not end:
        return base.date().replace(day=1)
    next_month = base.replace(day=28) + timedelta(days=4)
    return (next_month.replace(day=1) - timedelta(days=1)).date()


def resolve_rule_set(metadata_db, submission) -> str:
    coverage_start = _month_to_date(submission.coverage_start_month, end=False)
    coverage_end = _month_to_date(submission.coverage_end_month, end=True)

    with metadata_db.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT rule_set_id, layout_version, effective_start, effective_end
                FROM validation_rule_set
                WHERE status='ACTIVE' AND file_type=:file_type
                """
            ),
            {"file_type": submission.file_type},
        ).fetchall()

    def _window_match(row) -> bool:
        if row.effective_start is None and row.effective_end is None:
            return True
        sub_start = coverage_start or coverage_end
        sub_end = coverage_end or coverage_start
        if sub_start is None and sub_end is None:
            return False
        win_start = row.effective_start or date.min
        win_end = row.effective_end or date.max
        return sub_start <= win_end and sub_end >= win_start

    tiers: dict[int, list[str]] = {1: [], 2: [], 3: [], 4: []}
    for row in rows:
        layout_match = submission.layout_version and row.layout_version == submission.layout_version
        window_match = _window_match(row)
        if layout_match and window_match:
            tiers[1].append(row.rule_set_id)
        elif layout_match and row.effective_start is None and row.effective_end is None:
            tiers[2].append(row.rule_set_id)
        elif (
            row.layout_version is None
            and (row.effective_start is not None or row.effective_end is not None)
            and window_match
        ):
            tiers[3].append(row.rule_set_id)
        elif (
            row.layout_version is None and row.effective_start is None and row.effective_end is None
        ):
            tiers[4].append(row.rule_set_id)

    for tier in (1, 2, 3, 4):
        ids = sorted(set(tiers[tier]))
        if len(ids) == 1:
            return ids[0]
        if len(ids) > 1:
            raise RuntimeError(f"Multiple ACTIVE validation rule sets in tier {tier}: {ids}")

    raise RuntimeError(
        f"No ACTIVE validation rule set found for file_type={submission.file_type} layout_version={submission.layout_version}"
    )


def load_rules_for_set(metadata_db, rule_set_id: str) -> list[ResolvedRule]:
    with metadata_db.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                    r.rule_id,
                    r.name,
                    r.description,
                    r.rule_kind,
                    COALESCE(rsr.severity_override, r.default_severity) AS severity,
                    COALESCE(rsr.threshold_type_override, r.default_threshold_type) AS threshold_type,
                    COALESCE(rsr.threshold_value_override, r.default_threshold_value) AS threshold_value,
                    r.definition_json,
                    rsr.params_override_json,
                    r.sql_template
                FROM validation_rule_set_rule rsr
                JOIN validation_rule r ON r.rule_id = rsr.rule_id
                WHERE rsr.rule_set_id = :rule_set_id AND rsr.enabled = 1
                ORDER BY r.name, r.rule_id
                """
            ),
            {"rule_set_id": rule_set_id},
        ).fetchall()

    resolved: list[ResolvedRule] = []
    for row in rows:
        params = json.loads(row.definition_json)
        if row.params_override_json:
            params.update(json.loads(row.params_override_json))
        resolved.append(
            ResolvedRule(
                rule_id=row.rule_id,
                name=row.name,
                description=row.description,
                rule_kind=row.rule_kind,
                severity=row.severity,
                threshold_type=row.threshold_type,
                threshold_value=float(row.threshold_value),
                params=params,
                sql_template=row.sql_template,
            )
        )
    return resolved


def compile_rule_to_queries(rule: ResolvedRule) -> tuple[str, str]:
    kind = rule.rule_kind
    if kind == "NOT_NULL":
        col = rule.params["column"]
        where = f'"{col}" IS NULL'
        return (
            f"SELECT count(*) AS violations FROM silver WHERE {where}",
            f"SELECT * FROM silver WHERE {where} LIMIT 100",
        )
    if kind == "RANGE":
        col = rule.params["column"]
        min_v = float(rule.params["min"])
        max_v = float(rule.params["max"])
        numeric_col = f'TRY_CAST("{col}" AS DOUBLE)'
        where = (
            f"{numeric_col} IS NOT NULL AND ({numeric_col} < {min_v} OR {numeric_col} > {max_v})"
        )
        return (
            f"SELECT count(*) AS violations FROM silver WHERE {where}",
            f"SELECT * FROM silver WHERE {where} LIMIT 100",
        )
    if kind == "ALLOWED_VALUES":
        col = rule.params["column"]
        quoted = ", ".join(
            [f"'{str(v).replace(chr(39), chr(39) * 2)}'" for v in rule.params["values"]]
        )
        where = f'"{col}" IS NOT NULL AND "{col}" NOT IN ({quoted})'
        return (
            f"SELECT count(*) AS violations FROM silver WHERE {where}",
            f"SELECT * FROM silver WHERE {where} LIMIT 100",
        )
    if kind == "REGEX":
        col = rule.params["column"]
        pattern = str(rule.params["pattern"]).replace("'", "''")
        where = f'"{col}" IS NOT NULL AND regexp_matches(CAST("{col}" AS VARCHAR), \'{pattern}\') = FALSE'
        return (
            f"SELECT count(*) AS violations FROM silver WHERE {where}",
            f"SELECT * FROM silver WHERE {where} LIMIT 100",
        )
    if kind == "UNIQUE_KEY":
        cols = rule.params["columns"]
        key_group = ", ".join([f'"{c}"' for c in cols])
        join_terms = " AND ".join([f's."{c}" = d."{c}"' for c in cols])
        count_sql = (
            "WITH dup AS ("
            f"SELECT {key_group}, count(*) AS c FROM silver GROUP BY {key_group} HAVING count(*) > 1"
            ") SELECT COALESCE(sum(c), 0) FROM dup"
        )
        sample_sql = (
            "WITH dup AS ("
            f"SELECT {key_group} FROM silver GROUP BY {key_group} HAVING count(*) > 1"
            ") SELECT s.* FROM silver s JOIN dup d ON "
            f"{join_terms} LIMIT 100"
        )
        return count_sql, sample_sql
    if kind == "CUSTOM_SQL":
        user_sql = rule.sql_template or rule.params.get("sql")
        if not user_sql:
            raise ValueError(f"Rule {rule.rule_id} missing SQL body")
        _validate_custom_sql(user_sql)
        return f"SELECT count(*) FROM ({user_sql}) t", f"SELECT * FROM ({user_sql}) t LIMIT 100"
    raise ValueError(f"Unsupported rule kind: {kind}")


def _evaluate_rule(
    rule: ResolvedRule, violations_count: int, total_rows: int
) -> tuple[float, bool]:
    violations_rate = (violations_count / total_rows) if total_rows > 0 else 0.0
    if rule.threshold_type == "COUNT":
        passed = violations_count <= rule.threshold_value
    elif rule.threshold_type == "RATE":
        passed = violations_rate <= rule.threshold_value
    else:
        raise ValueError(f"Unsupported threshold type: {rule.threshold_type}")
    return violations_rate, passed


def write_sample_artifact(
    con: duckdb.DuckDBPyConnection,
    object_store: ObjectStore,
    sample_sql: str,
    submission_id: str,
    validation_run_id: str,
    rule_id: str,
) -> str:
    with TemporaryDirectory() as tempdir:
        out_file = Path(tempdir) / "sample.parquet"
        con.execute(f"COPY ({sample_sql}) TO '{out_file.as_posix()}' (FORMAT PARQUET)")
        key = f"validation/{submission_id}/{validation_run_id}/samples/{rule_id}/sample.parquet"
        object_store.put_bytes(key, out_file.read_bytes(), "application/octet-stream")
    return key


def _map_outcome_to_submission_status(outcome: str) -> str:
    if outcome == "FAIL_HARD":
        return "VALIDATION_FAILED"
    if outcome == "PASS_WITH_WARNINGS":
        return "VALIDATED_WITH_WARNINGS"
    return "VALIDATED"


def run_validation_engine(
    *,
    metadata_db,
    object_store: ObjectStore,
    submission,
    validation_run_id: str,
    rule_set_id: str,
    started_at: datetime,
    logger,
) -> ValidationReport:
    rules = load_rules_for_set(metadata_db, rule_set_id)
    if not rules:
        raise RuntimeError(f"Validation rule set {rule_set_id} has no enabled rules")

    silver_prefix = (
        f"silver/{submission.submitter_id}/{submission.file_type}/{submission.submission_id}"
    )
    s3_glob = f"s3://{os.getenv('S3_BUCKET', 'health-raw')}/{silver_prefix}/**/*.parquet"

    con = duckdb.connect()
    staged_dir: TemporaryDirectory[str] | None = None
    s3_enabled = _configure_s3(con)
    if s3_enabled:
        con.execute(f"CREATE OR REPLACE VIEW silver AS SELECT * FROM read_parquet('{s3_glob}')")
    else:
        staged_dir = TemporaryDirectory()
        local_root = Path(staged_dir.name) / "silver"
        parquet_count = 0
        for obj in object_store.list_objects(f"{silver_prefix}/"):
            if not obj.key.endswith(".parquet"):
                continue
            rel = obj.key[len(f"{silver_prefix}/") :]
            out = local_root / rel
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_bytes(object_store.get_bytes(obj.key))
            parquet_count += 1

        if parquet_count == 0:
            raise RuntimeError(f"No silver parquet files found under {silver_prefix}")

        con.execute(
            f"CREATE OR REPLACE VIEW silver AS SELECT * FROM read_parquet('{local_root.as_posix()}/**/*.parquet')"
        )
        logger.info("DuckDB httpfs unavailable; validation used locally staged parquet inputs")

    total_rows = int(con.execute("SELECT count(*) FROM silver").fetchone()[0] or 0)
    rows_by_month = {
        str(month): int(count)
        for month, count in con.execute(
            "SELECT atomic_month, count(*) FROM silver GROUP BY 1 ORDER BY 1"
        ).fetchall()
    }

    finding_rows = []
    rule_results: list[RuleResult] = []
    any_hard_fail = False
    any_soft_fail = False

    for rule in rules:
        count_sql, sample_sql = compile_rule_to_queries(rule)
        violations_count = int(con.execute(count_sql).fetchone()[0] or 0)
        violations_rate, passed = _evaluate_rule(rule, violations_count, total_rows)

        sample_key = None
        if violations_count > 0:
            sample_key = write_sample_artifact(
                con,
                object_store,
                sample_sql,
                submission.submission_id,
                validation_run_id,
                rule.rule_id,
            )
            logger.info("Wrote validation sample for rule %s to %s", rule.rule_id, sample_key)

        if not passed and rule.severity == "HARD":
            any_hard_fail = True
        if not passed and rule.severity == "SOFT":
            any_soft_fail = True

        logger.info(
            "Validation rule %s (%s) violations=%s rate=%.6f threshold=%s %.6f passed=%s",
            rule.rule_id,
            rule.rule_kind,
            violations_count,
            violations_rate,
            rule.threshold_type,
            rule.threshold_value,
            passed,
        )

        finding_rows.append(
            {
                "validation_run_id": validation_run_id,
                "rule_id": rule.rule_id,
                "scope_month": None,
                "violations_count": violations_count,
                "denominator_count": total_rows,
                "violations_rate": violations_rate,
                "sample_object_key": sample_key,
                "passed": passed,
                "computed_at": datetime.utcnow(),
            }
        )
        rule_results.append(
            RuleResult(
                rule_id=rule.rule_id,
                name=rule.name,
                rule_kind=rule.rule_kind,
                severity=rule.severity,
                threshold_type=rule.threshold_type,
                threshold_value=rule.threshold_value,
                violations_count=violations_count,
                denominator_count=total_rows,
                violations_rate=violations_rate,
                passed=passed,
                sample_object_key=sample_key,
            )
        )

    if any_hard_fail:
        outcome = "FAIL_HARD"
    elif any_soft_fail:
        outcome = "PASS_WITH_WARNINGS"
    else:
        outcome = "PASS"

    submission_status = _map_outcome_to_submission_status(outcome)
    report_key = f"validation/{submission.submission_id}/{validation_run_id}/validation_report.json"
    ended_at = datetime.utcnow()

    report_body = {
        "submission": {
            "submission_id": submission.submission_id,
            "submitter_id": submission.submitter_id,
            "file_type": submission.file_type,
            "layout_id": submission.layout_id,
            "layout_version": submission.layout_version,
            "coverage_start_month": submission.coverage_start_month,
            "coverage_end_month": submission.coverage_end_month,
        },
        "validation_run_id": validation_run_id,
        "rule_set_id": rule_set_id,
        "rules": [
            {
                "rule_id": rule.rule_id,
                "name": rule.name,
                "rule_kind": rule.rule_kind,
                "severity": rule.severity,
                "threshold_type": rule.threshold_type,
                "threshold_value": rule.threshold_value,
                "params": rule.params,
            }
            for rule in rules
        ],
        "engine": {
            "name": "duckdb",
            "version": duckdb.__version__,
        },
        "totals": {
            "total_rows": total_rows,
            "rows_by_month": rows_by_month,
        },
        "results": [asdict(result) for result in rule_results],
        "overall_outcome": outcome,
        "derived_submission_status": submission_status,
        "started_at": started_at.isoformat(),
        "ended_at": ended_at.isoformat(),
    }
    object_store.put_bytes(
        report_key, json.dumps(report_body, indent=2).encode("utf-8"), "application/json"
    )

    with metadata_db.begin() as conn:
        for row in finding_rows:
            conn.execute(
                text(
                    """
                    INSERT INTO validation_finding(
                        validation_run_id, rule_id, scope_month, violations_count, denominator_count,
                        violations_rate, sample_object_key, passed, computed_at
                    ) VALUES (
                        :validation_run_id, :rule_id, :scope_month, :violations_count, :denominator_count,
                        :violations_rate, :sample_object_key, :passed, :computed_at
                    )
                    """
                ),
                row,
            )

    report = ValidationReport(
        submission_id=submission.submission_id,
        validation_run_id=validation_run_id,
        rule_set_id=rule_set_id,
        outcome=outcome,
        submission_status=submission_status,
        total_rows=total_rows,
        rows_by_month=rows_by_month,
        report_object_key=report_key,
        findings=rule_results,
    )

    con.close()
    if staged_dir is not None:
        staged_dir.cleanup()
    return report
