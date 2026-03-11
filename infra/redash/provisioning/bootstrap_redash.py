#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent
QUERY_DIR = BASE_DIR / "queries"

MANIFEST = {
    "dashboards": [
        {"name": "Operations Overview", "slug": "operations-overview"},
        {"name": "Submitter Health", "slug": "submitter-health"},
        {"name": "Month Control Plane", "slug": "month-control-plane"},
    ],
    "queries": [
        {
            "name": "Ops: Submission Status Distribution",
            "file": "01_ops_submission_status.sql",
            "dashboard": "operations-overview",
            "schedule": {"interval": 3600},
        },
        {
            "name": "Ops: Daily KPIs (30d)",
            "file": "02_ops_daily_kpis_30d.sql",
            "dashboard": "operations-overview",
            "schedule": {"interval": 3600},
        },
        {
            "name": "Ops: Canonical Pending or Failed",
            "file": "03_ops_canonical_pending.sql",
            "dashboard": "operations-overview",
            "schedule": {"interval": 3600},
        },
        {
            "name": "Submitter: Fail/Warn Rates",
            "file": "04_submitter_fail_warn_rates.sql",
            "dashboard": "submitter-health",
            "schedule": {"interval": 3600},
        },
        {
            "name": "Submitter: Top Failing Rules",
            "file": "05_submitter_top_rules.sql",
            "dashboard": "submitter-health",
            "schedule": {"interval": 3600},
        },
        {
            "name": "Month: Control Plane Grid",
            "file": "06_month_control_plane.sql",
            "dashboard": "month-control-plane",
            "schedule": {"interval": 3600},
        },
    ],
    "alerts": [
        {
            "name": "Canonical failures in 30d > 0",
            "query_name": "Ops: Daily KPIs (30d)",
            "options": {"op": ">", "value": 0, "column": "canonical_failed"},
            "rearm": 0,
        },
        {
            "name": "Validation failures in 30d > 0",
            "query_name": "Ops: Daily KPIs (30d)",
            "options": {"op": ">", "value": 0, "column": "validation_runs_failed"},
            "rearm": 0,
        },
    ],
}


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "dashboard"


def _request(method: str, url: str, api_key: str, payload: dict[str, Any] | None = None) -> Any:
    body = None
    headers = {"Authorization": f"Key {api_key}"}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            raw = resp.read().decode("utf-8")
            if not raw:
                return {}
            return json.loads(raw)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} {method} {url}: {detail}") from exc


def api(method: str, base_url: str, api_key: str, path: str, payload: dict[str, Any] | None = None) -> Any:
    url = f"{base_url.rstrip('/')}{path}"
    return _request(method, url, api_key, payload)


def list_dashboards(base_url: str, api_key: str) -> list[dict[str, Any]]:
    page = 1
    out: list[dict[str, Any]] = []
    while True:
        response = api("GET", base_url, api_key, f"/api/dashboards?page={page}&page_size=250")
        results = response.get("results", [])
        out.extend(results)
        if not results or len(results) < 250:
            break
        page += 1
    return out


def ensure_dashboard(base_url: str, api_key: str, name: str, slug: str) -> dict[str, Any]:
    inferred_slug = slug or _slugify(name)
    dashboards = list_dashboards(base_url, api_key)
    for dashboard in dashboards:
        if dashboard.get("slug") == inferred_slug or dashboard.get("name") == name:
            print(f"dashboard exists: {name} ({dashboard.get('id')})")
            return dashboard

    created = api("POST", base_url, api_key, "/api/dashboards", {"name": name})
    print(f"dashboard created: {name}")
    return created


def list_queries(base_url: str, api_key: str) -> dict[str, dict[str, Any]]:
    page = 1
    out: dict[str, dict[str, Any]] = {}
    while True:
        response = api("GET", base_url, api_key, f"/api/queries?page={page}&page_size=250")
        results = response.get("results", [])
        for row in results:
            out[row["name"]] = row
        if not results or len(results) < 250:
            break
        page += 1
    return out


def ensure_query(
    base_url: str,
    api_key: str,
    data_source_id: int,
    existing_queries: dict[str, dict[str, Any]],
    name: str,
    sql_text: str,
    schedule: dict[str, Any],
) -> dict[str, Any]:
    payload = {
        "name": name,
        "query": sql_text,
        "data_source_id": data_source_id,
        "description": "Auto-provisioned by infra/redash/provisioning/bootstrap_redash.py",
        "schedule": schedule,
        "options": {"parameters": []},
        "is_draft": False,
    }

    existing = existing_queries.get(name)
    query_id = existing["id"] if existing else find_query_id_by_name(name)
    if query_id:
        updated = api("POST", base_url, api_key, f"/api/queries/{query_id}", payload)
        print(f"query updated: {name} (id={query_id})")
        return updated

    created = api("POST", base_url, api_key, "/api/queries", payload)
    # Some Redash builds still create new queries as drafts despite payload.
    api("POST", base_url, api_key, f"/api/queries/{created['id']}", {"is_draft": False})
    print(f"query created: {name} (id={created.get('id')})")
    return created


def find_query_id_by_name(name: str) -> int | None:
    try:
        from redash import create_app, models  # type: ignore
    except Exception:
        return None

    app = create_app()
    with app.app_context():
        query = models.Query.query.filter_by(name=name).order_by(models.Query.id.desc()).first()
        if not query:
            return None
        return int(query.id)


def attach_query_to_dashboard(
    base_url: str,
    api_key: str,
    dashboard_id: int,
    query_id: int,
) -> None:
    query = api("GET", base_url, api_key, f"/api/queries/{query_id}")
    visualizations = query.get("visualizations") or []
    table_vis = next((v for v in visualizations if v.get("type") == "TABLE"), None)
    vis = table_vis or (visualizations[0] if visualizations else None)
    if not vis:
        print(f"warning: query {query_id} has no visualization; skipped widget")
        return

    widgets = (
        api("GET", base_url, api_key, f"/api/dashboards/{dashboard_id}")
        .get("dashboard", {})
        .get("widgets", [])
    )
    for widget in widgets:
        visualization = widget.get("visualization") or {}
        if visualization.get("id") == vis.get("id"):
            print(f"widget exists for query {query_id} on dashboard {dashboard_id}")
            return

    payload = {
        "dashboard_id": dashboard_id,
        "visualization_id": vis["id"],
        "width": 1,
        "options": {},
        "text": "",
    }
    api("POST", base_url, api_key, "/api/widgets", payload)
    print(f"widget created for query {query_id} on dashboard {dashboard_id}")


def ensure_alert(
    base_url: str,
    api_key: str,
    query_id: int,
    name: str,
    options: dict[str, Any],
    rearm: int,
) -> None:
    existing_alerts_raw = api("GET", base_url, api_key, "/api/alerts?page=1&page_size=250")
    if isinstance(existing_alerts_raw, dict):
        existing_alerts = existing_alerts_raw.get("results", [])
    else:
        existing_alerts = existing_alerts_raw
    for alert in existing_alerts:
        if alert.get("name") == name:
            alert_id = alert["id"]
            payload = {
                "name": name,
                "query_id": query_id,
                "options": options,
                "rearm": rearm,
            }
            api("POST", base_url, api_key, f"/api/alerts/{alert_id}", payload)
            print(f"alert updated: {name}")
            return

    payload = {
        "name": name,
        "query_id": query_id,
        "options": options,
        "rearm": rearm,
    }
    api("POST", base_url, api_key, "/api/alerts", payload)
    print(f"alert created: {name}")


def resolve_data_source_id(base_url: str, api_key: str) -> int:
    ds_id = os.getenv("REDASH_DATA_SOURCE_ID")
    if ds_id:
        return int(ds_id)

    ds_name = os.getenv("REDASH_DATA_SOURCE_NAME", "Platform Metadata Postgres")
    data_sources = api("GET", base_url, api_key, "/api/data_sources")
    for ds in data_sources:
        if ds.get("name") == ds_name:
            return int(ds["id"])

    raise RuntimeError(
        f"Could not find datasource named '{ds_name}'. Set REDASH_DATA_SOURCE_ID or create datasource first."
    )


def main() -> int:
    base_url = os.getenv("REDASH_URL", "http://localhost:5000")
    api_key = os.getenv("REDASH_API_KEY")
    if not api_key:
        print("REDASH_API_KEY is required", file=sys.stderr)
        return 2

    data_source_id = resolve_data_source_id(base_url, api_key)
    print(f"using data_source_id={data_source_id}")

    dashboards: dict[str, dict[str, Any]] = {}
    for d in MANIFEST["dashboards"]:
        obj = ensure_dashboard(base_url, api_key, d["name"], d["slug"])
        dashboards[d["slug"]] = {"id": int(obj["id"])}

    existing_queries = list_queries(base_url, api_key)
    query_ids: dict[str, int] = {}

    for q in MANIFEST["queries"]:
        sql_path = QUERY_DIR / q["file"]
        sql_text = sql_path.read_text(encoding="utf-8")
        result = ensure_query(
            base_url=base_url,
            api_key=api_key,
            data_source_id=data_source_id,
            existing_queries=existing_queries,
            name=q["name"],
            sql_text=sql_text,
            schedule=q["schedule"],
        )
        qid = int(result["id"])
        query_ids[q["name"]] = qid

        dash_id = dashboards[q["dashboard"]]["id"]
        attach_query_to_dashboard(base_url, api_key, dash_id, qid)

    for a in MANIFEST["alerts"]:
        query_id = query_ids[a["query_name"]]
        ensure_alert(
            base_url=base_url,
            api_key=api_key,
            query_id=query_id,
            name=a["name"],
            options=a["options"],
            rearm=a["rearm"],
        )

    print("redash bootstrap complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
