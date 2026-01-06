"""
ServiceNow-driven VM backup validation report (RSC GraphQL).

What this job does:
- Reads ServiceNow incidents from `tickets.json` (produced by `fetch_servicenow.py`).
- For each VM node, queries Rubrik Security Cloud (RSC) GraphQL to fetch:
  - SLA domain
  - last backup time
  - whether a backup exists within the last N hours
  - backup running signal (from protectionStatus)
- Writes JSON artifacts:
  - `L2Backup/combined_backup_report.json` (consumed by `update_servicenow.py`)
  - `updated_tickets.json` (tickets augmented with per-node backup results)

Behavior changes requested:
- Remove fileset check (not applicable in this pipeline).
- Include on-demand backups in the 24h freshness evaluation.
- Ignore servers where a backup is running (do not update ServiceNow for them).
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List

COMBINED_REPORT_PATH = os.getenv(
    "COMBINED_REPORT_JSON",
    os.path.join("L2Backup", "combined_backup_report.json"),
)
SERVICENOW_TICKETS_JSON = os.getenv("SERVICENOW_TICKETS_JSON", "tickets.json")
UPDATED_TICKETS_JSON = os.getenv("UPDATED_TICKETS_JSON", "updated_tickets.json")

STALE_HOURS = int(os.getenv("STATUS_WINDOW_HOURS", os.getenv("BACKUP_STALE_HOURS", "24")))

try:
    # When imported as a package module (preferred)
    from . import check_last_backup
except ImportError:  # pragma: no cover
    # When executed as a standalone script
    import check_last_backup

HEADER = (
    f"{'Server':25} | "
    f"{'In Rubrik':9} | "
    f"{f'Backup <{STALE_HOURS}h':12} | "
    f"{'Backup Running':13} | "
    f"{'Last Backup':20} | "
    f"SLA Domain"
)


def _format_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


def _load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: str, payload: Any) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def _result_row_defaults(server: str) -> Dict[str, Any]:
    return {
        "server": server,
        "in_rubrik": "NO",
        "status": "NO",  # YES if backup within window hours
        "last_backup": "N/A",
        "sla_domain": "N/A",
        "backup_running": False,
        "successful_backup_count": 0,
        "checked_at": _format_dt(datetime.now(timezone.utc)),
    }


def _print_section(title: str, rows: List[Dict]) -> None:
    print(f"{title}:")
    print(HEADER)
    for row in rows:
        print(
            f"{row['server']:25} | "
            f"{row['in_rubrik']:9} | "
            f"{row.get('status','NO'):12} | "
            f"{str(bool(row.get('backup_running'))):13} | "
            f"{row.get('last_backup','N/A'):20} | "
            f"{row.get('sla_domain','N/A')}"
        )
    print()


def main() -> None:
    if not os.path.exists(SERVICENOW_TICKETS_JSON):
        print(f"[ERROR] {SERVICENOW_TICKETS_JSON} not found")
        return

    tickets = _load_json(SERVICENOW_TICKETS_JSON)
    if not isinstance(tickets, list):
        print("[ERROR] tickets.json is not a JSON list")
        return

    servers = sorted(
        {
            (node or "").strip().lower()
            for t in tickets
            for node in (t.get("nodes") or [])
            if node and str(node).strip()
        }
    )
    if not servers:
        print("[WARN] No servers found in tickets.json. Nothing to do.")
        _write_json(
            COMBINED_REPORT_PATH,
            {
                "generated_at": _format_dt(datetime.now(timezone.utc)),
                "stale_hours": STALE_HOURS,
                "results": [],
            },
        )
        _write_json(UPDATED_TICKETS_JSON, tickets)
        return

    # RSC GraphQL evaluation (includes on-demand snapshots in the 24h window)
    # NOTE: "ignore if backup is running" means we will omit those servers from
    # the combined report results so ServiceNow is not updated for them.
    try:
        raw_rows = check_last_backup.run(
            servers=servers,
            persist=False,
            show_summary=False,
            show_progress=True,
        )
    except SystemExit:
        # Keep downstream artifacts stable even if Rubrik auth/config is missing.
        raw_rows = [_result_row_defaults(s) for s in servers]

    rows: List[Dict[str, Any]] = []
    per_server: Dict[str, Dict[str, Any]] = {}
    for rec in raw_rows:
        srv = rec.get("server")
        if not srv:
            continue
        # Ignore running backups (do not update ServiceNow for these)
        if rec.get("backup_running") is True:
            per_server[srv] = rec
            continue
        rows.append(rec)
        per_server[srv] = rec

    report = {
        "generated_at": _format_dt(datetime.now(timezone.utc)),
        "stale_hours": STALE_HOURS,
        "results": rows,
    }
    _write_json(COMBINED_REPORT_PATH, report)

    # Augment tickets with per-node backup info for auditing/consumption.
    updated_tickets: List[Dict[str, Any]] = []
    for t in tickets:
        t2 = dict(t)
        nodes = [n.strip().lower() for n in (t.get("nodes") or []) if n and str(n).strip()]
        t2["backup_results"] = [per_server.get(n, _result_row_defaults(n)) for n in nodes]
        updated_tickets.append(t2)
    _write_json(UPDATED_TICKETS_JSON, updated_tickets)

    print("Results:")
    _print_section("VM backup information", rows)


if __name__ == "__main__":
    main()
