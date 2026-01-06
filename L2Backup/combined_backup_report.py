"""
ServiceNow-driven VM backup validation + remediation.

What this job does (per user requirement):
- Reads ServiceNow tickets from tickets.json (nodes already derived from u_node
  or extracted from description by fetch_servicenow.py).
- For each VM node:
  - Checks latest snapshot time.
  - If no snapshot in last 24h, triggers an on-demand backup UNLESS a backup
    is already running.
- Writes JSON artifacts:
  - L2Backup/combined_backup_report.json (consumed by update_servicenow.py)
  - updated_tickets.json (tickets augmented with backup status)
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

COMBINED_REPORT_PATH = os.getenv(
    "COMBINED_REPORT_JSON",
    os.path.join("L2Backup", "combined_backup_report.json"),
)
SERVICENOW_TICKETS_JSON = os.getenv("SERVICENOW_TICKETS_JSON", "tickets.json")
UPDATED_TICKETS_JSON = os.getenv("UPDATED_TICKETS_JSON", "updated_tickets.json")

STALE_HOURS = int(os.getenv("BACKUP_STALE_HOURS", "24"))

# Rubrik connectivity (do not hardcode secrets)
RUBRIK_BASE_URL = (
    os.getenv("RUBRIK_BASE_URL")
    or os.getenv("RUBRIK_URL")
    or os.getenv("RUBRIK_CLUSTER_URL")
    or ""
).rstrip("/")
RUBRIK_TOKEN = os.getenv("RUBRIK_TOKEN") or os.getenv("RUBRIK_API_TOKEN") or ""
RUBRIK_TIMEOUT_SECS = int(os.getenv("RUBRIK_TIMEOUT_SECS", "60"))
RUBRIK_VERIFY_SSL = os.getenv("RUBRIK_VERIFY_SSL", "true").strip().lower() not in (
    "0",
    "false",
    "no",
)

HEADER = (
    f"{'Server':25} | "
    f"{'In Rubrik':9} | "
    f"{'Backup <24h':10} | "
    f"{'Backup Running':13} | "
    f"{'Triggered':9} | "
    f"{'Last Backup':20} | "
    f"SLA Domain"
)


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    v = str(value).strip()
    if not v or v.upper() == "N/A":
        return None

    # Common formats we might see from Rubrik APIs
    try:
        # ISO8601
        dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc)
    except ValueError:
        pass

    for fmt in ("%Y-%m-%d %H:%M:%S %Z", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            dt = datetime.strptime(v, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            continue

    return None


def _format_dt(dt: Optional[datetime]) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC") if dt else "N/A"


def _load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: str, payload: Any) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


@dataclass(frozen=True)
class RubrikVM:
    id: str
    name: str
    effective_sla_domain: str = "N/A"


class RubrikClient:
    def __init__(self, base_url: str, token: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )
        if token:
            self.session.headers.update({"Authorization": f"Bearer {token}"})

    def _url(self, path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        return f"{self.base_url}{path}"

    def get_vm_by_name(self, name: str) -> Optional[RubrikVM]:
        """
        Attempts Rubrik CDM REST lookup: GET /api/v1/vmware/vm?name=<name>
        """
        url = self._url("/api/v1/vmware/vm")
        try:
            r = self.session.get(
                url,
                params={"name": name},
                timeout=RUBRIK_TIMEOUT_SECS,
                verify=RUBRIK_VERIFY_SSL,
            )
        except Exception:
            return None

        if r.status_code != 200:
            return None

        try:
            payload = r.json()
        except Exception:
            return None

        # payload might be a list or a dict with "data"
        candidates: List[Dict[str, Any]] = []
        if isinstance(payload, list):
            candidates = payload
        elif isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, list):
                candidates = data

        if not candidates:
            return None

        name_l = name.lower()
        chosen = None
        for item in candidates:
            if str(item.get("name", "")).lower() == name_l:
                chosen = item
                break
        if chosen is None:
            chosen = candidates[0]

        vm_id = chosen.get("id") or chosen.get("vmId") or chosen.get("fid")
        vm_name = chosen.get("name") or name
        sla = (
            chosen.get("effectiveSlaDomainName")
            or chosen.get("effectiveSlaDomain", {}).get("name")
            or chosen.get("configuredSlaDomainName")
            or "N/A"
        )

        if not vm_id:
            return None

        return RubrikVM(id=str(vm_id), name=str(vm_name), effective_sla_domain=str(sla))

    def get_latest_snapshot(self, vm_id: str) -> Tuple[Optional[datetime], Optional[str]]:
        """
        Attempts Rubrik CDM REST snapshot list:
        GET /api/v1/vmware/vm/<id>/snapshot
        """
        url = self._url(f"/api/v1/vmware/vm/{vm_id}/snapshot")
        r = self.session.get(
            url,
            timeout=RUBRIK_TIMEOUT_SECS,
            verify=RUBRIK_VERIFY_SSL,
        )
        if r.status_code != 200:
            return None, None

        try:
            payload = r.json()
        except Exception:
            return None, None

        snaps: List[Dict[str, Any]] = []
        if isinstance(payload, list):
            snaps = payload
        elif isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, list):
                snaps = data
            elif isinstance(payload.get("snapshots"), list):
                snaps = payload["snapshots"]

        if not snaps:
            return None, None

        # Find max by date-like field
        best_dt: Optional[datetime] = None
        best_status: Optional[str] = None
        for s in snaps:
            dt = _parse_dt(s.get("date") or s.get("snapshotDate") or s.get("creationTime"))
            if not dt:
                continue
            if best_dt is None or dt > best_dt:
                best_dt = dt
                best_status = str(s.get("status") or s.get("state") or "").upper() or None

        return best_dt, best_status

    def trigger_on_demand_backup(self, vm_id: str) -> Tuple[bool, bool, Optional[str]]:
        """
        Triggers a VM snapshot (backup) via CDM REST:
        POST /api/v1/vmware/vm/<id>/snapshot

        Returns: (triggered, already_running, error_message)
        """
        url = self._url(f"/api/v1/vmware/vm/{vm_id}/snapshot")
        try:
            r = self.session.post(
                url,
                json={},
                timeout=RUBRIK_TIMEOUT_SECS,
                verify=RUBRIK_VERIFY_SSL,
            )
        except Exception as e:
            return False, False, str(e)

        if r.status_code in (200, 201, 202, 204):
            return True, False, None

        # Heuristic: many systems return 409 or message indicating in-progress
        body = ""
        try:
            body = r.text or ""
        except Exception:
            body = ""

        if r.status_code == 409 or ("running" in body.lower()) or ("in progress" in body.lower()):
            return False, True, None

        return False, False, f"HTTP {r.status_code}: {body[:500]}"


def _result_row_defaults(server: str) -> Dict[str, Any]:
    return {
        "server": server,
        "in_rubrik": "NO",
        "status": "NO",  # YES if backup within 24 hours
        "last_backup": "N/A",
        "sla_domain": "N/A",
        "backup_running": False,
        "backup_triggered": False,
        "backup_trigger_error": None,
        "checked_at": _format_dt(datetime.now(timezone.utc)),
    }


def _print_section(title: str, rows: List[Dict]) -> None:
    print(f"{title}:")
    print(HEADER)
    for row in rows:
        print(
            f"{row['server']:25} | "
            f"{row['in_rubrik']:9} | "
            f"{row.get('status','NO'):10} | "
            f"{str(bool(row.get('backup_running'))):13} | "
            f"{str(bool(row.get('backup_triggered'))):9} | "
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

    if not RUBRIK_BASE_URL:
        print("[ERROR] RUBRIK_BASE_URL (or RUBRIK_URL) not set; cannot query/trigger backups.")
        rows = [_result_row_defaults(s) for s in servers]
        _write_json(
            COMBINED_REPORT_PATH,
            {
                "generated_at": _format_dt(datetime.now(timezone.utc)),
                "stale_hours": STALE_HOURS,
                "results": rows,
            },
        )
        # Best-effort augmentation so downstream jobs have a stable artifact.
        updated_tickets: List[Dict[str, Any]] = []
        for t in tickets:
            t2 = dict(t)
            nodes = [n.strip().lower() for n in (t.get("nodes") or []) if n and str(n).strip()]
            t2["backup_results"] = [_result_row_defaults(n) for n in nodes]
            updated_tickets.append(t2)
        _write_json(UPDATED_TICKETS_JSON, updated_tickets)
        return

    client = RubrikClient(RUBRIK_BASE_URL, RUBRIK_TOKEN)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=STALE_HOURS)

    rows: List[Dict[str, Any]] = []
    per_server: Dict[str, Dict[str, Any]] = {}

    for srv in servers:
        row = _result_row_defaults(srv)

        vm = client.get_vm_by_name(srv)
        if not vm:
            rows.append(row)
            per_server[srv] = row
            continue

        row["in_rubrik"] = "YES"
        row["sla_domain"] = vm.effective_sla_domain or "N/A"

        last_dt, snap_status = client.get_latest_snapshot(vm.id)
        if last_dt:
            row["last_backup"] = _format_dt(last_dt)

        # Detect "running" if API indicates so; otherwise rely on trigger response.
        if snap_status and snap_status in ("RUNNING", "IN_PROGRESS", "QUEUED"):
            row["backup_running"] = True

        # Determine whether backup is fresh enough
        if last_dt and last_dt >= cutoff:
            row["status"] = "YES"
            rows.append(row)
            per_server[srv] = row
            continue

        # Backup is stale/missing → attempt trigger if not already running
        if row["backup_running"]:
            rows.append(row)
            per_server[srv] = row
            continue

        triggered, already_running, err = client.trigger_on_demand_backup(vm.id)
        row["backup_triggered"] = bool(triggered)
        row["backup_running"] = bool(row["backup_running"] or already_running)
        row["backup_trigger_error"] = err

        rows.append(row)
        per_server[srv] = row

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
