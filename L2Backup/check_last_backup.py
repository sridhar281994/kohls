"""
Orchestrates VM backup checks and prints a combined report.

MODIFIED:
- Servers are loaded from ServiceNow JSON (tickets.json → nodes[])
- SERVER_LIST_PATH and serverlist_loader are removed
- ONLY VM backup (check_last_backup.py) is used
- Fileset contribution is completely removed
- ServiceNow is NOT updated here
"""

from __future__ import annotations

import os
import json
from datetime import datetime
from typing import Dict, List, Optional

import check_last_backup

# ==========================================
# CONFIGURATION
# ==========================================
SERVICENOW_TICKETS_JSON = os.getenv("SERVICENOW_TICKETS_JSON", "tickets.json")

HEADER = (
    f"{'Server':25} | "
    f"{'In Rubrik':9} | "
    f"{'Total successful backups (60d)':30} | "
    f"{'Last Backup':20} | "
    f"SLA Domain"
)

# ==========================================
# DATE HELPERS
# ==========================================
def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value or value.upper() == "N/A":
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S %Z")
    except ValueError:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None


def _format_dt(dt: Optional[datetime]) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC") if dt else "N/A"


# ==========================================
# LOAD SERVERS FROM tickets.json
# ==========================================
def load_servers_from_tickets(json_path: str) -> List[str]:
    if not os.path.isfile(json_path):
        raise FileNotFoundError(f"{json_path} not found")

    with open(json_path, "r", encoding="utf-8") as f:
        tickets = json.load(f)

    servers = sorted({
        node.lower()
        for ticket in tickets
        for node in ticket.get("nodes", [])
        if node
    })

    return servers


# ==========================================
# VM-ONLY SUMMARIZATION LOGIC
# ==========================================
def _summarize_vm_results(
    servers: List[str],
    vm_results: List[Dict],
) -> List[Dict]:
    vm_index = {r.get("server"): r for r in vm_results}
    rows = []

    for srv in servers:
        vm = vm_index.get(srv)

        if not vm:
            rows.append({
                "server": srv,
                "in_rubrik": "NO",
                "successful_backup_count": 0,
                "last_backup": "N/A",
                "sla_domain": "N/A",
            })
            continue

        cnt = vm.get("successful_backup_count")
        if cnt is None:
            cnt = 1 if vm.get("status") == "YES" else 0

        rows.append({
            "server": srv,
            "in_rubrik": vm.get("in_rubrik", "NO"),
            "successful_backup_count": cnt,
            "last_backup": vm.get("last_backup", "N/A"),
            "sla_domain": vm.get("sla_domain", "N/A"),
        })

    return rows


# ==========================================
# OUTPUT
# ==========================================
def _print_section(rows: List[Dict]) -> None:
    print(HEADER)
    for row in rows:
        print(
            f"{row['server']:25} | "
            f"{row['in_rubrik']:9} | "
            f"{int(row['successful_backup_count']):30d} | "
            f"{row['last_backup']:20} | "
            f"{row['sla_domain']}"
        )
    print()


# ==========================================
# MAIN
# ==========================================
def main() -> None:
    try:
        servers = load_servers_from_tickets(SERVICENOW_TICKETS_JSON)
    except FileNotFoundError as exc:
        print(f"[ERROR] {exc}")
        return

    if not servers:
        print("[WARN] No servers found in tickets.json. Nothing to report.")
        return

    # ✅ FIXED CALL — show_summary REMOVED
    vm_results = check_last_backup.run(
        persist=False,
        show_progress=False,
    )

    vm_rows = _summarize_vm_results(servers, vm_results)

    print("VM Backup Results:")
    _print_section(vm_rows)


if __name__ == "__main__":
    main()
