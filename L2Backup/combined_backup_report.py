"""
Orchestrates VM backup checks and prints a combined report.

FIXED:
- check_last_backup has NO run() function
- Uses check_last_backup.main()
- Reads results from the JSON output file
- No internal function calls
- Zero behavior change to check_last_backup
"""

from __future__ import annotations

import os
import json
from typing import Dict, List

import check_last_backup

# ==========================================
# CONFIGURATION
# ==========================================
SERVICENOW_TICKETS_JSON = os.getenv("SERVICENOW_TICKETS_JSON", "tickets.json")

JOB_ID = os.getenv("CI_JOB_NAME", "default").replace(" ", "_")
RUBRIK_RESULT_JSON = os.getenv(
    "RUBRIK_RESULT_JSON",
    f"L2Backup/partial_results_{JOB_ID}.json"
)

HEADER = (
    f"{'Server':25} | "
    f"{'In Rubrik':9} | "
    f"{'Total successful backups (60d)':30} | "
    f"{'Last Backup':20} | "
    f"SLA Domain"
)

# ==========================================
# LOAD SERVERS FROM tickets.json
# ==========================================
def load_servers_from_tickets(json_path: str) -> List[str]:
    if not os.path.isfile(json_path):
        raise FileNotFoundError(f"{json_path} not found")

    with open(json_path, "r", encoding="utf-8") as f:
        tickets = json.load(f)

    return sorted({
        node.lower()
        for t in tickets
        for node in t.get("nodes", [])
        if node
    })


# ==========================================
# PRINT RESULTS
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
    # Step 1: Load servers
    try:
        servers = load_servers_from_tickets(SERVICENOW_TICKETS_JSON)
    except FileNotFoundError as exc:
        print(f"[ERROR] {exc}")
        return

    if not servers:
        print("[WARN] No servers found in tickets.json. Nothing to report.")
        return

    # Step 2: Execute check_last_backup (NO function calls)
    print("[STEP] Running Rubrik VM backup validation...")
    check_last_backup.main()

    # Step 3: Read output JSON
    if not os.path.exists(RUBRIK_RESULT_JSON):
        print(f"[ERROR] Expected result file not found: {RUBRIK_RESULT_JSON}")
        return

    with open(RUBRIK_RESULT_JSON, "r", encoding="utf-8") as f:
        vm_results = json.load(f)

    if not vm_results:
        print("[WARN] No VM results found.")
        return

    # Step 4: Filter only requested servers
    index = {r["server"].lower(): r for r in vm_results}
    rows = []

    for srv in servers:
        r = index.get(srv)
        if not r:
            rows.append({
                "server": srv,
                "in_rubrik": "NO",
                "successful_backup_count": 0,
                "last_backup": "N/A",
                "sla_domain": "N/A",
                "status": "NO",
            })
        else:
            rows.append(r)

    # Step 5: Print report
    print("\nVM Backup Results:")
    _print_section(rows)

    failed = [r["server"] for r in rows if r.get("status") != "YES"]
    if failed:
        print(
            "\n[INFO] Backup initiated for the failed server(s): "
            + ", ".join(failed)
        )
    else:
        print("\n[INFO] All servers have recent backups.")


if __name__ == "__main__":
    main()
