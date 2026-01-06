"""Orchestrates VM backup checks and prints a VM-only report."""

from __future__ import annotations

import os
from datetime import datetime
from typing import Dict, List, Optional

import check_last_backup
from serverlist_loader import load_server_list

HEADER = (
    f"{'Server':25} | "
    f"{'In Rubrik':9} | "
    f"{'Total successful backups (60d)':30} | "
    f"{'Last Backup':20} | "
    f"SLA Domain"
)


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


def _summarize_vm_section(servers: List[str], vm_results: List[Dict]) -> List[Dict]:
    index = {entry.get("server"): entry for entry in vm_results}
    rows = []
    for srv in servers:
        payload = index.get(srv)
        if not payload:
            rows.append({
                "server": srv,
                "in_rubrik": "NO",
                "successful_backup_count": 0,
                "last_backup": "N/A",
                "sla_domain": "N/A",
            })
            continue
        success_count = payload.get("successful_backup_count")
        if success_count is None:
            success_count = 1 if payload.get("status") == "YES" else 0
        rows.append({
            "server": srv,
            "in_rubrik": payload.get("in_rubrik", "NO"),
            "successful_backup_count": success_count,
            "last_backup": payload.get("last_backup", "N/A"),
            "sla_domain": payload.get("sla_domain", "N/A"),
        })
    return rows


def _print_section(title: str, rows: List[Dict]) -> None:
    print(f"{title}:")
    print(HEADER)
    for row in rows:
        count = row.get("successful_backup_count") or 0
        sla = row.get("sla_domain", "N/A")
        print(
            f"{row['server']:25} | "
            f"{row['in_rubrik']:9} | "
            f"{int(count):30d} | "
            f"{row['last_backup']:20} | "
            f"{sla}"
        )
    print()


def main() -> None:
    default_path = os.getenv("SERVER_LIST_PATH", check_last_backup.SERVER_LIST_PATH)
    try:
        servers = load_server_list(default_path)
    except FileNotFoundError as exc:
        print(f"[ERROR] {exc}")
        return

    if not servers:
        print("[WARN] No servers provided. Nothing to report.")
        return

    vm_results = check_last_backup.run(
        servers=servers,
        persist=False,
        show_progress=False,
    )

    print("Results:")
    _print_section("VM backup information", _summarize_vm_section(servers, vm_results))


if __name__ == "__main__":
    main()
