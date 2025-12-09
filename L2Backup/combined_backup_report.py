"""Orchestrates VM + fileset backup checks and prints a combined report."""

from __future__ import annotations

import os
from datetime import datetime
from typing import Dict, List, Optional

import check_filesets
import check_last_backup
from serverlist_loader import load_server_list

HEADER = "Server\t|In Rubrik\t|total count of successfull backup| Last Backup date"


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
        })
    return rows


def _summarize_fileset_section(servers: List[str], fileset_results: List[Dict]) -> List[Dict]:
    rows = []
    for srv in servers:
        matches = [entry for entry in fileset_results if entry.get("server") == srv]
        if not matches:
            rows.append({
                "server": srv,
                "in_rubrik": "NO",
                "successful_backup_count": 0,
                "last_backup": "N/A",
            })
            continue

        in_rubrik = "YES" if any(m.get("in_rubrik") == "YES" for m in matches) else "NO"
        success_count = 0
        latest_dt = None
        for m in matches:
            count = m.get("successful_backup_count")
            if count is None:
                count = 1 if m.get("status") == "YES" else 0
            success_count += count

            dt = _parse_dt(m.get("last_backup"))
            if dt and (latest_dt is None or dt > latest_dt):
                latest_dt = dt

        rows.append({
            "server": srv,
            "in_rubrik": in_rubrik,
            "successful_backup_count": success_count,
            "last_backup": _format_dt(latest_dt),
        })
    return rows


def _print_section(title: str, rows: List[Dict]) -> None:
    print(f"{title}:")
    print(HEADER)
    for row in rows:
        print(
            f"{row['server']:25} | "
            f"{row['in_rubrik']:9} | "
            f"{row['successful_backup_count']:>6} | "
            f"{row['last_backup']}"
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
        show_summary=False,
        show_progress=False,
    )
    fileset_results = check_filesets.run(
        servers=servers,
        persist=False,
        show_summary=False,
        show_progress=False,
    )

    print("Results:")
    _print_section("VMbackup information", _summarize_vm_section(servers, vm_results))
    _print_section("Fileset backup information", _summarize_fileset_section(servers, fileset_results))


if __name__ == "__main__":
    main()

