"""
Create a simple ServiceNow server-name artifact.

DO NOT modify fetch_servicenow.py: it already extracts nodes from u_node
or falls back to parsing the description and writes tickets.json.

This script reads tickets.json and writes servicenow_servers.json with:
- servers: unique, lowercase server names
- tickets: minimal mapping of incident number -> nodes
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List


def _load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main() -> None:
    tickets_path = os.getenv("SERVICENOW_TICKETS_JSON", "tickets.json")
    out_path = os.getenv("SERVICENOW_SERVERS_JSON", "servicenow_servers.json")

    if not os.path.exists(tickets_path):
        raise SystemExit(f"[ERROR] {tickets_path} not found")

    tickets = _load_json(tickets_path)
    if not isinstance(tickets, list):
        raise SystemExit("[ERROR] tickets.json must be a JSON list")

    servers = sorted(
        {
            (node or "").strip().lower()
            for t in tickets
            for node in (t.get("nodes") or [])
            if node and str(node).strip()
        }
    )

    minimal_tickets: List[Dict[str, Any]] = []
    for t in tickets:
        number = t.get("number")
        nodes = [n.strip().lower() for n in (t.get("nodes") or []) if n and str(n).strip()]
        minimal_tickets.append(
            {
                "number": number,
                "sys_id": t.get("sys_id"),
                "nodes": nodes,
                "incident_state": t.get("incident_state"),
            }
        )

    payload: Dict[str, Any] = {
        "servers": servers,
        "tickets": minimal_tickets,
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"[OK] Wrote {out_path} with {len(servers)} server(s)")


if __name__ == "__main__":
    main()

