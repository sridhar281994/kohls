"""
Simple server list loader.

Expected file format:
- One server name per line
- Blank lines are ignored
- Lines starting with '#' are treated as comments
"""

from __future__ import annotations

from typing import Callable, List, Optional


def load_server_list(path: str, logger: Optional[Callable[[str], None]] = None) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read().splitlines()

    servers: List[str] = []
    for line in raw:
        s = (line or "").strip()
        if not s or s.startswith("#"):
            continue
        servers.append(s.lower())

    # Deduplicate while preserving order
    seen = set()
    out: List[str] = []
    for s in servers:
        if s not in seen:
            out.append(s)
            seen.add(s)

    if logger:
        logger(f"[INFO] Loaded {len(out)} servers from {path}")

    return out

