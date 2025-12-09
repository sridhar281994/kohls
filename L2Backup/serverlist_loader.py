"""Helpers for resolving the list of servers to inspect."""

from __future__ import annotations

import os
import re
from typing import Callable, Iterable, List, Optional

_SPLIT_RE = re.compile(r"[,\s;]+")


def _log(message: str, logger: Optional[Callable[[str], None]]) -> None:
    if logger:
        logger(message)


def _normalize_servers(raw_values: Iterable[str]) -> List[str]:
    servers: List[str] = []
    for raw in raw_values:
        for token in _SPLIT_RE.split(raw):
            token = token.strip().lower()
            if token:
                servers.append(token)
    return servers


def load_server_list(
    default_path: str,
    *,
    env_var_name: str = "serverlist",
    logger: Optional[Callable[[str], None]] = print,
) -> List[str]:
    """
    Resolve the list of servers from an environment variable or a fallback file.

    Priority:
      1. $serverlist (as provided by CI) or its uppercase variant.
      2. $SERVER_LIST_PATH if set.
      3. Provided `default_path`.
    """

    env_candidates = (env_var_name, env_var_name.upper())
    for candidate in env_candidates:
        env_value = os.getenv(candidate)
        if env_value:
            servers = _normalize_servers([env_value])
            if servers:
                _log(f"[INFO] Loaded {len(servers)} servers from ${candidate}.", logger)
                return servers
            _log(f"[WARN] Environment variable {candidate} is set but empty.", logger)
            break

    fallback_path = os.getenv("SERVER_LIST_PATH", default_path)
    if not os.path.exists(fallback_path):
        raise FileNotFoundError(f"Server list not found: {fallback_path}")

    with open(fallback_path) as handle:
        servers = _normalize_servers(handle.readlines())

    _log(f"[INFO] Loaded {len(servers)} servers from {fallback_path}.", logger)
    return servers

