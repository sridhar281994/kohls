"""
Optimized Rubrik (RSC) backup checker.

This module is used by `combined_backup_report.py` to:
- look up protected objects and their SLA domain
- fetch recent snapshots + last backup time
- treat backups as "YES" only if a snapshot exists within the last N hours
- optionally detect "backup running" from the object's protection status

Notes:
- Uses Rubrik Security Cloud (RSC) GraphQL (`/api/graphql`) and client token auth.
- Works with proxy settings (HTTP(S)_PROXY).
- UTC-aware timestamps.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import requests

try:
    # When imported as a package module (e.g. `import L2Backup.check_last_backup`)
    from . import gqls
    from .serverlist_loader import load_server_list
except ImportError:  # pragma: no cover
    # When executed as a standalone script (e.g. `python3 L2Backup/check_last_backup.py`)
    import gqls
    from serverlist_loader import load_server_list

# ==========================================
# CONFIGURATION
# ==========================================
RSC_FQDN = os.getenv("RSC_FQDN", "kohls.my.rubrik.com")
CID = os.getenv("RUBRIK_CLIENT_ID")
CSECRET = os.getenv("RUBRIK_CLIENT_SECRET")

PROXY = os.getenv("HTTP_PROXY") or os.getenv("HTTPS_PROXY")
PROXIES = {"http": PROXY, "https": PROXY} if PROXY else None

SERVER_LIST_PATH = os.getenv("SERVER_LIST_PATH", "L2Backup/serverslist1")
STATUS_WINDOW_HOURS = int(os.getenv("STATUS_WINDOW_HOURS", "24"))
COUNT_WINDOW_DAYS = int(os.getenv("COUNT_WINDOW_DAYS", "60"))

# Disable TLS warnings
requests.packages.urllib3.disable_warnings()

# ==========================================
# Rubrik GraphQL Client (10 s timeout)
# ==========================================


class Rubrik:
    def __init__(self, fqdn: str, cid: Optional[str], csecret: Optional[str], *, verbose: bool = True):
        self.fqdn = fqdn
        self.cid = cid
        self.csecret = csecret
        self.verbose = verbose
        self.tok = self._auth()

    def _auth(self) -> str:
        if not self.cid or not self.csecret:
            if self.verbose:
                print("[ERROR] RUBRIK_CLIENT_ID/RUBRIK_CLIENT_SECRET not set.")
            raise SystemExit(1)

        url = f"https://{self.fqdn}/api/client_token"
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.cid,
            "client_secret": self.csecret,
        }
        try:
            if self.verbose:
                print(f"[AUTH] Connecting to {self.fqdn} ...")
            r = requests.post(url, data=payload, proxies=PROXIES, timeout=10)
            r.raise_for_status()
            if self.verbose:
                print(f"[OK] Authenticated successfully to {self.fqdn}\n")
            tok = r.json().get("access_token")
            if not tok:
                raise RuntimeError("Missing access_token in auth response")
            return tok
        except Exception as e:
            if self.verbose:
                print(f"[ERROR] Auth failed: {e}")
            raise SystemExit(1)

    def q(self, query: str, vars: Optional[dict] = None) -> Optional[dict]:
        """Single fast GraphQL query (10 s timeout)."""
        hdr = {"Authorization": f"Bearer {self.tok}", "Content-Type": "application/json"}
        try:
            r = requests.post(
                f"https://{self.fqdn}/api/graphql",
                json={"query": query, "variables": vars or {}},
                headers=hdr,
                proxies=PROXIES,
                timeout=10,
            )
            if r.status_code != 200:
                if self.verbose:
                    print(f"[WARN] GraphQL {r.status_code} {r.reason}")
                return None
            return r.json()
        except requests.exceptions.Timeout:
            if self.verbose:
                print("[TIMEOUT] Rubrik query timed out (10 s limit).")
            return None
        except Exception as e:
            if self.verbose:
                print(f"[ERROR] GraphQL query failed: {e}")
            return None


def _parse_snapshot_date(date_str: Optional[str]) -> Optional[datetime]:
    if not date_str:
        return None
    try:
        dt = datetime.fromisoformat(str(date_str).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _format_snapshot_date(dt: Optional[datetime]) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC") if dt else "N/A"


def _is_backup_running_from_protection_status(status: Optional[str]) -> bool:
    if not status:
        return False
    s = str(status).strip().upper()
    # Common Rubrik protectionStatus values include PROTECTED/UNPROTECTED/PROTECTING.
    return s in {"PROTECTING", "PROTECTION_IN_PROGRESS", "IN_PROGRESS", "RUNNING"}


def run(
    servers: Optional[List[str]] = None,
    *,
    persist: bool = False,
    show_summary: bool = True,
    show_progress: bool = True,
) -> List[dict]:
    """
    Returns rows shaped for `update_servicenow.py` consumption:
      - server, in_rubrik, status, last_backup, sla_domain
    Plus extra fields used by reports:
      - successful_backup_count, backup_running
    """
    logger = print if show_progress else None

    if servers is None:
        try:
            servers = load_server_list(SERVER_LIST_PATH, logger=logger)
        except FileNotFoundError as exc:
            if logger:
                logger(f"[ERROR] {exc}")
            return []
    else:
        servers = [srv.strip().lower() for srv in servers if srv and str(srv).strip()]
        if logger:
            logger(f"[INFO] Loaded {len(servers)} servers from provided input.\n")

    if not servers:
        if logger:
            logger("[WARN] No servers supplied. Skipping Rubrik checks.")
        return []

    rsc = Rubrik(RSC_FQDN, CID, CSECRET, verbose=show_progress)

    # Build index: server -> {id, sla_domain, protection_status}
    if logger:
        logger("[STEP] Building Rubrik object index (by SLA)...")

    sla_data = rsc.q(gqls.slaListQuery, json.loads(gqls.slaListQueryVars))
    sla_edges = sla_data.get("data", {}).get("slaDomains", {}).get("edges", []) if sla_data else []

    idmap: Dict[str, dict] = {}
    for edge in sla_edges:
        sla_node = edge.get("node") or {}
        sid = sla_node.get("id")
        sla_name = sla_node.get("name") or "N/A"
        if not sid:
            continue

        pobj = rsc.q(
            gqls.protectedObjectListQuery,
            json.loads(gqls.protectedObjectListQueryVars.replace("REPLACEME", str(sid))),
        )
        if not pobj or "data" not in pobj:
            continue

        for e in pobj["data"].get("slaProtectedObjects", {}).get("edges", []):
            node = e.get("node") or {}
            name = str(node.get("name") or "").strip().lower()
            oid = node.get("id")
            if not name or not oid:
                continue
            # Keep first seen mapping to avoid flip-flopping for odd edge cases.
            if name not in idmap:
                idmap[name] = {
                    "id": oid,
                    "sla_domain": sla_name,
                    "protection_status": node.get("protectionStatus"),
                }

    if logger:
        logger(f"[OK] Indexed {len(idmap)} Rubrik objects.\n")

    now = datetime.now(timezone.utc)
    recent_cutoff = now - timedelta(hours=STATUS_WINDOW_HOURS)
    count_cutoff = now - timedelta(days=COUNT_WINDOW_DAYS)

    results: List[dict] = []

    for idx, srv in enumerate(servers, 1):
        if logger and idx % 50 == 0:
            logger(f"[HEARTBEAT] Processed {idx} servers so far...")

        entry = idmap.get(srv)
        if not entry:
            results.append(
                {
                    "server": srv,
                    "in_rubrik": "NO",
                    "last_backup": "N/A",
                    "status": "NO",
                    "successful_backup_count": 0,
                    "sla_domain": "N/A",
                    "backup_running": False,
                }
            )
            continue

        rid = entry["id"]
        sla_domain = entry.get("sla_domain") or "N/A"
        backup_running = _is_backup_running_from_protection_status(entry.get("protection_status"))

        vars_payload = json.loads(gqls.odsSnapshotListfromSnappableVars.replace("REPLACEME", str(rid)))
        # Ensure we pull enough edges for 60d counting (RSC defaults can be small).
        vars_payload["first"] = max(int(vars_payload.get("first") or 0), 200)

        snaps = rsc.q(gqls.odsSnapshotListfromSnappable, vars_payload)
        conn = snaps.get("data", {}).get("snapshotsListConnection") if snaps else None
        edges = conn.get("edges", []) if conn else []

        latest_dt: Optional[datetime] = None
        latest_dt_non_ondemand_recent: Optional[datetime] = None
        latest_dt_ondemand_recent: Optional[datetime] = None
        success_count = 0

        for edge in edges:
            node = edge.get("node") or {}
            dt = _parse_snapshot_date(node.get("date"))
            if not dt:
                continue
            if dt > now:
                continue

            if latest_dt is None or dt > latest_dt:
                latest_dt = dt

            if dt >= count_cutoff:
                success_count += 1

            if dt >= recent_cutoff:
                if node.get("isOnDemandSnapshot") is True:
                    if latest_dt_ondemand_recent is None or dt > latest_dt_ondemand_recent:
                        latest_dt_ondemand_recent = dt
                else:
                    if latest_dt_non_ondemand_recent is None or dt > latest_dt_non_ondemand_recent:
                        latest_dt_non_ondemand_recent = dt

        # Fallback to SLA from snapshot if index didn't resolve (rare).
        if sla_domain == "N/A" and edges:
            maybe = (edges[0].get("node") or {}).get("slaDomain") or {}
            sla_domain = maybe.get("name") or sla_domain

        # "Include on-demand backup if snapshot not available for last 24 hours"
        # Means: treat on-demand snapshot within the window as valid even if no "regular" snapshot exists.
        has_recent_regular = latest_dt_non_ondemand_recent is not None
        has_recent_ondemand = latest_dt_ondemand_recent is not None
        backed_up = "YES" if (has_recent_regular or has_recent_ondemand) else "NO"

        dt_str = _format_snapshot_date(latest_dt)

        result = {
            "server": srv,
            "in_rubrik": "YES",
            "last_backup": dt_str,
            "status": backed_up,
            "successful_backup_count": success_count,
            "sla_domain": sla_domain or "N/A",
            "backup_running": bool(backup_running),
        }
        results.append(result)

        if logger:
            logger(
                f"{srv:25} | In Rubrik: YES | Backup: {backed_up:3} | "
                f"Running: {str(backup_running):5} | {dt_str}"
            )

    if show_summary and logger:
        total = len(results)
        success = sum(1 for r in results if r.get("status") == "YES")
        failed = sum(1 for r in results if r.get("in_rubrik") == "YES" and r.get("status") != "YES")
        missing = sum(1 for r in results if r.get("in_rubrik") != "YES")
        logger("=" * 55)
        logger(f"Total Servers : {total}")
        logger(f"Successful    : {success}")
        logger(f"Failed        : {failed}")
        logger(f"Not in Rubrik : {missing}")
        logger("=" * 55)

    if persist:
        # Kept for backward compat with older pipelines; default is false.
        job_id = os.getenv("CI_JOB_NAME", "default").replace(" ", "_")
        out_file = f"L2Backup/partial_results_{job_id}.json"
        os.makedirs(os.path.dirname(out_file), exist_ok=True)
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2)
        if logger:
            logger(f"[SAVE] {out_file} written.")

    if logger:
        logger("[DONE] Backup check complete.\n")

    return results


def main() -> None:
    run(servers=None, persist=True, show_summary=True, show_progress=True)


if __name__ == "__main__":
    main()
