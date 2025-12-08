#!/usr/bin/env python3
"""
Unified Rubrik CDM backup checker for filesets and VM snapshots.

Features
--------
- Single authentication and GraphQL client per cluster
- Supports multiple clusters via RUBRIK_CLUSTERS (comma/newline separated)
- Accepts server names directly from the `serverlist` CICD variable (comma/newline separated)
- Checks only today's or yesterday's snapshots (UTC-aware)
- Emits snapshot counts and SLA names in the console output
- No filesystem artifacts required; everything is streamed to the job log

Environment variables
---------------------
RUBRIK_CLUSTERS             Comma/newline separated list of Rubrik cluster FQDNs
RSC_FQDN                    Fallback single cluster FQDN (default: kohls.my.rubrik.com)
RUBRIK_TOKEN_URL            Optional auth URL template (supports {cluster} or {fqdn})
RUBRIK_CLIENT_ID            OAuth client id for CDM
RUBRIK_CLIENT_SECRET        OAuth client secret for CDM
HTTP_PROXY / HTTPS_PROXY    Optional proxy endpoints
serverlist / SERVER_NAMES   Comma/newline separated server names (preferred)
"""

import json
import os
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests

import gpls

# Disable TLS warnings for self-signed Rubrik clusters
requests.packages.urllib3.disable_warnings()


# ==========================================
# CONFIGURATION
# ==========================================
RSC_FQDN = os.getenv("RSC_FQDN", "kohls.my.rubrik.com")
CID = os.getenv("RUBRIK_CLIENT_ID")
CSECRET = os.getenv("RUBRIK_CLIENT_SECRET")
PROXY = os.getenv("HTTP_PROXY") or os.getenv("HTTPS_PROXY")
PROXIES = {"http": PROXY, "https": PROXY} if PROXY else None
REQUEST_TIMEOUT = int(os.getenv("RUBRIK_REQUEST_TIMEOUT", "10"))
TOKEN_URL_TEMPLATE = os.getenv("RUBRIK_TOKEN_URL")
CLUSTERS_RAW = os.getenv("RUBRIK_CLUSTERS")

SERVER_NAMES_RAW = os.getenv("SERVER_NAMES") or os.getenv("serverlist")

VERBOSE_OUTPUT = os.getenv("RUBRIK_VERBOSE", "").strip().lower() in {"1", "true", "yes", "on"}
_RESULT_HEADER_EMITTED = False


def vprint(message: str = "", *, force: bool = False) -> None:
    if VERBOSE_OUTPUT or force:
        print(message)


# ==========================================
# Rubrik GraphQL Client
# ==========================================
class Rubrik:
    def __init__(self, fqdn: str, cid: str, csecret: str, token_url_template: Optional[str] = None):
        if not cid or not csecret:
            raise SystemExit("RUBRIK_CLIENT_ID and RUBRIK_CLIENT_SECRET must be set.")
        self.fqdn = fqdn
        self.cid = cid
        self.csecret = csecret
        self.token_url_template = token_url_template
        self.tok = self._auth()

    def _auth(self) -> str:
        url = self._token_url()
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.cid,
            "client_secret": self.csecret,
        }
        try:
            vprint(f"[AUTH] Connecting to {self.fqdn} ...")
            resp = requests.post(
                url,
                data=payload,
                proxies=PROXIES,
                timeout=REQUEST_TIMEOUT,
                verify=False,
            )
            resp.raise_for_status()
            vprint(f"[OK] Authenticated successfully to {self.fqdn}\n")
            return resp.json().get("access_token")
        except Exception as exc:
            vprint(f"[ERROR] Auth failed: {exc}", force=True)
            raise SystemExit(1)

    def q(self, query: str, vars: Optional[Dict] = None) -> Optional[Dict]:
        """Execute GraphQL query with shared timeout."""
        hdr = {"Authorization": f"Bearer {self.tok}", "Content-Type": "application/json"}
        try:
            resp = requests.post(
                f"https://{self.fqdn}/api/graphql",
                json={"query": query, "variables": vars or {}},
                headers=hdr,
                proxies=PROXIES,
                timeout=REQUEST_TIMEOUT,
                verify=False,
            )
            if resp.status_code != 200:
                vprint(f"[WARN] GraphQL {resp.status_code} {resp.reason}", force=True)
                try:
                    vprint(resp.text[:400], force=True)
                except Exception:
                    pass
                return None
            return resp.json()
        except requests.exceptions.Timeout:
            vprint("[TIMEOUT] Rubrik query timed out.", force=True)
            return None
        except Exception as exc:
            vprint(f"[ERROR] GraphQL query failed: {exc}", force=True)
            return None

    def _token_url(self) -> str:
        if self.token_url_template:
            template = self.token_url_template.strip()
            try:
                return template.format(cluster=self.fqdn, fqdn=self.fqdn)
            except KeyError:
                return template
        return f"https://{self.fqdn}/api/client_token"


# ==========================================
# Helper Functions
# ==========================================
def _parse_inline_servers(raw: str) -> List[str]:
    return [token.strip().lower() for token in re.split(r"[,\n]", raw) if token.strip()]


def _load_servers_from_file(path: str, label: str) -> List[str]:
    if not os.path.exists(path):
        raise SystemExit(f"[ERROR] {label} not found: {path}")
    with open(path) as handle:
        entries = [line.strip().lower() for line in handle if line.strip()]
    vprint(f"[INFO] Loaded {len(entries)} servers from {path} ({label})")
    return entries


def load_server_list() -> List[str]:
    if SERVER_NAMES_RAW:
        if os.path.exists(SERVER_NAMES_RAW):
            return _load_servers_from_file(SERVER_NAMES_RAW, "serverlist")
        entries = _parse_inline_servers(SERVER_NAMES_RAW)
        if entries:
            vprint(f"[INFO] Loaded {len(entries)} servers from inline 'serverlist' variable")
            return entries
        raise SystemExit("[ERROR] 'serverlist' variable is set but empty.")

    legacy_path = "L2Backup/serverslist5"
    if os.path.exists(legacy_path):
        return _load_servers_from_file(legacy_path, "legacy default server list")

    raise SystemExit("[ERROR] No server names provided. Set the 'serverlist' CICD variable.")


def resolve_clusters() -> List[str]:
    if CLUSTERS_RAW:
        clusters = [c.strip() for c in re.split(r"[,\n]", CLUSTERS_RAW) if c.strip()]
        if clusters:
            vprint(f"[INFO] Checking {len(clusters)} Rubrik cluster(s) from RUBRIK_CLUSTERS")
            return clusters
    vprint("[INFO] RUBRIK_CLUSTERS not set; using single cluster from RSC_FQDN")
    return [RSC_FQDN]


def _safe_path_string(physical_path_field):
    if not physical_path_field:
        return "n/a"
    try:
        if isinstance(physical_path_field, list):
            return ", ".join([p.get("name", "") for p in physical_path_field]).lower()
        return physical_path_field.get("name", "n/a").lower()
    except Exception:
        return "n/a"


def fuzzy_match(server: str, fileset: Dict) -> bool:
    s = server.lower()
    return (s in fileset.get("server", "")) or (s in (fileset.get("path") or ""))


def print_result_header() -> None:
    """Emit the shared column header for result rows."""
    global _RESULT_HEADER_EMITTED
    if _RESULT_HEADER_EMITTED:
        return
    _RESULT_HEADER_EMITTED = True
    print("Server|Type|SLA|Last Backup|Snapshot Count")


def emit_result_line(server: str, entry_type: str, sla: str, last_backup: str, count: int) -> None:
    """Print a normalized result line to simplify downstream parsing."""
    print_result_header()
    emit_sla = sla or "N/A"
    emit_last = last_backup or "N/A"
    emit_count = count if isinstance(count, int) else 0
    print(f"{server}|{entry_type}|{emit_sla}|{emit_last}|{emit_count}")


def latest_snapshot_after_cutoff(rsc: Rubrik, snappable_id: str) -> Tuple[str, str, int, str]:
    try:
        vars_json = json.loads(
            gpls.odsSnapshotListfromSnappableVars.replace("REPLACEME", snappable_id)
        )
    except Exception:
        vars_json = {"snappableId": snappable_id}
    vars_json.setdefault("first", 50)
    snaps = rsc.q(gpls.odsSnapshotListfromSnappable, vars_json)
    conn = snaps.get("data", {}).get("snapshotsListConnection") if snaps else None
    edges = (conn or {}).get("edges", []) if conn else []
    if not edges:
        return "NO", "N/A", 0, "N/A"

    latest = edges[0].get("node", {})
    sla_name = (latest.get("slaDomain") or {}).get("name", "N/A")
    dt = latest.get("date")
    try:
        snap_dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))
    except Exception:
        snap_dt = None

    if not snap_dt:
        return "NO", "N/A", len(edges), sla_name

    now = datetime.now(timezone.utc)
    days_diff = (now.date() - snap_dt.date()).days
    status = "YES" if days_diff in (0, 1) else "NO"
    return status, snap_dt.strftime("%Y-%m-%d %H:%M:%S UTC"), len(edges), sla_name


# ==========================================
# Fileset Helpers
# ==========================================
def fetch_all_filesets(rsc: Rubrik) -> List[Dict]:
    vprint("[STEP] Fetching all filesets from Rubrik CDM...")
    all_fs: List[Dict] = []

    win_vars = json.loads(gpls.filesetWindowsVars)
    win_vars["first"] = 500
    win_data = rsc.q(gpls.filesetTemplateQuery, win_vars)
    win_edges = (
        win_data.get("data", {}).get("filesetTemplates", {}).get("edges", []) if win_data else []
    )
    for edge in win_edges:
        node = edge.get("node", {})
        fs_name = node.get("name", "N/A")
        cluster = node.get("cluster", {}).get("name", "N/A")
        children = node.get("physicalChildConnection", {}).get("edges", []) or []
        for child_edge in children:
            child = child_edge.get("node", {})
            all_fs.append(
                {
                    "snappable_id": child.get("id"),
                    "server": (child.get("name") or "n/a").lower(),
                    "fileset": fs_name,
                    "cluster": cluster,
                    "sla": (child.get("effectiveSlaDomain", {}) or {}).get("name", "N/A"),
                    "path": _safe_path_string(child.get("physicalPath")),
                    "type": "WINDOWS_FILESET",
                }
            )
    vprint(f"[OK] Found {len([x for x in all_fs if x['type'] == 'WINDOWS_FILESET'])} Windows filesets.")

    lin_vars = json.loads(gpls.filesetLinuxVars)
    lin_vars["first"] = 500
    lin_data = rsc.q(gpls.filesetTemplateQuery, lin_vars)
    lin_edges = (
        lin_data.get("data", {}).get("filesetTemplates", {}).get("edges", []) if lin_data else []
    )
    for edge in lin_edges:
        node = edge.get("node", {})
        fs_name = node.get("name", "N/A")
        cluster = node.get("cluster", {}).get("name", "N/A")
        children = node.get("physicalChildConnection", {}).get("edges", []) or []
        for child_edge in children:
            child = child_edge.get("node", {})
            all_fs.append(
                {
                    "snappable_id": child.get("id"),
                    "server": (child.get("name") or "n/a").lower(),
                    "fileset": fs_name,
                    "cluster": cluster,
                    "sla": (child.get("effectiveSlaDomain", {}) or {}).get("name", "N/A"),
                    "path": _safe_path_string(child.get("physicalPath")),
                    "type": "LINUX_FILESET",
                }
            )
    vprint(f"[OK] Found {len([x for x in all_fs if x['type'] == 'LINUX_FILESET'])} Linux filesets.")
    vprint(f"[INFO] Total filesets fetched: {len(all_fs)}")
    return all_fs


def check_filesets(rsc: Rubrik, serverlist: List[str]) -> List[Dict]:
    all_filesets = fetch_all_filesets(rsc)

    matches_by_server: Dict[str, List[Dict]] = {}
    total_matches = 0
    for srv in serverlist:
        srv_matches = [fs for fs in all_filesets if fuzzy_match(srv, fs)]
        for match in srv_matches:
            match["_requested_server"] = srv
        if not srv_matches:
            vprint(f"[WARN] No fileset match for {srv}")
        matches_by_server[srv] = srv_matches
        total_matches += len(srv_matches)

    vprint(f"[STEP] Checking last fileset backup for {total_matches} matched entries...\n")

    results: List[Dict] = []
    for srv, srv_matches in matches_by_server.items():
        if not srv_matches:
            placeholder = {
                "server": srv,
                "type": "FILESET",
                "cluster": rsc.fqdn,
                "in_rubrik": "NO",
                "fileset": "N/A",
                "last_backup": "N/A",
                "status": "NO",
                "snapshot_count": 0,
                "sla_domain": "N/A",
            }
            results.append(placeholder)
            emit_result_line(
                srv,
                "fileset",
                placeholder["sla_domain"],
                placeholder["last_backup"],
                placeholder["snapshot_count"],
            )
            continue

        for fs in srv_matches:
            snappable_id = fs.get("snappable_id")
            server_display = fs.get("_requested_server", fs.get("server", "n/a"))
            if not snappable_id:
                result = {
                    "server": server_display,
                    "type": fs.get("type", "FILESET"),
                    "cluster": fs.get("cluster", "N/A"),
                    "in_rubrik": "NO",
                    "fileset": fs.get("fileset", "N/A"),
                    "last_backup": "N/A",
                    "status": "NO",
                    "snapshot_count": 0,
                    "sla_domain": fs.get("sla", "N/A"),
                }
                results.append(result)
                emit_result_line(
                    server_display,
                    "fileset",
                    result["sla_domain"],
                    result["last_backup"],
                    result["snapshot_count"],
                )
                continue

            status, dt_str, snap_count, sla_name = latest_snapshot_after_cutoff(rsc, snappable_id)
            result = {
                "server": server_display,
                "type": fs.get("type", "FILESET"),
                "cluster": fs.get("cluster", "N/A"),
                "in_rubrik": "YES",
                "fileset": fs.get("fileset", "N/A"),
                "last_backup": dt_str,
                "status": status,
                "snapshot_count": snap_count,
                "sla_domain": fs.get("sla", sla_name),
            }
            results.append(result)
            emit_result_line(server_display, "fileset", result["sla_domain"], dt_str, snap_count)
    return results


# ==========================================
# VM Snapshot Helpers
# ==========================================
def build_vm_object_index(rsc: Rubrik) -> Dict[str, str]:
    vprint("[STEP] Building Rubrik VM object index...")
    sla_vars = json.loads(gpls.slaListQueryVars)
    sla_data = rsc.q(gpls.slaListQuery, sla_vars)
    sla_edges = sla_data.get("data", {}).get("slaDomains", {}).get("edges", []) if sla_data else []
    idmap: Dict[str, str] = {}

    for edge in sla_edges:
        sla_id = edge.get("node", {}).get("id")
        if not sla_id:
            continue
        pobj = rsc.q(
            gpls.protectedObjectListQuery,
            json.loads(gpls.protectedObjectListQueryVars.replace("REPLACEME", sla_id)),
        )
        edges = (
            pobj.get("data", {}).get("slaProtectedObjects", {}).get("edges", []) if pobj else []
        )
        for obj_edge in edges:
            node = obj_edge.get("node", {})
            name = node.get("name")
            rid = node.get("id")
            if not name or not rid:
                continue
            idmap[name.lower()] = rid

    vprint(f"[OK] Indexed {len(idmap)} Rubrik objects.\n")
    return idmap


def check_vm_snapshots(rsc: Rubrik, serverlist: List[str], idmap: Dict[str, str]) -> List[Dict]:
    results: List[Dict] = []
    now = datetime.now(timezone.utc)

    for idx, srv in enumerate(serverlist, 1):
        if idx % 50 == 0:
            vprint(f"[HEARTBEAT] Processed {idx} servers for VM snapshots...")

        rid = idmap.get(srv)
        if not rid:
            placeholder = {
                "server": srv,
                "in_rubrik": "NO",
                "last_backup": "N/A",
                "status": "NO",
                "snapshot_count": 0,
                "sla_domain": "N/A",
            }
            results.append(placeholder)
            emit_result_line(srv, "vmsnapshot", placeholder["sla_domain"], placeholder["last_backup"], 0)
            continue

        try:
            vars_json = json.loads(
                gpls.odsSnapshotListfromSnappableVars.replace("REPLACEME", rid)
            )
        except Exception:
            vars_json = {"snappableId": rid}
        vars_json.setdefault("first", 50)

        snaps = rsc.q(gpls.odsSnapshotListfromSnappable, vars_json)
        conn = snaps.get("data", {}).get("snapshotsListConnection") if snaps else None
        edges = conn.get("edges", []) if conn else []
        if not edges:
            placeholder = {
                "server": srv,
                "in_rubrik": "YES",
                "last_backup": "N/A",
                "status": "NO",
                "snapshot_count": 0,
                "sla_domain": "N/A",
            }
            results.append(placeholder)
            emit_result_line(srv, "vmsnapshot", placeholder["sla_domain"], placeholder["last_backup"], 0)
            continue

        latest = edges[0].get("node", {})
        sla_name = (latest.get("slaDomain") or {}).get("name", "N/A")
        dt = latest.get("date")
        try:
            snap_dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))
        except Exception:
            snap_dt = None

        if snap_dt:
            days_diff = (now.date() - snap_dt.date()).days
            backed_up = "YES" if days_diff in (0, 1) else "NO"
            dt_str = snap_dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        else:
            backed_up = "NO"
            dt_str = "N/A"

        snapshot_count = len(edges)
        result = {
            "server": srv,
            "in_rubrik": "YES",
            "last_backup": dt_str,
            "status": backed_up,
            "snapshot_count": snapshot_count,
            "sla_domain": sla_name,
        }
        results.append(result)
        emit_result_line(srv, "vmsnapshot", sla_name, dt_str, snapshot_count)
    return results


# ==========================================
# SUMMARY
# ==========================================
def summarize(results: List[Dict], label: str) -> None:
    if not VERBOSE_OUTPUT:
        return
    total = len(results)
    success = sum(1 for entry in results if entry.get("status") == "YES")
    failed = total - success
    vprint("=" * 55)
    vprint(f"{label} Summary")
    vprint(f"Total Servers : {total}")
    vprint(f"Successful    : {success}")
    vprint(f"Failed        : {failed}")
    vprint("=" * 55 + "\n")


# ==========================================
# MAIN
# ==========================================
def main():
    servers = load_server_list()
    clusters = resolve_clusters()

    for cluster in clusters:
        vprint("\n" + "=" * 70)
        vprint(f"[CLUSTER] {cluster}")
        vprint("=" * 70)

        rsc = Rubrik(cluster, CID, CSECRET, token_url_template=TOKEN_URL_TEMPLATE)

        fileset_results = check_filesets(rsc, servers)
        summarize(fileset_results, f"Fileset | {cluster}")

        vm_index = build_vm_object_index(rsc)
        vm_results = check_vm_snapshots(rsc, servers, vm_index)
        summarize(vm_results, f"VM Snapshot | {cluster}")

    vprint("[DONE] Combined Rubrik backup checks complete.")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as exc:
        vprint(f"[FATAL] {exc}", force=True)
        raise SystemExit(1)
