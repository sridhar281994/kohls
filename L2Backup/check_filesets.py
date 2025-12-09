"""
Optimized Rubrik fileset backup checker — checks only today's and yesterday's backups.
If the latest backup is older than 2 days, it's marked as "NO".
"""

import os, json, requests
from datetime import datetime, timezone
from typing import List, Optional

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

SERVER_LIST_PATH = os.getenv("SERVER_LIST_PATH", "L2Backup/serverslist5")
OUT_FILE = os.getenv("OUT_FILE", "L2Backup/partial_results_check_filesets.json")
ALL_FILESETS_DUMP = os.getenv("ALL_FILESETS_DUMP", "L2Backup/all_filesets_dump.json")
STATUS_WINDOW_DAYS = int(os.getenv("STATUS_WINDOW_DAYS", "2"))
COUNT_WINDOW_DAYS = int(os.getenv("COUNT_WINDOW_DAYS", "60"))

# Disable TLS warnings
requests.packages.urllib3.disable_warnings()

# ==========================================
# Rubrik GraphQL Client
# ==========================================
class Rubrik:
    def __init__(self, fqdn, cid, csecret, *, verbose: bool = True):
        self.fqdn = fqdn
        self.cid = cid
        self.csecret = csecret
        self.verbose = verbose
        self.tok = self._auth()

    def _auth(self):
        url = f"https://{self.fqdn}/api/client_token"
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.cid,
            "client_secret": self.csecret,
        }
        try:
            if self.verbose:
                print(f"[AUTH] Connecting to {self.fqdn} ...")
            r = requests.post(url, data=payload, proxies=PROXIES, timeout=10, verify=False)
            r.raise_for_status()
            if self.verbose:
                print(f"[OK] Authenticated successfully to {self.fqdn}\n")
            return r.json().get("access_token")
        except Exception as e:
            if self.verbose:
                print(f"[ERROR] Auth failed: {e}")
            raise SystemExit(1)

    def q(self, query, vars=None):
        """Execute GraphQL query (10s timeout)"""
        hdr = {"Authorization": f"Bearer {self.tok}", "Content-Type": "application/json"}
        try:
            r = requests.post(
                f"https://{self.fqdn}/api/graphql",
                json={"query": query, "variables": vars or {}},
                headers=hdr,
                proxies=PROXIES,
                timeout=10,
                verify=False,
            )
            if r.status_code != 200 and self.verbose:
                print(f"[WARN] GraphQL {r.status_code} {r.reason}")
                try:
                    print(r.text[:400])
                except Exception:
                    pass
                return None
            return r.json()
        except requests.exceptions.Timeout:
            if self.verbose:
                print("[TIMEOUT] Rubrik query timed out (10s).")
            return None
        except Exception as e:
            if self.verbose:
                print(f"[ERROR] GraphQL query failed: {e}")
            return None

# ==========================================
# Helper Functions
# ==========================================
def _safe_path_string(physicalPathField):
    """Handles both list and dict forms of physicalPath"""
    if not physicalPathField:
        return "n/a"
    try:
        if isinstance(physicalPathField, list):
            return ", ".join([p.get("name", "") for p in physicalPathField]).lower()
        return physicalPathField.get("name", "n/a").lower()
    except Exception:
        return "n/a"


def fuzzy_match(server, fileset):
    """Match server name within fileset server/path fields"""
    s = server.lower()
    return (s in fileset["server"]) or (s in (fileset["path"] or ""))


def _parse_snapshot_date(date_str: Optional[str]) -> Optional[datetime]:
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except Exception:
        return None


def _format_snapshot_date(dt: Optional[datetime]) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC") if dt else "N/A"


def latest_snapshot_after_cutoff(rsc, snappable_id):
    """Check if last snapshot was taken today or yesterday"""
    try:
        vars_json = json.loads(gqls.odsSnapshotListfromSnappableVars.replace("REPLACEME", snappable_id))
    except Exception:
        vars_json = {"snappableId": snappable_id}
    snaps = rsc.q(gqls.odsSnapshotListfromSnappable, vars_json)
    conn = snaps.get("data", {}).get("snapshotsListConnection") if snaps else None
    edges = (conn or {}).get("edges", []) if conn else []
    if not edges:
        return "NO", "N/A", 0

    latest = edges[0].get("node", {})
    latest_dt = _parse_snapshot_date(latest.get("date"))
    if not latest_dt:
        return "NO", "N/A", 0

    now = datetime.now(timezone.utc)
    recent_count = 0
    recent_status = False
    for edge in edges:
        node_dt = _parse_snapshot_date(edge.get("node", {}).get("date"))
        if not node_dt:
            continue
        days_diff = (now.date() - node_dt.date()).days
        if days_diff < 0:
            continue
        if days_diff <= STATUS_WINDOW_DAYS:
            recent_status = True
        if days_diff <= COUNT_WINDOW_DAYS:
            recent_count += 1

    backed_up = "YES" if recent_status else "NO"
    dt_str = _format_snapshot_date(latest_dt)
    return backed_up, dt_str, recent_count


def fetch_all_filesets(rsc, *, logger=print):
    """Pull all Windows + Linux filesets"""
    if logger:
        logger("[STEP] Fetching all filesets from Rubrik CDM...")

    all_fs = []

    # WINDOWS
    if logger:
        logger("[STEP] Collecting all WINDOWS_FILESET filesets ...")
    win_vars = json.loads(gqls.filesetWindowsVars)
    win_vars["first"] = 500
    win_data = rsc.q(gqls.filesetTemplateQuery, win_vars)
    win_edges = win_data.get("data", {}).get("filesetTemplates", {}).get("edges", []) if win_data else []
    for e in win_edges:
        node = e.get("node", {})
        fs_name = node.get("name", "N/A")
        cluster = node.get("cluster", {}).get("name", "N/A")
        children = node.get("physicalChildConnection", {}).get("edges", []) or []
        for ch in children:
            child = ch.get("node", {})
            all_fs.append({
                "snappable_id": child.get("id"),
                "server": (child.get("name") or "n/a").lower(),
                "fileset": fs_name,
                "cluster": cluster,
                "sla": (child.get("effectiveSlaDomain", {}) or {}).get("name", "N/A"),
                "path": _safe_path_string(child.get("physicalPath")),
                "type": "WINDOWS_FILESET",
            })
    if logger:
        logger(f"[OK] Found {len([x for x in all_fs if x['type']=='WINDOWS_FILESET'])} WINDOWS_FILESET filesets.")

    # LINUX
    if logger:
        logger("[STEP] Collecting all LINUX_FILESET filesets ...")
    lin_vars = json.loads(gqls.filesetLinuxVars)
    lin_vars["first"] = 500
    lin_data = rsc.q(gqls.filesetTemplateQuery, lin_vars)
    lin_edges = lin_data.get("data", {}).get("filesetTemplates", {}).get("edges", []) if lin_data else []
    for e in lin_edges:
        node = e.get("node", {})
        fs_name = node.get("name", "N/A")
        cluster = node.get("cluster", {}).get("name", "N/A")
        children = node.get("physicalChildConnection", {}).get("edges", []) or []
        for ch in children:
            child = ch.get("node", {})
            all_fs.append({
                "snappable_id": child.get("id"),
                "server": (child.get("name") or "n/a").lower(),
                "fileset": fs_name,
                "cluster": cluster,
                "sla": (child.get("effectiveSlaDomain", {}) or {}).get("name", "N/A"),
                "path": _safe_path_string(child.get("physicalPath")),
                "type": "LINUX_FILESET",
            })
    if logger:
        logger(f"[OK] Found {len([x for x in all_fs if x['type']=='LINUX_FILESET'])} LINUX_FILESET filesets.")
        logger(f"[INFO] Total filesets fetched: {len(all_fs)}")
    return all_fs


# ==========================================
# MAIN
# ==========================================
def run(
    servers: Optional[List[str]] = None,
    *,
    persist: bool = True,
    show_summary: bool = True,
    show_progress: bool = True,
) -> List[dict]:
    logger = print if show_progress else None

    if servers is None:
        try:
            servers = load_server_list(SERVER_LIST_PATH, logger=logger)
        except FileNotFoundError as exc:
            if logger:
                logger(f"[ERROR] {exc}")
            return []
    else:
        servers = [srv.strip().lower() for srv in servers if srv and srv.strip()]
        if logger:
            logger(f"[INFO] Loaded {len(servers)} servers from provided input.\n")

    if not servers:
        if logger:
            logger("[WARN] No servers supplied. Skipping fileset checks.")
        return []

    rsc = Rubrik(RSC_FQDN, CID, CSECRET, verbose=show_progress)

    all_filesets = fetch_all_filesets(rsc, logger=logger)

    if persist:
        os.makedirs(os.path.dirname(ALL_FILESETS_DUMP), exist_ok=True)
        with open(ALL_FILESETS_DUMP, "w", encoding="utf-8") as f:
            json.dump(all_filesets, f, indent=2)
        if logger:
            logger(f"[SAVE] Full fileset dump saved to {ALL_FILESETS_DUMP}\n")

    matches = []
    for srv in servers:
        srv_matches = [fs for fs in all_filesets if fuzzy_match(srv, fs)]
        for m in srv_matches:
            m["_requested_server"] = srv
        matches.extend(srv_matches)
        if logger and not srv_matches:
            logger(f"[WARN] No fileset match for {srv}")

    if logger:
        logger(f"[STEP] Checking last backup for {len(matches)} matched filesets...\n")

    results = []
    for idx, fs in enumerate(matches, 1):
        snappable_id = fs.get("snappable_id")
        requested_server = fs.get("_requested_server", fs.get("server", "n/a"))
        if not snappable_id:
            results.append({
                "server": requested_server,
                "type": fs.get("type", "FILESET"),
                "cluster": fs.get("cluster", "N/A"),
                "in_rubrik": "NO",
                "fileset": fs.get("fileset", "N/A"),
                "last_backup": "N/A",
                "status": "NO",
                "successful_backup_count": 0,
                "sla_domain": fs.get("sla", "N/A"),
            })
            continue

        status, dt_str, success_count = latest_snapshot_after_cutoff(rsc, snappable_id)
        results.append({
            "server": requested_server,
            "type": fs.get("type", "FILESET"),
            "cluster": fs.get("cluster", "N/A"),
            "in_rubrik": "YES",
            "fileset": fs.get("fileset", "N/A"),
            "last_backup": dt_str,
            "status": status,
            "successful_backup_count": success_count,
            "sla_domain": fs.get("sla", "N/A"),
        })
        if logger:
            logger(f"{requested_server:25} | In Rubrik: YES | Backup: {status:3} | {dt_str}")

    if show_summary and logger:
        total = len(servers)
        success = sum(1 for r in results if r["status"] == "YES")
        failed = total - success
        logger("=" * 55)
        logger(f"Total Servers : {total}")
        logger(f"Successful    : {success}")
        logger(f"Failed        : {failed}")
        logger("=" * 55)

    if persist:
        os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
        with open(OUT_FILE, "w") as f:
            json.dump(results, f, indent=2)
        if logger:
            logger(f"[SAVE] {OUT_FILE} written.")

    if logger:
        logger("[DONE] Fileset backup check complete.\n")

    return results


def main():
    try:
        run()
    except Exception as e:
        print(f"[FATAL] {e}")


if __name__ == "__main__":
    main()
