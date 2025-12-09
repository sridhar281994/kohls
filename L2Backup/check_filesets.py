"""
Optimized Rubrik fileset backup checker — checks only today's and yesterday's backups.
If the latest backup is older than 2 days, it's marked as "NO".
"""

import os, json, requests
from datetime import datetime, timedelta, timezone
import gqls

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

# Disable TLS warnings
requests.packages.urllib3.disable_warnings()

# ==========================================
# Rubrik GraphQL Client
# ==========================================
class Rubrik:
    def __init__(self, fqdn, cid, csecret):
        self.fqdn = fqdn
        self.cid = cid
        self.csecret = csecret
        self.tok = self._auth()

    def _auth(self):
        url = f"https://{self.fqdn}/api/client_token"
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.cid,
            "client_secret": self.csecret,
        }
        try:
            print(f"[AUTH] Connecting to {self.fqdn} ...")
            r = requests.post(url, data=payload, proxies=PROXIES, timeout=10, verify=False)
            r.raise_for_status()
            print(f"[OK] Authenticated successfully to {self.fqdn}\n")
            return r.json().get("access_token")
        except Exception as e:
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
            if r.status_code != 200:
                print(f"[WARN] GraphQL {r.status_code} {r.reason}")
                try:
                    print(r.text[:400])
                except Exception:
                    pass
                return None
            return r.json()
        except requests.exceptions.Timeout:
            print("[TIMEOUT] Rubrik query timed out (10s).")
            return None
        except Exception as e:
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
        return "NO", "N/A"

    latest = edges[0].get("node", {})
    dt = latest.get("date")
    try:
        d = datetime.fromisoformat(dt.replace("Z", "+00:00"))
    except Exception:
        d = None

    if not d:
        return "NO", "N/A"

    # --- New Logic: Accept only today's or yesterday's backup ---
    now = datetime.now(timezone.utc)
    days_diff = (now.date() - d.date()).days
    backed_up = "YES" if days_diff in [0, 1] else "NO"

    dt_str = d.strftime("%Y-%m-%d %H:%M:%S UTC")
    return backed_up, dt_str


def fetch_all_filesets(rsc):
    """Pull all Windows + Linux filesets"""
    print("[STEP] Fetching all filesets from Rubrik CDM...")

    all_fs = []

    # WINDOWS
    print("[STEP] Collecting all WINDOWS_FILESET filesets ...")
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
    print(f"[OK] Found {len([x for x in all_fs if x['type']=='WINDOWS_FILESET'])} WINDOWS_FILESET filesets.")

    # LINUX
    print("[STEP] Collecting all LINUX_FILESET filesets ...")
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
    print(f"[OK] Found {len([x for x in all_fs if x['type']=='LINUX_FILESET'])} LINUX_FILESET filesets.")
    print(f"[INFO] Total filesets fetched: {len(all_fs)}")
    return all_fs


# ==========================================
# MAIN
# ==========================================
def main():
    # Load servers
    if not os.path.exists(SERVER_LIST_PATH):
        print(f"[ERROR] Server list not found: {SERVER_LIST_PATH}")
        return
    with open(SERVER_LIST_PATH) as f:
        servers = [x.strip().lower() for x in f if x.strip()]
    print(f"[INFO] Loaded {len(servers)} servers from {SERVER_LIST_PATH}\n")

    rsc = Rubrik(RSC_FQDN, CID, CSECRET)

    # Fetch filesets once
    all_filesets = fetch_all_filesets(rsc)

    # Save full dump for artifacts
    os.makedirs(os.path.dirname(ALL_FILESETS_DUMP), exist_ok=True)
    with open(ALL_FILESETS_DUMP, "w", encoding="utf-8") as f:
        json.dump(all_filesets, f, indent=2)
    print(f"[SAVE] Full fileset dump saved to {ALL_FILESETS_DUMP}\n")

    # Match servers
    matches = []
    for srv in servers:
        srv_matches = [fs for fs in all_filesets if fuzzy_match(srv, fs)]
        for m in srv_matches:
            m["_requested_server"] = srv
        matches.extend(srv_matches)
        if not srv_matches:
            print(f"[WARN] No fileset match for {srv}")

    print(f"[STEP] Checking last backup for {len(matches)} matched filesets...\n")

    # Check snapshot recency
    results = []
    for idx, fs in enumerate(matches, 1):
        snappable_id = fs.get("snappable_id")
        if not snappable_id:
            results.append({
                "server": fs.get("_requested_server", fs.get("server", "n/a")),
                "type": fs.get("type", "FILESET"),
                "cluster": fs.get("cluster", "N/A"),
                "in_rubrik": "NO",
                "fileset": fs.get("fileset", "N/A"),
                "last_backup": "N/A",
                "status": "NO"
            })
            continue

        status, dt_str = latest_snapshot_after_cutoff(rsc, snappable_id)
        results.append({
            "server": fs.get("_requested_server", fs.get("server", "n/a")),
            "type": fs.get("type", "FILESET"),
            "cluster": fs.get("cluster", "N/A"),
            "in_rubrik": "YES",
            "fileset": fs.get("fileset", "N/A"),
            "last_backup": dt_str,
            "status": status
        })
        print(f"{fs.get('_requested_server', fs.get('server','n/a')):25} | In Rubrik: YES | Backup: {status:3} | {dt_str}")

    # Summary
    total = len(servers)
    success = sum(1 for r in results if r["status"] == "YES")
    failed = total - success
    print("=" * 55)
    print(f"Total Servers : {total}")
    print(f"Successful    : {success}")
    print(f"Failed        : {failed}")
    print("=" * 55)

    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    with open(OUT_FILE, "w") as f:
        json.dump(results, f, indent=2)
    print(f"[SAVE] {OUT_FILE} written.\n[DONE] Fileset backup check complete.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[FATAL] {e}")
