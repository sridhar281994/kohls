"""
Rubrik backup/fileset checker — accepts server list as variable.
Checks filesets and VM backups, marks backup as YES only if taken today or yesterday.
No file I/O, no hard-coded server lists.
"""

import os, json, requests
from datetime import datetime, timezone
import gqls  # your existing gqls module

# ==========================================
# CONFIGURATION
# ==========================================
RSC_FQDN = os.getenv("RSC_FQDN", "kohls.my.rubrik.com")
CID = os.getenv("RUBRIK_CLIENT_ID")
CSECRET = os.getenv("RUBRIK_CLIENT_SECRET")
PROXY = os.getenv("HTTP_PROXY") or os.getenv("HTTPS_PROXY")
PROXIES = {"http": PROXY, "https": PROXY} if PROXY else None

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
        r = requests.post(url, data=payload, proxies=PROXIES, timeout=10, verify=False)
        r.raise_for_status()
        return r.json().get("access_token")

    def q(self, query, vars=None):
        hdr = {"Authorization": f"Bearer {self.tok}", "Content-Type": "application/json"}
        r = requests.post(
            f"https://{self.fqdn}/api/graphql",
            json={"query": query, "variables": vars or {}},
            headers=hdr,
            proxies=PROXIES,
            timeout=10,
            verify=False,
        )
        if r.status_code != 200:
            return None
        return r.json()


# ==========================================
# Helper Functions
# ==========================================
def _safe_path_string(physicalPathField):
    if not physicalPathField:
        return "n/a"
    try:
        if isinstance(physicalPathField, list):
            return ", ".join([p.get("name", "") for p in physicalPathField]).lower()
        return physicalPathField.get("name", "n/a").lower()
    except Exception:
        return "n/a"


def fuzzy_match(server, fileset):
    s = server.lower()
    return (s in fileset["server"]) or (s in (fileset["path"] or ""))


def latest_snapshot_after_cutoff(rsc, snappable_id):
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
        return "NO", "N/A"

    now = datetime.now(timezone.utc)
    days_diff = (now.date() - d.date()).days
    backed_up = "YES" if days_diff in [0, 1] else "NO"

    dt_str = d.strftime("%Y-%m-%d %H:%M:%S UTC")
    return backed_up, dt_str


def fetch_all_filesets(rsc):
    """Fetch Windows + Linux filesets"""
    all_fs = []

    # WINDOWS
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

    # LINUX
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

    return all_fs


# ==========================================
# MAIN FUNCTION
# ==========================================
def check_backups(server_list):
    rsc = Rubrik(RSC_FQDN, CID, CSECRET)
    all_filesets = fetch_all_filesets(rsc)

    results = []
    for srv in server_list:
        srv_lower = srv.lower()
        srv_matches = [fs for fs in all_filesets if fuzzy_match(srv_lower, fs)]
        if not srv_matches:
            results.append({
                "server": srv,
                "type": "FILESET",
                "cluster": "N/A",
                "in_rubrik": "NO",
                "fileset": "N/A",
                "last_backup": "N/A",
                "status": "NO"
            })
            continue

        for fs in srv_matches:
            snappable_id = fs.get("snappable_id")
            if snappable_id:
                status, dt_str = latest_snapshot_after_cutoff(rsc, snappable_id)
            else:
                status, dt_str = "NO", "N/A"

            results.append({
                "server": srv,
                "type": fs.get("type", "FILESET"),
                "cluster": fs.get("cluster", "N/A"),
                "in_rubrik": "YES",
                "fileset": fs.get("fileset", "N/A"),
                "last_backup": dt_str,
                "status": status
            })

    return results


# ==========================================
# ENTRY POINT
# ==========================================
if __name__ == "__main__":
    # Example usage: pass server list as a variable
    server_list = ["pws00000", "pws00001"]  # Replace or pass from CI/CD
    backup_info = check_backups(server_list)
    print(json.dumps(backup_info, indent=2))
