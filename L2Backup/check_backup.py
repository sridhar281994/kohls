"""
Rubrik backup/fileset checker — accepts server list as variable.
Checks filesets and VM backups, marks backup as YES only if taken today or yesterday.
No file I/O, no hard-coded server lists.
"""

import os, json, requests, logging
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

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s | %(message)s",
)
LOGGER = logging.getLogger("backup_checker")


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


def _normalize_hostname(name):
    if not name:
        return ""
    name = name.strip().lower()
    return name.split(".")[0]


def match_weight(server, entry):
    """
    Return a weight indicating how strong the match is between the requested
    server and a Rubrik object. Higher weight values win, and 0 means no match.
    3 = exact hostname match (case-insensitive, ignoring domain suffix)
    2 = partial hostname match (substring)
    1 = match via fileset path text
    """
    target = _normalize_hostname(server)
    if not target:
        return 0

    entry_host_norm = entry.get("server_norm", "")
    entry_host_lower = entry.get("server_lower", "")

    if target == entry_host_norm or target == entry_host_lower:
        return 3
    if target in entry_host_lower:
        return 2

    path = entry.get("path") or ""
    if target and target in path:
        return 1

    return 0


def latest_snapshot_after_cutoff(rsc, snappable_id):
    try:
        vars_json = json.loads(gqls.odsSnapshotListfromSnappableVars.replace("REPLACEME", snappable_id))
    except Exception:
        vars_json = {"snappableId": snappable_id}
    LOGGER.debug("Querying snapshots for snappable_id=%s", snappable_id)
    snaps = rsc.q(gqls.odsSnapshotListfromSnappable, vars_json)
    conn = snaps.get("data", {}).get("snapshotsListConnection") if snaps else None
    edges = (conn or {}).get("edges", []) if conn else []
    if not edges:
        LOGGER.debug("No snapshots returned for snappable_id=%s", snappable_id)
        return "NO", "N/A", None

    latest = edges[0].get("node", {})
    dt = latest.get("date")
    try:
        d = datetime.fromisoformat(dt.replace("Z", "+00:00"))
    except Exception:
        LOGGER.warning("Snapshot date parse failed for snappable_id=%s raw=%s", snappable_id, dt)
        return "NO", "N/A", None

    now = datetime.now(timezone.utc)
    days_diff = (now.date() - d.date()).days
    backed_up = "YES" if days_diff in [0, 1] else "NO"

    dt_str = d.strftime("%Y-%m-%d %H:%M:%S UTC")
    return backed_up, dt_str, d


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
            obj_name = (child.get("name") or "n/a").strip()
            all_fs.append({
                "snappable_id": child.get("id"),
                "object_name": obj_name or "n/a",
                "server_lower": obj_name.lower(),
                "server_norm": _normalize_hostname(obj_name),
                "fileset": fs_name,
                "cluster": cluster,
                "sla": (child.get("effectiveSlaDomain", {}) or {}).get("name", "N/A"),
                "path": _safe_path_string(child.get("physicalPath")),
                "type": "FILESYSTEM",
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
            obj_name = (child.get("name") or "n/a").strip()
            all_fs.append({
                "snappable_id": child.get("id"),
                "object_name": obj_name or "n/a",
                "server_lower": obj_name.lower(),
                "server_norm": _normalize_hostname(obj_name),
                "fileset": fs_name,
                "cluster": cluster,
                "sla": (child.get("effectiveSlaDomain", {}) or {}).get("name", "N/A"),
                "path": _safe_path_string(child.get("physicalPath")),
                "type": "FILESYSTEM",
            })

    return all_fs


def fetch_all_vms(rsc):
    """Fetch VMware VMs"""
    all_vms = []
    vm_vars = json.loads(gqls.vmVars)
    vm_data = rsc.q(gqls.vmQuery, vm_vars)
    vm_edges = vm_data.get("data", {}).get("vsphereVmConnection", {}).get("edges", []) if vm_data else []
    for e in vm_edges:
        node = e.get("node", {})
        obj_name = (node.get("name") or "n/a").strip()
        all_vms.append({
            "snappable_id": node.get("id"),
            "object_name": obj_name or "n/a",
            "server_lower": obj_name.lower(),
            "server_norm": _normalize_hostname(obj_name),
            "fileset": "N/A",
            "cluster": node.get("cluster", {}).get("name", "N/A"),
            "sla": (node.get("effectiveSlaDomain", {}) or {}).get("name", "N/A"),
            "path": "",
            "type": "VM",
        })

    return all_vms


# ==========================================
# MAIN FUNCTION
# ==========================================
def check_backups(server_list):
    rsc = Rubrik(RSC_FQDN, CID, CSECRET)
    filesets = fetch_all_filesets(rsc)
    vms = fetch_all_vms(rsc)
    all_objects = filesets + vms
    LOGGER.info("Loaded %d filesets and %d VMs (total=%d)", len(filesets), len(vms), len(all_objects))

    results = []
    for srv in server_list:
        LOGGER.info("Evaluating server=%s", srv)
        matches_with_weight = []
        for robj in all_objects:
            weight = match_weight(srv, robj)
            if weight:
                matches_with_weight.append((robj, weight))
                LOGGER.debug(
                    "Match weight=%s server=%s candidate=%s fileset=%s cluster=%s path=%s",
                    weight,
                    srv,
                    robj.get("object_name"),
                    robj.get("fileset"),
                    robj.get("cluster"),
                    robj.get("path"),
                )

        if not matches_with_weight:
            LOGGER.warning("No Rubrik objects matched server=%s", srv)
            results.append({
                "server": srv,
                "type": "N/A",
                "cluster": "N/A",
                "in_rubrik": "NO",
                "fileset_or_vm": "N/A",
                "last_backup": "N/A",
                "status": "NO",
                "sla": "N/A"
            })
            continue

        matches_grouped = {}
        for obj, weight in matches_with_weight:
            obj_type = obj.get("type", "UNKNOWN")
            matches_grouped.setdefault(obj_type, []).append((obj, weight))

        for obj_type, entries in matches_grouped.items():
            LOGGER.debug("Server=%s evaluating type=%s matches=%d", srv, obj_type, len(entries))
            best_entry = None
            best_weight = -1
            best_dt = None
            best_status = "NO"
            best_dt_str = "N/A"

            for obj, weight in entries:
                snappable_id = obj.get("snappable_id")
                status, dt_str, dt_obj = (latest_snapshot_after_cutoff(rsc, snappable_id) if snappable_id else ("NO", "N/A", None))
                LOGGER.debug(
                    "Server=%s type=%s candidate=%s weight=%s snapshot_status=%s last_backup=%s",
                    srv,
                    obj_type,
                    obj.get("object_name"),
                    weight,
                    status,
                    dt_str,
                )

                if best_entry is None:
                    best_entry = obj
                    best_weight = weight
                    best_dt = dt_obj
                    best_status = status
                    best_dt_str = dt_str
                    continue

                replace = False
                if weight > best_weight:
                    replace = True
                elif weight == best_weight:
                    if dt_obj and (best_dt is None or dt_obj > best_dt):
                        replace = True

                if replace:
                    best_entry = obj
                    best_weight = weight
                    best_dt = dt_obj
                    best_status = status
                    best_dt_str = dt_str

            if not best_entry:
                continue

            results.append({
                "server": srv,
                "type": obj_type,
                "cluster": best_entry.get("cluster", "N/A"),
                "in_rubrik": "YES",
                "fileset_or_vm": best_entry.get("object_name", "N/A"),
                "fileset": best_entry.get("fileset", "N/A"),
                "last_backup": best_dt_str,
                "status": best_status,
                "sla": best_entry.get("sla", "N/A"),
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
