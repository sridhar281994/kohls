"""
Rubrik backup checker — latest backup per server
Includes SLA domain, type (fileset/vmsnapshot), cluster.
"""

import os, json, requests
from datetime import datetime, timezone
import gqls  # Make sure gqls.py contains necessary GraphQL queries

# ==========================================
# CONFIGURATION
# ==========================================
RSC_FQDN = os.getenv("RSC_FQDN", "kohls.my.rubrik.com")
CID = os.getenv("RUBRIK_CLIENT_ID")
CSECRET = os.getenv("RUBRIK_CLIENT_SECRET")
PROXY = os.getenv("HTTP_PROXY") or os.getenv("HTTPS_PROXY")
PROXIES = {"http": PROXY, "https": PROXY} if PROXY else None

OUT_FILE = os.getenv("OUT_FILE", "L2Backup/backup_results.json")

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
                detail = _describe_graphql_error(r)
                if detail:
                    print(detail)
                return None
            data = r.json()
            if isinstance(data, dict) and data.get("errors"):
                print("[WARN] GraphQL returned errors:")
                for err in data["errors"]:
                    msg = err.get("message", "Unknown error")
                    path = ".".join(err.get("path", [])) if err.get("path") else ""
                    loc = f" (path: {path})" if path else ""
                    print(f"  - {msg}{loc}")
                return None
            return data
        except Exception as e:
            print(f"[ERROR] GraphQL query failed: {e}")
            return None

# ==========================================
# Helper Functions
# ==========================================
def _describe_graphql_error(response):
    try:
        data = response.json()
    except ValueError:
        return response.text.strip()

    errors = data.get("errors")
    if errors:
        summary = []
        for err in errors:
            msg = err.get("message", "Unknown error")
            path = ".".join(err.get("path", [])) if err.get("path") else ""
            summary.append(f"- {msg}{f' (path: {path})' if path else ''}")
        return "\n".join(summary)

    return json.dumps(data, indent=2) if data else None

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
    return (s in fileset["server"]) or (s in (fileset.get("path") or ""))

def latest_snapshot_after_cutoff(rsc, snappable_id):
    try:
        vars_json = json.loads(gqls.odsSnapshotListfromSnappableVars.replace("REPLACEME", snappable_id))
    except Exception:
        vars_json = {"snappableId": snappable_id}
    snaps = rsc.q(gqls.odsSnapshotListfromSnappable, vars_json)
    edges = snaps.get("data", {}).get("snapshotsListConnection", {}).get("edges", []) if snaps else []
    if not edges:
        return "NO", "N/A", None

    latest = edges[0].get("node", {})
    dt = latest.get("date")
    try:
        d = datetime.fromisoformat(dt.replace("Z", "+00:00"))
    except Exception:
        d = None

    if not d:
        return "NO", "N/A", None

    now = datetime.now(timezone.utc)
    days_diff = (now.date() - d.date()).days
    backed_up = "YES" if days_diff in [0, 1] else "NO"
    dt_str = d.strftime("%Y-%m-%d %H:%M:%S UTC")
    return backed_up, dt_str, d

def fetch_all_filesets(rsc):
    all_fs = []
    # WINDOWS FILESETS
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
                "type": "fileset",
            })
    # LINUX FILESETS
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
                "type": "fileset",
            })
    return all_fs

def fetch_all_vmsnapshots(rsc):
    all_vms = []
    vm_data = rsc.q(gqls.vmQuery, json.loads(gqls.vmVars))
    vm_edges = vm_data.get("data", {}).get("vms", {}).get("edges", []) if vm_data else []
    for e in vm_edges:
        node = e.get("node", {})
        all_vms.append({
            "snappable_id": node.get("id"),
            "server": (node.get("name") or "n/a").lower(),
            "cluster": node.get("cluster", {}).get("name", "N/A"),
            "sla": (node.get("effectiveSlaDomain", {}) or {}).get("name", "N/A"),
            "type": "vmsnapshot",
        })
    return all_vms

# ==========================================
# MAIN
# ==========================================
def main():
    server_env = os.getenv("serverlist", "")
    if not server_env.strip():
        print("[ERROR] serverlist variable is empty.")
        return
    servers = [s.strip().lower() for s in server_env.split(",") if s.strip()]
    if not servers:
        print("[ERROR] serverlist variable contains no valid entries.")
        return

    rsc = Rubrik(RSC_FQDN, CID, CSECRET)

    all_filesets = fetch_all_filesets(rsc)
    all_vms = fetch_all_vmsnapshots(rsc)
    all_snappables = all_filesets + all_vms

    results = []

    for srv in servers:
        srv_matches = [fs for fs in all_snappables if fuzzy_match(srv, fs)]
        if not srv_matches:
            continue

        latest_backup = None
        latest_obj = None
        for m in srv_matches:
            snappable_id = m.get("snappable_id")
            if snappable_id:
                status, dt_str, dt_obj = latest_snapshot_after_cutoff(rsc, snappable_id)
            else:
                status, dt_str, dt_obj = "NO", "N/A", None

            if dt_obj and (latest_backup is None or dt_obj > latest_backup):
                latest_backup = dt_obj
                latest_obj = {
                    "server": srv,
                    "in_rubrik": "YES",
                    "backup_status": status,
                    "last_backup": dt_str,
                    "type": m.get("type"),
                    "sla": m.get("sla"),
                    "cluster": m.get("cluster"),
                }

        if latest_obj:
            results.append(latest_obj)
            print(f"{srv:25} | In Rubrik: YES | Backup: {latest_obj['backup_status']:3} | "
                  f"{latest_obj['last_backup']} | type: {latest_obj['type']} | "
                  f"SLA: {latest_obj['sla']} | Cluster: {latest_obj['cluster']}")

    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    with open(OUT_FILE, "w") as f:
        json.dump(results, f, indent=2)
    print(f"[SAVE] {OUT_FILE} written.\n[DONE] Backup check complete.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[FATAL] {e}")
