"""
Optimized Rubrik backup checker — marks backup as YES only if taken today or yesterday.
Anything older than 2 days is marked as NO.
Windows-safe, proxy-friendly, UTC-aware.
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

SERVER_LIST_PATH = os.getenv("SERVER_LIST_PATH", "L2Backup/serverslist1")
job_id = os.getenv("CI_JOB_NAME", "default").replace(" ", "_")
OUT_FILE = f"L2Backup/partial_results_{job_id}.json"
STATUS_WINDOW_DAYS = int(os.getenv("STATUS_WINDOW_DAYS", "2"))
COUNT_WINDOW_DAYS = int(os.getenv("COUNT_WINDOW_DAYS", "60"))

# Disable TLS warnings
requests.packages.urllib3.disable_warnings()

# ==========================================
# Rubrik GraphQL Client (10 s timeout)
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
            r = requests.post(url, data=payload, proxies=PROXIES, timeout=10)
            r.raise_for_status()
            if self.verbose:
                print(f"[OK] Authenticated successfully to {self.fqdn}\n")
            return r.json().get("access_token")
        except Exception as e:
            if self.verbose:
                print(f"[ERROR] Auth failed: {e}")
            raise SystemExit(1)

    def q(self, query, vars=None):
        """Single fast GraphQL query (10 s timeout)"""
        hdr = {"Authorization": f"Bearer {self.tok}", "Content-Type": "application/json"}
        try:
            r = requests.post(
                f"https://{self.fqdn}/api/graphql",
                json={"query": query, "variables": vars or {}},
                headers=hdr,
                proxies=PROXIES,
                timeout=10,
            )
            if r.status_code != 200 and self.verbose:
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
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except Exception:
        return None


def _format_snapshot_date(dt: Optional[datetime]) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC") if dt else "N/A"


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
            logger("[WARN] No servers supplied. Skipping Rubrik checks.")
        return []

    rsc = Rubrik(RSC_FQDN, CID, CSECRET, verbose=show_progress)

    if logger:
        logger("[STEP] Building Rubrik object index...")
    sla_data = rsc.q(gqls.slaListQuery, json.loads(gqls.slaListQueryVars))
    sla_edges = sla_data.get("data", {}).get("slaDomains", {}).get("edges", []) if sla_data else []
    idmap = {}

    for edge in sla_edges:
        sid = edge["node"]["id"]
        pobj = rsc.q(
            gqls.protectedObjectListQuery,
            json.loads(gqls.protectedObjectListQueryVars.replace("REPLACEME", sid)),
        )
        if not pobj or "data" not in pobj:
            continue
        for e in pobj["data"].get("slaProtectedObjects", {}).get("edges", []):
            node = e["node"]
            idmap[node["name"].lower()] = node["id"]

    if logger:
        logger(f"[OK] Indexed {len(idmap)} Rubrik objects.\n")

    results = []
    now = datetime.now(timezone.utc)

    for idx, srv in enumerate(servers, 1):
        if logger and idx % 50 == 0:
            logger(f"[HEARTBEAT] Processed {idx} servers so far...")

        rid = idmap.get(srv)
        if not rid:
            results.append({
                "server": srv,
                "in_rubrik": "NO",
                "last_backup": "N/A",
                "status": "NO",
                "successful_backup_count": 0,
                "sla_domain": "N/A",
            })
            continue

        snaps = rsc.q(
            gqls.odsSnapshotListfromSnappable,
            json.loads(gqls.odsSnapshotListfromSnappableVars.replace("REPLACEME", rid)),
        )
        conn = snaps.get("data", {}).get("snapshotsListConnection") if snaps else None
        edges = conn.get("edges", []) if conn else []
        if not edges:
            results.append({
                "server": srv,
                "in_rubrik": "YES",
                "last_backup": "N/A",
                "status": "NO",
                "successful_backup_count": 0,
                "sla_domain": "N/A",
            })
            continue

        latest = edges[0].get("node", {})
        latest_dt = _parse_snapshot_date(latest.get("date"))

        recent_status = False
        success_count = 0
        for edge in edges:
            node = edge.get("node", {})
            dt = _parse_snapshot_date(node.get("date"))
            if not dt:
                continue
            days_diff = (now.date() - dt.date()).days
            if days_diff < 0:
                continue
            if days_diff <= STATUS_WINDOW_DAYS:
                recent_status = True
            if days_diff <= COUNT_WINDOW_DAYS:
                success_count += 1

        backed_up = "YES" if recent_status else "NO"
        dt_str = _format_snapshot_date(latest_dt)
        sla_domain = latest.get("slaDomain", {}).get("name") or "N/A"

        result = {
            "server": srv,
            "in_rubrik": "YES",
            "last_backup": dt_str,
            "status": backed_up,
            "successful_backup_count": success_count,
            "sla_domain": sla_domain,
        }
        results.append(result)

        if logger:
            logger(f"{srv:25} | In Rubrik: YES | Backup: {backed_up:3} | {dt_str}")

    if show_summary and logger:
        total = len(results)
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
        logger("[DONE] Backup check complete.\n")

    return results


def main():
    run()


if __name__ == "__main__":
    main()
