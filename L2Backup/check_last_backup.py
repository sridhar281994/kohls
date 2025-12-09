"""
Optimized Rubrik backup checker — marks backup as YES only if taken today or yesterday.
Anything older than 2 days is marked as NO.
Windows-safe, proxy-friendly, UTC-aware.
"""

import os, json, time, requests
from datetime import datetime, timezone
import gqls

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

# Disable TLS warnings
requests.packages.urllib3.disable_warnings()

# ==========================================
# Rubrik GraphQL Client (10 s timeout)
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
            r = requests.post(url, data=payload, proxies=PROXIES, timeout=10)
            r.raise_for_status()
            print(f"[OK] Authenticated successfully to {self.fqdn}\n")
            return r.json().get("access_token")
        except Exception as e:
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
            if r.status_code != 200:
                print(f"[WARN] GraphQL {r.status_code} {r.reason}")
                return None
            return r.json()
        except requests.exceptions.Timeout:
            print("[TIMEOUT] Rubrik query timed out (10 s limit).")
            return None
        except Exception as e:
            print(f"[ERROR] GraphQL query failed: {e}")
            return None


# ==========================================
# MAIN
# ==========================================
def main():
    if not os.path.exists(SERVER_LIST_PATH):
        print(f"[ERROR] Server list not found: {SERVER_LIST_PATH}")
        return

    with open(SERVER_LIST_PATH) as f:
        servers = [x.strip().lower() for x in f if x.strip()]
    print(f"[INFO] Loaded {len(servers)} servers from {SERVER_LIST_PATH}\n")

    rsc = Rubrik(RSC_FQDN, CID, CSECRET)

    # STEP 1 – Build Rubrik object index
    print("[STEP] Building Rubrik object index...")
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

    print(f"[OK] Indexed {len(idmap)} Rubrik objects.\n")

    # STEP 2 – Check each server
    results = []
    now = datetime.now(timezone.utc)

    for idx, srv in enumerate(servers, 1):
        if idx % 50 == 0:
            print(f"[HEARTBEAT] Processed {idx} servers so far...")

        rid = idmap.get(srv)
        if not rid:
            results.append({"server": srv, "in_rubrik": "NO", "last_backup": "N/A", "status": "NO"})
            continue

        snaps = rsc.q(
            gqls.odsSnapshotListfromSnappable,
            json.loads(gqls.odsSnapshotListfromSnappableVars.replace("REPLACEME", rid)),
        )
        conn = snaps.get("data", {}).get("snapshotsListConnection") if snaps else None
        edges = conn.get("edges", []) if conn else []
        if not edges:
            results.append({"server": srv, "in_rubrik": "YES", "last_backup": "N/A", "status": "NO"})
            continue

        latest = edges[0]["node"]
        date = latest.get("date")
        try:
            d = datetime.fromisoformat(date.replace("Z", "+00:00"))
        except Exception:
            d = None

        # ✅ Accept backup only if it's from today or yesterday
        if d:
            days_diff = (now.date() - d.date()).days
            backed_up = "YES" if days_diff in [0, 1] else "NO"
        else:
            backed_up = "NO"

        dt_str = d.strftime("%Y-%m-%d %H:%M:%S UTC") if d else "N/A"

        results.append({
            "server": srv,
            "in_rubrik": "YES",
            "last_backup": dt_str,
            "status": backed_up
        })
        print(f"{srv:25} | In Rubrik: YES | Backup: {backed_up:3} | {dt_str}")

    # STEP 3 – Summary
    total = len(results)
    success = sum(1 for r in results if r["status"] == "YES")
    failed = total - success
    print("=" * 55)
    print(f"Total Servers : {total}")
    print(f"Successful    : {success}")
    print(f"Failed        : {failed}")
    print("=" * 55)

    # STEP 4 – Save results
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    with open(OUT_FILE, "w") as f:
        json.dump(results, f, indent=2)
    print(f"[SAVE] {OUT_FILE} written.\n[DONE] Backup check complete.")


if __name__ == "__main__":
    main()
