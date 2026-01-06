"""
Check VM backup status and trigger backup if needed.
Reads tickets.json, queries Rubrik, triggers backup if > 24h, saves results.
"""

import os
import json
import requests
import urllib3
from datetime import datetime, timedelta, timezone

# Disable insecure warning for self-signed certs
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Config
RUBRIK_URL = os.getenv("RUBRIK_CLUSTER_ADDRESS")
RUBRIK_TOKEN = os.getenv("RUBRIK_API_TOKEN")
RUBRIK_USER = os.getenv("RUBRIK_USERNAME")
RUBRIK_PASS = os.getenv("RUBRIK_PASSWORD")

TICKETS_JSON = "tickets.json"
OUTPUT_JSON = "L2Backup/combined_backup_report.json"

if not RUBRIK_URL:
    print("[ERROR] RUBRIK_CLUSTER_ADDRESS not set")
    raise SystemExit(1)

# Ensure URL has protocol
if not RUBRIK_URL.startswith("http"):
    RUBRIK_URL = f"https://{RUBRIK_URL}"

GRAPHQL_ENDPOINT = f"{RUBRIK_URL}/api/v1/graphql"

def get_headers():
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if RUBRIK_TOKEN:
        headers["Authorization"] = f"Bearer {RUBRIK_TOKEN}"
    elif RUBRIK_USER and RUBRIK_PASS:
        # Basic auth is handled in request, but token is preferred
        pass 
    return headers

def get_auth():
    if not RUBRIK_TOKEN and RUBRIK_USER and RUBRIK_PASS:
        return (RUBRIK_USER, RUBRIK_PASS)
    return None

def run_query(query, variables=None):
    try:
        resp = requests.post(
            GRAPHQL_ENDPOINT,
            json={"query": query, "variables": variables},
            headers=get_headers(),
            auth=get_auth(),
            verify=False,
            timeout=30
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"[ERROR] GraphQL query failed: {e}")
        return None

# -----------------------------------------------------------------------------
# QUERIES
# -----------------------------------------------------------------------------

QUERY_VM_SEARCH = """
query VSphereVmListQuery($name: String!) {
  vsphereVmConnection(filter: [{field: NAME, texts: [$name]}]) {
    edges {
      node {
        id
        name
        effectiveSlaDomain {
          id
          name
        }
      }
    }
  }
}
"""

QUERY_SNAPSHOTS = """
query SnapshotsListSingleQuery($snappableId: String!) {
  snapshotOfASnappableConnection(workloadId: $snappableId, first: 1, sortBy: CREATION_TIME, sortOrder: DESC) {
    edges {
      node {
        id
        date
        ... on CdmSnapshot {
            snapshotRetentionInfo {
              localInfo {
                expirationTime
              }
            }
        }
      }
    }
  }
}
"""

MUTATION_TRIGGER_BACKUP = """
mutation CreateOnDemandSnapshot($id: String!) {
  createOnDemandSnapshot(id: $id, config: {}) {
    id
    status
    links {
      href
      rel
    }
  }
}
"""

# -----------------------------------------------------------------------------
# MAIN LOGIC
# -----------------------------------------------------------------------------

def main():
    if not os.path.exists(TICKETS_JSON):
        print(f"[ERROR] {TICKETS_JSON} not found")
        raise SystemExit(1)

    with open(TICKETS_JSON, "r") as f:
        tickets = json.load(f)

    # Extract unique servers
    servers = set()
    for t in tickets:
        for node in t.get("nodes", []):
            if node:
                servers.add(node)
    
    print(f"[INFO] Found {len(servers)} servers to check.")
    
    results = []

    for server in servers:
        print(f"--- Checking {server} ---")
        
        # 1. Find VM ID
        res = run_query(QUERY_VM_SEARCH, {"name": server})
        if not res or "data" not in res:
            print(f"  [WARN] Failed to search VM")
            results.append({"server": server, "status": "FAILED", "note": "API Error"})
            continue

        edges = res["data"]["vsphereVmConnection"]["edges"]
        if not edges:
            print(f"  [WARN] VM not found in Rubrik")
            results.append({"server": server, "status": "FAILED", "note": "VM Not Found"})
            continue
            
        vm_node = edges[0]["node"]
        vm_id = vm_node["id"]
        sla_name = vm_node.get("effectiveSlaDomain", {}).get("name", "N/A")
        print(f"  [INFO] Found VM ID: {vm_id}, SLA: {sla_name}")

        # 2. Check Snapshots
        snap_res = run_query(QUERY_SNAPSHOTS, {"snappableId": vm_id})
        last_backup_str = "N/A"
        needs_backup = False
        
        if snap_res and "data" in snap_res:
            snap_edges = snap_res["data"]["snapshotOfASnappableConnection"]["edges"]
            if snap_edges:
                last_snap = snap_edges[0]["node"]
                date_str = last_snap["date"] # e.g. 2023-10-27T10:00:00Z
                last_backup_str = date_str
                
                # Check age
                try:
                    # Handle Z
                    dt_str = date_str.replace("Z", "+00:00")
                    last_dt = datetime.fromisoformat(dt_str)
                    now = datetime.now(timezone.utc)
                    age = now - last_dt
                    
                    if age > timedelta(hours=24):
                        print(f"  [WARN] Last backup was {age} ago (> 24h)")
                        needs_backup = True
                    else:
                        print(f"  [OK] Last backup was {age} ago")
                except Exception as e:
                    print(f"  [ERROR] Date parse error: {e}")
                    needs_backup = True
            else:
                print(f"  [WARN] No snapshots found")
                needs_backup = True
        
        status = "YES" # Default to YES/OK if recent backup exists
        
        if needs_backup:
            status = "NO" # Initially NO
            print(f"  [ACTION] Triggering backup...")
            # Trigger
            trig_res = run_query(MUTATION_TRIGGER_BACKUP, {"id": vm_id})
            
            if trig_res and "data" in trig_res and trig_res["data"]["createOnDemandSnapshot"]:
                status_obj = trig_res["data"]["createOnDemandSnapshot"]
                req_status = status_obj.get("status")
                print(f"  [INFO] Backup Triggered. Status: {req_status}")
                # Update status to indicate we triggered it
                # Logic: If triggered, we might consider it "Handled" or "In Progress"
                # But existing update_servicenow expects "YES" for Resolved.
                # If we triggered it, it's not "Done" yet.
                # User says: "Capture the backup result and update the same JSON artifact... Update ServiceNow... If valid backup exists... close."
                # If we JUST triggered it, it doesn't exist yet. So we can't close it.
                status = "TRIGGERED"
            elif trig_res and "errors" in trig_res:
                err_msg = json.dumps(trig_res["errors"])
                print(f"  [ERROR] Trigger failed: {err_msg}")
                # Check if "in progress"
                if "progress" in err_msg.lower() or "already" in err_msg.lower():
                     print("  [INFO] Backup already in progress (inferred)")
                     status = "IN_PROGRESS"
                else:
                     status = "FAILED"
            else:
                status = "FAILED"

        # Map to output format
        # update_servicenow.py logic: status="OK" if status_raw=="YES" else "FAILED"
        # If I output "TRIGGERED" or "IN_PROGRESS", update_servicenow will map it to "FAILED" (Active). This is correct.
        
        results.append({
            "server": server,
            "status": status, # YES, NO, TRIGGERED, IN_PROGRESS, FAILED
            "last_backup": last_backup_str,
            "sla_domain": sla_name
        })

    # Save
    out_data = {"results": results}
    with open(OUTPUT_JSON, "w") as f:
        json.dump(out_data, f, indent=2)
    
    print(f"[DONE] Saved results to {OUTPUT_JSON}")

if __name__ == "__main__":
    main()
