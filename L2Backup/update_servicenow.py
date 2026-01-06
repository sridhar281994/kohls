"""
Update ServiceNow incidents based on Rubrik snapshot validation results.
Compatible with Windows runners (no Unicode characters).

FINAL:
- Reads combined_backup_report.json
- Maps servers to incidents using tickets.json
- Keeps incident_state logic EXACTLY the same as original script
"""

import os
import json
import requests
from urllib.parse import urlsplit

# ==========================================
# ENVIRONMENT VARIABLES
# ==========================================
SN_USER = os.getenv("SN_USER")
SN_PASS = os.getenv("SN_PASS")
SERVICENOW_URL = os.getenv("SERVICENOW_URL", "").strip()
SERVICENOW_INSTANCE = os.getenv("SERVICENOW_INSTANCE", "kohls")  # legacy fallback
ALLOWED_CLOSER = os.getenv("ALLOWED_CLOSER", "").strip() or None

COMBINED_REPORT = os.getenv(
    "COMBINED_REPORT_JSON",
    "L2Backup/combined_backup_report.json"
)
TICKETS_JSON = os.getenv(
    "SERVICENOW_TICKETS_JSON",
    "tickets.json"
)

# ==========================================
# VALIDATION
# ==========================================
if not os.path.exists(COMBINED_REPORT):
    raise SystemExit("[ERROR] combined_backup_report.json not found")

if not os.path.exists(TICKETS_JSON):
    raise SystemExit("[ERROR] tickets.json not found")

if not SN_USER or not SN_PASS:
    raise SystemExit("[ERROR] SN_USER/SN_PASS not set")

def _servicenow_base_url() -> str:
    """
    Prefer SERVICENOW_URL. If it's a full API/query URL, reduce to scheme+host.
    Fallback to SERVICENOW_INSTANCE legacy behavior.
    """
    if SERVICENOW_URL:
        parts = urlsplit(SERVICENOW_URL)
        if parts.scheme and parts.netloc:
            return f"{parts.scheme}://{parts.netloc}".rstrip("/")
        return SERVICENOW_URL.rstrip("/")
    return f"https://{SERVICENOW_INSTANCE}.service-now.com"

SN_BASE_URL = _servicenow_base_url()

# ==========================================
# LOAD FILES
# ==========================================
with open(COMBINED_REPORT, "r", encoding="utf-8") as f:
    combined = json.load(f)

results = combined.get("results", [])
if not results:
    print("[INFO] No Rubrik results found to update ServiceNow.")
    raise SystemExit(0)

with open(TICKETS_JSON, "r", encoding="utf-8") as f:
    tickets = json.load(f)

# ==========================================
# BUILD SERVER → INCIDENT MAP
# ==========================================
server_to_ticket = {}

for t in tickets:
    number = t.get("number")
    sys_id = t.get("sys_id")
    for node in t.get("nodes", []):
        if node:
            server_to_ticket[node.lower()] = {
                "number": number,
                "sys_id": sys_id,
            }

print("[UPDATE] Starting ServiceNow update process...")
updated, failed = 0, 0

# ==========================================
# PROCESS EACH RESULT
# ==========================================
for rec in results:
    server = rec.get("server")
    status_raw = rec.get("status")
    last_backup = rec.get("last_backup") or "N/A"
    sla_domain = rec.get("sla_domain") or "N/A"

    if not server:
        continue

    ticket = server_to_ticket.get(server.lower())
    if not ticket:
        print(f"[SKIP] No incident mapped for server {server}")
        continue

    number = ticket["number"]
    sys_id = ticket["sys_id"]

    # --------------------------------------
    # KEEP STATUS LOGIC SAME AS ORIGINAL
    # Original script expects:
    #   status == "OK" → Resolved
    # --------------------------------------
    status = "OK" if status_raw == "YES" else "FAILED"

    url = f"{SN_BASE_URL}/api/now/table/incident/{sys_id}"

    notes = (
        "Rubrik Backup Validation Result:\n"
        f"Node: {server}\n"
        f"SLA Domain: {sla_domain}\n"
        f"Status: {status}\n"
        f"Last Snapshot: {last_backup}\n"
    )

    payload = {
        "work_notes": notes,
        # DO NOT CHANGE THIS LOGIC
        "incident_state": "Resolved" if status == "OK" else "Active",
    }

    # Some ServiceNow instances require a specific "closer" identity.
    # Only attach it when resolving.
    if status == "OK" and ALLOWED_CLOSER:
        # These fields vary by instance; if one is not writable, ServiceNow will ignore/reject.
        payload["resolved_by"] = ALLOWED_CLOSER

    try:
        response = requests.patch(
            url,
            auth=(SN_USER, SN_PASS),
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload),
            timeout=30,
        )

        if response.status_code in (200, 204):
            print(f"[OK] Updated {number}: Status={status}")
            updated += 1
        else:
            print(f"[FAIL] {number}: HTTP {response.status_code}")
            failed += 1

    except Exception as e:
        print(f"[ERROR] Failed to update {number}: {e}")
        failed += 1

# ==========================================
# SUMMARY
# ==========================================
print(f"\n[SUMMARY] Updated: {updated} | Failed: {failed}")
print("[DONE] ServiceNow updates completed.")
