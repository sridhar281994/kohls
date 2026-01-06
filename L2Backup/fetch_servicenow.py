"""
Fetch incidents from ServiceNow (handles XML <response><result> structure)
If u_node is empty, extract VM/server name from the description.
Runner tag: senow
"""

import os
import re
import json
import requests
import xml.etree.ElementTree as ET

SERVICENOW_URL = os.getenv("SERVICENOW_URL")
SN_USER = os.getenv("SN_USER")
SN_PASS = os.getenv("SN_PASS")
NODE_FIELD = os.getenv("NODE_FIELD", "u_node").strip() or "u_node"

if not SERVICENOW_URL:
    print("[ERROR] SERVICENOW_URL variable not set!")
    raise SystemExit(1)

# Force XML output with display values
if "sysparm_display_value" not in SERVICENOW_URL:
    if "?" in SERVICENOW_URL:
        SERVICENOW_URL += "&sysparm_display_value=all"
    else:
        SERVICENOW_URL += "?sysparm_display_value=all"

print(f"[SNOW] Fetching incidents from:\n{SERVICENOW_URL}\n")

try:
    resp = requests.get(
        SERVICENOW_URL,
        auth=(SN_USER, SN_PASS),
        headers={"Accept": "application/xml"},
        timeout=60
    )
    resp.raise_for_status()
except Exception as e:
    print(f"[ERROR] Failed to query ServiceNow: {e}")
    raise SystemExit(1)

tickets = []
print(f"[INFO] Response content type: {resp.headers.get('Content-Type')}")

# ----------------------------------------------------------------------
# XML Parsing
# ----------------------------------------------------------------------
try:
    root = ET.fromstring(resp.text)
    results = root.findall(".//result")
    print(f"[DEBUG] Found {len(results)} <result> entries")

    for res in results:
        number = res.findtext("number") or "(unknown)"
        desc = (res.findtext("description") or "").strip().replace("\n", " ")

        node_elem = res.find(NODE_FIELD)
        state_elem = res.find("incident_state")

        node = None
        state = None

        # Try direct extraction from XML
        if node_elem is not None:
            node = node_elem.findtext("display_value") or node_elem.findtext("value")
        if state_elem is not None:
            state = state_elem.findtext("display_value") or state_elem.findtext("value")

        # Fallback: extract from description using regex
        if not node and desc:
            # Look for VM name in quotes or after "Object Name:"
            match = re.search(r"vSphere VM\s+'([\w\-]+)'", desc, re.IGNORECASE)
            if not match:
                match = re.search(r"Object Name:\s*([\w\-]+)", desc, re.IGNORECASE)
            if match:
                node = match.group(1)
                print(f"  [INFO] Extracted node '{node}' from description")

        # Default to Active if missing
        if not state:
            state = "Active"

        print(f"  [DEBUG] {number}: node={node}, state={state}")

        tickets.append({
            "number": number,
            "sys_id": res.findtext("sys_id"),
            "nodes": [node] if node else [],
            "incident_state": state,
            "description": desc
        })

except Exception as e:
    print(f"[ERROR] XML parse failed: {e}")
    print(resp.text[:800])
    raise SystemExit(1)

# ----------------------------------------------------------------------
# Save JSON output
# ----------------------------------------------------------------------
with open("tickets.json", "w") as f:
    json.dump(tickets, f, indent=2)

print(f"\n[SNOW] Saved {len(tickets)} incident(s) → tickets.json")
print("[SNOW] Done.")
