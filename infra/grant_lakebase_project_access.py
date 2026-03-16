#!/usr/bin/env python3
"""
Grant App Service Principals access to the Lakebase Autoscaling project.

Uses the SDK share_project API to grant CAN_USE on the Lakebase project
to both App SPs, enabling them to call generate_database_credential().
"""
import os
import sys
import json
import requests
from pathlib import Path
from dotenv import load_dotenv

root = Path(__file__).parent.parent
load_dotenv(root / "generated_config.env", override=True)

host = os.environ["DATABRICKS_HOST"].rstrip("/")
token = os.environ["DATABRICKS_TOKEN"]
project_id = os.environ["LAKEBASE_INSTANCE"]
mobile_sp = os.environ.get("MOBILE_APP_SP_CLIENT_ID", "")
mobile_sp_id = os.environ.get("MOBILE_APP_SP_ID", "")
dashboard_sp = os.environ.get("DASHBOARD_APP_SP_CLIENT_ID", "")
dashboard_sp_id = os.environ.get("DASHBOARD_APP_SP_ID", "")

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
}

print("=== Attempting to grant Lakebase project access to App SPs ===")
print(f"  Project: {project_id}")
print(f"  Mobile SP:    {mobile_sp} (numeric: {mobile_sp_id})")
print(f"  Dashboard SP: {dashboard_sp} (numeric: {dashboard_sp_id})")
print()

# Method 1: Try the postgres share project API
print("--- Method 1: SDK share_project ---")
try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient(host=os.environ["DATABRICKS_HOST"], token=token)

    # Check if there's a share/permissions method
    pg = w.postgres
    methods = [m for m in dir(pg) if not m.startswith("_")]
    print(f"  Available w.postgres methods: {methods}")
except Exception as e:
    print(f"  SDK method check failed: {e}")

# Method 2: Try various REST API paths for Lakebase Autoscaling permissions
print("\n--- Method 2: REST API permission endpoints ---")
api_paths = [
    f"/api/2.0/permissions/postgres-projects/{project_id}",
    f"/api/2.0/permissions/postgres/{project_id}",
    f"/api/2.0/permissions/database-instances/{project_id}",
    f"/api/2.0/postgres/projects/{project_id}/permissions",
    f"/api/2.0/postgres/v1/projects/{project_id}/permissions",
    f"/api/2.0/permissions/lakebase/{project_id}",
]

# First GET existing permissions
for path in api_paths:
    url = f"{host}{path}"
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        print(f"  GET {path} => 200")
        print(f"      {resp.json()}")
        break
    else:
        print(f"  GET {path} => {resp.status_code}: {resp.text[:120]}")

# Then try PATCH to grant
print()
for sp_client_id, sp_numeric_id, sp_name in [
    (mobile_sp, mobile_sp_id, "Mobile"),
    (dashboard_sp, dashboard_sp_id, "Dashboard"),
]:
    if not sp_client_id:
        continue
    print(f"\n  Granting to {sp_name} SP ({sp_client_id}, numeric={sp_numeric_id})...")

    for path in api_paths:
        # Try with client_id (UUID)
        payload = {
            "access_control_list": [{
                "service_principal_name": sp_client_id,
                "permission_level": "CAN_USE",
            }]
        }
        url = f"{host}{path}"
        resp = requests.patch(url, headers=headers, json=payload)
        if resp.status_code in (200, 201):
            print(f"    PATCH {path} (client_id) => SUCCESS")
            print(f"      {resp.json()}")
            break
        
        # Try with numeric ID
        payload2 = {
            "access_control_list": [{
                "service_principal_name": sp_numeric_id,
                "permission_level": "CAN_USE",
            }]
        }
        resp2 = requests.patch(url, headers=headers, json=payload2)
        if resp2.status_code in (200, 201):
            print(f"    PATCH {path} (numeric_id) => SUCCESS")
            print(f"      {resp2.json()}")
            break

# Method 3: Try IAM-style role bindings
print("\n--- Method 3: IAM role bindings ---")
for sp_client_id, sp_numeric_id, sp_name in [
    (mobile_sp, mobile_sp_id, "Mobile"),
    (dashboard_sp, dashboard_sp_id, "Dashboard"),
]:
    if not sp_numeric_id:
        continue
    
    # Try setting IAM policy on the postgres project
    iam_paths = [
        f"/api/2.0/postgres/projects/{project_id}:setIamPolicy",
        f"/api/2.0/postgres/v1/projects/{project_id}:setIamPolicy",
    ]
    for path in iam_paths:
        payload = {
            "policy": {
                "bindings": [{
                    "role": "roles/postgres.user",
                    "members": [f"servicePrincipals/{sp_numeric_id}"],
                }]
            }
        }
        url = f"{host}{path}"
        resp = requests.post(url, headers=headers, json=payload)
        print(f"  POST {path} for {sp_name} => {resp.status_code}: {resp.text[:120]}")
        if resp.status_code in (200, 201):
            break

# Method 4: Check get/set IAM
print("\n--- Method 4: Get existing IAM policy ---")
iam_get_paths = [
    f"/api/2.0/postgres/projects/{project_id}:getIamPolicy",
    f"/api/2.0/postgres/v1/projects/{project_id}:getIamPolicy",
]
for path in iam_get_paths:
    url = f"{host}{path}"
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        print(f"  GET {path} => 200")
        data = resp.json()
        print(f"      {json.dumps(data, indent=2)[:500]}")
    else:
        resp2 = requests.post(url, headers=headers, json={})
        if resp2.status_code == 200:
            print(f"  POST {path} => 200")
            data = resp2.json()
            print(f"      {json.dumps(data, indent=2)[:500]}")
        else:
            print(f"  {path} => GET:{resp.status_code} POST:{resp2.status_code}")

print("\nDone.")
