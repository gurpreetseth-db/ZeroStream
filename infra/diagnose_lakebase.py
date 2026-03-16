#!/usr/bin/env python3
"""Diagnose Lakebase auth: check endpoint, credentials, PG roles."""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

root = Path(__file__).parent.parent
load_dotenv(root / "generated_config.env", override=True)

from databricks.sdk import WorkspaceClient

w = WorkspaceClient(
    host=os.environ["DATABRICKS_HOST"],
    token=os.environ["DATABRICKS_TOKEN"],
)

project_id = os.environ["LAKEBASE_INSTANCE"]

# 1. List actual endpoints
print("=== Listing endpoints ===")
endpoints = list(w.postgres.list_endpoints(
    parent=f"projects/{project_id}/branches/production"
))
for ep in endpoints:
    h = ep.status.hosts.host if ep.status and ep.status.hosts else "unknown"
    t = ep.spec.endpoint_type if ep.spec else "unknown"
    print(f"  Name: {ep.name}")
    print(f"  Host: {h}")
    print(f"  Type: {t}")
    print()

# 2. Compare with config
config_ep = os.environ.get("LAKEBASE_ENDPOINT", "")
print(f"Config  LAKEBASE_ENDPOINT: {config_ep}")
if not endpoints:
    print(">>> No endpoints found!")
    sys.exit(1)

actual_ep = endpoints[0].name
print(f"Actual  endpoint name:     {actual_ep}")
if config_ep != actual_ep:
    print(">>> MISMATCH!")
else:
    print(">>> Match OK")

# 3. Generate credential and test
print("\n=== Generating credential ===")
cred = w.postgres.generate_database_credential(endpoint=actual_ep)
print(f"  Token length: {len(cred.token) if cred.token else 0}")
print(f"  Token prefix: {cred.token[:30]}..." if cred.token else "  NO TOKEN")

username = w.current_user.me().user_name
host = endpoints[0].status.hosts.host
print(f"  Username: {username}")
print(f"  Host:     {host}")

# 4. Test PG connection as workspace user
print("\n=== Testing PG connection as workspace user ===")
import psycopg2
db = os.environ.get("LAKEBASE_DATABASES", "databricks_postgres")
try:
    conn = psycopg2.connect(
        host=host, port=5432, dbname=db,
        user=username, password=cred.token,
        sslmode="require", connect_timeout=10,
    )
    cur = conn.cursor()
    cur.execute("SELECT current_user, current_database()")
    row = cur.fetchone()
    print(f"  Connected! user={row[0]}, db={row[1]}")

    # List PG roles with UUID-like names
    cur.execute("SELECT rolname FROM pg_roles ORDER BY rolname")
    roles = cur.fetchall()
    print(f"  All PG roles: {[r[0] for r in roles]}")

    cur.close()
    conn.close()
except Exception as e:
    print(f"  FAILED: {e}")

# 5. Check SP permissions on project
print("\n=== App SP info ===")
mobile_sp = os.environ.get("MOBILE_APP_SP_CLIENT_ID", "")
dashboard_sp = os.environ.get("DASHBOARD_APP_SP_CLIENT_ID", "")
print(f"  Mobile SP:    {mobile_sp}")
print(f"  Dashboard SP: {dashboard_sp}")

# 6. Try granting project permissions to SPs
print("\n=== Granting Lakebase project permissions to App SPs ===")
import requests
api_host = os.environ["DATABRICKS_HOST"].rstrip("/")
token = os.environ["DATABRICKS_TOKEN"]
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
}

for sp_id, sp_name in [(mobile_sp, "Mobile App"), (dashboard_sp, "Dashboard App")]:
    if not sp_id:
        continue
    print(f"\n  Granting CAN_USE to {sp_name} ({sp_id})...")

    # Try multiple API paths
    api_paths = [
        f"/api/2.0/permissions/postgres-projects/{project_id}",
        f"/api/2.0/permissions/database-instances/{project_id}",
        f"/api/2.0/permissions/postgres/projects/{project_id}",
    ]
    payload = {
        "access_control_list": [{
            "service_principal_name": sp_id,
            "permission_level": "CAN_USE",
        }]
    }

    for path in api_paths:
        url = f"{api_host}{path}"
        resp = requests.patch(url, headers=headers, json=payload)
        print(f"    {path} => {resp.status_code}")
        if resp.status_code in (200, 201):
            print(f"    SUCCESS: {resp.json()}")
            break
        else:
            print(f"    {resp.text[:200]}")

# 7. Try generating a credential scoped to the SP (if possible)
print("\n=== Testing credential for Mobile SP ===")
if mobile_sp:
    # The SP needs to authenticate itself to generate its own credential
    # We can't do that from here — but let's check if we can grant PG access
    try:
        cred2 = w.postgres.generate_database_credential(endpoint=actual_ep)
        conn2 = psycopg2.connect(
            host=host, port=5432, dbname=db,
            user=username, password=cred2.token,
            sslmode="require", connect_timeout=10,
        )
        conn2.autocommit = True
        cur2 = conn2.cursor()

        # Grant the SP user access to the database and schema
        schema = os.environ.get("LAKEBASE_SCHEMA", "public")
        table_name = os.environ.get("TABLE_NAME", "sensor_stream")

        # In Lakebase Autoscaling, SP roles are auto-created
        # But we need to GRANT them access to the schema/tables
        for sp_id, sp_name in [(mobile_sp, "Mobile"), (dashboard_sp, "Dashboard")]:
            if not sp_id:
                continue
            quoted_sp = f'"{sp_id}"'
            print(f"\n  Granting PG permissions to {sp_name} ({sp_id})...")

            grants = [
                f'GRANT CONNECT ON DATABASE "{db}" TO {quoted_sp}',
                f'GRANT USAGE ON SCHEMA "{schema}" TO {quoted_sp}',
                f'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA "{schema}" TO {quoted_sp}',
                f'GRANT ALL PRIVILEGES ON SCHEMA "{schema}" TO {quoted_sp}',
            ]
            for sql in grants:
                try:
                    cur2.execute(sql)
                    print(f"    OK: {sql}")
                except Exception as e:
                    print(f"    FAIL: {sql}")
                    print(f"          {e}")

        cur2.close()
        conn2.close()
    except Exception as e:
        print(f"  Could not grant PG permissions: {e}")

print("\nDone.")
