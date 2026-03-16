#!/usr/bin/env python3
"""Grant PG-level permissions to App SP roles in Lakebase."""
import os
import sys
from pathlib import Path

root = Path(__file__).parent.parent

# Load config — generated_config.env overrides .env and shell env
for env_file in [root / ".env", root / "generated_config.env"]:
    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ[k.strip()] = v.strip().strip('"')

# Ensure vendored SDK
whl = str(root / "dashboard_app" / "wheels" / "databricks_sdk-0.97.0-py3-none-any.whl")
if os.path.exists(whl):
    for mod in list(sys.modules.keys()):
        if mod == "databricks" or mod.startswith("databricks."):
            del sys.modules[mod]
    sys.path.insert(0, whl)

from databricks.sdk import WorkspaceClient
import psycopg2

w = WorkspaceClient(
    host=os.environ["DATABRICKS_HOST"],
    token=os.environ["DATABRICKS_TOKEN"],
)

ep_name = os.environ["LAKEBASE_ENDPOINT"]
host = os.environ["LAKEBASE_HOST"]
db = os.environ.get("LAKEBASE_DATABASES", "databricks_postgres")
schema = os.environ.get("LAKEBASE_SCHEMA", "public")
table = os.environ.get("TABLE_NAME", "sensor_stream")
mobile_sp = os.environ.get("MOBILE_APP_SP_CLIENT_ID", "")
dashboard_sp = os.environ.get("DASHBOARD_APP_SP_CLIENT_ID", "")

# Get fresh credential as workspace user
cred = w.postgres.generate_database_credential(endpoint=ep_name)
username = w.current_user.me().user_name
print(f"Connecting as: {username}")

conn = psycopg2.connect(
    host=host, port=5432, dbname=db,
    user=username, password=cred.token,
    sslmode="require", connect_timeout=10,
)
conn.autocommit = True
cur = conn.cursor()

for sp_id, sp_name in [(mobile_sp, "Mobile"), (dashboard_sp, "Dashboard")]:
    if not sp_id:
        continue
    quoted = '"' + sp_id + '"'
    print(f"\nSetting up {sp_name} SP ({sp_id})...")

    # CREATE ROLE with LOGIN
    try:
        cur.execute(f"CREATE ROLE {quoted} WITH LOGIN")
        print(f"  Created role")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"  Role already exists")
        else:
            print(f"  CREATE ROLE failed: {e}")

    # GRANT permissions
    grants = [
        f'GRANT CONNECT ON DATABASE "{db}" TO {quoted}',
        f'GRANT USAGE ON SCHEMA "{schema}" TO {quoted}',
        f'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA "{schema}" TO {quoted}',
        f'GRANT ALL PRIVILEGES ON SCHEMA "{schema}" TO {quoted}',
        f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{schema}" GRANT ALL ON TABLES TO {quoted}',
    ]
    for sql in grants:
        try:
            cur.execute(sql)
            print(f"  OK: {sql}")
        except Exception as e:
            print(f"  FAIL: {sql}")
            print(f"        {e}")

# Verify
cur.execute("SELECT rolname FROM pg_roles WHERE rolname LIKE '%-%' ORDER BY rolname")
roles = [r[0] for r in cur.fetchall()]
print(f"\nUUID roles now: {roles}")

cur.close()
conn.close()
print("Done!")
