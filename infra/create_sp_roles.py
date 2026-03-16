#!/usr/bin/env python3
"""Create Lakebase OAuth roles for App Service Principals on the new project."""
import os
import sys
from pathlib import Path

# Load config — generated_config.env overrides .env
root = Path(__file__).parent.parent
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
from databricks.sdk.service.postgres import (
    Role, RoleRoleSpec, RoleAuthMethod, RoleIdentityType,
    RoleMembershipRole,
)

w = WorkspaceClient(
    host=os.environ["DATABRICKS_HOST"],
    token=os.environ["DATABRICKS_TOKEN"],
)

project_id = os.environ["LAKEBASE_INSTANCE"]
branch = f"projects/{project_id}/branches/production"

# List existing roles
print("=== Existing roles ===")
for r in w.postgres.list_roles(parent=branch):
    auth = getattr(r.status, "auth_method", "?")
    ident = getattr(r.status, "identity_type", "?")
    pg = getattr(r.status, "postgres_role", "?")
    print(f"  {r.name}  auth={auth}  identity={ident}  pg_role={pg}")

sps = [
    (os.environ.get("MOBILE_APP_SP_CLIENT_ID", ""), "Mobile"),
    (os.environ.get("DASHBOARD_APP_SP_CLIENT_ID", ""), "Dashboard"),
]

for sp_client_id, name in sps:
    if not sp_client_id:
        print(f"\n  Skipping {name} — no SP_CLIENT_ID set")
        continue
    print(f"\n=== Creating role for {name} SP: {sp_client_id} ===")
    # role_id must match ^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$ — prefix if starts with digit
    role_id = sp_client_id if sp_client_id[0].isalpha() else f"sp-{sp_client_id}"
    try:
        op = w.postgres.create_role(
            parent=branch,
            role=Role(spec=RoleRoleSpec(
                auth_method=RoleAuthMethod.LAKEBASE_OAUTH_V1,
                identity_type=RoleIdentityType.SERVICE_PRINCIPAL,
                postgres_role=sp_client_id,
                membership_roles=[RoleMembershipRole.DATABRICKS_SUPERUSER],
            )),
            role_id=role_id,
        )
        result = op.wait()
        print(f"  ✅ Created: {result.name}  auth={result.status.auth_method}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"  ℹ️  Role already exists")
        else:
            print(f"  ❌ Failed: {e}")

print("\n=== Final role list ===")
for r in w.postgres.list_roles(parent=branch):
    auth = getattr(r.status, "auth_method", "?")
    ident = getattr(r.status, "identity_type", "?")
    pg = getattr(r.status, "postgres_role", "?")
    print(f"  {r.name}  auth={auth}  identity={ident}  pg_role={pg}")

print("\nDone.")
