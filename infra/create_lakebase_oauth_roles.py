#!/usr/bin/env python3
"""Create proper Lakebase OAuth roles for App SPs."""
import os
from pathlib import Path
from dotenv import load_dotenv

root = Path(__file__).parent.parent
load_dotenv(root / "generated_config.env", override=True)

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.postgres import (
    Role, RoleRoleSpec, RoleAuthMethod, RoleIdentityType,
    RoleMembershipRole,
)

# List all enum values
print("=== RoleAuthMethod values ===")
for m in RoleAuthMethod:
    print(f"  {m.name} = {m.value}")

print("\n=== RoleIdentityType values ===")
for m in RoleIdentityType:
    print(f"  {m.name} = {m.value}")

print("\n=== RoleMembershipRole values ===")
for m in RoleMembershipRole:
    print(f"  {m.name} = {m.value}")

w = WorkspaceClient(
    host=os.environ["DATABRICKS_HOST"],
    token=os.environ["DATABRICKS_TOKEN"],
)

project_id = os.environ["LAKEBASE_INSTANCE"]
mobile_sp = os.environ.get("MOBILE_APP_SP_CLIENT_ID", "")
mobile_sp_id = os.environ.get("MOBILE_APP_SP_ID", "")
dashboard_sp = os.environ.get("DASHBOARD_APP_SP_CLIENT_ID", "")
dashboard_sp_id = os.environ.get("DASHBOARD_APP_SP_ID", "")
branch = f"projects/{project_id}/branches/production"

for sp_client_id, sp_numeric_id, sp_name in [
    (mobile_sp, mobile_sp_id, "Mobile"),
    (dashboard_sp, dashboard_sp_id, "Dashboard"),
]:
    if not sp_client_id:
        continue
    print(f"\n=== Creating role for {sp_name} SP ===")
    
    spec = RoleRoleSpec(
        auth_method=RoleAuthMethod.LAKEBASE_OAUTH_V1,
        identity_type=RoleIdentityType.SERVICE_PRINCIPAL,
        postgres_role=sp_client_id,
        membership_roles=[RoleMembershipRole.DATABRICKS_SUPERUSER],
    )
    print(f"  Spec: {spec}")
    
    try:
        op = w.postgres.create_role(
            parent=branch,
            role=Role(spec=spec),
            role_id=sp_client_id,
        )
        result = op.wait()
        print(f"  Created: {result.name}")
        print(f"  Auth: {result.status.auth_method}")
        print(f"  Identity: {result.status.identity_type}")
        print(f"  PG role: {result.status.postgres_role}")
    except Exception as e:
        print(f"  Failed: {e}")
        # Try without membership_roles
        try:
            spec2 = RoleRoleSpec(
                auth_method=RoleAuthMethod.LAKEBASE_OAUTH_V1,
                identity_type=RoleIdentityType.SERVICE_PRINCIPAL,
                postgres_role=sp_client_id,
            )
            op = w.postgres.create_role(
                parent=branch,
                role=Role(spec=spec2),
                role_id=sp_client_id,
            )
            result = op.wait()
            print(f"  Created (no membership): {result.name}")
            print(f"  Auth: {result.status.auth_method}")
            print(f"  Identity: {result.status.identity_type}")
        except Exception as e2:
            print(f"  Also failed: {e2}")

# Verify
print("\n=== All roles now ===")
for r in w.postgres.list_roles(parent=branch):
    s = r.status
    print(f"  {s.postgres_role}: auth={s.auth_method}, identity={s.identity_type}, memberships={s.membership_roles}")

print("\nDone.")
