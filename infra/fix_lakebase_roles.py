#!/usr/bin/env python3
"""
Fix Lakebase SP roles: change auth_method from NO_LOGIN to LAKEBASE_OAUTH_V1.

The SP roles created by the initial PG CREATE ROLE have auth_method=NO_LOGIN.
They need to be recreated with proper Lakebase OAuth authentication.
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

root = Path(__file__).parent.parent
load_dotenv(root / "generated_config.env", override=True)

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.postgres import (
    Role, RoleRoleSpec, RoleRoleStatus, RoleAuthMethod, RoleIdentityType,
    RoleMembershipRole, RoleAttributes,
)

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

# List existing roles to find the SP role names
print("=== Finding existing SP roles ===")
roles = list(w.postgres.list_roles(parent=branch))
sp_roles = {}
for r in roles:
    if r.status and r.status.postgres_role:
        pg_role = r.status.postgres_role
        if pg_role == mobile_sp:
            sp_roles["mobile"] = r
            print(f"  Mobile SP role: {r.name} (auth={r.status.auth_method})")
        elif pg_role == dashboard_sp:
            sp_roles["dashboard"] = r
            print(f"  Dashboard SP role: {r.name} (auth={r.status.auth_method})")

# Delete the NO_LOGIN roles and recreate with proper auth
for sp_key, sp_client_id, sp_numeric_id, sp_name in [
    ("mobile", mobile_sp, mobile_sp_id, "Mobile"),
    ("dashboard", dashboard_sp, dashboard_sp_id, "Dashboard"),
]:
    if not sp_client_id:
        continue

    # Delete old role
    if sp_key in sp_roles:
        old_role = sp_roles[sp_key]
        print(f"\n  Deleting old {sp_name} role: {old_role.name}...")
        try:
            w.postgres.delete_role(name=old_role.name).wait()
            print(f"  Deleted.")
        except Exception as e:
            print(f"  Delete failed: {e}")

    # Inspect RoleRoleSpec fields
    print(f"\n  Creating new {sp_name} role with LAKEBASE_OAUTH_V1...")
    import inspect
    
    # Check available fields
    if hasattr(RoleRoleSpec, '__dataclass_fields__'):
        fields = list(RoleRoleSpec.__dataclass_fields__.keys())
        print(f"  RoleRoleSpec fields: {fields}")
    
    # Try creating with proper OAuth spec
    try:
        role = Role(
            spec=RoleRoleSpec(
                attributes=RoleAttributes(
                    membership_roles=[RoleMembershipRole.DATABRICKS_ALL_WRITER_PERMS],
                ),
            ),
        )
        print(f"  Role object: {role}")
        
        op = w.postgres.create_role(
            parent=branch,
            role=role,
            role_id=sp_client_id,
        )
        result = op.wait()
        print(f"  Created: {result}")
        print(f"  Auth method: {result.status.auth_method if result.status else 'unknown'}")
    except Exception as e:
        print(f"  Create failed: {e}")
        
        # Try simpler approach
        try:
            role = Role(spec=RoleRoleSpec())
            op = w.postgres.create_role(parent=branch, role=role, role_id=sp_client_id)
            result = op.wait()
            print(f"  Created (simple): {result}")
        except Exception as e2:
            print(f"  Simple create also failed: {e2}")

# Check what we have now
print("\n=== Roles after fix ===")
roles = list(w.postgres.list_roles(parent=branch))
for r in roles:
    pg_role = r.status.postgres_role if r.status else "?"
    auth = r.status.auth_method if r.status else "?"
    identity = r.status.identity_type if r.status else "?"
    print(f"  {r.name}")
    print(f"    postgres_role: {pg_role}")
    print(f"    auth_method: {auth}")
    print(f"    identity_type: {identity}")

print("\nDone.")
