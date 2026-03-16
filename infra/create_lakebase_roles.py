#!/usr/bin/env python3
"""
Grant App SPs access to Lakebase Autoscaling via SDK role API.

Uses w.postgres.create_role() and w.postgres.list_roles() to create
Postgres roles for the App SPs, enabling them to authenticate.
"""
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
mobile_sp = os.environ.get("MOBILE_APP_SP_CLIENT_ID", "")
mobile_sp_id = os.environ.get("MOBILE_APP_SP_ID", "")
dashboard_sp = os.environ.get("DASHBOARD_APP_SP_CLIENT_ID", "")
dashboard_sp_id = os.environ.get("DASHBOARD_APP_SP_ID", "")
branch = f"projects/{project_id}/branches/production"

# List existing Lakebase roles
print("=== Existing Lakebase roles ===")
try:
    roles = list(w.postgres.list_roles(parent=branch))
    for r in roles:
        print(f"  {r.name} => {r}")
except Exception as e:
    print(f"  list_roles failed: {e}")

# Inspect create_role signature
print("\n=== create_role signature ===")
import inspect
sig = inspect.signature(w.postgres.create_role)
print(f"  {sig}")

# Try to find Role spec types
print("\n=== Role/RoleSpec types ===")
try:
    from databricks.sdk.service import postgres as pg_module
    role_types = [x for x in dir(pg_module) if 'role' in x.lower()]
    print(f"  Role-related types: {role_types}")
    
    if hasattr(pg_module, 'Role'):
        print(f"  Role fields: {[f for f in dir(pg_module.Role) if not f.startswith('_')]}")
    if hasattr(pg_module, 'RoleSpec'):
        print(f"  RoleSpec fields: {[f for f in dir(pg_module.RoleSpec) if not f.startswith('_')]}")
except Exception as e:
    print(f"  import error: {e}")

# Create roles for SPs
print("\n=== Creating roles for App SPs ===")
for sp_client_id, sp_numeric_id, sp_name in [
    (mobile_sp, mobile_sp_id, "Mobile"),
    (dashboard_sp, dashboard_sp_id, "Dashboard"),
]:
    if not sp_client_id:
        continue
    print(f"\n  Creating role for {sp_name} SP ({sp_client_id})...")
    
    try:
        # Try with service principal reference
        from databricks.sdk.service.postgres import Role, RoleSpec
        
        # Inspect RoleSpec to see what fields it needs
        spec_fields = {f.name: f for f in RoleSpec.__dataclass_fields__.values()} if hasattr(RoleSpec, '__dataclass_fields__') else {}
        print(f"  RoleSpec fields: {list(spec_fields.keys()) if spec_fields else 'unknown'}")
        
        # Try creating with principal
        role = w.postgres.create_role(
            parent=branch,
            role=Role(
                spec=RoleSpec(
                    principal=f"servicePrincipals/{sp_numeric_id}",
                )
            ),
            role_id=sp_client_id,
        )
        print(f"  Created role: {role}")
    except TypeError as e:
        print(f"  TypeError: {e}")
        # Try alternative approach
        try:
            role = w.postgres.create_role(
                parent=branch,
                role=Role(
                    spec=RoleSpec()
                ),
                role_id=sp_client_id,
            )
            print(f"  Created role (no principal): {role}")
        except Exception as e2:
            print(f"  Alt failed: {e2}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"  Role already exists")
        else:
            print(f"  Failed: {e}")

# List roles again to see what we have
print("\n=== Roles after creation ===")
try:
    roles = list(w.postgres.list_roles(parent=branch))
    for r in roles:
        print(f"  {r.name}")
        if hasattr(r, 'spec'):
            print(f"    spec: {r.spec}")
except Exception as e:
    print(f"  Failed: {e}")

print("\nDone.")
