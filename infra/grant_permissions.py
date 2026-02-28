#!/usr/bin/env python3
"""
Grant permissions to App Service Principals.

Grants:
1. SQL Warehouse CAN_USE:
   - Mobile App SP
   - Dashboard App SP

2. Unity Catalog for Dashboard App (read-only):
   - USE_CATALOG on catalog
   - USE_SCHEMA on schema
   - SELECT on table

3. Unity Catalog for Mobile App (write access for streaming):
   - USE_CATALOG on catalog
   - USE_SCHEMA on schema
   - SELECT, MODIFY on table

4. Unity Catalog for ZeroBus SP (write access for ingestion):
   - USE_CATALOG on catalog
   - USE_SCHEMA on schema
   - SELECT, MODIFY on table

5. Lakebase access:
   - Dashboard App SP: CAN_USE
   - ZeroBus SP: CAN_USE (for OAuth M2M access)

Reads service principal IDs from generated_config.env

Uses REST API directly for compatibility with older SDK versions.
"""
import os
import sys
import requests
from pathlib import Path
from dotenv import load_dotenv

# Load environment from .env and generated_config.env
script_dir = Path(__file__).parent
root_dir = script_dir.parent
if (root_dir / ".env").exists():
    load_dotenv(root_dir / ".env")
if (root_dir / "generated_config.env").exists():
    load_dotenv(root_dir / "generated_config.env", override=True)


def get_auth():
    """Get Databricks host and token."""
    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN")
    
    if not host or not token:
        raise ValueError("DATABRICKS_HOST and DATABRICKS_TOKEN must be set")
    
    return host, token


def load_generated_config() -> dict:
    """Load generated config from file in main directory."""
    script_dir = os.path.dirname(__file__)
    root_dir = os.path.dirname(script_dir)
    config_file = os.path.join(root_dir, "generated_config.env")
    
    config = {}
    if os.path.exists(config_file):
        with open(config_file, "r") as f:
            for line in f:
                line = line.strip()
                if line and "=" in line and not line.startswith("#"):
                    key, val = line.split("=", 1)
                    config[key] = val
    
    return config


def grant_warehouse_permission(
    host: str,
    token: str,
    warehouse_id: str, 
    sp_client_id: str, 
    sp_name: str
) -> bool:
    """Grant CAN_USE permission on warehouse to a service principal."""
    
    print(f"  🔓 Granting CAN_USE on warehouse to {sp_name}...")
    print(f"     Warehouse ID: {warehouse_id}")
    print(f"     Service Principal Client ID: {sp_client_id}")
    
    try:
        # Update permissions via REST API
        url = f"{host}/api/2.0/permissions/sql/warehouses/{warehouse_id}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        payload = {
            "access_control_list": [
                {
                    "service_principal_name": sp_client_id,
                    "permission_level": "CAN_USE"
                }
            ]
        }
        
        resp = requests.patch(url, headers=headers, json=payload)
        
        if resp.status_code not in (200, 201):
            raise Exception(f"API error {resp.status_code}: {resp.text}")
        
        print(f"  ✅ Permission granted to {sp_name}")
        return True
        
    except Exception as e:
        print(f"  ⚠️  Could not grant permission: {e}")
        print(f"     You may need to grant this manually in Databricks UI:")
        print(f"     SQL Warehouses → {warehouse_id} → Permissions → Add {sp_name} with CAN_USE")
        return False


def grant_catalog_permission(
    host: str,
    token: str,
    catalog: str,
    sp_client_id: str,
    sp_name: str,
    permission: str = "USE_CATALOG"
) -> bool:
    """Grant permission on Unity Catalog to a service principal."""
    
    print(f"  🔓 Granting {permission} on catalog '{catalog}' to {sp_name}...")
    
    try:
        url = f"{host}/api/2.1/unity-catalog/permissions/catalog/{catalog}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        payload = {
            "changes": [
                {
                    "principal": sp_client_id,
                    "add": [permission]
                }
            ]
        }
        
        resp = requests.patch(url, headers=headers, json=payload)
        
        if resp.status_code not in (200, 201):
            raise Exception(f"API error {resp.status_code}: {resp.text}")
        
        print(f"  ✅ Catalog permission granted")
        return True
        
    except Exception as e:
        print(f"  ⚠️  Could not grant catalog permission: {e}")
        return False


def grant_schema_permission(
    host: str,
    token: str,
    catalog: str,
    schema: str,
    sp_client_id: str,
    sp_name: str,
    permissions: list = None
) -> bool:
    """Grant schema permissions to a service principal."""
    
    if permissions is None:
        permissions = ["USE_SCHEMA", "SELECT"]
    
    print(f"  🔓 Granting {', '.join(permissions)} on '{catalog}.{schema}' to {sp_name}...")
    
    try:
        url = f"{host}/api/2.1/unity-catalog/permissions/schema/{catalog}.{schema}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        payload = {
            "changes": [
                {
                    "principal": sp_client_id,
                    "add": permissions
                }
            ]
        }
        
        resp = requests.patch(url, headers=headers, json=payload)
        
        if resp.status_code not in (200, 201):
            raise Exception(f"API error {resp.status_code}: {resp.text}")
        
        print(f"  ✅ Schema permission granted on {catalog}.{schema}")
        return True
        
    except Exception as e:
        print(f"  ⚠️  Could not grant schema permission: {e}")
        return False


def grant_table_permission(
    host: str,
    token: str,
    catalog: str,
    schema: str,
    table: str,
    sp_client_id: str,
    sp_name: str,
    permissions: list = None
) -> bool:
    """Grant table permissions (SELECT, MODIFY) to a service principal."""
    
    if permissions is None:
        permissions = ["SELECT", "MODIFY"]
    
    full_table = f"{catalog}.{schema}.{table}"
    print(f"  🔓 Granting {', '.join(permissions)} on '{full_table}' to {sp_name}...")
    
    try:
        url = f"{host}/api/2.1/unity-catalog/permissions/table/{full_table}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        payload = {
            "changes": [
                {
                    "principal": sp_client_id,
                    "add": permissions
                }
            ]
        }
        
        resp = requests.patch(url, headers=headers, json=payload)
        
        if resp.status_code not in (200, 201):
            raise Exception(f"API error {resp.status_code}: {resp.text}")
        
        print(f"  ✅ Table permission granted on {full_table}")
        return True
        
    except Exception as e:
        print(f"  ⚠️  Could not grant table permission: {e}")
        return False


def grant_lakebase_permission(
    host: str,
    token: str,
    instance_name: str,
    sp_client_id: str,
    sp_name: str,
    permission: str = "CAN_USE"
) -> bool:
    """Grant access to Lakebase instance to a service principal.
    
    Uses the Lakebase database permissions API.
    """
    
    print(f"  🔓 Granting {permission} on Lakebase '{instance_name}' to {sp_name}...")
    print(f"     Service Principal Client ID: {sp_client_id}")
    
    try:
        # First, get the Lakebase instance ID
        list_url = f"{host}/api/2.0/database/instances"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        
        resp = requests.get(list_url, headers=headers)
        if resp.status_code != 200:
            raise Exception(f"Failed to list Lakebase instances: {resp.status_code}")
        
        instances = resp.json().get("database_instances", [])
        instance_id = None
        for inst in instances:
            if inst.get("name") == instance_name:
                instance_id = inst.get("uid")  # API returns "uid" not "id"
                break
        
        if not instance_id:
            print(f"  ⚠️  Lakebase instance '{instance_name}' not found")
            return False
        
        # Grant permission via REST API
        perm_url = f"{host}/api/2.0/permissions/database-instances/{instance_name}"
        payload = {
            "access_control_list": [
                {
                    "service_principal_name": sp_client_id,
                    "permission_level": permission
                }
            ]
        }
        
        resp = requests.patch(perm_url, headers=headers, json=payload)
        
        if resp.status_code not in (200, 201):
            raise Exception(f"API error {resp.status_code}: {resp.text}")
        
        print(f"  ✅ Lakebase permission granted to {sp_name}")
        return True
        
    except Exception as e:
        print(f"  ⚠️  Could not grant Lakebase permission via API")
        print(f"     (Lakebase permissions API may require manual configuration)")
        print(f"     Grant manually: Compute → Lakebase → {instance_name} → Permissions → Add {sp_name} → {permission}")
        return False
        return False


def main():
    try:
        host, token = get_auth()
        
        # Load generated config
        config = load_generated_config()
        
        warehouse_id = config.get("DATABRICKS_WAREHOUSE_ID")
        mobile_sp_client_id = config.get("MOBILE_APP_SP_CLIENT_ID")
        dashboard_sp_client_id = config.get("DASHBOARD_APP_SP_CLIENT_ID")
        zerobus_sp_client_id = config.get("ZEROBUS_CLIENT_ID")
        
        if not warehouse_id:
            print("  ⚠️  No warehouse ID found in generated_config.env")
            print("     Run create_warehouse.py first")
            return
        
        if not dashboard_sp_client_id:
            print("  ⚠️  No dashboard app SP found in generated_config.env")
            print("     Run create_apps.py first")
            return
        
        success_count = 0
        total_count = 0
        
        catalog = os.environ.get("CATALOG")
        schema = os.environ.get("SCHEMA")
        table = os.environ.get("TABLE_NAME")
        
        # ─────────────────────────────────────────────────────────────────────
        # SQL Warehouse Permissions
        # ─────────────────────────────────────────────────────────────────────
        print("\n  Granting SQL Warehouse Permissions...")
        print("  " + "─" * 45)
        
        # Mobile App needs CAN_USE on warehouse
        if mobile_sp_client_id:
            total_count += 1
            if grant_warehouse_permission(
                host, token, warehouse_id, mobile_sp_client_id, "Mobile App"
            ):
                success_count += 1
        else:
            print("  ℹ️  Mobile app SP not found - skipping")
        
        # Dashboard App needs CAN_USE on warehouse
        total_count += 1
        if grant_warehouse_permission(
            host, token, warehouse_id, dashboard_sp_client_id, "Dashboard App"
        ):
            success_count += 1
        
        # ─────────────────────────────────────────────────────────────────────
        # Unity Catalog Permissions for Dashboard App (read-only)
        # ─────────────────────────────────────────────────────────────────────
        if catalog:
            print("\n  Granting Unity Catalog Permissions to Dashboard App...")
            print("  " + "─" * 45)
            
            total_count += 1
            if grant_catalog_permission(
                host, token, catalog, dashboard_sp_client_id, "Dashboard App", "USE_CATALOG"
            ):
                success_count += 1
            
            if schema:
                total_count += 1
                if grant_schema_permission(
                    host, token, catalog, schema, dashboard_sp_client_id, "Dashboard App",
                    ["USE_SCHEMA"]
                ):
                    success_count += 1
                
                if table:
                    total_count += 1
                    if grant_table_permission(
                        host, token, catalog, schema, table, dashboard_sp_client_id, "Dashboard App",
                        ["SELECT"]
                    ):
                        success_count += 1
        
        # ─────────────────────────────────────────────────────────────────────
        # Unity Catalog Permissions for Mobile App (write access for streaming)
        # ─────────────────────────────────────────────────────────────────────
        if catalog and mobile_sp_client_id:
            print("\n  Granting Unity Catalog Permissions to Mobile App...")
            print("  " + "─" * 45)
            
            total_count += 1
            if grant_catalog_permission(
                host, token, catalog, mobile_sp_client_id, "Mobile App", "USE_CATALOG"
            ):
                success_count += 1
            
            if schema:
                total_count += 1
                if grant_schema_permission(
                    host, token, catalog, schema, mobile_sp_client_id, "Mobile App",
                    ["USE_SCHEMA"]
                ):
                    success_count += 1
                
                if table:
                    total_count += 1
                    if grant_table_permission(
                        host, token, catalog, schema, table, mobile_sp_client_id, "Mobile App",
                        ["SELECT", "MODIFY"]
                    ):
                        success_count += 1
        
        # ─────────────────────────────────────────────────────────────────────
        # Unity Catalog Permissions for ZeroBus SP (write access for ingestion)
        # ─────────────────────────────────────────────────────────────────────
        if catalog and zerobus_sp_client_id:
            print("\n  Granting Unity Catalog Permissions to ZeroBus SP...")
            print("  " + "─" * 45)
            
            total_count += 1
            if grant_catalog_permission(
                host, token, catalog, zerobus_sp_client_id, "ZeroBus SP", "USE_CATALOG"
            ):
                success_count += 1
            
            if schema:
                total_count += 1
                if grant_schema_permission(
                    host, token, catalog, schema, zerobus_sp_client_id, "ZeroBus SP",
                    ["USE_SCHEMA"]
                ):
                    success_count += 1
                
                if table:
                    total_count += 1
                    if grant_table_permission(
                        host, token, catalog, schema, table, zerobus_sp_client_id, "ZeroBus SP",
                        ["SELECT", "MODIFY"]
                    ):
                        success_count += 1
        
        # ─────────────────────────────────────────────────────────────────────
        # Lakebase Permissions (Dashboard App and ZeroBus SP)
        # ─────────────────────────────────────────────────────────────────────
        lakebase_instance = config.get("LAKEBASE_INSTANCE") or os.environ.get("LAKEBASE_INSTANCE")
        if lakebase_instance:
            print("\n  Granting Lakebase Permissions...")
            print("  " + "─" * 45)
            
            # Dashboard App needs Lakebase access
            total_count += 1
            if grant_lakebase_permission(
                host, token, lakebase_instance, dashboard_sp_client_id, "Dashboard App"
            ):
                success_count += 1
            
            # ZeroBus SP needs Lakebase access (for OAuth M2M verification)
            if zerobus_sp_client_id:
                total_count += 1
                if grant_lakebase_permission(
                    host, token, lakebase_instance, zerobus_sp_client_id, "ZeroBus SP"
                ):
                    success_count += 1
        
        print(f"\n  Summary: {success_count}/{total_count} permissions configured")
        
        if success_count < total_count:
            print("\n  ⚠️  Some permissions need manual configuration")
            print("     Check the Databricks UI for:")
            print("     1. SQL Warehouses → Permissions")
            print("     2. Data → Catalog → Permissions")
            print("     3. Compute → Lakebase → Permissions")
        
    except Exception as e:
        print(f"  ❌ Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
