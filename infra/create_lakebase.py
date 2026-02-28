#!/usr/bin/env python3
"""
Create Lakebase (Databricks Managed PostgreSQL) Instance.

Reads from environment variables:
- LAKEBASE_INSTANCE: Name of the Lakebase instance
- LAKEBASE_CATALOG: Unity Catalog to register in (optional)
- LAKEBASE_DATABASE: Database name (optional)
- LAKEBASE_CAPACITY: Instance capacity (CU_1, CU_2, CU_4, CU_8) default CU_1

Uses REST API directly for compatibility with older SDK versions.
"""
import os
import sys
import time
import requests


def get_auth():
    """Get Databricks host and token."""
    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN")
    
    if not host or not token:
        raise ValueError("DATABRICKS_HOST and DATABRICKS_TOKEN must be set")
    
    return host, token


def get_instance(host: str, token: str, name: str) -> dict | None:
    """Get a Lakebase instance by name."""
    url = f"{host}/api/2.0/database/instances/{name}"
    headers = {"Authorization": f"Bearer {token}"}
    
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        return resp.json()
    return None


def create_lakebase_instance(host: str, token: str) -> dict:
    """Create a Lakebase Provisioned instance."""
    
    instance_name = os.environ.get("LAKEBASE_INSTANCE")
    capacity = os.environ.get("LAKEBASE_CAPACITY", "CU_1").strip()
    
    if not instance_name:
        print("  ⚠️  LAKEBASE_INSTANCE not set - skipping Lakebase creation")
        return {"name": None, "status": "skipped"}
    
    # Check if already exists
    existing = get_instance(host, token, instance_name)
    if existing:
        print(f"  ℹ️  Lakebase instance '{instance_name}' already exists")
        print(f"     DNS: {existing.get('read_write_dns')}")
        print(f"     State: {existing.get('state')}")
        return {
            "name": instance_name,
            "dns": existing.get("read_write_dns"),
            "state": existing.get("state"),
            "status": "exists",
        }
    
    print(f"  🗄️  Creating Lakebase instance: {instance_name}")
    print(f"     Capacity: {capacity}")
    
    try:
        url = f"{host}/api/2.0/database/instances"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        payload = {
            "name": instance_name,
            "capacity": capacity,
            "stopped": False,
        }
        
        resp = requests.post(url, headers=headers, json=payload)
        
        if resp.status_code not in (200, 201):
            raise Exception(f"API error {resp.status_code}: {resp.text}")
        
        instance = resp.json()
        
        # Wait for instance to be ready
        print(f"     Waiting for instance to be ready...")
        for _ in range(60):
            time.sleep(5)
            info = get_instance(host, token, instance_name)
            if info:
                state = info.get("state", "")
                if state == "RUNNING":
                    print(f"  ✅ Lakebase instance created and running")
                    print(f"     DNS: {info.get('read_write_dns')}")
                    return {
                        "name": instance_name,
                        "dns": info.get("read_write_dns"),
                        "state": state,
                        "status": "created",
                    }
                elif state == "FAILED":
                    raise Exception("Instance creation failed")
                print(f"     State: {state}...")
        
        # Return even if not fully ready
        return {
            "name": instance_name,
            "dns": instance.get("read_write_dns"),
            "state": instance.get("state", "PENDING"),
            "status": "created",
        }
        
    except Exception as e:
        print(f"  ⚠️  Could not create Lakebase instance: {e}")
        return {
            "name": instance_name,
            "dns": None,
            "status": f"error: {e}",
        }


def register_with_catalog(host: str, token: str, instance_name: str) -> bool:
    """Register Lakebase instance with Unity Catalog."""
    
    # Use the CATALOG env var, not LAKEBASE_INSTANCE
    catalog = os.environ.get("LAKEBASE_CATALOG", "").strip()
    database_name = os.environ.get("LAKEBASE_DATABASES", "").strip()
    
    if not catalog:
        print("  ℹ️  CATALOG not set - skipping Unity Catalog registration")
        return False
    
    if not database_name:
        print("  ℹ️  LAKEBASE_DATABASES not set - skipping Unity Catalog registration")
        return False
    
    print(f"  📋 Registering Lakebase with Unity Catalog:")
    print(f"     Catalog: {catalog}")
    print(f"     Instance: {instance_name}")
    print(f"     Database: {database_name}")
    
    try:
        url = f"{host}/api/2.0/database/catalogs"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        # API uses 'name' not 'catalog_name'
        payload = {
            "name": f"{catalog}",
            "database_instance_name": instance_name,
            "database_name": database_name,
        }
        
        resp = requests.post(url, headers=headers, json=payload)
        
        if resp.status_code not in (200, 201):
            # May already be registered
            if "already registered" in resp.text.lower() or "already exists" in resp.text.lower():
                print(f"  ℹ️  Already registered with catalog '{catalog}'")
                return True
            print(f"  ⚠️  Registration failed: {resp.text}")
            return False
        
        print(f"  ✅ Registered with catalog '{catalog}'")
        return True
        
    except Exception as e:
        print(f"  ⚠️  Could not register with catalog: {e}")
        return False


def save_config(instance_info: dict):
    """Save Lakebase info to generated config file in main directory."""
    script_dir = os.path.dirname(__file__)
    root_dir = os.path.dirname(script_dir)
    config_file = os.path.join(root_dir, "generated_config.env")
    
    # Collect values to append
    lines = ["\n# ── Lakebase Instance ───────────────────────────────────────────"]
    
    if instance_info.get("name"):
        lines.append(f"LAKEBASE_INSTANCE={instance_info['name']}")
        
    if instance_info.get("dns"):
        lines.append(f"LAKEBASE_HOST={instance_info['dns']}")
        lines.append("LAKEBASE_PORT=5432")
    
    # Include database and schema from env (if not already in base config)
    lakebase_db = os.environ.get("LAKEBASE_DATABASES") or os.environ.get("LAKEBASE_DATABASE")
    lakebase_schema = os.environ.get("LAKEBASE_SCHEMA")
    
    if lakebase_db:
        lines.append(f"LAKEBASE_DATABASES={lakebase_db}")
    if lakebase_schema:
        lines.append(f"LAKEBASE_SCHEMA={lakebase_schema}")
    
    # Append to file
    with open(config_file, "a") as f:
        f.write("\n".join(lines))
        f.write("\n")
    
    print(f"\n  💾 Lakebase configuration saved to generated_config.env")


def main():
    try:
        host, token = get_auth()
        
        instance_name = os.environ.get("LAKEBASE_INSTANCE")
        
        if not instance_name:
            print("\n  ℹ️  LAKEBASE_INSTANCE not set - skipping Lakebase setup")
            return
        
        print(f"\n  Lakebase Configuration:")
        print(f"     Instance: {instance_name}")
        print(f"     Capacity: {os.environ.get('LAKEBASE_CAPACITY', 'CU_1')}")
        
        # Create instance
        instance_info = create_lakebase_instance(host, token)
        

        # Call create_synced_table.py
        import subprocess
        sync_script = os.path.join(os.path.dirname(__file__), "create_synced_table.py")
        print("\n▶ Running create_synced_table.py to create synced table...")
        result = subprocess.run([sys.executable, sync_script], capture_output=True, text=True)
        print(result.stdout)
        if result.returncode != 0:
            print(f"❌ create_synced_table.py failed: {result.stderr}")
            sys.exit(result.returncode)


        # Register with catalog if instance was created/exists
        if instance_info.get("name") and instance_info.get("status") in ("created", "exists"):
            register_with_catalog(host, token, instance_info["name"])
        
        # Save config
        save_config(instance_info)
        
    except Exception as e:
        print(f"  ❌ Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
