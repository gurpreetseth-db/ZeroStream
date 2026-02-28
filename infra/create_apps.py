#!/usr/bin/env python3
"""
Create Databricks Apps for ZeroStream.

Reads from environment variables:
- MOBILE_APP_NAME: Name of mobile simulator app
- DASHBOARD_APP_NAME: Name of backend dashboard app
- APP_COMPUTE_SIZE: Compute size (SMALL, MEDIUM, LARGE)

Creates blank apps that can be deployed to later.
Stores service principal IDs for permission grants.

Uses REST API directly for compatibility with older SDK versions.
"""
import os
import sys
import time
import requests
from pathlib import Path
from dotenv import load_dotenv

# Load generated_config.env to get DATABRICKS_WAREHOUSE_ID from earlier steps
script_dir = Path(__file__).parent
root_dir = script_dir.parent
generated_config = root_dir / "generated_config.env"
if generated_config.exists():
    load_dotenv(generated_config, override=True)


def get_auth():
    """Get Databricks host and token."""
    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN")
    
    if not host or not token:
        raise ValueError("DATABRICKS_HOST and DATABRICKS_TOKEN must be set")
    
    return host, token


def app_exists(host: str, token: str, name: str) -> dict | None:
    """Check if an app exists and return it."""
    url = f"{host}/api/2.0/apps/{name}"
    headers = {"Authorization": f"Bearer {token}"}
    
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        return resp.json()
    return None


def create_app(host: str, token: str, name: str, description: str, compute_size: str) -> dict:
    """Create a Databricks App or return existing one."""
    
    # Normalize app name: lowercase, alphanumeric and dashes only
    normalized_name = name.lower().strip()
    
    existing = app_exists(host, token, normalized_name)
    if existing:
        print(f"  ℹ️  App '{normalized_name}' already exists")
        print(f"     URL: {existing.get('url')}")
        print(f"     Service Principal ID: {existing.get('service_principal_id')}")
        return {
            "name": existing.get("name"),
            "url": existing.get("url"),
            "service_principal_id": existing.get("service_principal_id"),
            "service_principal_client_id": existing.get("service_principal_client_id"),
            "status": "exists",
        }
    
    print(f"  📱 Creating app: {normalized_name}")
    print(f"     Compute size: {compute_size}")
    
    try:
        # Create app via REST API
        url = f"{host}/api/2.0/apps"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        payload = {
            "name": normalized_name,
            "description": description,
        }
        
        # Only add warehouse resource if ID is available
        warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "").strip()
        if warehouse_id:
            payload["resources"] = [
                {
                    "name": "sql-warehouse",
                    "sql_warehouse": {
                        "id": warehouse_id,
                        "permission": "CAN_USE"
                    } 
                }
            ]
            print(f"     SQL Warehouse: {warehouse_id}")
        else:
            print(f"     ℹ️  No warehouse ID - will add resource later via grant_permissions.py")
        
        resp = requests.post(url, headers=headers, json=payload)
        
        if resp.status_code not in (200, 201):
            raise Exception(f"API error {resp.status_code}: {resp.text}")
        
        # Wait for creation to complete
        print(f"     Waiting for app creation...")
        app_info = None
        for _ in range(30):
            time.sleep(2)
            app_info = app_exists(host, token, normalized_name)
            if app_info and app_info.get("service_principal_id"):
                break
        
        if not app_info:
            app_info = resp.json()
        
        print(f"  ✅ App '{normalized_name}' created")
        print(f"     URL: {app_info.get('url') or 'Pending...'}")
        print(f"     Service Principal ID: {app_info.get('service_principal_id')}")
        
        return {
            "name": app_info.get("name"),
            "url": app_info.get("url"),
            "service_principal_id": app_info.get("service_principal_id"),
            "service_principal_client_id": app_info.get("service_principal_client_id"),
            "status": "created",
        }
        
    except Exception as e:
        print(f"  ⚠️  Could not create app '{normalized_name}': {e}")
        return {
            "name": normalized_name,
            "url": None,
            "service_principal_id": None,
            "status": f"error: {e}",
        }


def save_config(apps: dict):
    """Save app info to generated config file in main directory."""
    script_dir = os.path.dirname(__file__)
    root_dir = os.path.dirname(script_dir)
    config_file = os.path.join(root_dir, "generated_config.env")
    
    # Collect values to append
    lines = ["\n# ── Databricks Apps ─────────────────────────────────────────────"]
    
    if apps.get("mobile"):
        lines.append(f"MOBILE_APP={apps['mobile']['name']}")
        if apps["mobile"].get("url"):
            lines.append(f"MOBILE_APP_URL={apps['mobile']['url']}")
        if apps["mobile"].get("service_principal_id"):
            lines.append(f"MOBILE_APP_SP_ID={apps['mobile']['service_principal_id']}")
        if apps["mobile"].get("service_principal_client_id"):
            lines.append(f"MOBILE_APP_SP_CLIENT_ID={apps['mobile']['service_principal_client_id']}")
    
    if apps.get("dashboard"):
        lines.append(f"DASHBOARD_APP={apps['dashboard']['name']}")
        if apps["dashboard"].get("url"):
            lines.append(f"DASHBOARD_APP_URL={apps['dashboard']['url']}")
        if apps["dashboard"].get("service_principal_id"):
            lines.append(f"DASHBOARD_APP_SP_ID={apps['dashboard']['service_principal_id']}")
        if apps["dashboard"].get("service_principal_client_id"):
            lines.append(f"DASHBOARD_APP_SP_CLIENT_ID={apps['dashboard']['service_principal_client_id']}")
    

    
    # Append to file
    with open(config_file, "a") as f:
        f.write("\n".join(lines))
        f.write("\n")
    
    print(f"\n  💾 App configuration saved to generated_config.env")


def main():
    try:
        host, token = get_auth()
        
        # Read config from env
        mobile_app_name = os.environ.get("MOBILE_APP_NAME")
        dashboard_app_name = os.environ.get("DASHBOARD_APP_NAME")
        compute_size = os.environ.get("APP_COMPUTE_SIZE", "MEDIUM")
        
        apps = {}
        
        # Create mobile app
        if mobile_app_name:
            print("\n  Creating Mobile Simulator App...")
            apps["mobile"] = create_app(
                host,
                token,
                mobile_app_name,
                "ZeroStream Mobile Simulator - generates GPS sensor data via ZeroBus",
                compute_size,
            )
        
        # Create dashboard app
        if dashboard_app_name:
            print("\n  Creating Dashboard App...")
            apps["dashboard"] = create_app(
                host,
                token,
                dashboard_app_name,
                "ZeroStream Dashboard - displays client locations and movement tracks",
                compute_size,
            )
        
       
        # Save config
        save_config(apps)
        
    except Exception as e:
        print(f"  ❌ Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
