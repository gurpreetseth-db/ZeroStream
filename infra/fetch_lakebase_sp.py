#!/usr/bin/env python3
"""
Fetch or create the Lakebase Service Principal (SP) for the given Lakebase instance.
If the SP exists, fetch its client ID. If not, create it. Update generated_config.env.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

script_dir = Path(__file__).parent
root_dir = script_dir.parent
if (root_dir / ".env").exists():
    load_dotenv(root_dir / ".env")
if (root_dir / "generated_config.env").exists():
    load_dotenv(root_dir / "generated_config.env", override=True)

import requests

def get_auth():
    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN")
    if not host or not token:
        raise ValueError("DATABRICKS_HOST and DATABRICKS_TOKEN must be set")
    return host, token

def update_generated_config(key, value):
    config_path = root_dir / "generated_config.env"
    lines = []
    found = False
    if config_path.exists():
        with open(config_path) as f:
            for line in f:
                if line.strip().startswith(f"{key}="):
                    lines.append(f"{key}={value}\n")
                    found = True
                else:
                    lines.append(line)
    if not found:
        lines.append(f"{key}={value}\n")
    with open(config_path, "w") as f:
        f.writelines(lines)

def fetch_lakebase_sp():
    host, token = get_auth()
    instance = os.environ.get("LAKEBASE_INSTANCE")
    if not instance:
        print("No LAKEBASE_INSTANCE set in env")
        return None
    # Query the Lakebase instance for its SP
    url = f"{host}/api/2.0/sql/lakebase-instances/{instance}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        data = resp.json()
        sp_id = data.get("service_principal_id") or data.get("service_principal_client_id")
        if sp_id:
            print(f"Lakebase SP client ID: {sp_id}")
            update_generated_config("LAKEBASE_SP_CLIENT_ID", sp_id)
            return sp_id
        else:
            print("Lakebase SP not found in instance details.")
            return None
    else:
        print(f"Failed to fetch Lakebase instance: {resp.status_code} {resp.text}")
        return None

if __name__ == "__main__":
    fetch_lakebase_sp()
