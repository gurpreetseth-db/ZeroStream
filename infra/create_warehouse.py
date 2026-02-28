#!/usr/bin/env python3
"""
Create a Databricks SQL Warehouse for ZeroStream.

Reads from environment variables:
- WAREHOUSE_NAME: Name of the warehouse
- WAREHOUSE_TYPE: Type (PRO, CLASSIC)
- WAREHOUSE_CLUSTER_SIZE: Size (2X-Small, X-Small, Small, Medium, Large, etc.)
- WAREHOUSE_MAX_NUM_CLUSTERS: Max number of clusters (default: 1)
- WAREHOUSE_ENABLE_SERVERLESS_COMPUTE: Enable serverless (true/false)

Output: Saves warehouse ID to generated_config.env
"""
import os
import sys
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    CreateWarehouseRequestWarehouseType,
    SpotInstancePolicy,
    State,
)


def get_client() -> WorkspaceClient:
    """Initialize Databricks SDK client."""
    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    
    if not host or not token:
        raise ValueError("DATABRICKS_HOST and DATABRICKS_TOKEN must be set")
    
    return WorkspaceClient(host=host, token=token)


def find_existing_warehouse(client: WorkspaceClient, name: str):
    """Check if a warehouse with the given name already exists."""
    warehouses = client.warehouses.list()
    for wh in warehouses:
        if wh.name == name:
            return wh
    return None


def create_warehouse(client: WorkspaceClient) -> str:
    """Create a new SQL warehouse or return existing one."""
    
    warehouse_name = os.environ.get("WAREHOUSE_NAME", "ZeroStream-Warehouse")
    warehouse_type = os.environ.get("WAREHOUSE_TYPE", "PRO").upper()
    cluster_size = os.environ.get("WAREHOUSE_CLUSTER_SIZE", "Small")
    max_num_clusters = int(os.environ.get("WAREHOUSE_MAX_NUM_CLUSTERS", "1"))
    enable_serverless = os.environ.get("WAREHOUSE_ENABLE_SERVERLESS_COMPUTE", "true").lower() == "true"
    auto_stop_mins = int(os.environ.get("WAREHOUSE_AUTO_STOP", "10"))
    
    # Check if warehouse already exists
    existing = find_existing_warehouse(client, warehouse_name)
    if existing:
        print(f"  ℹ️  Warehouse '{warehouse_name}' already exists")
        print(f"     ID: {existing.id}")
        print(f"     State: {existing.state}")
        
        # Start it if stopped
        if existing.state == State.STOPPED:
            print(f"  🚀 Starting warehouse...")
            client.warehouses.start(existing.id)
            for _ in range(30):
                wh = client.warehouses.get(existing.id)
                if wh.state == State.RUNNING:
                    break
                time.sleep(5)
        
        return existing.id
    
    # Map warehouse type string to enum
    type_map = {
        "PRO": CreateWarehouseRequestWarehouseType.PRO,
        "CLASSIC": CreateWarehouseRequestWarehouseType.CLASSIC,
    }
    wh_type_enum = type_map.get(warehouse_type, CreateWarehouseRequestWarehouseType.PRO)
    
    # Create new warehouse
    print(f"  📦 Creating SQL Warehouse: {warehouse_name}")
    print(f"     Type: {warehouse_type}")
    print(f"     Cluster Size: {cluster_size}")
    print(f"     Max Clusters: {max_num_clusters}")
    print(f"     Serverless: {enable_serverless}")
    print(f"     Auto-stop: {auto_stop_mins} minutes")
    
    warehouse = client.warehouses.create_and_wait(
        name=warehouse_name,
        cluster_size=cluster_size,
        warehouse_type=wh_type_enum,
        max_num_clusters=max_num_clusters,
        auto_stop_mins=auto_stop_mins,
        spot_instance_policy=SpotInstancePolicy.COST_OPTIMIZED,
        enable_serverless_compute=enable_serverless,
    )
    
    print(f"  ✅ Warehouse created!")
    print(f"     ID: {warehouse.id}")
    print(f"     State: {warehouse.state}")
    
    return warehouse.id


def save_config(warehouse_id: str):
    """Save warehouse ID to generated config file in main directory."""
    script_dir = os.path.dirname(__file__)
    root_dir = os.path.dirname(script_dir)
    config_file = os.path.join(root_dir, "generated_config.env")
    
    # Read existing config as key-value dict (preserves values)
    config = {}
    if os.path.exists(config_file):
        with open(config_file, "r") as f:
            for line in f:
                line = line.strip()
                if line and "=" in line and not line.startswith("#"):
                    key, val = line.split("=", 1)
                    config[key] = val
    
    # Update with warehouse config
    config["DATABRICKS_WAREHOUSE_ID"] = warehouse_id
    
    # Append to file (setup_infra.sh creates the base structure)
    with open(config_file, "a") as f:
        f.write("\n# ── SQL Warehouse ────────────────────────────────────────────\n")
        f.write(f"DATABRICKS_WAREHOUSE_ID={warehouse_id}\n")
    
    print(f"\n  💾 Warehouse ID saved to generated_config.env")
    print(f"     DATABRICKS_WAREHOUSE_ID={warehouse_id}")


def main():
    try:
        client = get_client()
        warehouse_id = create_warehouse(client)
        save_config(warehouse_id)
        
    except Exception as e:
        print(f"  ❌ Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
