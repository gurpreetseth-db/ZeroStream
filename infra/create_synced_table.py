#!/usr/bin/env python3
"""
Create Lakebase Synced Table for real-time Delta to PostgreSQL sync.

Reads from environment variables:
- CATALOG: Unity Catalog name
- SCHEMA: Schema name
- TABLE_NAME: Delta table name
- LAKEBASE_INSTANCE: Lakebase instance name
- LAKEBASE_DATABASES: Lakebase database name  
- LAKEBASE_SCHEMA: Lakebase schema name (default: public)

Uses Databricks CLI for synced table operations (REST API not yet GA).
"""
import os
import sys
import json
import subprocess


def run_cli(cmd: list[str], timeout: int = 120) -> tuple[bool, str]:
    """Run a Databricks CLI command and return (success, output)."""
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        output = result.stdout + result.stderr
        return result.returncode == 0, output.strip()
    except subprocess.TimeoutExpired:
        return False, "Command timed out"
    except Exception as e:
        return False, str(e)


def get_synced_table(table_name: str) -> dict | None:
    """Get a synced table by full name using CLI."""
    cmd = [
        "databricks", "database", "get-synced-database-table",
        table_name,
        "--output", "json",
    ]
    success, output = run_cli(cmd)
    if success and output:
        try:
            return json.loads(output)
        except json.JSONDecodeError:
            return None
    return None


def create_synced_table(
    source_table: str,
    synced_table_name: str,
    instance_name: str,
    database_name: str,
    primary_key: str,
) -> tuple[bool, str, dict]:
    """Create a synced table from Delta to Lakebase using CLI."""
    
    print(f"  🔄 Creating synced table: {synced_table_name}")
    print(f"     Source: {source_table}")
    print(f"     Instance: {instance_name}")
    print(f"     Database: {database_name}")
    print(f"     Primary Key: {primary_key}")
    
    spec = {
        "name": synced_table_name,
        "database_instance_name": instance_name,
        "logical_database_name": database_name,
        "spec": {
            "source_table_full_name": source_table,
            "scheduling_policy": "CONTINUOUS",
            "primary_key_columns": [primary_key],
        }
    }

    
    cmd = [
        "databricks", "database", "create-synced-database-table",
        "--json", json.dumps(spec),
    ]
    
    success, output = run_cli(cmd, timeout=180)
    
    if success:
        try:
            result = json.loads(output)
            return True, output, result
        except json.JSONDecodeError:
            return True, output, {}
    
    return False, output, {}


def update_config(key: str, value: str, config_file: str):
    """Update or add a key in the config file."""
    if not os.path.exists(config_file):
        return
    
    with open(config_file, "r") as f:
        lines = f.readlines()
    
    found = False
    for i, line in enumerate(lines):
        if line.startswith(f"{key}="):
            lines[i] = f"{key}={value}\n"
            found = True
            break
    
    if not found:
        lines.append(f"{key}={value}\n")
    
    with open(config_file, "w") as f:
        f.writelines(lines)


def main():
    """Main entry point."""
    print("\n" + "=" * 60)
    print("  Lakebase Synced Table Setup")
    print("  (Delta → PostgreSQL Real-time Sync)")
    print("=" * 60 + "\n")
    
    # Get required environment variables
    catalog = os.environ.get("CATALOG", "").strip()
    schema = os.environ.get("SCHEMA", "").strip()
    table_name = os.environ.get("TABLE_NAME", "").strip()
    lakebase_instance = os.environ.get("LAKEBASE_INSTANCE", "").strip()
    lakebase_database = os.environ.get("LAKEBASE_DATABASES", "").strip()
    lakebase_schema = os.environ.get("LAKEBASE_SCHEMA", "public").strip()
    primary_key = os.environ.get("SYNCED_TABLE_PRIMARY_KEY", "event_id").strip()
    
    # Validate required vars
    missing = []
    if not catalog:
        missing.append("CATALOG")
    if not schema:
        missing.append("SCHEMA")
    if not table_name:
        missing.append("TABLE_NAME")
    if not lakebase_instance:
        missing.append("LAKEBASE_INSTANCE")
    if not lakebase_database:
        missing.append("LAKEBASE_DATABASES")
    
    if missing:
        print(f"  ⚠️  Missing required environment variables: {', '.join(missing)}")
        print("     Skipping synced table creation")
        return True  # Not a fatal error, just skip
    
    # Build table names
    source_table = f"{catalog}.{schema}.{table_name}"
    synced_table_name = f"{catalog}.{schema}.{table_name}_synced"
    
    print(f"  Configuration:")
    print(f"    Source Delta Table  : {source_table}")
    print(f"    Synced Table Name   : {synced_table_name}")
    print(f"    Lakebase Instance   : {lakebase_instance}")
    print(f"    Lakebase Database   : {lakebase_database}")
    print(f"    Lakebase Schema     : {lakebase_schema}")
    print(f"    Primary Key         : {primary_key}")
    print()
    
    # Check if synced table already exists
    print("Step 1/3 - Checking for existing synced table...")
    existing = get_synced_table(synced_table_name)
    
    if existing:
        sync_status = existing.get("data_synchronization_status", {})
        state = sync_status.get("detailed_state", "UNKNOWN")
        uc_state = existing.get("unity_catalog_provisioning_state", "UNKNOWN")
        pipeline_id = sync_status.get("pipeline_id", "N/A")
        
        print(f"  ✅ Synced table already exists: {synced_table_name}")
        print(f"     State      : {state}")
        print(f"     UC State   : {uc_state}")
        print(f"     Pipeline ID: {pipeline_id}")
        
        # Update config
        config_file = os.path.join(os.path.dirname(__file__), "..", "generated_config.env")
        if os.path.exists(config_file):
            update_config("SYNCED_TABLE_NAME", synced_table_name, config_file)
            if pipeline_id and pipeline_id != "N/A":
                update_config("SYNCED_TABLE_PIPELINE_ID", pipeline_id, config_file)
        
        if "ONLINE" in state or uc_state == "ACTIVE":
            print(f"\n  ✅ Synced table is active and syncing data")
        else:
            print(f"\n  ⚠️  Synced table exists but state is: {state}")
            print(f"     Check the pipeline for issues if data is not syncing")
        
        return True
    
    # Also check destination-side synced table name
    dest_synced_name = f"{lakebase_database}.{lakebase_schema}.{table_name}_synced"
    print(f"Step 2/3 - Checking destination: {dest_synced_name}...")
    existing_dest = get_synced_table(dest_synced_name)
    
    if existing_dest:
        sync_status = existing_dest.get("data_synchronization_status", {})
        state = sync_status.get("detailed_state", "UNKNOWN")
        print(f"  ✅ Synced table exists at destination: {dest_synced_name}")
        print(f"     State: {state}")
        return True
    
    # Create new synced table
    print(f"Step 3/3 - Creating synced table...")
    success, output, result = create_synced_table(
        source_table=source_table,
        synced_table_name=synced_table_name,
        instance_name=lakebase_instance,
        database_name=lakebase_database,
        primary_key=primary_key,
    )
    
    if not success:
        print(f"  ❌ Failed to create synced table")
        print(f"     {output}")
        
        if "already exists" in output.lower():
            print(f"\n  ℹ️  The synced table may exist with a different name")
            return True  # Not fatal
        elif "instance" in output.lower():
            print(f"\n  ℹ️  Verify Lakebase instance '{lakebase_instance}' exists and is running")
        elif "not found" in output.lower():
            print(f"\n  ℹ️  Verify source table '{source_table}' exists")
        elif "databricks" in output.lower() and "not found" in output.lower():
            print(f"\n  ℹ️  Databricks CLI not found or not configured")
            print(f"     Run: pip install databricks-cli && databricks configure")
        
        return False
    
    # Parse result
    sync_status = result.get("data_synchronization_status", {})
    pipeline_id = sync_status.get("pipeline_id", "N/A")
    state = sync_status.get("detailed_state", "PROVISIONING")
    
    print(f"  ✅ Synced table creation initiated")
    print(f"     Pipeline ID: {pipeline_id}")
    print(f"     State: {state}")
    
    # Update config file
    config_file = os.path.join(os.path.dirname(__file__), "..", "generated_config.env")
    if os.path.exists(config_file):
        update_config("SYNCED_TABLE_NAME", synced_table_name, config_file)
        if pipeline_id and pipeline_id != "N/A":
            update_config("SYNCED_TABLE_PIPELINE_ID", pipeline_id, config_file)
    
    # Show pipeline link
    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    if pipeline_id and pipeline_id != "N/A" and host:
        print(f"\n  View pipeline: {host}#joblist/pipelines/{pipeline_id}")
    
    print("\n  The synced table will automatically sync data from")
    print(f"  Delta table [{source_table}] to Lakebase PostgreSQL.")
    print("  Using TRIGGERED scheduling - syncs when source changes.")
    print("\n  ⏳ Note: Table may take a few minutes to become fully ONLINE")
    
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
