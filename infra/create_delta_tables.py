#!/usr/bin/env python3
"""
Create Delta tables for ZeroStream.

This script creates:
1. Unity Catalog (if not exists) - with optional storage location
2. Schema (if not exists) - with optional storage location
3. Delta table (drops if exists, recreates fresh)
4. Disables checkConstraints feature

Environment variables used:
- CATALOG: Catalog name
- SCHEMA: Schema name
- TABLE_NAME: Table name
- CATALOG_STORAGE_LOCATION: Optional storage location for catalog
- SCHEMA_STORAGE_LOCATION: Optional storage location for schema
"""
import os
import sys
import time

# Add parent dir to path for config import
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


def get_client() -> WorkspaceClient:
    """Initialize Databricks SDK client."""
    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    
    if not host or not token:
        raise ValueError("DATABRICKS_HOST and DATABRICKS_TOKEN must be set")
    
    return WorkspaceClient(host=host, token=token)


def get_warehouse_id() -> str:
    """Get warehouse ID from generated config or environment."""
    # First try generated config
    script_dir = os.path.dirname(__file__)
    root_dir = os.path.dirname(script_dir)
    config_file = os.path.join(root_dir, "generated_config.env")
    
    if os.path.exists(config_file):
        with open(config_file, "r") as f:
            for line in f:
                line = line.strip()
                if line.startswith("DATABRICKS_WAREHOUSE_ID="):
                    return line.split("=", 1)[1]
    
    # Fall back to environment
    wh_id = os.environ.get("DATABRICKS_WAREHOUSE_ID")
    if not wh_id:
        raise ValueError("DATABRICKS_WAREHOUSE_ID not found in generated_config.env or environment")
    
    return wh_id


def execute_sql(client: WorkspaceClient, warehouse_id: str, sql: str, description: str, allow_fail: bool = False) -> bool:
    """Execute a SQL statement and wait for completion."""
    print(f"  ⏳ {description}...")
    try:
        stmt = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql,
            wait_timeout="50s",
        )

        # Poll until complete
        max_wait = 120
        waited = 0
        while stmt.status.state in (StatementState.PENDING, StatementState.RUNNING):
            time.sleep(2)
            waited += 2
            stmt = client.statement_execution.get_statement(stmt.statement_id)
            if waited >= max_wait:
                print(f"  ⚠️  Timeout waiting for: {description}")
                return False

        if stmt.status.state == StatementState.SUCCEEDED:
            print(f"  ✅ {description}")
            return True
        else:
            err = getattr(stmt.status, "error", None)
            err_msg = str(err) if err else "Unknown error"
            if allow_fail:
                print(f"  ⚠️  {description} skipped: {err_msg}")
                return True
            print(f"  ❌ {description} failed: {err_msg}")
            return False

    except Exception as e:
        if allow_fail:
            print(f"  ⚠️  {description} skipped: {e}")
            return True
        print(f"  ❌ {description} error: {e}")
        return False


def check_catalog_exists(client: WorkspaceClient, warehouse_id: str, catalog: str) -> bool:
    """Check if catalog already exists."""
    sql = f"SHOW CATALOGS LIKE '{catalog}'"
    try:
        stmt = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql,
            wait_timeout="30s",
        )
        
        # Wait for completion
        max_wait = 30
        waited = 0
        while stmt.status.state in (StatementState.PENDING, StatementState.RUNNING):
            time.sleep(1)
            waited += 1
            stmt = client.statement_execution.get_statement(stmt.statement_id)
            if waited >= max_wait:
                return False
        
        if stmt.status.state == StatementState.SUCCEEDED:
            result = stmt.result
            if result and result.data_array:
                return len(result.data_array) > 0
        return False
    except Exception:
        return False


def check_schema_exists(client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str) -> bool:
    """Check if schema already exists."""
    sql = f"SHOW SCHEMAS IN `{catalog}` LIKE '{schema}'"
    try:
        stmt = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql,
            wait_timeout="30s",
        )
        
        # Wait for completion
        max_wait = 30
        waited = 0
        while stmt.status.state in (StatementState.PENDING, StatementState.RUNNING):
            time.sleep(1)
            waited += 1
            stmt = client.statement_execution.get_statement(stmt.statement_id)
            if waited >= max_wait:
                return False
        
        if stmt.status.state == StatementState.SUCCEEDED:
            result = stmt.result
            if result and result.data_array:
                return len(result.data_array) > 0
        return False
    except Exception:
        return False


def main():
    print("\n" + "=" * 55)
    print("  Creating Delta Tables for ZeroStream")
    print("=" * 55 + "\n")

    client = get_client()
    warehouse_id = get_warehouse_id()

    # Get config
    catalog = os.environ.get("CATALOG")
    schema = os.environ.get("SCHEMA")
    table = os.environ.get("TABLE_NAME", "sensor_stream")
    
    # Storage locations (optional)
    catalog_location = os.environ.get("CATALOG_STORAGE_LOCATION", "").strip()
    schema_location = os.environ.get("SCHEMA_STORAGE_LOCATION", "").strip()
    
    if not catalog or not schema:
        print("  ❌ CATALOG and SCHEMA must be set")
        sys.exit(1)
    
    print(f"  Catalog  : {catalog}")
    print(f"  Schema   : {schema}")
    print(f"  Table    : {table}")
    if catalog_location:
        print(f"  Catalog Location: {catalog_location}")
    if schema_location:
        print(f"  Schema Location : {schema_location}")
    print()

    success_count = 0
    total_count = 0

    # ─────────────────────────────────────────────────────────────────────
    # Step 1: Create Catalog (if not exists)
    # ─────────────────────────────────────────────────────────────────────
    print("  Step 1: Checking/Creating Catalog...")
    print("  " + "─" * 45)
    
    catalog_exists = check_catalog_exists(client, warehouse_id, catalog)
    
    if catalog_exists:
        print(f"  ✅ Catalog '{catalog}' already exists")
        success_count += 1
        total_count += 1
    else:
        total_count += 1
        if catalog_location:
            sql = f"CREATE CATALOG IF NOT EXISTS `{catalog}` MANAGED LOCATION '{catalog_location}' COMMENT 'ZeroStream catalog'"
        else:
            sql = f"CREATE CATALOG IF NOT EXISTS `{catalog}` COMMENT 'ZeroStream catalog'"
        
        if execute_sql(client, warehouse_id, sql, f"Create catalog {catalog}", allow_fail=True):
            success_count += 1
        else:
            print(f"  ❌ Failed to create catalog. Check permissions and storage location.")
            sys.exit(1)

    # ─────────────────────────────────────────────────────────────────────
    # Step 2: Create Schema (if not exists)
    # ─────────────────────────────────────────────────────────────────────
    print("\n  Step 2: Checking/Creating Schema...")
    print("  " + "─" * 45)
    
    schema_exists = check_schema_exists(client, warehouse_id, catalog, schema)
    
    if schema_exists:
        print(f"  ✅ Schema '{catalog}.{schema}' already exists")
        success_count += 1
        total_count += 1
    else:
        total_count += 1
        if schema_location:
            sql = f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}` MANAGED LOCATION '{schema_location}' COMMENT 'ZeroBus sensor streaming schema'"
        else:
            sql = f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}` COMMENT 'ZeroBus sensor streaming schema'"
        
        if execute_sql(client, warehouse_id, sql, f"Create schema {catalog}.{schema}", allow_fail=False):
            success_count += 1
        else:
            print(f"  ❌ Failed to create schema.")
            sys.exit(1)

    # ─────────────────────────────────────────────────────────────────────
    # Step 3: Drop existing table (if exists)
    # ─────────────────────────────────────────────────────────────────────
    print("\n  Step 3: Dropping existing table (if exists)...")
    print("  " + "─" * 45)
    
    total_count += 1
    sql = f"DROP TABLE IF EXISTS `{catalog}`.`{schema}`.`{table}`"
    if execute_sql(client, warehouse_id, sql, f"Drop table {catalog}.{schema}.{table}", allow_fail=True):
        success_count += 1

    # ─────────────────────────────────────────────────────────────────────
    # Step 4: Create Delta Table
    # ─────────────────────────────────────────────────────────────────────
    print("\n  Step 4: Creating Delta Table...")
    print("  " + "─" * 45)
    
    total_count += 1
    create_table_sql = f"""
    CREATE TABLE `{catalog}`.`{schema}`.`{table}` (
        event_id          STRING        NOT NULL
                          COMMENT 'UUID v4 generated per event',
        connection_id     STRING        NOT NULL
                          COMMENT 'Simulated device connection ID',
        device_name       STRING
                          COMMENT 'Human-readable device label',
        event_timestamp   TIMESTAMP     NOT NULL
                          COMMENT 'Event generation time',
        event_date        DATE          NOT NULL
                          COMMENT 'Event generation date (device clock)',
        ingested_at       TIMESTAMP
                          COMMENT 'Landing time in Delta Lake',
        latitude          DOUBLE
                          COMMENT 'GPS latitude decimal degrees',
        longitude         DOUBLE
                          COMMENT 'GPS longitude decimal degrees',
        altitude_m        DOUBLE
                          COMMENT 'Altitude metres',
        heading_deg       DOUBLE
                          COMMENT 'Compass heading 0-360 degrees',
        pitch_deg         DOUBLE
                          COMMENT 'Pitch -90 to +90 degrees',
        roll_deg          DOUBLE
                          COMMENT 'Roll -180 to +180 degrees',
        accel_x           DOUBLE
                          COMMENT 'Acceleration X axis m/s2',
        accel_y           DOUBLE
                          COMMENT 'Acceleration Y axis m/s2',
        accel_z           DOUBLE
                          COMMENT 'Acceleration Z axis m/s2',
        accel_magnitude   DOUBLE
                          COMMENT 'Total acceleration magnitude m/s2',
        gyro_x            DOUBLE
                          COMMENT 'Rotation X axis deg/s',
        gyro_y            DOUBLE
                          COMMENT 'Rotation Y axis deg/s',
        gyro_z            DOUBLE
                          COMMENT 'Rotation Z axis deg/s',
        speed_kmh         DOUBLE
                          COMMENT 'Estimated speed km/h',
        battery_pct       INT
                          COMMENT 'Simulated battery percentage',
        signal_strength   INT
                          COMMENT 'Simulated RSSI dBm',
        zerobus_topic     STRING
                          COMMENT 'ZeroBus topic name',
        zerobus_offset    BIGINT
                          COMMENT 'ZeroBus message offset',
        payload_bytes     INT
                          COMMENT 'Raw payload size bytes'
    )
    USING DELTA
    PARTITIONED BY (event_date)
    TBLPROPERTIES (
        'delta.enableChangeDataFeed'             = 'true',
        'delta.autoOptimize.optimizeWrite'       = 'true',
        'delta.autoOptimize.autoCompact'         = 'true',
        'delta.columnMapping.mode'               = 'name',
        'delta.minReaderVersion'                 = '2',
        'delta.minWriterVersion'                 = '5',
        'pipelines.autoOptimize.zOrderCols'      = 'connection_id,event_timestamp',
        'delta.targetFileSize'                   = '134217728',
        'delta.checkpointInterval'               = '10'
    )
    COMMENT 'ZeroStream sensor events - source of truth'
    """
    
    if execute_sql(client, warehouse_id, create_table_sql.strip(), f"Create table {catalog}.{schema}.{table}", allow_fail=False):
        success_count += 1
    else:
        print(f"  ❌ Failed to create table.")
        sys.exit(1)

    # ─────────────────────────────────────────────────────────────────────
    # Step 5: Disable checkConstraints feature
    # ─────────────────────────────────────────────────────────────────────
    print("\n  Step 5: Disabling checkConstraints feature...")
    print("  " + "─" * 45)
    
    total_count += 1
    sql = f"ALTER TABLE `{catalog}`.`{schema}`.`{table}` DROP FEATURE checkConstraints"
    if execute_sql(client, warehouse_id, sql, "Disable checkConstraints", allow_fail=True):
        success_count += 1

    # ─────────────────────────────────────────────────────────────────────
    # Summary
    # ─────────────────────────────────────────────────────────────────────
    print("\n" + "=" * 55)
    print(f"  ✅ Delta Table Setup Complete ({success_count}/{total_count})")
    print("=" * 55)
    print(f"  Table        : {catalog}.{schema}.{table}")
    print(f"  CDF          : enabled")
    print(f"  checkConstraints: disabled")
    print(f"  Z-Order      : connection_id, event_timestamp")
    print()


if __name__ == "__main__":
    main()
