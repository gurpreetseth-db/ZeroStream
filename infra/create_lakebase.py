#!/usr/bin/env python3
"""
Create Lakebase Autoscaling Project for ZeroStream.

Creates a Lakebase Autoscaling project with:
- Production branch with autoscaling compute
- Custom database and schema
- Sensor stream table for direct writes from mobile app

Reads from environment variables:
- LAKEBASE_INSTANCE: Project ID for the Lakebase project
- LAKEBASE_DATABASES: Database name to create
- LAKEBASE_SCHEMA: Schema name to create
- LAKEBASE_MIN_CAPACITY: Minimum autoscaling CU (e.g. 8.0)
- LAKEBASE_MAX_CAPACITY: Maximum autoscaling CU (e.g. 16.0)
- LAKEBASE_SCALE_TO_ZERO: Minutes of inactivity before scale-to-zero
- LAKEBASE_PG_VERSION: PostgreSQL version (default: 17, supported: 16, 17)
- TABLE_NAME: Table name to create in Lakebase

Uses Databricks SDK w.postgres API for Lakebase Autoscaling.
"""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Supported Lakebase Postgres versions (update when new versions are available)
SUPPORTED_PG_VERSIONS = ["16", "17"]
DEFAULT_PG_VERSION = "17"


def _ensure_postgres_sdk():
    """Ensure the SDK with w.postgres support is loaded.

    If the installed SDK is too old, load the vendored 0.97.0 wheel.
    """
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient.__new__(WorkspaceClient)
        # Quick check without full init
        import databricks.sdk.service.postgres  # noqa: F401
        return  # installed SDK is new enough
    except (ImportError, ModuleNotFoundError):
        pass

    # Try vendored wheel
    root = os.path.dirname(os.path.dirname(__file__))
    whl = os.path.join(root, "dashboard_app", "wheels", "databricks_sdk-0.97.0-py3-none-any.whl")
    if not os.path.exists(whl):
        print(f"  ❌ SDK too old and vendored wheel not found at {whl}")
        print("     Upgrade: pip install 'databricks-sdk>=0.81.0'")
        sys.exit(1)

    # Purge old SDK modules and prepend vendored wheel
    for mod_name in list(sys.modules.keys()):
        if mod_name == "databricks" or mod_name.startswith("databricks."):
            del sys.modules[mod_name]
    sys.path.insert(0, whl)
    print("  ℹ️  Using vendored databricks-sdk 0.97.0 from wheel")


# Ensure SDK before any other imports
_ensure_postgres_sdk()


def get_workspace_client():
    """Get a WorkspaceClient using PAT token."""
    from databricks.sdk import WorkspaceClient

    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN")

    if not host or not token:
        raise ValueError("DATABRICKS_HOST and DATABRICKS_TOKEN must be set")

    return WorkspaceClient(host=host, token=token)


def get_or_create_project(w, project_id: str, display_name: str, pg_version: str) -> dict:
    """Create or get a Lakebase Autoscaling project."""
    from databricks.sdk.service.postgres import Project, ProjectSpec

    # Check if project already exists
    try:
        existing = w.postgres.get_project(name=f"projects/{project_id}")
        if existing:
            existing_ver = getattr(getattr(existing, "spec", None), "pg_version", None)
            print(f"  ℹ️  Project '{project_id}' already exists (PG version: {existing_ver})")
            if existing_ver and existing_ver != pg_version:
                print(f"  ⚠️  Existing project uses PG {existing_ver}, requested PG {pg_version}")
                print(f"     PG version cannot be changed after project creation.")
            return existing
    except Exception:
        pass  # Project doesn't exist, create it

    print(f"  🗄️  Creating Lakebase Autoscaling project: {project_id}")
    print(f"     Display Name: {display_name}")
    print(f"     PG Version:   {pg_version}")

    operation = w.postgres.create_project(
        project=Project(
            spec=ProjectSpec(
                display_name=display_name,
                pg_version=pg_version,
            )
        ),
        project_id=project_id,
    )

    print(f"     Waiting for project to be ready...")
    result = operation.wait()
    print(f"  ✅ Project created: {result.name} (PG {pg_version})")
    return result


def configure_endpoint(w, project_id: str, min_cu: float, max_cu: float, scale_to_zero_minutes: int):
    """Configure the production branch's primary endpoint with autoscaling."""
    from databricks.sdk.service.postgres import Duration, Endpoint, EndpointSpec, EndpointType, FieldMask

    branch_path = f"projects/{project_id}/branches/production"

    # List endpoints to find the primary one
    endpoints = list(w.postgres.list_endpoints(parent=branch_path))
    if not endpoints:
        raise RuntimeError(f"No endpoints found for {branch_path}")

    ep = endpoints[0]
    ep_name = ep.name
    print(f"  ⚙️  Configuring endpoint: {ep_name}")
    print(f"     Autoscaling: {min_cu} - {max_cu} CU")
    print(f"     Scale-to-zero: {scale_to_zero_minutes} minutes")

    # Step 1: Update autoscaling CU limits
    print(f"     Updating CU range to {min_cu} - {max_cu}...")
    w.postgres.update_endpoint(
        name=ep_name,
        endpoint=Endpoint(
            name=ep_name,
            spec=EndpointSpec(
                endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE,
                autoscaling_limit_min_cu=min_cu,
                autoscaling_limit_max_cu=max_cu,
            ),
        ),
        update_mask=FieldMask(field_mask=[
            "spec.autoscaling_limit_min_cu",
            "spec.autoscaling_limit_max_cu",
        ]),
    ).wait()

    # Step 2: Configure scale-to-zero timeout (separate update)
    if scale_to_zero_minutes > 0:
        scale_to_zero_seconds = scale_to_zero_minutes * 60
        print(f"     Setting scale-to-zero timeout: {scale_to_zero_seconds}s")
        try:
            w.postgres.update_endpoint(
                name=ep_name,
                endpoint=Endpoint(
                    name=ep_name,
                    spec=EndpointSpec(
                        endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE,
                        suspend_timeout_duration=Duration(seconds=scale_to_zero_seconds),
                    ),
                ),
                update_mask=FieldMask(field_mask=["spec.suspend_timeout_duration"]),
            ).wait()
        except Exception as e:
            print(f"  ⚠️  Could not set scale-to-zero timeout: {e}")
            print(f"     Configure manually in Lakebase UI")

    # Verify the update was applied
    endpoint = w.postgres.get_endpoint(name=ep_name)
    host = endpoint.status.hosts.host
    actual_min = getattr(endpoint.status, 'autoscaling_limit_min_cu', None)
    actual_max = getattr(endpoint.status, 'autoscaling_limit_max_cu', None)
    print(f"  ✅ Endpoint configured")
    print(f"     Host: {host}")
    print(f"     Actual CU range: {actual_min} - {actual_max}")

    if actual_min != min_cu or actual_max != max_cu:
        print(f"  ⚠️  CU range mismatch! Expected {min_cu}-{max_cu}, got {actual_min}-{actual_max}")
        print(f"     The endpoint may still be applying the update. Check Lakebase UI.")

    return ep_name, host


def create_database_and_table(w, project_id: str, endpoint_name: str, host: str):
    """Connect to Lakebase PG and create database, schema, and table."""
    import psycopg2

    database_name = os.environ.get("LAKEBASE_DATABASES", "").strip()
    schema_name = os.environ.get("LAKEBASE_SCHEMA", "public").strip()
    table_name = os.environ.get("TABLE_NAME", "sensor_stream").strip()

    # Generate OAuth token
    print(f"  🔑 Generating OAuth credential for database setup...")
    cred = w.postgres.generate_database_credential(endpoint=endpoint_name)
    username = w.current_user.me().user_name

    # Step 1: Create custom database (connect to default databricks_postgres)
    if database_name and database_name != "databricks_postgres":
        print(f"  📦 Creating database '{database_name}'...")
        try:
            conn = psycopg2.connect(
                host=host,
                port=5432,
                dbname="databricks_postgres",
                user=username,
                password=cred.token,
                sslmode="require",
            )
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(f'CREATE DATABASE "{database_name}"')
            cur.close()
            conn.close()
            print(f"  ✅ Database '{database_name}' created")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"  ℹ️  Database '{database_name}' already exists")
            else:
                print(f"  ⚠️  Could not create database: {e}")
                print(f"     Will use default 'databricks_postgres' database")
                database_name = "databricks_postgres"
    else:
        database_name = database_name or "databricks_postgres"

    # Step 2: Create schema and table in target database
    # Refresh token in case it expired during database creation
    cred = w.postgres.generate_database_credential(endpoint=endpoint_name)

    print(f"  📋 Creating schema '{schema_name}' and table '{table_name}'...")
    conn = psycopg2.connect(
        host=host,
        port=5432,
        dbname=database_name,
        user=username,
        password=cred.token,
        sslmode="require",
    )
    conn.autocommit = True
    cur = conn.cursor()

    # Create schema
    if schema_name and schema_name != "public":
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"')
        print(f"  ✅ Schema '{schema_name}' created")

    # Create table
    fqtn = f'"{schema_name}"."{table_name}"' if schema_name else f'"{table_name}"'
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {fqtn} (
        event_id          TEXT          NOT NULL PRIMARY KEY,
        connection_id     TEXT          NOT NULL,
        device_name       TEXT,
        event_timestamp   TIMESTAMP WITH TIME ZONE NOT NULL,
        event_date        DATE          NOT NULL,
        ingested_at       TIMESTAMP WITH TIME ZONE,
        latitude          DOUBLE PRECISION,
        longitude         DOUBLE PRECISION,
        altitude_m        DOUBLE PRECISION,
        heading_deg       DOUBLE PRECISION,
        pitch_deg         DOUBLE PRECISION,
        roll_deg          DOUBLE PRECISION,
        accel_x           DOUBLE PRECISION,
        accel_y           DOUBLE PRECISION,
        accel_z           DOUBLE PRECISION,
        accel_magnitude   DOUBLE PRECISION,
        gyro_x            DOUBLE PRECISION,
        gyro_y            DOUBLE PRECISION,
        gyro_z            DOUBLE PRECISION,
        speed_kmh         DOUBLE PRECISION,
        battery_pct       INTEGER,
        signal_strength   INTEGER,
        zerobus_topic     TEXT,
        zerobus_offset    BIGINT,
        payload_bytes     INTEGER
    )
    """
    cur.execute(create_table_sql)
    print(f"  ✅ Table {fqtn} created")

    # Create indexes for query performance
    idx_prefix = table_name.replace("-", "_")
    cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{idx_prefix}_conn_ts ON {fqtn} (connection_id, event_timestamp)')
    cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{idx_prefix}_ts ON {fqtn} (event_timestamp)')
    cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{idx_prefix}_conn_lat_lon ON {fqtn} (connection_id) WHERE latitude IS NOT NULL AND longitude IS NOT NULL')
    print(f"  ✅ Indexes created")

    cur.close()
    conn.close()
    return database_name


def save_config(project_id: str, endpoint_name: str, host: str, database_name: str):
    """Save Lakebase Autoscaling info to generated config file."""
    script_dir = os.path.dirname(__file__)
    root_dir = os.path.dirname(script_dir)
    config_file = os.path.join(root_dir, "generated_config.env")

    lakebase_schema = os.environ.get("LAKEBASE_SCHEMA", "public")
    table_name = os.environ.get("TABLE_NAME", "sensor_stream")

    lines = ["\n# ── Lakebase Autoscaling Instance ─────────────────────────────────"]
    lines.append(f"LAKEBASE_INSTANCE={project_id}")
    lines.append(f"LAKEBASE_HOST={host}")
    lines.append("LAKEBASE_PORT=5432")
    lines.append(f"LAKEBASE_DATABASES={database_name}")
    lines.append(f"LAKEBASE_SCHEMA={lakebase_schema}")
    lines.append(f"LAKEBASE_TABLE={table_name}")
    lines.append(f"LAKEBASE_ENDPOINT={endpoint_name}")

    with open(config_file, "a") as f:
        f.write("\n".join(lines))
        f.write("\n")

    print(f"\n  💾 Lakebase configuration saved to generated_config.env")


def main():
    try:
        project_id = os.environ.get("LAKEBASE_INSTANCE", "").strip()
        if not project_id:
            print("\n  ℹ️  LAKEBASE_INSTANCE not set - skipping Lakebase setup")
            return

        min_cu = float(os.environ.get("LAKEBASE_MIN_CAPACITY", "0.5"))
        max_cu = float(os.environ.get("LAKEBASE_MAX_CAPACITY", "8.0"))
        scale_to_zero = int(os.environ.get("LAKEBASE_SCALE_TO_ZERO", "5"))
        pg_version = os.environ.get("LAKEBASE_PG_VERSION", DEFAULT_PG_VERSION).strip()

        # Validate PG version
        if pg_version not in SUPPORTED_PG_VERSIONS:
            print(f"  ❌ Unsupported PG version '{pg_version}'. Supported: {', '.join(SUPPORTED_PG_VERSIONS)}")
            sys.exit(1)

        print(f"\n  Lakebase Autoscaling Configuration:")
        print(f"     Project ID     : {project_id}")
        print(f"     PG Version     : {pg_version}")
        print(f"     Min Capacity   : {min_cu} CU")
        print(f"     Max Capacity   : {max_cu} CU")
        print(f"     Scale-to-Zero  : {scale_to_zero} minutes")

        w = get_workspace_client()

        # Step 1: Create or get project
        project = get_or_create_project(w, project_id, f"ZeroStream - {project_id}", pg_version)

        # Step 2: Configure endpoint with autoscaling
        endpoint_name, host = configure_endpoint(w, project_id, min_cu, max_cu, scale_to_zero)

        # Step 3: Create database and table
        database_name = create_database_and_table(w, project_id, endpoint_name, host)

        # Step 4: Save config
        save_config(project_id, endpoint_name, host, database_name)

    except Exception as e:
        print(f"  ❌ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
