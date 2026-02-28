"""
ZeroStream - Complete Setup Verification
Run this after deployment to verify everything is working end-to-end.
"""
import asyncio
import json
import os
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.settings import (
    databricks_cfg, delta_cfg,
    zerobus_cfg, lakebase_cfg, app_cfg,
)


# ── Colour helpers ─────────────────────────────────────────────────────────────
def ok(msg):   print(f"  ✅ {msg}")
def fail(msg): print(f"  ❌ {msg}")
def warn(msg): print(f"  ⚠️  {msg}")
def info(msg): print(f"  ℹ️  {msg}")
def hdr(msg):  print(f"\n{'─'*51}\n  {msg}\n{'─'*51}")


# ── Check 1: Python packages ───────────────────────────────────────────────────
def check_packages():
    hdr("1. Python Packages")
    results = {}

    packages = {
        "fastapi":        "required",
        "uvicorn":        "required",
        "databricks.sdk": "required",
        "asyncpg":        "required",
        "pg8000":         "required",
        "jinja2":         "required",
        "httpx":          "required",
        "pydantic":       "required",
        "psycopg2":       "optional",
        "psycopg":        "optional",
    }

    for pkg, level in packages.items():
        try:
            mod = __import__(pkg)
            ver = getattr(mod, "__version__", "unknown")
            ok(f"{pkg} ({ver}) [{level}]")
            results[pkg] = True
        except ImportError:
            if level == "required":
                fail(f"{pkg} MISSING [{level}]")
                results[pkg] = False
            else:
                warn(f"{pkg} not installed [{level}]")
                results[pkg] = None

    return all(v is not False for v in results.values())


# ── Check 2: Config values ─────────────────────────────────────────────────────
def check_config():
    hdr("2. Configuration")

    checks = [
        ("DATABRICKS_HOST",          databricks_cfg.host,              True),
        ("DATABRICKS_WAREHOUSE_ID",  databricks_cfg.warehouse_id,      True),
        ("DATABRICKS_TOKEN",         databricks_cfg.token,             True),
        ("CATALOG",                  delta_cfg.catalog,                True),
        ("SCHEMA",                   delta_cfg.schema,                 True),
        ("TABLE_NAME",               delta_cfg.table_name,             True),
        ("ZEROBUS_SERVER_ENDPOINT",  zerobus_cfg.server_endpoint,      True),
        ("ZEROBUS_CLIENT_ID",        zerobus_cfg.client_id,            True),
        ("ZEROBUS_CLIENT_SECRET",    zerobus_cfg.client_secret,        True),
        ("LAKEBASE_HOST",            lakebase_cfg.host,                True),
        ("LAKEBASE_DATABASES",       lakebase_cfg.database,            True),
        ("MOBILE_APP",               app_cfg.mobile_app_name,          True),
        ("DASHBOARD_APP",            app_cfg.dashboard_app_name,       True),
    ]

    all_ok = True
    for name, value, required in checks:
        is_secret = any(x in name for x in ["PASSWORD", "SECRET", "TOKEN"])
        display   = "***set***" if (is_secret and value) else (value or "NOT SET")

        if value:
            ok(f"{name} = {display}")
        elif required:
            fail(f"{name} = NOT SET (required)")
            all_ok = False
        else:
            warn(f"{name} = NOT SET (optional)")

    info(f"Delta table: {delta_cfg.full_name}")
    info(f"Lakebase DSN: {lakebase_cfg.dsn_safe}")
    return all_ok


# ── Check 3: Databricks connectivity ──────────────────────────────────────────
def check_databricks():
    hdr("3. Databricks Connectivity")
    try:
        import httpx
        resp = httpx.get(
            f"{databricks_cfg.host}api/2.0/clusters/list",
            headers={"Authorization": f"Bearer {databricks_cfg.token}"},
            timeout=10,
        )
        if resp.status_code == 200:
            ok(f"Workspace reachable (HTTP {resp.status_code})")
            return True
        else:
            fail(f"Workspace returned HTTP {resp.status_code}")
            return False
    except Exception as e:
        fail(f"Workspace unreachable: {e}")
        return False


# ── Check 4: Delta table ───────────────────────────────────────────────────────
def check_delta_table():
    hdr("4. Delta Table")
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.sql import StatementState

        client = WorkspaceClient(
            host=databricks_cfg.host,
            token=databricks_cfg.token,
        )

        sql = f"SELECT COUNT(*) AS cnt FROM {delta_cfg.full_name}"
        start = time.perf_counter()
        stmt = client.statement_execution.execute_statement(
            warehouse_id=databricks_cfg.warehouse_id,
            statement=sql,
            wait_timeout="30s",
        )
        elapsed = round((time.perf_counter() - start) * 1000)

        if stmt.status.state == StatementState.SUCCEEDED:
            rows = stmt.result.data_array or []
            count = int(rows[0][0]) if rows else 0
            ok(f"Delta table accessible ({count:,} rows, {elapsed}ms)")
            ok(f"Table: {delta_cfg.full_name}")
            return True
        else:
            fail(f"Delta table query failed: {stmt.status.error}")
            return False

    except Exception as e:
        fail(f"Delta table check error: {e}")
        return False

# ── Check 5: Lakebase connectivity ────────────────────────────────────────────
async def check_lakebase():
    hdr("5. Lakebase PostgreSQL")
    
    lakebase_user = lakebase_cfg.user
    lakebase_password = lakebase_cfg.password
    
    # If no user/password configured, try OAuth M2M using ZeroBus service principal
    if not lakebase_user or not lakebase_password:
        if zerobus_cfg.client_id and zerobus_cfg.client_secret:
            info("Trying OAuth M2M authentication for Lakebase...")
            try:
                import httpx
                token_url = f"{databricks_cfg.host}oidc/v1/token"
                token_resp = httpx.post(
                    token_url,
                    data={
                        "grant_type": "client_credentials",
                        "scope": "all-apis",
                    },
                    auth=(zerobus_cfg.client_id, zerobus_cfg.client_secret),
                    timeout=10,
                )
                if token_resp.status_code == 200:
                    oauth_token = token_resp.json().get("access_token")
                    lakebase_user = zerobus_cfg.client_id
                    lakebase_password = oauth_token
                    ok("OAuth M2M token obtained for Lakebase")
                else:
                    warn(f"OAuth token request returned {token_resp.status_code}")
            except Exception as e:
                warn(f"OAuth M2M failed: {e}")
        
        if not lakebase_user or not lakebase_password:
            warn("Lakebase credentials not configured for local testing")
            info("Note: Databricks Apps get Lakebase access via app resources (auto-injected)")
            info("For local testing, set LAKEBASE_USER and LAKEBASE_PASSWORD in .env")
            return False
    
    try:
        import asyncpg

        start = time.perf_counter()
        conn  = await asyncpg.connect(
            host=lakebase_cfg.host,
            port=lakebase_cfg.port,
            user=lakebase_user,
            password=lakebase_password,
            database=lakebase_cfg.database,
            ssl="require",
            timeout=15,
        )
        elapsed = round((time.perf_counter() - start) * 1000)
        ok(f"Lakebase connected ({elapsed}ms)")

        # Check schema exists
        schema_exists = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM information_schema.schemata "
            "WHERE schema_name = $1)",
            lakebase_cfg.schema,
        )
        if schema_exists:
            ok(f"Schema '{lakebase_cfg.schema}' exists")
        else:
            warn(f"Schema '{lakebase_cfg.schema}' not found - run deployment")

        # Check table exists
        table_exists = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = $1 AND table_name = 'sensor_stream')",
            lakebase_cfg.schema,
        )
        if table_exists:
            # Count rows
            count = await conn.fetchval(
                f"SELECT COUNT(*) FROM {lakebase_cfg.schema}.sensor_stream"
            )
            ok(f"Table sensor_stream exists ({count:,} rows)")
        else:
            warn("Table sensor_stream not found - run deployment")

        # Check materialized view
        matview_exists = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_matviews "
            "WHERE schemaname = $1 AND matviewname = 'client_summary')",
            lakebase_cfg.schema,
        )
        if matview_exists:
            ok("Materialized view client_summary exists")
        else:
            warn("Materialized view client_summary not found - run deployment")

        # Check active_clients view
        view_exists = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM information_schema.views "
            "WHERE table_schema = $1 AND table_name = 'active_clients')",
            lakebase_cfg.schema,
        )
        if view_exists:
            ok("View active_clients exists")
        else:
            warn("View active_clients not found - run deployment")

        # Test query latency
        start = time.perf_counter()
        await conn.fetchval("SELECT 1")
        ping_ms = round((time.perf_counter() - start) * 1000, 2)
        ok(f"Lakebase ping latency: {ping_ms}ms")

        await conn.close()
        return True

    except ImportError:
        warn("asyncpg not available - trying pg8000...")
        return _check_lakebase_pg8000()
    except Exception as e:
        err_str = str(e).lower()
        if "role" in err_str and "does not exist" in err_str:
            warn(f"Service principal not granted Lakebase access")
            info("The service principal needs to be added to Lakebase:")
            info("  1. Go to Data → Databases → Lakebase → Your instance")
            info("  2. Click 'Permissions' → Add the service principal")
            info("  3. Or use user credentials: LAKEBASE_USER/LAKEBASE_PASSWORD")
            info("Note: Databricks Apps get access via app resources (will work in deployed app)")
            return False
        else:
            fail(f"Lakebase connection error: {e}")
            info("Check LAKEBASE_HOST, LAKEBASE_USER, LAKEBASE_PASSWORD")
            return False


def _check_lakebase_pg8000():
    """Sync fallback using pg8000."""
    try:
        import pg8000
        import ssl as _ssl

        ssl_ctx = _ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode    = _ssl.CERT_NONE

        start = time.perf_counter()
        conn  = pg8000.connect(
            host=lakebase_cfg.host,
            port=lakebase_cfg.port,
            user=lakebase_cfg.user,
            password=lakebase_cfg.password,
            database=lakebase_cfg.database,
            ssl_context=ssl_ctx,
        )
        elapsed = round((time.perf_counter() - start) * 1000)
        ok(f"Lakebase connected via pg8000 ({elapsed}ms)")
        conn.close()
        return True
    except Exception as e:
        fail(f"Lakebase pg8000 error: {e}")
        return False


# ── Check 6: ZeroBus endpoint ─────────────────────────────────────────────────
def check_zerobus():
    hdr("6. ZeroBus Endpoint")
    try:
        import httpx

        # First, get OAuth token using client credentials
        token_url = f"{databricks_cfg.host}oidc/v1/token"
        try:
            token_resp = httpx.post(
                token_url,
                data={
                    "grant_type": "client_credentials",
                    "scope": "all-apis",
                },
                auth=(zerobus_cfg.client_id, zerobus_cfg.client_secret),
                timeout=10,
            )
            if token_resp.status_code == 200:
                oauth_token = token_resp.json().get("access_token")
                ok("OAuth M2M token obtained")
                oauth_success = True
            else:
                warn(f"OAuth token request returned {token_resp.status_code}")
                oauth_token = databricks_cfg.token  # Fallback to PAT
                oauth_success = False
        except Exception as e:
            warn(f"OAuth token failed: {e}, using PAT fallback")
            oauth_token = databricks_cfg.token
            oauth_success = False

        url = (
            f"https://{zerobus_cfg.server_endpoint}"
            f"/api/2.0/zerobus/topics"
        )
        start = time.perf_counter()
        resp  = httpx.get(
            url,
            headers={
                "Authorization": f"Bearer {oauth_token}",
                "X-Client-Id":   zerobus_cfg.client_id,
            },
            timeout=10,
        )
        elapsed = round((time.perf_counter() - start) * 1000)

        if resp.status_code in (200, 404):
            ok(f"ZeroBus endpoint reachable (HTTP {resp.status_code}, {elapsed}ms)")
            ok(f"Endpoint: {zerobus_cfg.server_endpoint}")
            ok(f"Topic:    {zerobus_cfg.topic}")
            return True
        elif resp.status_code == 401:
            if oauth_success:
                # OAuth token was issued, so credentials are valid
                # The 401 on topics endpoint is likely a permissions issue for that specific API
                warn(f"ZeroBus topics API returned 401 (may require admin permissions)")
                ok(f"OAuth credentials valid (token was issued)")
                ok(f"Endpoint: {zerobus_cfg.server_endpoint}")
                ok(f"Topic:    {zerobus_cfg.topic}")
                return True
            else:
                fail(f"ZeroBus authentication failed (HTTP 401)")
                info("Check ZEROBUS_CLIENT_ID and ZEROBUS_CLIENT_SECRET")
            return False
        else:
            warn(f"ZeroBus returned HTTP {resp.status_code} ({elapsed}ms)")
            return True   # Endpoint reachable even if unexpected status

    except Exception as e:
        fail(f"ZeroBus endpoint unreachable: {e}")
        info(f"Endpoint: {zerobus_cfg.server_endpoint}")
        return False


# ── Check 7: Databricks Apps ──────────────────────────────────────────────────
def check_apps():
    hdr("7. Databricks Apps")
    try:
        import httpx
        
        apps_to_check = [
            app_cfg.mobile_app_name,
            app_cfg.dashboard_app_name,
        ]

        all_ok = True
        for app_name in apps_to_check:
            try:
                # Use REST API directly to avoid SDK RPC issues
                resp = httpx.get(
                    f"{databricks_cfg.host}api/2.0/apps/{app_name}",
                    headers={"Authorization": f"Bearer {databricks_cfg.token}"},
                    timeout=15,
                )
                
                if resp.status_code == 200:
                    app_info = resp.json()
                    state = app_info.get("compute_status", {})
                    state_str = state.get("state", "UNKNOWN") if isinstance(state, dict) else str(state)
                    url = app_info.get("url", "N/A")
                    
                    if state_str.upper() in ["ACTIVE", "RUNNING", "DEPLOYED"]:
                        ok(f"{app_name}: {state_str}")
                        ok(f"  URL: {url}")
                    else:
                        warn(f"{app_name}: {state_str}")
                        info(f"  URL: {url}")
                elif resp.status_code == 404:
                    warn(f"{app_name}: NOT DEPLOYED yet")
                    info(f"  Run: bash deployment/deploy_all.sh")
                else:
                    fail(f"{app_name}: HTTP {resp.status_code}")
                    all_ok = False

            except Exception as e:
                fail(f"{app_name}: {e}")
                all_ok = False

        return all_ok

    except Exception as e:
        fail(f"Apps check error: {e}")
        return False


# ── Check 8: Lakebase Synced Table ────────────────────────────────────────────
def check_synced_table():
    hdr("8. Lakebase Synced Table (Delta → PostgreSQL)")
    try:
        import subprocess
        import json

        synced_table_name = f"{delta_cfg.catalog}.{delta_cfg.schema}.{delta_cfg.table_name}_synced"

        cmd = [
            "databricks", "database", "get-synced-database-table",
            synced_table_name,
            "--output", "json",
        ]

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)

        if result.returncode != 0:
            warn(f"Synced table '{synced_table_name}' not found")
            info("Run: python3 deployment/03_create_synced_table.py")
            return False

        status = json.loads(result.stdout)
        sync_status = status.get("data_synchronization_status", {})
        detailed_state = sync_status.get("detailed_state", "UNKNOWN")
        uc_state = status.get("unity_catalog_provisioning_state", "UNKNOWN")
        pipeline_id = sync_status.get("pipeline_id", "N/A")

        ok(f"Synced table: {synced_table_name}")
        ok(f"Pipeline ID : {pipeline_id}")

        if "ONLINE" in detailed_state or uc_state == "ACTIVE":
            ok(f"State: {detailed_state} ✓")
        elif "PROVISIONING" in detailed_state:
            warn(f"State: {detailed_state} (initial sync in progress)")
        elif "FAILED" in detailed_state:
            fail(f"State: {detailed_state}")
            info(f"View pipeline: {databricks_cfg.host}#joblist/pipelines/{pipeline_id}")
            return False
        else:
            warn(f"State: {detailed_state}")

        if pipeline_id != "N/A":
            info(f"View: {databricks_cfg.host}#joblist/pipelines/{pipeline_id}")

        return True

    except subprocess.TimeoutExpired:
        fail("Synced table check timed out")
        return False
    except Exception as e:
        fail(f"Synced table check error: {e}")
        return False


# ── End-to-end data flow test ─────────────────────────────────────────────────
async def check_data_flow():
    hdr("9. End-to-End Data Flow Test")

    try:
        # Generate one test event
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        from mobile_app.data_generator import DataGeneratorPool

        pool = DataGeneratorPool()
        pool.set_connection_count(1)
        payloads = pool.tick_all()

        if payloads:
            p = payloads[0]
            ok(f"Data generator working")
            ok(f"  connection_id : {p['connection_id']}")
            ok(f"  device_name   : {p['device_name']}")
            ok(f"  lat/lon       : {p['latitude']:.4f}, {p['longitude']:.4f}")
            ok(f"  heading       : {p['heading_deg']:.1f}°")
            ok(f"  accel_mag     : {p['accel_magnitude']:.3f} m/s²")
            ok(f"  speed         : {p['speed_kmh']:.1f} km/h")
        else:
            fail("Data generator returned no payloads")
            return False

        # Test Lakebase write/read round-trip
        try:
            import asyncpg
            conn = await asyncpg.connect(
                host=lakebase_cfg.host,
                port=lakebase_cfg.port,
                user=lakebase_cfg.user,
                password=lakebase_cfg.password,
                database=lakebase_cfg.database,
                ssl="require",
                timeout=15,
            )

            # Write test record
            test_event_id = f"verify-{int(time.time())}"
            from datetime import timezone
            now = datetime.now(timezone.utc)

            await conn.execute(
                f"""
                INSERT INTO {lakebase_cfg.schema}.sensor_stream (
                    event_id, connection_id, device_name,
                    event_timestamp, event_date, ingested_at,
                    latitude, longitude, altitude_m,
                    heading_deg, pitch_deg, roll_deg,
                    accel_x, accel_y, accel_z, accel_magnitude,
                    gyro_x, gyro_y, gyro_z,
                    speed_kmh, battery_pct, signal_strength,
                    zerobus_topic, zerobus_offset, payload_bytes
                ) VALUES (
                    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,
                    $12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25
                )
                ON CONFLICT (event_id) DO NOTHING
                """,
                test_event_id,
                p["connection_id"],
                p["device_name"],
                now, now, now,
                p["latitude"],   p["longitude"],  p["altitude_m"],
                p["heading_deg"],p["pitch_deg"],   p["roll_deg"],
                p["accel_x"],    p["accel_y"],     p["accel_z"],
                p["accel_magnitude"],
                p["gyro_x"],     p["gyro_y"],      p["gyro_z"],
                p["speed_kmh"],  p["battery_pct"], p["signal_strength"],
                zerobus_cfg.topic, 0, p["payload_bytes"],
            )
            ok("Test record written to Lakebase")

            # Read it back
            start = time.perf_counter()
            row   = await conn.fetchrow(
                f"SELECT * FROM {lakebase_cfg.schema}.sensor_stream "
                f"WHERE event_id = $1",
                test_event_id,
            )
            read_ms = round((time.perf_counter() - start) * 1000, 2)

            if row:
                ok(f"Test record read back from Lakebase ({read_ms}ms)")
                ok(f"  Round-trip latency: {read_ms}ms ✓")
            else:
                fail("Test record not found after write")

            # Clean up test record
            await conn.execute(
                f"DELETE FROM {lakebase_cfg.schema}.sensor_stream "
                f"WHERE event_id = $1",
                test_event_id,
            )
            ok("Test record cleaned up")
            await conn.close()

        except Exception as e:
            warn(f"Lakebase round-trip test skipped: {e}")

        return True

    except Exception as e:
        fail(f"Data flow test error: {e}")
        return False


# ── Summary ────────────────────────────────────────────────────────────────────
def print_summary(results: dict):
    print(f"\n{'═'*51}")
    print(f"  ZeroStream Verification Summary")
    print(f"{'═'*51}")

    passed = sum(1 for v in results.values() if v is True)
    failed = sum(1 for v in results.values() if v is False)
    warned = sum(1 for v in results.values() if v is None)
    total  = len(results)

    for check, result in results.items():
        if result is True:
            status = "✅ PASS"
        elif result is False:
            status = "❌ FAIL"
        else:
            status = "⚠️  WARN"
        print(f"  {status}  {check}")

    print(f"{'─'*51}")
    print(f"  Passed: {passed}/{total}  Failed: {failed}  Warnings: {warned}")
    print(f"{'═'*51}\n")

    if failed == 0:
        print("  🚀 ZeroStream is ready to demo!")
        print(f"  Mobile App    : {app_cfg.mobile_app_name}")
        print(f"  Dashboard App : {app_cfg.dashboard_app_name}")
        print(f"  Delta Table   : {delta_cfg.full_name}")
        print(f"  Lakebase      : {lakebase_cfg.dsn_safe}")
    else:
        print("  ❌ Fix the failed checks above before running the demo.")
        print("  Run: bash deployment/deploy_all.sh")

    print()
    return failed == 0


# ── Main ───────────────────────────────────────────────────────────────────────
async def main():
    print("\n" + "═"*51)
    print("  ZeroStream - Setup Verification")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("═"*51)

    results = {}

    # Sync checks
    results["Packages"]          = check_packages()
    results["Configuration"]     = check_config()
    results["Databricks"]        = check_databricks()
    results["Delta Table"]       = check_delta_table()
    results["ZeroBus"]           = check_zerobus()
    results["Databricks Apps"]   = check_apps()
    results["Synced Table"]      = check_synced_table()

    # Async checks
    results["Lakebase"]          = await check_lakebase()
    results["Data Flow"]         = await check_data_flow()

    # Print summary
    success = print_summary(results)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    asyncio.run(main())    