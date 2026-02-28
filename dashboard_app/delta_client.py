"""
Delta / DBSQL client for the ZeroBus raw data view.
Uses Databricks SQL Statement Execution API via SDK.
"""
import logging
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from config.settings import databricks_cfg, delta_cfg

logger = logging.getLogger("delta_client")

# ── SDK client singleton ───────────────────────────────────────────────────────
_sdk_client: Optional[WorkspaceClient] = None


def get_sdk_client() -> WorkspaceClient:
    global _sdk_client
    if _sdk_client is None:
        # In Databricks Apps, SDK auto-authenticates via OAuth env vars
        # Don't pass explicit token when OAuth is available (causes conflict)
        if os.environ.get("DATABRICKS_CLIENT_ID"):
            # Running in Databricks Apps - use auto-configured OAuth
            logger.info("Using Databricks Apps OAuth authentication")
            _sdk_client = WorkspaceClient()
        elif databricks_cfg.token:
            # Running locally with PAT token
            logger.info("Using PAT token authentication")
            _sdk_client = WorkspaceClient(
                host=databricks_cfg.host,
                token=databricks_cfg.token,
            )
        else:
            # Let SDK try to auto-detect
            logger.info("Using auto-detected authentication")
            _sdk_client = WorkspaceClient()

        logger.info("✅ Databricks SDK client initialised")
    return _sdk_client


# ── Query executor ─────────────────────────────────────────────────────────────
def execute_sql(
    sql: str,
    timeout: str = "50s",
) -> Tuple[List[Dict[str, Any]], float]:
    """
    Execute SQL against the Delta table via DBSQL warehouse.
    Returns (rows_as_dicts, elapsed_ms).
    """
    client = get_sdk_client()
    start  = time.perf_counter()

    try:
        statement = client.statement_execution.execute_statement(
            warehouse_id=databricks_cfg.warehouse_id,
            statement=sql,
            wait_timeout=timeout,
        )

        # Poll until complete
        while statement.status.state in (
            StatementState.PENDING,
            StatementState.RUNNING,
        ):
            time.sleep(0.5)
            statement = client.statement_execution.get_statement(
                statement.statement_id
            )

        elapsed_ms = round((time.perf_counter() - start) * 1000, 2)

        if statement.status.state != StatementState.SUCCEEDED:
            err = statement.status.error
            logger.error(f"SQL failed: {err}")
            return [], elapsed_ms

        # Parse result
        schema = statement.manifest.schema.columns
        col_names = [c.name for c in schema]

        rows = []
        if statement.result and statement.result.data_array:
            for raw_row in statement.result.data_array:
                rows.append(dict(zip(col_names, raw_row)))

        return rows, elapsed_ms

    except Exception as e:
        elapsed_ms = round((time.perf_counter() - start) * 1000, 2)
        logger.error(f"execute_sql error: {e}")
        return [], elapsed_ms


# ── ZeroBus stream view ────────────────────────────────────────────────────────
def get_zerobus_stream(
    limit: int = 500,
    offset: int = 0,
    connection_id: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], float]:
    """
    Fetch recent sensor events from the Delta table.
    This is the 'SHOW DATA SERVED BY ZEROBUS' view.
    """
    where = ""
    if connection_id:
        safe_cid = connection_id.replace("'", "''")
        where = f"WHERE connection_id = '{safe_cid}'"

    sql = f"""
        SELECT
            event_id,
            connection_id,
            device_name,
            DATE_FORMAT(event_timestamp, 'yyyy-MM-dd HH:mm:ss.SSS') AS event_timestamp,
            DATE_FORMAT(event_date, 'yyyy-MM-dd') AS event_date,
            ROUND(latitude,  6)  AS latitude,
            ROUND(longitude, 6)  AS longitude,
            ROUND(altitude_m, 1) AS altitude_m,
            ROUND(heading_deg, 1) AS heading_deg,
            ROUND(pitch_deg,   1) AS pitch_deg,
            ROUND(roll_deg,    1) AS roll_deg,
            ROUND(accel_x,     3) AS accel_x,
            ROUND(accel_y,     3) AS accel_y,
            ROUND(accel_z,     3) AS accel_z,
            ROUND(accel_magnitude, 3) AS accel_magnitude,
            ROUND(gyro_x,  3) AS gyro_x,
            ROUND(gyro_y,  3) AS gyro_y,
            ROUND(gyro_z,  3) AS gyro_z,
            ROUND(speed_kmh, 1) AS speed_kmh,
            battery_pct,
            signal_strength,
            zerobus_topic,
            zerobus_offset,
            payload_bytes,
            DATE_FORMAT(ingested_at, 'yyyy-MM-dd HH:mm:ss') AS ingested_at
        FROM {delta_cfg.full_name}
        {where}
        ORDER BY event_timestamp DESC
        LIMIT  {limit}
        OFFSET {offset}
    """
    return execute_sql(sql)


def get_stream_count(connection_id: Optional[str] = None) -> int:
    """Return total row count for pagination."""
    where = ""
    if connection_id:
        safe_cid = connection_id.replace("'", "''")
        where = f"WHERE connection_id = '{safe_cid}'"

    sql = f"SELECT COUNT(*) AS cnt FROM {delta_cfg.full_name} {where}"
    rows, _ = execute_sql(sql)
    if rows:
        return int(rows[0].get("cnt", 0))
    return 0


# ── Dashboard queries (via Delta) ──────────────────────────────────────────────
# These mirror the Lakebase functions but query Delta directly via SQL warehouse

def get_dashboard_summary() -> Dict[str, Any]:
    """Get summary statistics for the dashboard (all-time stats)."""
    sql = f"""
        SELECT 
            COUNT(DISTINCT connection_id) as unique_clients,
            COUNT(*) as total_events,
            COALESCE(SUM(payload_bytes), 0) as total_payload_bytes,
            MAX(event_timestamp) as last_event_time
        FROM {delta_cfg.full_name}
    """
    rows, _ = execute_sql(sql)
    
    if rows:
        row = rows[0]
        return {
            "unique_clients": int(row.get("unique_clients") or 0),
            "total_events": int(row.get("total_events") or 0),
            "total_payload_bytes": int(row.get("total_payload_bytes") or 0),
            "last_event_time": row.get("last_event_time"),
        }
    return {
        "unique_clients": 0,
        "total_events": 0,
        "total_payload_bytes": 0,
        "last_event_time": None,
    }


def get_client_list(limit: int = 100, offset: int = 0) -> Tuple[List[Dict[str, Any]], int]:
    """Get list of ALL clients with their aggregate stats."""
    # Get all clients with their stats
    sql = f"""
        SELECT 
            connection_id,
            FIRST(device_name) as device_name,
            COUNT(*) as event_count,
            COALESCE(SUM(payload_bytes), 0) as total_bytes,
            MAX(event_timestamp) as last_event,
            MIN(event_timestamp) as first_event
        FROM {delta_cfg.full_name}
        GROUP BY connection_id
        ORDER BY last_event DESC
        LIMIT {limit} OFFSET {offset}
    """
    rows, _ = execute_sql(sql)
    
    from datetime import datetime, timedelta, timezone
    now = datetime.now(timezone.utc)
    active_threshold = now - timedelta(minutes=5)
    
    clients = []
    for row in rows:
        last_event = row.get("last_event")
        # Check if active (event within last 5 minutes)
        is_active = False
        if last_event:
            try:
                if isinstance(last_event, str):
                    # Parse the timestamp string
                    last_dt = datetime.fromisoformat(last_event.replace('Z', '+00:00'))
                else:
                    last_dt = last_event
                if last_dt.tzinfo is None:
                    last_dt = last_dt.replace(tzinfo=timezone.utc)
                is_active = last_dt > active_threshold
            except:
                pass
        
        clients.append({
            "connection_id": row.get("connection_id"),
            "device_name": row.get("device_name"),
            "event_count": int(row.get("event_count") or 0),
            "total_bytes": int(row.get("total_bytes") or 0),
            "last_event": last_event,
            "last_event_time": last_event,  # Alias for JS compatibility
            "first_event": row.get("first_event"),
            "is_active": is_active,
        })
    
    # Get total count
    count_sql = f"SELECT COUNT(DISTINCT connection_id) as cnt FROM {delta_cfg.full_name}"
    count_rows, _ = execute_sql(count_sql)
    total = int(count_rows[0].get("cnt") or 0) if count_rows else 0
    
    return clients, total


def get_client_track(connection_id: str, limit: int = 1000) -> List[Dict[str, Any]]:
    """Get location track for a specific client, ordered oldest to newest."""
    safe_cid = connection_id.replace("'", "''")
    sql = f"""
        SELECT 
            latitude, longitude, 
            DATE_FORMAT(event_timestamp, 'yyyy-MM-dd HH:mm:ss') as event_time,
            speed_kmh, heading_deg, battery_pct
        FROM {delta_cfg.full_name}
        WHERE connection_id = '{safe_cid}'
          AND latitude IS NOT NULL 
          AND longitude IS NOT NULL
        ORDER BY event_timestamp ASC
        LIMIT {limit}
    """
    rows, _ = execute_sql(sql)
    
    track = []
    for row in rows:
        track.append({
            "lat": float(row.get("latitude") or 0),
            "lon": float(row.get("longitude") or 0),  # JS uses 'lon' not 'lng'
            "lng": float(row.get("longitude") or 0),  # Keep both for compatibility
            "event_time": row.get("event_time"),
            "speed_kmh": float(row.get("speed_kmh") or 0),
            "heading_deg": float(row.get("heading_deg") or 0),
            "battery_pct": int(row.get("battery_pct") or 0),
        })
    return track


def get_all_latest_locations() -> Tuple[List[Dict[str, Any]], int]:
    """Get latest location for each client with stats."""
    sql = f"""
        WITH ranked AS (
            SELECT 
                connection_id, device_name, latitude, longitude,
                event_timestamp, battery_pct, signal_strength, speed_kmh,
                ROW_NUMBER() OVER (PARTITION BY connection_id ORDER BY event_timestamp DESC) as rn
            FROM {delta_cfg.full_name}
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        ),
        client_stats AS (
            SELECT 
                connection_id,
                COUNT(*) as event_count,
                COALESCE(SUM(payload_bytes), 0) as total_bytes
            FROM {delta_cfg.full_name}
            GROUP BY connection_id
        )
        SELECT 
            r.connection_id, r.device_name, r.latitude, r.longitude,
            DATE_FORMAT(r.event_timestamp, 'yyyy-MM-dd HH:mm:ss') as event_time,
            r.battery_pct, r.signal_strength, r.speed_kmh,
            s.event_count, s.total_bytes
        FROM ranked r
        JOIN client_stats s ON r.connection_id = s.connection_id
        WHERE r.rn = 1
    """
    rows, _ = execute_sql(sql)
    
    from datetime import datetime, timedelta, timezone
    now = datetime.now(timezone.utc)
    active_threshold = now - timedelta(minutes=5)
    
    locations = []
    for row in rows:
        event_time = row.get("event_time")
        is_active = False
        try:
            if event_time:
                last_dt = datetime.strptime(event_time, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                is_active = last_dt > active_threshold
        except:
            pass
        
        locations.append({
            "connection_id": row.get("connection_id"),
            "device_name": row.get("device_name"),
            "latitude": float(row.get("latitude") or 0),  # JS map expects latitude/longitude
            "longitude": float(row.get("longitude") or 0),
            "lat": float(row.get("latitude") or 0),  # Keep both for compatibility
            "lng": float(row.get("longitude") or 0),
            "event_time": event_time,
            "battery_pct": int(row.get("battery_pct") or 0),
            "signal_strength": int(row.get("signal_strength") or 0),
            "speed_kmh": float(row.get("speed_kmh") or 0),
            "event_count": int(row.get("event_count") or 0),
            "total_bytes": int(row.get("total_bytes") or 0),
            "is_active": is_active,
        })
    
    return locations, len(locations)


def get_client_summary(connection_id: str) -> Dict[str, Any]:
    """Get detailed summary for a specific client."""
    safe_cid = connection_id.replace("'", "''")
    
    sql = f"""
        SELECT 
            COUNT(*) as total_events,
            COALESCE(SUM(payload_bytes), 0) as total_bytes,
            MIN(event_timestamp) as first_event,
            MAX(event_timestamp) as last_event,
            ROUND(AVG(speed_kmh), 1) as avg_speed,
            ROUND(AVG(battery_pct), 0) as avg_battery,
            FIRST(device_name) as device_name
        FROM {delta_cfg.full_name}
        WHERE connection_id = '{safe_cid}'
    """
    rows, _ = execute_sql(sql)
    
    if rows:
        row = rows[0]
        # Get latest position
        pos_sql = f"""
            SELECT latitude, longitude, 
                   DATE_FORMAT(event_timestamp, 'yyyy-MM-dd HH:mm:ss') as event_time
            FROM {delta_cfg.full_name}
            WHERE connection_id = '{safe_cid}'
              AND latitude IS NOT NULL AND longitude IS NOT NULL
            ORDER BY event_timestamp DESC
            LIMIT 1
        """
        pos_rows, _ = execute_sql(pos_sql)
        
        pos = None
        if pos_rows:
            pos = {
                "latitude": float(pos_rows[0].get("latitude") or 0),
                "longitude": float(pos_rows[0].get("longitude") or 0),
                "lat": float(pos_rows[0].get("latitude") or 0),
                "lng": float(pos_rows[0].get("longitude") or 0),
                "event_time": pos_rows[0].get("event_time"),
            }
        
        # Check if active
        from datetime import datetime, timedelta, timezone
        is_active = False
        last_event = row.get("last_event")
        if last_event:
            try:
                now = datetime.now(timezone.utc)
                active_threshold = now - timedelta(minutes=5)
                if isinstance(last_event, str):
                    last_dt = datetime.fromisoformat(last_event.replace('Z', '+00:00'))
                else:
                    last_dt = last_event
                if last_dt.tzinfo is None:
                    last_dt = last_dt.replace(tzinfo=timezone.utc)
                is_active = last_dt > active_threshold
            except:
                pass
        
        return {
            "connection_id": connection_id,
            "device_name": row.get("device_name"),
            "total_events": int(row.get("total_events") or 0),
            "total_bytes": int(row.get("total_bytes") or 0),
            "first_event": row.get("first_event"),
            "last_event": row.get("last_event"),
            "avg_speed": float(row.get("avg_speed") or 0),
            "avg_battery": int(row.get("avg_battery") or 0),
            "latest": pos,  # JS expects 'latest' not 'latest_position'
            "latest_position": pos,  # Keep for compatibility
            "is_active": is_active,
        }
    
    return {
        "connection_id": connection_id,
        "device_name": None,
        "total_events": 0,
        "total_bytes": 0,
        "first_event": None,
        "last_event": None,
        "avg_speed": 0,
        "avg_battery": 0,
        "latest_position": None,
    }