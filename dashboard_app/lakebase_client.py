"""
Lakebase PostgreSQL client for ZeroStream dashboard.
Uses psycopg2 for queries to the Lakebase Autoscaling table.

Authentication:
  In Databricks Apps runtime: uses auto-injected PGUSER/PGPASSWORD env vars.
  Locally: uses Databricks SDK w.postgres.generate_database_credential() to
  obtain OAuth tokens. Tokens expire every hour and are auto-refreshed at the
  55-minute mark.
"""
import asyncio
import logging
import os
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import psycopg2

from config.settings import lakebase_cfg

try:
    from databricks.sdk import WorkspaceClient
except ImportError:
    WorkspaceClient = None

# Table name — uses TABLE_NAME (direct write table, not synced)
_TABLE = lakebase_cfg.table or os.environ.get("TABLE_NAME", "sensor_stream")
_SCHEMA = lakebase_cfg.schema or "public"
_FQTN = f'"{_SCHEMA}"."{_TABLE}"'

logger = logging.getLogger("lakebase_client")

# ── Token + connection state ───────────────────────────────────────────────────
_conn:             Optional[psycopg2.extensions.connection] = None
_conn_lock:        asyncio.Lock                             = asyncio.Lock()
_current_token:    str                                      = ""
_current_user:     str                                      = ""      # PG username from SDK
_token_expires_at: float                                    = 0.0     # unix timestamp
_REFRESH_BUFFER:   int                                      = 300     # refresh 5 min before expiry

# Databricks SDK client (lazy init — only used for local dev)
_ws_client = None


def _get_ws_client() -> WorkspaceClient:
    """Return a singleton WorkspaceClient. Auto-authenticates via env vars in Databricks Apps."""
    global _ws_client
    if _ws_client is None:
        _ws_client = WorkspaceClient()
        logger.info("✅ Databricks WorkspaceClient initialised")
    return _ws_client


# ── Token management ───────────────────────────────────────────────────────────

def _token_needs_refresh() -> bool:
    """Return True if token is missing, expired, or within the refresh buffer window."""
    if not _current_token:
        return True
    return time.time() >= (_token_expires_at - _REFRESH_BUFFER)


def _refresh_token() -> str:
    """
    Use the Databricks SDK to generate a fresh OAuth token for Lakebase Autoscaling.
    Updates module-level _current_token and _token_expires_at.

    w.postgres.generate_database_credential() is the official SDK method for
    Lakebase Autoscaling — it handles OAuth plumbing and returns a short-lived token.
    """
    global _current_token, _current_user, _token_expires_at

    endpoint_name = lakebase_cfg.endpoint
    project_id = lakebase_cfg.instance

    if not endpoint_name and not project_id:
        raise RuntimeError(
            "LAKEBASE_ENDPOINT or LAKEBASE_INSTANCE is not set. "
            "Run infra/setup_infra.sh to provision the Lakebase project."
        )

    logger.info(f"🔄 Refreshing Lakebase OAuth token for project '{project_id}'...")

    w = _get_ws_client()

    # Discover endpoint if not explicitly configured
    if not endpoint_name and project_id:
        endpoints = list(w.postgres.list_endpoints(
            parent=f"projects/{project_id}/branches/production"
        ))
        if endpoints:
            endpoint_name = endpoints[0].name
        else:
            raise RuntimeError(
                f"No endpoints found for projects/{project_id}/branches/production"
            )

    cred = w.postgres.generate_database_credential(
        endpoint=endpoint_name,
    )

    if not cred or not cred.token:
        raise RuntimeError("generate_database_credential() returned no token")

    _current_token    = cred.token
    _current_user     = w.current_user.me().user_name
    # SDK doesn't return expires_in, so assume 1 hour (standard Databricks token TTL)
    _token_expires_at = time.time() + 3600

    logger.info(f"✅ Lakebase OAuth token refreshed for user '{_current_user}' (valid ~1hr)")
    return _current_token


def _use_injected_fallback():
    """Fallback: use PGUSER/PGPASSWORD injected by the Databricks Apps resource block."""
    global _current_token, _current_user, _token_expires_at
    pw = os.environ.get("PGPASSWORD", "")
    user = os.environ.get("PGUSER", "")
    if pw and user:
        _current_token = pw
        _current_user = user
        _token_expires_at = time.time() + 3600  # assume 1hr
        logger.info(f"Using injected PGPASSWORD fallback for user '{user}'")
    else:
        raise RuntimeError("No SDK token and no PGPASSWORD available")


# ── Connection management ──────────────────────────────────────────────────────

async def _close_conn():
    """Close and discard the current connection."""
    global _conn
    if _conn is not None:
        try:
            _conn.close()
        except Exception as e:
            logger.warning(f"Error closing connection: {e}")
        _conn = None


def _reset_conn_sync():
    """Reset the connection from a sync/thread context (e.g. inside _run()).

    Safely closes the stale connection and sets _conn to None so the next
    get_conn() call will create a fresh one.  This is called when a query
    hits InterfaceError / OperationalError inside asyncio.to_thread().
    """
    global _conn
    old = _conn
    _conn = None
    if old is not None:
        try:
            old.close()
        except Exception:
            pass  # already broken — just discard


def _get_connection_sync():
    """Create a new psycopg2 connection (synchronous). Called from thread."""
    global _conn

    # Use the SDK-authenticated identity that generated the token
    pg_user = _current_user or lakebase_cfg.user

    # Always use the SDK-refreshed OAuth token
    pg_password = _current_token

    logger.info(f"Creating Lakebase connection → {lakebase_cfg.host}:{lakebase_cfg.port}")
    _conn = psycopg2.connect(
        user=pg_user,
        password=pg_password,
        dbname=lakebase_cfg.database,
        host=lakebase_cfg.host,
        port=lakebase_cfg.port,
        sslmode="require",
    )
    _conn.autocommit = True
    logger.info("✅ Lakebase connection ready")
    return _conn


async def get_conn():
    """
    Return a live connection, refreshing the OAuth token if needed.
    Uses w.postgres.generate_database_credential() to obtain fresh tokens.
    """
    global _conn

    async with _conn_lock:
        if _token_needs_refresh():
            logger.info("Lakebase token needs refresh...")
            try:
                await asyncio.to_thread(_refresh_token)
            except Exception as e:
                logger.warning(f"SDK token refresh failed: {e}")
                # Fallback: use PGPASSWORD/PGUSER injected by Apps runtime
                _use_injected_fallback()
            # Close existing connection — it's using the old token
            await _close_conn()

        # Create connection if it doesn't exist
        if _conn is None:
            try:
                await asyncio.to_thread(_get_connection_sync)
            except Exception as e:
                logger.error(f"Failed to create Lakebase connection: {e}")
                _conn = None
                raise

    return _conn


# ── Query helpers ──────────────────────────────────────────────────────────────

def _json_safe(val):
    """Convert Python types (datetime, Decimal, etc.) to JSON-serializable."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.isoformat()
    if hasattr(val, "as_integer_ratio"):  # Decimal / float-like
        return float(val)
    return val


def _json_row(row: dict) -> dict:
    """Make every value in a row JSON-serializable."""
    return {k: _json_safe(v) for k, v in row.items()}


def _is_active(ts_value, window_seconds: int = None) -> bool:
    """Return True if the timestamp is within the active window."""
    if window_seconds is None:
        window_seconds = lakebase_cfg.active_window_seconds or 300
    if ts_value is None:
        return False
    if isinstance(ts_value, str):
        try:
            ts_value = datetime.fromisoformat(ts_value.replace("Z", "+00:00"))
        except ValueError:
            return False
    if ts_value.tzinfo is None:
        ts_value = ts_value.replace(tzinfo=timezone.utc)
    return ts_value > (datetime.now(timezone.utc) - timedelta(seconds=window_seconds))


def _pg_to_dbapi(sql: str) -> str:
    """Convert $1, $2, ... PostgreSQL placeholders to %s for pg8000 DB-API."""
    return re.sub(r'\$\d+', '%s', sql)


_MAX_QUERY_RETRIES = 1  # retry once on stale connection


async def fetch_rows(sql: str, *args) -> List[Dict[str, Any]]:
    """Execute a query and return results as a list of JSON-safe dicts.

    Retries once on psycopg2.InterfaceError or OperationalError (stale /
    server-closed connection).  On retry the broken connection is discarded
    and get_conn() transparently creates a fresh one.
    """
    last_err: Optional[Exception] = None

    for attempt in range(_MAX_QUERY_RETRIES + 1):
        conn = await get_conn()

        def _run(c=conn):
            cursor = c.cursor()
            cursor.execute(_pg_to_dbapi(sql), args if args else None)
            if cursor.description is None:
                return []
            cols = [d[0] for d in cursor.description]
            return [_json_row(dict(zip(cols, row))) for row in cursor.fetchall()]

        try:
            return await asyncio.to_thread(_run)
        except (psycopg2.InterfaceError, psycopg2.OperationalError) as e:
            last_err = e
            logger.warning(
                f"Lakebase query failed (attempt {attempt + 1}/"
                f"{_MAX_QUERY_RETRIES + 1}): {e}"
            )
            # Discard the broken connection so get_conn() will reconnect
            _reset_conn_sync()

            if attempt < _MAX_QUERY_RETRIES:
                logger.info("Retrying with fresh connection...")
                continue
            # All retries exhausted — raise the last error
            logger.error(f"Lakebase query failed after {_MAX_QUERY_RETRIES + 1} attempts")
            raise

    # Should not reach here, but satisfy type checker
    raise last_err  # type: ignore[misc]


# ── Lakebase queries ───────────────────────────────────────────────────────────

async def get_zerobus_stream(
    limit: int = 100,
    offset: int = 0,
    connection_id: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], float]:
    """Fetch recent sensor events from the Lakebase table."""
    where = ""
    params = []
    if connection_id:
        where = "WHERE connection_id = $1"
        params.append(connection_id)

    sql = f"""
        SELECT
            event_id,
            connection_id,
            device_name,
            TO_CHAR(event_timestamp, 'YYYY-MM-DD HH24:MI:SS.MS') AS event_time,
            TO_CHAR(event_date, 'YYYY-MM-DD') AS event_date,
            ROUND(latitude::numeric,  6)  AS latitude,
            ROUND(longitude::numeric, 6)  AS longitude,
            ROUND(altitude_m::numeric, 1) AS altitude_m,
            ROUND(heading_deg::numeric, 1) AS heading_deg,
            ROUND(pitch_deg::numeric,   1) AS pitch_deg,
            ROUND(roll_deg::numeric,    1) AS roll_deg,
            ROUND(accel_x::numeric,     3) AS accel_x,
            ROUND(accel_y::numeric,     3) AS accel_y,
            ROUND(accel_z::numeric,     3) AS accel_z,
            ROUND(accel_magnitude::numeric, 3) AS accel_magnitude,
            ROUND(gyro_x::numeric,  3) AS gyro_x,
            ROUND(gyro_y::numeric,  3) AS gyro_y,
            ROUND(gyro_z::numeric,  3) AS gyro_z,
            ROUND(speed_kmh::numeric, 1) AS speed_kmh,
            battery_pct,
            signal_strength,
            zerobus_topic,
            zerobus_offset,
            payload_bytes,
            TO_CHAR(ingested_at, 'YYYY-MM-DD HH24:MI:SS') AS ingested_at
        FROM {_FQTN}
        {where}
        ORDER BY event_timestamp DESC
        LIMIT {limit}
        OFFSET {offset}
    """
    start = time.time()
    rows = await fetch_rows(sql, *params)
    elapsed_ms = round((time.time() - start) * 1000, 2)
    return rows, elapsed_ms


async def get_stream_count(connection_id: Optional[str] = None) -> int:
    """Return total row count for pagination."""
    where = ""
    params = []
    if connection_id:
        where = "WHERE connection_id = $1"
        params.append(connection_id)

    sql = f"SELECT COUNT(*) AS cnt FROM {_FQTN} {where}"
    rows = await fetch_rows(sql, *params)
    if rows:
        return int(rows[0].get("cnt", 0))
    return 0


async def get_dashboard_summary() -> Dict[str, Any]:
    sql = f"""
        SELECT
            COUNT(DISTINCT connection_id)   AS unique_clients,
            COUNT(*)                        AS total_events,
            COALESCE(SUM(payload_bytes), 0) AS total_payload_bytes,
            MAX(event_timestamp)            AS last_event_time
        FROM {_FQTN}
    """
    rows = await fetch_rows(sql)
    if rows:
        row = rows[0]
        return {
            "unique_clients":      int(row.get("unique_clients")      or 0),
            "total_events":        int(row.get("total_events")        or 0),
            "total_payload_bytes": int(row.get("total_payload_bytes") or 0),
            "last_event_time":     row.get("last_event_time"),
        }
    return {
        "unique_clients":      0,
        "total_events":        0,
        "total_payload_bytes": 0,
        "last_event_time":     None,
    }


async def get_client_list(
    limit: int = 100, offset: int = 0
) -> Tuple[List[Dict[str, Any]], int]:
    sql = f"""
        SELECT
            connection_id,
            MAX(device_name)     AS device_name,
            COUNT(*)             AS event_count,
            COALESCE(SUM(payload_bytes), 0) AS total_bytes,
            MAX(event_timestamp) AS last_event_time,
            MIN(event_timestamp) AS first_event
        FROM {_FQTN}
        GROUP BY connection_id
        ORDER BY last_event_time DESC
        LIMIT $1 OFFSET $2
    """
    rows = await fetch_rows(sql, limit, offset)

    clients = []
    for row in rows:
        active = _is_active(row.get("last_event_time"))
        clients.append({
            **row,
            "last_event":  row.get("last_event_time"),   # alias for JS
            "is_active":   active,
        })

    # Total distinct clients
    cnt_rows = await fetch_rows(f"SELECT COUNT(DISTINCT connection_id) AS cnt FROM {_FQTN}")
    total = int(cnt_rows[0]["cnt"]) if cnt_rows else len(clients)

    return clients, total


async def get_all_latest_locations() -> Tuple[List[Dict[str, Any]], int]:
    sql = f"""
        SELECT DISTINCT ON (connection_id)
            connection_id,
            device_name,
            latitude,
            longitude,
            event_timestamp,
            speed_kmh,
            battery_pct,
            signal_strength
        FROM {_FQTN}
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        ORDER BY connection_id, event_timestamp DESC
    """
    rows = await fetch_rows(sql)

    # Enrich with event_count + is_active
    cnt_sql = f"""
        SELECT connection_id, COUNT(*) AS event_count,
               COALESCE(SUM(payload_bytes), 0) AS total_bytes
        FROM {_FQTN} GROUP BY connection_id
    """
    stats = {r["connection_id"]: r for r in await fetch_rows(cnt_sql)}

    locations = []
    for row in rows:
        cid = row["connection_id"]
        s = stats.get(cid, {})
        active = _is_active(row.get("event_timestamp"))
        locations.append({
            **row,
            "event_count":  int(s.get("event_count", 0)),
            "total_bytes":  int(s.get("total_bytes", 0)),
            "is_active":    active,
        })

    return locations, len(locations)


async def get_client_track(
    connection_id: str, limit: int = 500
) -> List[Dict[str, Any]]:
    sql = f"""
        SELECT
            event_timestamp,
            latitude,
            longitude,
            altitude_m,
            heading_deg,
            speed_kmh,
            battery_pct
        FROM {_FQTN}
        WHERE connection_id = $1
          AND latitude IS NOT NULL AND longitude IS NOT NULL
        ORDER BY event_timestamp ASC
        LIMIT $2
    """
    rows = await fetch_rows(sql, connection_id, limit)

    # Return in the format the JS frontend expects
    track = []
    for p in rows:
        track.append({
            "lat":         float(p.get("latitude", 0)),
            "lon":         float(p.get("longitude", 0)),
            "lng":         float(p.get("longitude", 0)),
            "event_time":  p.get("event_timestamp"),
            "speed_kmh":   float(p.get("speed_kmh") or 0),
            "heading_deg": float(p.get("heading_deg") or 0),
            "battery_pct": int(p.get("battery_pct") or 0),
        })
    return track


async def get_client_detail(
    connection_id: str, include_track: bool = True, track_limit: int = 500
) -> Dict[str, Any]:
    """Full client detail: summary + latest position + optional track."""
    # Summary
    sum_sql = f"""
        SELECT
            connection_id,
            MAX(device_name)                     AS device_name,
            COUNT(*)                             AS total_events,
            COALESCE(SUM(payload_bytes), 0)      AS total_bytes,
            MAX(event_timestamp)                 AS last_event,
            MIN(event_timestamp)                 AS first_event,
            ROUND(AVG(speed_kmh)::numeric, 1)    AS avg_speed,
            ROUND(AVG(battery_pct)::numeric, 0)  AS avg_battery
        FROM {_FQTN}
        WHERE connection_id = $1
        GROUP BY connection_id
    """
    rows = await fetch_rows(sum_sql, connection_id)
    if not rows:
        return None

    row = rows[0]
    active = _is_active(row.get("last_event"))

    summary = {
        "connection_id": row.get("connection_id"),
        "device_name":   row.get("device_name"),
        "total_events":  int(row.get("total_events") or 0),
        "total_bytes":   int(row.get("total_bytes") or 0),
        "last_event":    row.get("last_event"),
        "first_event":   row.get("first_event"),
        "avg_speed":     float(row.get("avg_speed") or 0),
        "avg_battery":   float(row.get("avg_battery") or 0),
        "is_active":     active,
    }

    # Latest position
    loc_sql = f"""
        SELECT latitude, longitude
        FROM {_FQTN}
        WHERE connection_id = $1 AND latitude IS NOT NULL AND longitude IS NOT NULL
        ORDER BY event_timestamp DESC LIMIT 1
    """
    loc_rows = await fetch_rows(loc_sql, connection_id)
    if loc_rows:
        summary["latest"] = {
            "latitude":  float(loc_rows[0]["latitude"]),
            "longitude": float(loc_rows[0]["longitude"]),
        }

    result = {"summary": summary}

    if include_track:
        track = await get_client_track(connection_id, track_limit)
        result["track"] = track
        result["track_count"] = len(track)

    return result
