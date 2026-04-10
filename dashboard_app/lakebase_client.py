"""
Lakebase PostgreSQL client for ZeroStream dashboard.
Uses psycopg2 for queries to the Lakebase Autoscaling table.

Authentication (M2M OAuth — automatic token refresh):
  In Databricks Apps runtime: WorkspaceClient auto-authenticates using the
  app's service principal (DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET
  are injected by the runtime). The SP's application_id (= DATABRICKS_CLIENT_ID)
  is used as the PG username. OAuth tokens are generated via
  w.postgres.generate_database_credential() and refreshed automatically
  5 minutes before expiry.

  Locally: same SDK call, but authenticates using DATABRICKS_HOST + DATABRICKS_TOKEN.
  PG username = workspace user email.
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
_current_user:     str                                      = ""      # PG username
_token_expires_at: float                                    = 0.0     # unix timestamp
_REFRESH_BUFFER:   int                                      = 300     # refresh 5 min before expiry
_endpoint_name_cache: str                                   = ""      # cached endpoint name

# Databricks SDK client (lazy init)
_ws_client = None


def _get_ws_client() -> WorkspaceClient:
    """Return a singleton WorkspaceClient.

    In Databricks Apps: auto-authenticates via M2M OAuth using injected
    DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET env vars.
    Locally: uses DATABRICKS_HOST + DATABRICKS_TOKEN.
    """
    global _ws_client
    if _ws_client is None:
        _ws_client = WorkspaceClient()
        auth_type = "M2M/SP" if os.environ.get("DATABRICKS_CLIENT_ID") else "PAT/user"
        logger.info(f"Databricks WorkspaceClient initialised (auth: {auth_type})")
    return _ws_client


def _resolve_pg_username(w) -> str:
    """Determine the correct PG username for the current auth context.

    - Databricks Apps (SP): DATABRICKS_CLIENT_ID env var = SP application_id = PG role name
    - Local dev (user): workspace user email from w.current_user.me()
    """
    # In Databricks Apps, the runtime injects DATABRICKS_CLIENT_ID which is the
    # SP's application_id (UUID). This matches the Lakebase PG role name.
    sp_client_id = os.environ.get("DATABRICKS_CLIENT_ID", "")
    if sp_client_id:
        logger.info(f"PG username from SP application_id: {sp_client_id}")
        return sp_client_id

    # Fallback: interactive user context (local dev)
    try:
        me = w.current_user.me()
        username = me.user_name
        logger.info(f"PG username from workspace user: {username}")
        return username
    except Exception as e:
        logger.warning(f"Could not resolve PG username from current_user: {e}")
        # Last resort: check config
        if lakebase_cfg.user:
            return lakebase_cfg.user
        raise RuntimeError(
            "Cannot determine PG username. Set DATABRICKS_CLIENT_ID (Apps) "
            "or DATABRICKS_TOKEN (local dev)."
        )


def _resolve_endpoint_name(w) -> str:
    """Get the Lakebase endpoint name, discovering it if needed."""
    global _endpoint_name_cache

    if _endpoint_name_cache:
        return _endpoint_name_cache

    endpoint_name = lakebase_cfg.endpoint
    project_id = lakebase_cfg.instance

    if not endpoint_name and not project_id:
        raise RuntimeError(
            "LAKEBASE_ENDPOINT or LAKEBASE_INSTANCE is not set. "
            "Run infra/setup_infra.sh to provision the Lakebase project."
        )

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

    _endpoint_name_cache = endpoint_name
    return endpoint_name


# ── Token management ───────────────────────────────────────────────────────────

def _token_needs_refresh() -> bool:
    """Return True if token is missing, expired, or within the refresh buffer window."""
    if not _current_token:
        return True
    return time.time() >= (_token_expires_at - _REFRESH_BUFFER)


def _refresh_token() -> str:
    """Generate a fresh Lakebase OAuth token via the Databricks SDK.

    Uses w.postgres.generate_database_credential() which handles OAuth
    plumbing and returns a short-lived PG-compatible token.

    The PG username is resolved via _resolve_pg_username():
    - Apps runtime (M2M): SP's application_id from DATABRICKS_CLIENT_ID
    - Local dev: workspace user email
    """
    global _current_token, _current_user, _token_expires_at

    w = _get_ws_client()
    endpoint_name = _resolve_endpoint_name(w)

    logger.info(f"Refreshing Lakebase OAuth token (endpoint: {endpoint_name})...")

    cred = w.postgres.generate_database_credential(
        endpoint=endpoint_name,
    )

    if not cred or not cred.token:
        raise RuntimeError("generate_database_credential() returned no token")

    _current_token = cred.token
    _current_user = _resolve_pg_username(w)

    # Use expires_at from credential if available, else assume 1 hour
    if hasattr(cred, "expires_at") and cred.expires_at:
        try:
            exp = datetime.fromisoformat(str(cred.expires_at).replace("Z", "+00:00"))
            _token_expires_at = exp.timestamp()
        except Exception:
            _token_expires_at = time.time() + 3600
    else:
        _token_expires_at = time.time() + 3600

    ttl_min = int((_token_expires_at - time.time()) / 60)
    logger.info(f"Lakebase OAuth token refreshed for '{_current_user}' (valid ~{ttl_min}min)")
    return _current_token


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


def _force_token_reset():
    """Invalidate the cached token so the next get_conn() triggers a refresh."""
    global _current_token, _token_expires_at
    _current_token = ""
    _token_expires_at = 0.0


def _get_connection_sync():
    """Create a new psycopg2 connection (synchronous). Called from thread."""
    global _conn

    pg_user = _current_user
    pg_password = _current_token

    if not pg_user or not pg_password:
        raise RuntimeError(
            f"Missing PG credentials: user={'set' if pg_user else 'MISSING'}, "
            f"token={'set' if pg_password else 'MISSING'}"
        )

    logger.info(f"Creating Lakebase connection: user={pg_user} host={lakebase_cfg.host}:{lakebase_cfg.port}")
    _conn = psycopg2.connect(
        user=pg_user,
        password=pg_password,
        dbname=lakebase_cfg.database,
        host=lakebase_cfg.host,
        port=lakebase_cfg.port,
        sslmode="require",
        connect_timeout=15,
    )
    _conn.autocommit = True
    logger.info("Lakebase connection ready")
    return _conn


async def get_conn():
    """Return a live connection, refreshing the OAuth token if needed.

    Token lifecycle (M2M OAuth):
      1. generate_database_credential() → short-lived PG token (~1 hr)
      2. Auto-refresh 5 min before expiry
      3. On auth failure: force token refresh + reconnect
    """
    global _conn

    async with _conn_lock:
        if _token_needs_refresh():
            logger.info("Lakebase token needs refresh...")
            await asyncio.to_thread(_refresh_token)
            # Close existing connection — it's using the old token
            await _close_conn()

        # Create connection if it doesn't exist
        if _conn is None:
            try:
                await asyncio.to_thread(_get_connection_sync)
            except psycopg2.OperationalError as e:
                err_msg = str(e).lower()
                # Auth failure → force token refresh and retry once
                if "password" in err_msg or "authentication" in err_msg:
                    logger.warning(f"Auth failure on connect, forcing token refresh: {e}")
                    _force_token_reset()
                    await asyncio.to_thread(_refresh_token)
                    await asyncio.to_thread(_get_connection_sync)
                else:
                    raise
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


_MAX_QUERY_RETRIES = 2  # retry twice: once for stale conn, once for expired token


async def fetch_rows(sql: str, *args) -> List[Dict[str, Any]]:
    """Execute a query and return results as a list of JSON-safe dicts.

    Retries on psycopg2 errors with escalating recovery:
      attempt 1 fail → discard connection, reconnect with same token
      attempt 2 fail → force token refresh, reconnect with new token
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
                err_msg = str(e).lower()
                # On auth/password errors, force a full token refresh
                if "password" in err_msg or "authentication" in err_msg or attempt > 0:
                    logger.info("Forcing token refresh before retry...")
                    _force_token_reset()
                else:
                    logger.info("Retrying with fresh connection...")
                continue
            # All retries exhausted
            logger.error(f"Lakebase query failed after {_MAX_QUERY_RETRIES + 1} attempts")
            raise

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
