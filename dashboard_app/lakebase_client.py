"""
Lakebase PostgreSQL client for ZeroStream dashboard.
Uses asyncpg for async queries to the synced table.

Authentication:
  When deployed as a Databricks App with a Lakebase resource declared in app.yaml,
  Databricks automatically:
    - Creates a Postgres role for the app's service principal
    - Grants CONNECT + CREATE on the database
    - Injects PGHOST, PGDATABASE, PGUSER, PGPASSWORD, PGPORT as environment variables

  The OAuth token (PGPASSWORD) expires every hour. This client uses the Databricks SDK's
  w.database.generate_database_credential() to refresh it automatically before expiry,
  without any manual secret management.
"""
import asyncio
import logging
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import asyncpg
from databricks.sdk import WorkspaceClient

from config.settings import lakebase_cfg

# Synced table name — default to sensor_stream_synced if LAKEBASE_TABLE not set
_TABLE = lakebase_cfg.table or "sensor_stream_synced"
_SCHEMA = lakebase_cfg.schema or "public"
_FQTN = f"{_SCHEMA}.{_TABLE}"

logger = logging.getLogger("lakebase_client")

# ── Token + pool state ─────────────────────────────────────────────────────────
_pool:             Optional[asyncpg.pool.Pool] = None
_pool_lock:        asyncio.Lock                = asyncio.Lock()
_current_token:    str                         = ""
_token_expires_at: float                       = 0.0     # unix timestamp
_REFRESH_BUFFER:   int                         = 300     # refresh 5 min before expiry

# Databricks SDK client (lazy init)
_ws_client: Optional[WorkspaceClient] = None


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
    Use the Databricks SDK to generate a fresh OAuth token for Lakebase.
    Updates module-level _current_token and _token_expires_at.

    w.database.generate_database_credential() is the official SDK method —
    it handles all the OAuth plumbing and returns a short-lived token.
    """
    global _current_token, _token_expires_at

    instance_name = lakebase_cfg.instance
    if not instance_name:
        raise RuntimeError(
            "LAKEBASE_INSTANCE is not set. "
            "Run infra/setup_infra.sh to provision the Lakebase instance."
        )

    logger.info(f"🔄 Refreshing Lakebase OAuth token for instance '{instance_name}'...")

    w = _get_ws_client()

    # SDK has two credential APIs with different signatures:
    #   w.database.generate_database_credential(instance_names=[...], request_id=...)
    #       → Lakebase Provisioned (what we need)
    #   w.postgres.generate_database_credential(endpoint)
    #       → Lakebase Autoscale (different API, not for provisioned instances)
    # Always prefer w.database for provisioned Lakebase instances.
    if hasattr(w, "database"):
        cred = w.database.generate_database_credential(
            request_id     = str(uuid.uuid4()),
            instance_names = [instance_name],
        )
    elif hasattr(w, "postgres"):
        # Autoscale API fallback — endpoint format differs
        cred = w.postgres.generate_database_credential(
            endpoint=instance_name,
        )
    else:
        raise RuntimeError(
            "WorkspaceClient missing database/postgres client; upgrade databricks-sdk >= 0.54.0"
        )

    if not cred or not cred.token:
        raise RuntimeError("generate_database_credential() returned no token")

    _current_token    = cred.token
    # SDK doesn't return expires_in, so assume 1 hour (standard Databricks token TTL)
    _token_expires_at = time.time() + 3600

    logger.info("✅ Lakebase OAuth token refreshed (valid ~1hr, auto-refresh at 55min)")
    return _current_token


# ── Connection pool ────────────────────────────────────────────────────────────

async def _close_pool():
    """Close and discard the current pool."""
    global _pool
    if _pool is not None:
        try:
            await _pool.close()
        except Exception as e:
            logger.warning(f"Error closing pool: {e}")
        _pool = None


async def get_pool() -> asyncpg.pool.Pool:
    """
    Return a live connection pool, refreshing the OAuth token if needed.
    Uses asyncio.Lock so concurrent coroutines don't trigger multiple simultaneous refreshes.

    Connection params priority:
      - host/port/database: from LAKEBASE_* env vars (set in app.yaml / generated_config.env)
      - user: from PGUSER (auto-injected by Databricks Apps when Lakebase resource is configured)
      - password: freshly generated OAuth token via Databricks SDK
    """
    global _pool

    async with _pool_lock:
        # Check if token needs refresh
        if _token_needs_refresh():
            logger.info("Lakebase token needs refresh...")
            try:
                # generate_database_credential is sync — run in thread to avoid blocking event loop
                await asyncio.to_thread(_refresh_token)
            except Exception as e:
                logger.error(f"Token refresh failed: {e}")
                raise

            # Close existing pool — it's using the old (expired) token as password
            await _close_pool()

        # Create pool if it doesn't exist
        if _pool is None:
            # PGUSER is auto-injected by Databricks Apps when Lakebase resource is configured.
            # Fall back to LAKEBASE_USER or SP client_id if running locally.
            pg_user = (
                os.environ.get("PGUSER")
                or lakebase_cfg.user
                or os.environ.get("LAKEBASE_SP_CLIENT_ID", "")
            )

            logger.info(f"Creating Lakebase connection pool → {lakebase_cfg.dsn_safe}")
            try:
                _pool = await asyncpg.create_pool(
                    user     = pg_user,
                    password = _current_token,
                    database = lakebase_cfg.database,
                    host     = lakebase_cfg.host,
                    port     = lakebase_cfg.port,
                    ssl      = "require",
                    min_size = 1,
                    max_size = 4,
                )
                logger.info("✅ Lakebase connection pool ready")
            except Exception as e:
                logger.error(f"Failed to create Lakebase pool: {e}")
                _pool = None
                raise

    return _pool


# ── Query helpers ──────────────────────────────────────────────────────────────

def _json_safe(val):
    """Convert asyncpg types (datetime, Decimal, etc.) to JSON-serializable."""
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


async def fetch_rows(sql: str, *args) -> List[Dict[str, Any]]:
    """Execute a query and return results as a list of JSON-safe dicts."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *args)
        return [_json_row(dict(row)) for row in rows]


# ── Lakebase queries ───────────────────────────────────────────────────────────

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
