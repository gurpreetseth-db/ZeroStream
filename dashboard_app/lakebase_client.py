"""
Lakebase PostgreSQL client for ZeroStream dashboard.
Uses asyncpg for async queries to the synced table.
"""
import os
import asyncpg
import logging
from typing import Any, Dict, List, Optional, Tuple
from config.settings import lakebase_cfg

logger = logging.getLogger("lakebase_client")

# ── Connection pool ───────────────────────────────────────────────────────────
_pool: Optional[asyncpg.pool.Pool] = None

async def get_pool() -> asyncpg.pool.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            user=lakebase_cfg.user,
            password=lakebase_cfg.password,
            database=lakebase_cfg.database,
            host=lakebase_cfg.host,
            port=lakebase_cfg.port,
            ssl="require",
            min_size=1,
            max_size=4,
        )
    return _pool

# ── Query helpers ──────────────────────────────────────────────────────────────
async def fetch_rows(sql: str, *args) -> List[Dict[str, Any]]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *args)
        return [dict(row) for row in rows]

# ── Lakebase queries ───────────────────────────────────────────────────────────
async def get_dashboard_summary() -> Dict[str, Any]:
    sql = f"""
        SELECT 
            COUNT(DISTINCT connection_id) as unique_clients,
            COUNT(*) as total_events,
            COALESCE(SUM(payload_bytes), 0) as total_payload_bytes,
            MAX(event_timestamp) as last_event_time
        FROM {lakebase_cfg.schema}.{lakebase_cfg.table}
    """
    rows = await fetch_rows(sql)
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

async def get_client_list(limit: int = 100, offset: int = 0) -> Tuple[List[Dict[str, Any]], int]:
    sql = f"""
        SELECT connection_id, device_name,
               COUNT(*) as event_count,
               MAX(event_timestamp) as last_event_time
        FROM {lakebase_cfg.schema}.{lakebase_cfg.table}
        GROUP BY connection_id, device_name
        ORDER BY last_event_time DESC
        LIMIT $1 OFFSET $2
    """
    rows = await fetch_rows(sql, limit, offset)
    return rows, len(rows)

async def get_all_latest_locations() -> Tuple[List[Dict[str, Any]], int]:
    sql = f"""
        SELECT DISTINCT ON (connection_id)
            connection_id, device_name, latitude, longitude, event_timestamp
        FROM {lakebase_cfg.schema}.{lakebase_cfg.table}
        ORDER BY connection_id, event_timestamp DESC
    """
    rows = await fetch_rows(sql)
    return rows, len(rows)

async def get_client_track(connection_id: str, limit: int = 100) -> List[Dict[str, Any]]:
    sql = f"""
        SELECT event_timestamp, latitude, longitude, altitude_m, heading_deg, speed_kmh
        FROM {lakebase_cfg.schema}.{lakebase_cfg.table}
        WHERE connection_id = $1
        ORDER BY event_timestamp ASC
        LIMIT $2
    """
    rows = await fetch_rows(sql, connection_id, limit)
    return rows
