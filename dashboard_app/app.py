"""
ZeroStream Backend Dashboard - FastAPI
No pydantic BaseModel used - avoids Rust/pydantic-core build issues.
"""
import asyncio
import json
import logging
import os
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import uvicorn
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request

from config.settings import (
    lakebase_cfg, delta_cfg, app_cfg,
    zerobus_cfg, validate_config,
)
# Use Delta client for dashboard (Lakebase sync not working)
from delta_client import (
    get_zerobus_stream,
    get_stream_count,
    get_client_summary,
)
from lakebase_client import (
    get_dashboard_summary as lb_get_dashboard_summary,
    get_client_list as lb_get_client_list,
    get_all_latest_locations as lb_get_all_latest_locations,
    get_client_track as lb_get_client_track,
)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("dashboard_app")

# ── WebSocket clients ─────────────────────────────────────────────────────────
ws_clients: List[WebSocket] = []


# ── Background broadcast loop ─────────────────────────────────────────────────
async def _dashboard_broadcast_loop():
    """Push Delta updates to all connected WebSocket clients every 3s."""
    while True:
        try:
            if ws_clients:
                # These are sync functions from delta_client
                summary          = await lb_get_dashboard_summary()
                clients, _       = await lb_get_client_list()
                locations, _     = await lb_get_all_latest_locations()

                msg = json.dumps({
                    "type":      "dashboard_update",
                    "summary":   summary,
                    "clients":   clients,
                    "locations": locations,
                    "ts":        datetime.now(timezone.utc).isoformat(),
                })

                dead = []
                for ws in ws_clients:
                    try:
                        await ws.send_text(msg)
                    except Exception:
                        dead.append(ws)
                for ws in dead:
                    ws_clients.remove(ws)

        except Exception as e:
            logger.error(f"Broadcast loop error: {e}")

        await asyncio.sleep(3)


# ── Lifespan ──────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"🟢 ZeroStream Backend App starting")

    missing = validate_config()
    if missing:
        logger.warning(f"Missing config: {missing}")

    # Test Delta connection by getting summary
    try:
        summary = get_dashboard_summary()
        logger.info(f"✅ Delta connection ready - {summary.get('total_events', 0)} events")
    except Exception as e:
        logger.error(f"❌ Delta connection failed: {e}")

    broadcast_task = asyncio.create_task(_dashboard_broadcast_loop())
    yield

    broadcast_task.cancel()
    logger.info("🔴 Backend App shutdown complete.")


# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(
    title="ZeroStream Backend Dashboard",
    version="1.0.0",
    lifespan=lifespan,
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app.mount(
    "/static",
    StaticFiles(directory=os.path.join(BASE_DIR, "static")),
    name="static",
)
templates = Jinja2Templates(
    directory=os.path.join(BASE_DIR, "templates")
)


# ── Page routes ───────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request":           request,
            "app_name":          app_cfg.dashboard_app_name,
            "delta_table":       delta_cfg.full_name,
            "lakebase_instance": lakebase_cfg.instance,
            "active_window":     lakebase_cfg.active_window_seconds,
        },
    )


@app.get("/zerobus", response_class=HTMLResponse)
async def zerobus_view(request: Request):
    return templates.TemplateResponse(
        "zerobus_view.html",
        {
            "request":     request,
            "delta_table": delta_cfg.full_name,
            "topic":       zerobus_cfg.topic,
        },
    )


# ── API: Dashboard (Delta) ───────────────────────────────────────────────────
@app.get("/api/dashboard/summary")
async def api_dashboard_summary():
    try:
        return await lb_get_dashboard_summary()
    except Exception as e:
        logger.error(f"Summary error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/dashboard/clients")
async def api_dashboard_clients():
    try:
        clients, total = await lb_get_client_list()
        return {
            "clients":    clients,
            "count":      len(clients),
            "total":      total,
        }
    except Exception as e:
        logger.error(f"Clients error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/dashboard/locations")
async def api_dashboard_locations():
    try:
        locations, total = await lb_get_all_latest_locations()
        return {
            "locations":  locations,
            "count":      len(locations),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/dashboard/track/{connection_id}")
async def api_client_track(
    connection_id: str,
    limit: int = Query(default=200, le=500),
):
    try:
        track = await lb_get_client_track(connection_id, limit)
        return {
            "connection_id": connection_id,
            "track":         track,
            "count":         len(track),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/dashboard/client/{connection_id}")
async def api_client_summary(
    connection_id: str,
    include_track: bool = Query(default=True),
    track_limit: int = Query(default=500, le=2000),
):
    """Get detailed summary for a specific client including track (Lakebase)."""
    try:
        # Compose summary from Lakebase
        # Get all events for this client (for summary)
        sql = f"""
            SELECT 
                connection_id,
                FIRST(device_name) as device_name,
                COUNT(*) as total_events,
                COALESCE(SUM(payload_bytes), 0) as total_bytes,
                MAX(event_timestamp) as last_event,
                MIN(event_timestamp) as first_event,
                AVG(speed_kmh) as avg_speed,
                AVG(battery_pct) as avg_battery
            FROM {lakebase_cfg.catalog}.{lakebase_cfg.schema}.sensor_stream_synced
            WHERE connection_id = $1
        """
        rows = await lakebase_client.fetch_rows(sql, connection_id)
        if not rows or rows[0].get("total_events", 0) == 0:
            raise HTTPException(status_code=404, detail="Client not found")
        row = rows[0]
        logger.error(f"DEBUG: row values for summary: {row}")
        logger.error(f"DEBUG: types: total_events={type(row.get('total_events'))}, total_bytes={type(row.get('total_bytes'))}")
        summary = {
            "connection_id": row.get("connection_id"),
            "device_name": row.get("device_name"),
            "total_events": safe_int(row.get("total_events")),
            "total_bytes": safe_int(row.get("total_bytes")),
            "last_event": row.get("last_event"),
            "first_event": row.get("first_event"),
            "avg_speed": float(row.get("avg_speed") or 0),
            "avg_battery": float(row.get("avg_battery") or 0),
        }
        # Get latest location
        loc_sql = f"""
            SELECT latitude, longitude FROM {lakebase_cfg.catalog}.{lakebase_cfg.schema}.sensor_stream_synced
            WHERE connection_id = $1 AND latitude IS NOT NULL AND longitude IS NOT NULL
            ORDER BY event_timestamp DESC LIMIT 1
        """
        loc_rows = await lakebase_client.fetch_rows(loc_sql, connection_id)
        if loc_rows:
            summary["latest"] = {
                "latitude": float(loc_rows[0].get("latitude")),
                "longitude": float(loc_rows[0].get("longitude")),
            }
        result = {"summary": summary}
        if include_track:
            track = await lb_get_client_track(connection_id, track_limit)
            # Convert to JS-compatible format
            track_js = [
                {
                    "lat": float(p.get("latitude", 0)),
                    "lon": float(p.get("longitude", 0)),
                    "event_time": p.get("event_timestamp"),
                    "speed_kmh": float(p.get("speed_kmh", 0)),
                    "heading_deg": float(p.get("heading_deg", 0)),
                }
                for p in track if p.get("latitude") and p.get("longitude")
            ]
            result["track"] = track_js
            result["track_count"] = len(track_js)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Client summary error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ── API: ZeroBus stream (Delta) ───────────────────────────────────────────────
@app.get("/api/zerobus/stream")
async def api_zerobus_stream(
    limit:         int           = Query(default=100, le=500),
    offset:        int           = Query(default=0,   ge=0),
    connection_id: Optional[str] = Query(default=None),
):
    try:
        rows, elapsed_ms = get_zerobus_stream(
            limit=limit,
            offset=offset,
            connection_id=connection_id,
        )
        total = get_stream_count(connection_id)
        return {
            "rows":       rows,
            "count":      len(rows),
            "total":      total,
            "elapsed_ms": elapsed_ms,
            "limit":      limit,
            "offset":     offset,
        }
    except Exception as e:
        logger.error(f"ZeroBus stream error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/test-delta")
async def api_test_delta():
    """Test Delta/DBSQL connection - returns row count from sensor_stream."""
    try:
        total = get_stream_count()
        return {"status": "ok", "row_count": total, "table": delta_cfg.full_name}
    except Exception as e:
        logger.error(f"Delta test error: {e}")
        return {"status": "error", "detail": str(e)}


# ── WebSocket: real-time dashboard ────────────────────────────────────────────
@app.websocket("/ws/dashboard")
async def websocket_dashboard(websocket: WebSocket):
    await websocket.accept()
    ws_clients.append(websocket)
    logger.info(f"Dashboard WS connected (total: {len(ws_clients)})")

    try:
        # Send immediate first payload
        summary      = await lb_get_dashboard_summary()
        clients, _   = await lb_get_client_list()
        locations, _ = await lb_get_all_latest_locations()

        await websocket.send_text(json.dumps({
            "type":      "dashboard_update",
            "summary":   summary,
            "clients":   clients,
            "locations": locations,
            "ts":        datetime.now(timezone.utc).isoformat(),
        }))

        # Keep alive
        while True:
            msg = await websocket.receive_text()
            try:
                data = json.loads(msg)
                if data.get("type") == "ping":
                    await websocket.send_text(
                        json.dumps({"type": "pong"})
                    )
            except Exception:
                pass

    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.warning(f"Dashboard WS error: {e}")
    finally:
        if websocket in ws_clients:
            ws_clients.remove(websocket)
        logger.info(
            f"Dashboard WS disconnected (total: {len(ws_clients)})"
        )


# ── Health check ──────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {
        "status":   "ok",
        "app":      app_cfg.dashboard_app_name,
        "lakebase": lakebase_cfg.dsn_safe,
        "delta":    delta_cfg.full_name,
        "ts":       datetime.now(timezone.utc).isoformat(),
    }


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8001")),
        reload=False,
        log_level="info",
    )