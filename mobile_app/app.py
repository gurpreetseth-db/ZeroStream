"""
ZeroStream Mobile App - FastAPI
Replace pydantic BaseModel with simple dict validation
to avoid pydantic-core Rust build issues.
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
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request

from config.settings import zerobus_cfg, delta_cfg, app_cfg, validate_config
from data_generator import generator_pool
from zerobus_client import sensor_publisher

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("mobile_app")


# ── App state ─────────────────────────────────────────────────────────────────
class AppState:
    streaming_active:  bool = False
    connection_count:  int  = 0
    stream_task:       Optional[asyncio.Task] = None
    ws_clients:        List[WebSocket] = []
    last_payloads:     Dict[str, dict] = {}
    publish_stats:     Dict[str, Any]  = {}


state = AppState()


# ── Streaming loop ────────────────────────────────────────────────────────────
async def _streaming_loop():
    interval = zerobus_cfg.stream_interval_ms / 1000.0
    logger.info(f"🚀 Streaming loop started (interval={interval}s)")

    while state.streaming_active:
        try:
            payloads = generator_pool.tick_all()

            if payloads:
                for p in payloads:
                    state.last_payloads[p["connection_id"]] = p

                stats = sensor_publisher.publish(payloads)
                state.publish_stats = stats

                if state.ws_clients:
                    ws_message = json.dumps({
                        "type":  "sensor_update",
                        "count": len(payloads),
                        "stats": stats,
                        "data":  {p["connection_id"]: p for p in payloads},
                    })
                    dead = []
                    for ws in state.ws_clients:
                        try:
                            await ws.send_text(ws_message)
                        except Exception:
                            dead.append(ws)
                    for ws in dead:
                        state.ws_clients.remove(ws)

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Streaming loop error: {e}")

        await asyncio.sleep(interval)

    logger.info("⏹ Streaming loop stopped.")


# ── Lifespan ──────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"🟢 ZeroStream Mobile App starting")

    # Validate config
    missing = validate_config()
    if missing:
        logger.warning(f"Missing config vars: {missing}")

    sensor_publisher.zerobus.connect()
    yield

    state.streaming_active = False
    if state.stream_task:
        state.stream_task.cancel()
    sensor_publisher.zerobus.disconnect()
    logger.info("🔴 Mobile App shutdown complete.")


# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(
    title="ZeroStream Mobile Simulator",
    version="1.0.0",
    lifespan=lifespan,
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app.mount(
    "/static",
    StaticFiles(directory=os.path.join(BASE_DIR, "static")),
    name="static",
)
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))


# ── Routes ────────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {
            "request":          request,
            "app_name":         app_cfg.mobile_app_name,
            "delta_table":      delta_cfg.full_name,
            "zerobus_endpoint": zerobus_cfg.server_endpoint,
            "stream_interval":  zerobus_cfg.stream_interval_ms,
        },
    )


@app.get("/api/config")
async def get_config():
    return {
        "app_name":         app_cfg.mobile_app_name,
        "delta_table":      delta_cfg.full_name,
        "zerobus_endpoint": zerobus_cfg.server_endpoint,
        "stream_interval":  zerobus_cfg.stream_interval_ms,
        "active_window_s":  15,
    }


@app.post("/api/reset")
async def reset_state():
    """Reset all app state - called on page load/refresh."""
    global state
    
    # Stop streaming if active
    if state.streaming_active:
        state.streaming_active = False
        if state.stream_task:
            state.stream_task.cancel()
            state.stream_task = None
    
    # Clear all connections
    generator_pool.set_connection_count(0)
    
    # Reset state
    state.connection_count = 0
    state.last_payloads = {}
    state.publish_stats = {}
    
    logger.info("🔄 App state reset")
    
    return {
        "status": "reset",
        "streaming": False,
        "connection_count": 0,
    }


@app.post("/api/stream/configure")
async def configure_stream(request: Request):
    """Configure streaming - accepts JSON body without pydantic."""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    connection_count = int(body.get("connection_count", 0))
    active           = bool(body.get("active", False))

    # Clamp connection count
    connection_count = max(0, min(100, connection_count))

    active_ids = generator_pool.set_connection_count(connection_count)
    state.connection_count = connection_count

    if active and not state.streaming_active:
        state.streaming_active = True
        state.stream_task = asyncio.create_task(_streaming_loop())
        logger.info(f"▶️  Streaming started ({connection_count} connections)")

    elif not active and state.streaming_active:
        state.streaming_active = False
        if state.stream_task:
            state.stream_task.cancel()
            state.stream_task = None
        logger.info("⏹  Streaming stopped")

    return {
        "status":           "ok",
        "streaming":        state.streaming_active,
        "connection_count": connection_count,
        "active_ids":       active_ids,
    }


@app.get("/api/connections")
async def get_connections():
    connections = []
    pool_states = generator_pool.get_all_states()

    for cid, conn_state in pool_states.items():
        latest = state.last_payloads.get(cid, {})
        connections.append({
            "connection_id":   cid,
            "device_name":     conn_state.device_name,
            "city":            conn_state.city,
            "active":          state.streaming_active,
            "latest":          latest,
            "event_count":     conn_state._event_count,
            "battery_pct":     conn_state.battery_pct,
            "signal_strength": conn_state.signal_strength,
        })

    return {
        "connections":  connections,
        "streaming":    state.streaming_active,
        "total_count":  len(connections),
        "stats":        state.publish_stats,
    }


@app.get("/api/connections/{connection_id}")
async def get_connection_detail(connection_id: str):
    conn_state = generator_pool.get_connection(connection_id)
    if not conn_state:
        raise HTTPException(status_code=404, detail="Connection not found")

    latest = state.last_payloads.get(connection_id, {})
    return {
        "connection_id":   connection_id,
        "device_name":     conn_state.device_name,
        "city":            conn_state.city,
        "active":          state.streaming_active,
        "event_count":     conn_state._event_count,
        "latest":          latest,
        "battery_pct":     conn_state.battery_pct,
        "signal_strength": conn_state.signal_strength,
    }


@app.get("/api/stats")
async def get_stats():
    return {
        "streaming":         state.streaming_active,
        "connection_count":  generator_pool.count,
        "publish_stats":     state.publish_stats,
        "zerobus_connected": sensor_publisher.zerobus.is_connected,
    }


@app.websocket("/ws/stream")
async def websocket_stream(websocket: WebSocket):
    await websocket.accept()
    state.ws_clients.append(websocket)
    logger.info(f"WebSocket connected (total: {len(state.ws_clients)})")

    try:
        # Send initial state immediately
        await websocket.send_text(json.dumps({
            "type":      "init",
            "streaming": state.streaming_active,
            "count":     generator_pool.count,
            "data":      state.last_payloads,
        }))

        # Keep alive loop
        while True:
            msg = await websocket.receive_text()
            try:
                data = json.loads(msg)
                if data.get("type") == "ping":
                    await websocket.send_text(json.dumps({"type": "pong"}))
            except Exception:
                pass

    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.warning(f"WebSocket error: {e}")
    finally:
        if websocket in state.ws_clients:
            state.ws_clients.remove(websocket)
        logger.info(f"WebSocket disconnected (total: {len(state.ws_clients)})")


@app.get("/health")
async def health():
    return {
        "status":    "ok",
        "app":       app_cfg.mobile_app_name,
        "streaming": state.streaming_active,
        "connections": generator_pool.count,
        "ts":        datetime.now(timezone.utc).isoformat(),
    }


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8000")),
        reload=False,
        log_level="info",
    )