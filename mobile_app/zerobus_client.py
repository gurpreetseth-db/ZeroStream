"""
ZeroBus client using Databricks SDK.
Publishes sensor payloads to ZeroBus topic → Delta table.
Falls back to direct Delta SQL write if ZeroBus unavailable.
Smart PostgreSQL driver detection - no hard psycopg2 dependency.
"""
import json
import logging
import os
import sys
import time
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.settings import databricks_cfg, zerobus_cfg, delta_cfg

logger = logging.getLogger(__name__)


# ── OAuth Token Cache ─────────────────────────────────────────────────────────
_oauth_token_cache = {
    "access_token": None,
    "expires_at": 0,
}


def _get_oauth_token() -> str:
    """
    Get OAuth access token using M2M (machine-to-machine) flow.
    Uses client_id and client_secret to obtain a bearer token.
    Caches the token until expiry.
    """
    import httpx
    
    current_time = time.time()
    
    # Return cached token if still valid (with 60s buffer)
    if (_oauth_token_cache["access_token"] and 
        _oauth_token_cache["expires_at"] > current_time + 60):
        return _oauth_token_cache["access_token"]
    
    # Get new token using client credentials flow
    token_url = f"{databricks_cfg.host}oidc/v1/token"
    
    logger.info(f"Requesting OAuth token from {token_url}")
    
    try:
        resp = httpx.post(
            token_url,
            data={
                "grant_type": "client_credentials",
                "scope": "all-apis",
            },
            auth=(zerobus_cfg.client_id, zerobus_cfg.client_secret),
            timeout=30.0,
        )
        resp.raise_for_status()
        
        data = resp.json()
        access_token = data["access_token"]
        expires_in = data.get("expires_in", 3600)  # Default 1 hour
        
        # Cache the token
        _oauth_token_cache["access_token"] = access_token
        _oauth_token_cache["expires_at"] = current_time + expires_in
        
        logger.info(f"OAuth token obtained, expires in {expires_in}s")
        return access_token
        
    except Exception as e:
        logger.error(f"Failed to get OAuth token: {e}")
        # Fall back to PAT token if available
        if databricks_cfg.token:
            logger.warning("Falling back to PAT token authentication")
            return databricks_cfg.token
        raise


# ── Databricks SDK client singleton ───────────────────────────────────────────
_sdk_client = None


def _get_sdk_client():
    global _sdk_client
    if _sdk_client is None:
        from databricks.sdk import WorkspaceClient
        
        # In Databricks Apps, SDK auto-authenticates via OAuth env vars
        # Don't pass explicit token when OAuth is available (causes conflict)
        if os.environ.get("DATABRICKS_CLIENT_ID"):
            # Running in Databricks Apps - use auto-configured OAuth
            logger.info("Using Databricks Apps OAuth authentication")
            _sdk_client = WorkspaceClient()
        elif zerobus_cfg.client_id and zerobus_cfg.client_secret:
            # Use ZeroBus service principal OAuth M2M authentication
            logger.info("Using ZeroBus OAuth M2M authentication")
            _sdk_client = WorkspaceClient(
                host=databricks_cfg.host,
                client_id=zerobus_cfg.client_id,
                client_secret=zerobus_cfg.client_secret,
            )
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


# ── ZeroBus Publisher ─────────────────────────────────────────────────────────
class ZeroBusPublisher:
    """
    Publishes sensor events to ZeroBus via Databricks SDK.
    The ZeroBus topic is backed by the Delta table automatically.
    """

    def __init__(self):
        self._producer       = None
        self._connected      = False
        self._total_published = 0
        self._last_offset    = 0
        self._connect_attempted = False

    def connect(self) -> bool:
        """Establish ZeroBus producer connection."""
        if self._connect_attempted and not self._connected:
            return False   # Don't retry failed connections repeatedly

        self._connect_attempted = True
        try:
            client = _get_sdk_client()

            # ── Try native ZeroBus SDK producer ───────────────────────────────
            # The Databricks SDK exposes ZeroBus under different namespaces
            # depending on SDK version - try each in order
            producer = None

            # Attempt 1: sdk.zerobus namespace
            if hasattr(client, "zerobus"):
                try:
                    producer = client.zerobus.create_producer(
                        endpoint=zerobus_cfg.server_endpoint,
                        client_id=zerobus_cfg.client_id,
                        client_secret=zerobus_cfg.client_secret,
                        topic=zerobus_cfg.topic,
                        delta_table=delta_cfg.full_name,
                    )
                    logger.info("ZeroBus producer created via sdk.zerobus")
                except Exception as e:
                    logger.debug(f"sdk.zerobus failed: {e}")

            # Attempt 2: sdk.streaming namespace
            if producer is None and hasattr(client, "streaming"):
                try:
                    producer = client.streaming.create_producer(
                        topic=zerobus_cfg.topic,
                        endpoint=zerobus_cfg.server_endpoint,
                        credentials={
                            "client_id":     zerobus_cfg.client_id,
                            "client_secret": zerobus_cfg.client_secret,
                        },
                    )
                    logger.info("ZeroBus producer created via sdk.streaming")
                except Exception as e:
                    logger.debug(f"sdk.streaming failed: {e}")

            if producer is not None:
                self._producer  = producer
                self._connected = True
                logger.info(
                    f"✅ ZeroBus connected → "
                    f"topic:{zerobus_cfg.topic} "
                    f"table:{delta_cfg.full_name}"
                )
                return True

            # Attempt 3: REST API fallback
            logger.info("ZeroBus SDK producer not available → using REST API")
            self._connected = True   # REST handles it in publish_batch
            return True

        except Exception as e:
            logger.error(f"ZeroBus connect error: {e}")
            self._connected = False
            return False

    def publish_batch(self, payloads: List[Dict[str, Any]]) -> int:
        """
        Publish a batch of sensor payloads.
        Returns count of successfully published messages.
        """
        if not self._connected:
            self.connect()

        published = 0
        for payload in payloads:
            try:
                if self._producer is not None:
                    # Native SDK publish
                    result = self._producer.send(
                        key=payload["connection_id"].encode("utf-8"),
                        value=json.dumps(payload).encode("utf-8"),
                    )
                    self._last_offset = getattr(result, "offset", self._last_offset + 1)
                else:
                    # REST API publish
                    self._publish_via_rest(payload)
                    self._last_offset += 1

                payload["zerobus_topic"]  = zerobus_cfg.topic
                payload["zerobus_offset"] = self._last_offset
                published += 1
                self._total_published += 1

            except Exception as e:
                logger.error(f"Publish error for {payload.get('event_id', '?')}: {e}")

        return published

    def _publish_via_rest(self, payload: Dict[str, Any]):
        """REST API publish to ZeroBus HTTP endpoint using OAuth M2M authentication."""
        import httpx

        url = (
            f"https://{zerobus_cfg.server_endpoint}"
            f"/api/2.0/zerobus/topics/{zerobus_cfg.topic}/publish"
        )
        
        # Use OAuth token for authentication (preferred over PAT)
        token = _get_oauth_token()
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/json",
            "X-Client-Id":   zerobus_cfg.client_id,
        }
        body = {
            "records": [{
                "key":   payload["connection_id"],
                "value": json.dumps(payload),
            }]
        }
        resp = httpx.post(url, json=body, headers=headers, timeout=5.0)
        resp.raise_for_status()
        data = resp.json()
        offsets = data.get("offsets", [{}])
        if offsets:
            self._last_offset = offsets[0].get("offset", self._last_offset + 1)

    def disconnect(self):
        """Cleanly close the producer."""
        try:
            if self._producer is not None:
                self._producer.close()
        except Exception as e:
            logger.warning(f"Disconnect warning: {e}")
        finally:
            self._connected = False
            self._producer  = None

    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def total_published(self) -> int:
        return self._total_published


# ── Delta Direct Writer ───────────────────────────────────────────────────────
class DeltaDirectWriter:
    """
    Writes sensor records directly to Delta table via DBSQL.
    Used as fallback when ZeroBus is unavailable.
    """

    def __init__(self):
        self._total_written = 0

    def write_batch(self, payloads: List[Dict[str, Any]]) -> int:
        """Insert batch of records into Delta via SQL warehouse."""
        if not payloads:
            return 0

        client  = _get_sdk_client()
        written = 0

        # Build VALUES clause - process in chunks of 50
        chunk_size = 50
        for i in range(0, len(payloads), chunk_size):
            chunk = payloads[i:i + chunk_size]
            written += self._write_chunk(client, chunk)

        self._total_written += written
        return written

    def _write_chunk(self, client, payloads: List[Dict[str, Any]]) -> int:
        """Write a single chunk of records."""
        from databricks.sdk.service.sql import StatementState

        def _safe_str(v):
            if v is None:
                return "NULL"
            return f"'{str(v).replace(chr(39), chr(39)*2)}'"

        def _safe_num(v):
            if v is None:
                return "NULL"
            return str(v)

        def _safe_ts(v):
            if v is None:
                return "NULL"
            ts = str(v).replace("T", " ").replace("Z", "").split("+")[0]
            return f"TIMESTAMP '{ts}'"

        value_rows = []
        for p in payloads:
            value_rows.append(
                f"({_safe_str(p.get('event_id'))},"
                f"{_safe_str(p.get('connection_id'))},"
                f"{_safe_str(p.get('device_name'))},"
                f"{_safe_ts(p.get('event_timestamp'))},"
                f"{_safe_ts(p.get('event_date'))},"
                f"{_safe_ts(p.get('ingested_at'))},"
                f"{_safe_num(p.get('latitude'))},"
                f"{_safe_num(p.get('longitude'))},"
                f"{_safe_num(p.get('altitude_m'))},"
                f"{_safe_num(p.get('heading_deg'))},"
                f"{_safe_num(p.get('pitch_deg'))},"
                f"{_safe_num(p.get('roll_deg'))},"
                f"{_safe_num(p.get('accel_x'))},"
                f"{_safe_num(p.get('accel_y'))},"
                f"{_safe_num(p.get('accel_z'))},"
                f"{_safe_num(p.get('accel_magnitude'))},"
                f"{_safe_num(p.get('gyro_x'))},"
                f"{_safe_num(p.get('gyro_y'))},"
                f"{_safe_num(p.get('gyro_z'))},"
                f"{_safe_num(p.get('speed_kmh'))},"
                f"{_safe_num(p.get('battery_pct'))},"
                f"{_safe_num(p.get('signal_strength'))},"
                f"{_safe_str(p.get('zerobus_topic', zerobus_cfg.topic))},"
                f"{_safe_num(p.get('zerobus_offset', 0))},"
                f"{_safe_num(p.get('payload_bytes', 256))})"
            )

        sql = f"""
            INSERT INTO {delta_cfg.full_name}
            (event_id, connection_id, device_name,
             event_timestamp, event_date,ingested_at,
             latitude, longitude, altitude_m,
             heading_deg, pitch_deg, roll_deg,
             accel_x, accel_y, accel_z, accel_magnitude,
             gyro_x, gyro_y, gyro_z,
             speed_kmh, battery_pct, signal_strength,
             zerobus_topic, zerobus_offset, payload_bytes)
            VALUES {', '.join(value_rows)}
        """

        try:
            from databricks.sdk.service.sql import StatementState
            stmt = client.statement_execution.execute_statement(
                warehouse_id=databricks_cfg.warehouse_id,
                statement=sql.strip(),
                wait_timeout="30s",
            )

            # Poll if needed
            max_wait = 30
            waited   = 0
            while stmt.status.state in (
                StatementState.PENDING,
                StatementState.RUNNING,
            ) and waited < max_wait:
                time.sleep(1)
                waited += 1
                stmt = client.statement_execution.get_statement(
                    stmt.statement_id
                )

            if stmt.status.state == StatementState.SUCCEEDED:
                return len(payloads)
            else:
                logger.error(f"Delta write failed: {stmt.status.error}")
                return 0

        except Exception as e:
            logger.error(f"Delta write error: {e}")
            return 0

    @property
    def total_written(self) -> int:
        return self._total_written


# ── Unified Sensor Publisher ──────────────────────────────────────────────────
class SensorPublisher:
    """
    Primary  : ZeroBus SDK publisher → Delta table
    Fallback : Direct Delta SQL write via DBSQL warehouse
    Tracks stats for the UI status display.
    """

    def __init__(self):
        self.zerobus = ZeroBusPublisher()
        self.delta   = DeltaDirectWriter()
        self._stats  = {
            "total_published":   0,
            "zerobus_published": 0,
            "delta_published":   0,
            "errors":            0,
            "last_publish_ts":   None,
        }

    def publish(self, payloads: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Publish payloads.
        Uses Delta direct write (reliable) - ZeroBus SDK not yet fully available.
        """
        if not payloads:
            return dict(self._stats)

        start = time.perf_counter()

        # ── Delta direct write (primary method) ───────────────────────────────
        # ZeroBus SDK is not fully available, so write directly to Delta
        delta_count = self.delta.write_batch(payloads)
        self._stats["delta_published"] += delta_count
        
        errors = len(payloads) - delta_count
        self._stats["errors"] += errors

        total = delta_count
        self._stats["total_published"] += total
        self._stats["last_publish_ts"]  = time.time()
        self._stats["last_elapsed_ms"]  = round(
            (time.perf_counter() - start) * 1000, 2
        )

        if total > 0:
            logger.info(
                f"Published {total} events to Delta → {delta_cfg.full_name}"
            )

        return dict(self._stats)

    @property
    def stats(self) -> Dict[str, Any]:
        return dict(self._stats)


# ── Global singleton ──────────────────────────────────────────────────────────
sensor_publisher = SensorPublisher()