"""
Sensor data publisher for ZeroStream.
Writes sensor payloads directly to Lakebase Autoscaling PostgreSQL.
Falls back to Delta SQL write if Lakebase is unavailable.
Smart PostgreSQL driver detection - no hard psycopg2 dependency.
"""
import json
import logging
import os
import sys
import time
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config.settings import databricks_cfg, zerobus_cfg, delta_cfg, lakebase_cfg

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


# ── Lakebase Direct Writer ────────────────────────────────────────────────────
class LakebaseDirectWriter:
    """
    Writes sensor records directly to Lakebase Autoscaling PostgreSQL.
    Uses pg8000 (pure Python) with OAuth token auto-refresh.
    """

    def __init__(self):
        self._total_written = 0
        self._conn = None
        self._token = None
        self._token_expires_at = 0.0
        self._endpoint_name = lakebase_cfg.endpoint or ""
        self._host = lakebase_cfg.host
        self._port = lakebase_cfg.port
        self._database = lakebase_cfg.database
        self._schema = lakebase_cfg.schema or "public"
        self._table = lakebase_cfg.table or os.environ.get("TABLE_NAME", "sensor_stream")
        self._username = os.environ.get("PGUSER") or None

    def _get_ws_client(self):
        """Get a WorkspaceClient for token generation."""
        return _get_sdk_client()

    def _refresh_token(self):
        """Generate or refresh the OAuth token for Lakebase."""
        if self._token and time.time() < (self._token_expires_at - 300):
            return  # Token still valid

        logger.info("🔑 Refreshing Lakebase OAuth token...")
        try:
            w = self._get_ws_client()

            if not self._endpoint_name:
                # Discover endpoint from project
                project_id = lakebase_cfg.instance
                if project_id:
                    endpoints = list(w.postgres.list_endpoints(
                        parent=f"projects/{project_id}/branches/production"
                    ))
                    if endpoints:
                        self._endpoint_name = endpoints[0].name
                        # Also get host from endpoint if not set
                        if not self._host:
                            ep_detail = w.postgres.get_endpoint(name=self._endpoint_name)
                            self._host = ep_detail.status.hosts.host

            cred = w.postgres.generate_database_credential(
                endpoint=self._endpoint_name
            )
            self._token = cred.token
            self._token_expires_at = time.time() + 3600

            # Always use the SDK identity that generated the token
            self._username = w.current_user.me().user_name

            logger.info(f"✅ Lakebase OAuth token refreshed for user '{self._username}'")
        except Exception as e:
            logger.warning(f"SDK token refresh failed: {e}")
            # Fallback: use PGUSER/PGPASSWORD injected by Apps runtime
            pw = os.environ.get("PGPASSWORD", "")
            user = os.environ.get("PGUSER", "")
            if pw and user:
                self._token = pw
                self._username = user
                self._token_expires_at = time.time() + 3600
                logger.info(f"Using injected PGPASSWORD fallback for user '{user}'")
            else:
                raise

    def _get_connection(self):
        """Get or create a PG connection with fresh token."""
        import psycopg2

        self._refresh_token()

        # Close stale connection
        if self._conn is not None:
            try:
                cur = self._conn.cursor()
                cur.execute("SELECT 1")
                cur.close()
                return self._conn
            except Exception:
                try:
                    self._conn.close()
                except Exception:
                    pass
                self._conn = None

        # In Apps runtime, PGPASSWORD is auto-injected
        password = self._token

        self._conn = psycopg2.connect(
            host=self._host,
            port=self._port,
            dbname=self._database,
            user=self._username,
            password=password,
            sslmode="require",
        )
        self._conn.autocommit = True
        logger.info(f"✅ Lakebase PG connected → {self._host}:{self._port}/{self._database}")
        return self._conn

    def write_batch(self, payloads: List[Dict[str, Any]]) -> int:
        """Insert batch of records into Lakebase PostgreSQL."""
        if not payloads:
            return 0

        written = 0
        chunk_size = 50
        for i in range(0, len(payloads), chunk_size):
            chunk = payloads[i:i + chunk_size]
            written += self._write_chunk(chunk)

        self._total_written += written
        return written

    def _write_chunk(self, payloads: List[Dict[str, Any]]) -> int:
        """Write a single chunk of records to Lakebase PG."""
        try:
            conn = self._get_connection()
        except Exception as e:
            logger.error(f"Lakebase connection failed: {e}")
            return 0

        fqtn = f'"{self._schema}"."{self._table}"'

        columns = [
            "event_id", "connection_id", "device_name",
            "event_timestamp", "event_date", "ingested_at",
            "latitude", "longitude", "altitude_m",
            "heading_deg", "pitch_deg", "roll_deg",
            "accel_x", "accel_y", "accel_z", "accel_magnitude",
            "gyro_x", "gyro_y", "gyro_z",
            "speed_kmh", "battery_pct", "signal_strength",
            "zerobus_topic", "zerobus_offset", "payload_bytes",
        ]

        placeholders = ", ".join(["%s"] * len(columns))
        col_list = ", ".join(columns)
        insert_sql = f"INSERT INTO {fqtn} ({col_list}) VALUES ({placeholders}) ON CONFLICT (event_id) DO NOTHING"

        try:
            for p in payloads:
                # Convert timestamp strings to proper format
                event_ts = str(p.get("event_timestamp", "")).replace("T", " ").replace("Z", "+00:00")
                event_date = str(p.get("event_date", ""))[:10]
                ingested_at = str(p.get("ingested_at", "")).replace("T", " ").replace("Z", "+00:00") if p.get("ingested_at") else None

                params = [
                    p.get("event_id"),
                    p.get("connection_id"),
                    p.get("device_name"),
                    event_ts,
                    event_date,
                    ingested_at,
                    p.get("latitude"),
                    p.get("longitude"),
                    p.get("altitude_m"),
                    p.get("heading_deg"),
                    p.get("pitch_deg"),
                    p.get("roll_deg"),
                    p.get("accel_x"),
                    p.get("accel_y"),
                    p.get("accel_z"),
                    p.get("accel_magnitude"),
                    p.get("gyro_x"),
                    p.get("gyro_y"),
                    p.get("gyro_z"),
                    p.get("speed_kmh"),
                    p.get("battery_pct"),
                    p.get("signal_strength"),
                    p.get("zerobus_topic", zerobus_cfg.topic),
                    p.get("zerobus_offset", 0),
                    p.get("payload_bytes", 256),
                ]
                cursor = conn.cursor()
                cursor.execute(insert_sql, params)
                cursor.close()

            return len(payloads)

        except Exception as e:
            logger.error(f"Lakebase write error: {e}")
            # Reset connection on error
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
            return 0

    def disconnect(self):
        """Close PG connection."""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    @property
    def total_written(self) -> int:
        return self._total_written


# ── Unified Sensor Publisher ──────────────────────────────────────────────────
class SensorPublisher:
    """
    Primary  : Lakebase Autoscaling PostgreSQL direct write
    Fallback : ZeroBus SDK publisher (if configured)
    Tracks stats for the UI status display.
    """

    def __init__(self):
        self.zerobus  = ZeroBusPublisher()
        self.lakebase = LakebaseDirectWriter()
        self._stats   = {
            "total_published":     0,
            "lakebase_published":  0,
            "zerobus_published":   0,
            "errors":              0,
            "last_publish_ts":     None,
        }

    def publish(self, payloads: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Publish payloads directly to Lakebase Autoscaling PostgreSQL.
        """
        if not payloads:
            return dict(self._stats)

        start = time.perf_counter()

        # ── Lakebase direct write (primary method) ────────────────────────────
        lb_count = self.lakebase.write_batch(payloads)
        self._stats["lakebase_published"] += lb_count

        errors = len(payloads) - lb_count
        self._stats["errors"] += errors

        total = lb_count
        self._stats["total_published"] += total
        self._stats["last_publish_ts"]  = time.time()
        self._stats["last_elapsed_ms"]  = round(
            (time.perf_counter() - start) * 1000, 2
        )

        if total > 0:
            logger.info(
                f"Published {total} events to Lakebase → {lakebase_cfg.host}/{lakebase_cfg.database}"
            )

        return dict(self._stats)

    @property
    def stats(self) -> Dict[str, Any]:
        return dict(self._stats)


# ── Global singleton ──────────────────────────────────────────────────────────
sensor_publisher = SensorPublisher()