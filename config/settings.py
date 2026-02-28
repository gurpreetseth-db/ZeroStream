"""
Central configuration - works with pydantic v1 AND v2.
All values from environment variables.
No Rust compilation required.
"""
import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

# Load configuration from generated_config.env (primary) and .env (fallback)
# generated_config.env is the single source of truth after running infra/setup_infra.sh
root_dir = Path(__file__).parent.parent
generated_config = root_dir / "generated_config.env"
dotenv_config = root_dir / ".env"

# Load .env first (fallback), then generated_config.env (override)
#if dotenv_config.exists():
#    load_dotenv(dotenv_config)
if generated_config.exists():
    load_dotenv(generated_config, override=True)

# ── Pydantic version detection ─────────────────────────────────────────────────
try:
    import pydantic
    PYDANTIC_V2 = int(pydantic.__version__.split(".")[0]) >= 2
except ImportError:
    PYDANTIC_V2 = False

# ── Pure dataclass config (no pydantic dependency) ────────────────────────────
# Using plain Python dataclasses avoids ALL pydantic build issues
# while keeping the same clean API
from dataclasses import dataclass, field


@dataclass
class DatabricksConfig:
    host:         str = field(default_factory=lambda: os.environ.get("DATABRICKS_HOST", ""))
    warehouse_id: str = field(default_factory=lambda: os.environ.get("DATABRICKS_WAREHOUSE_ID", ""))
    token:        str = field(default_factory=lambda: os.environ.get("DATABRICKS_TOKEN", ""))

    def __post_init__(self):
        # Ensure host has trailing slash
        if self.host and not self.host.endswith("/"):
            self.host = self.host + "/"


@dataclass
class DeltaConfig:
    catalog:    str = field(default_factory=lambda: os.environ.get("CATALOG", ""))
    schema:     str = field(default_factory=lambda: os.environ.get("SCHEMA", ""))
    table_name: str = field(default_factory=lambda: os.environ.get("TABLE_NAME", ""))

    @property
    def full_name(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table_name}"

    @property
    def full_name_quoted(self) -> str:
        return f"`{self.catalog}`.`{self.schema}`.`{self.table_name}`"


@dataclass
class ZeroBusConfig:
    server_endpoint:  str = field(default_factory=lambda: os.environ.get("ZEROBUS_SERVER_ENDPOINT", ""))
    client_id:        str = field(default_factory=lambda: os.environ.get("ZEROBUS_CLIENT_ID", ""))
    client_secret:    str = field(default_factory=lambda: os.environ.get("ZEROBUS_CLIENT_SECRET", ""))
    topic:            str = field(default_factory=lambda: os.environ.get("ZEROBUS_TOPIC", ""))
    stream_interval_ms: int = field(default_factory=lambda: int(os.environ.get("STREAM_INTERVAL_MS", "")))


@dataclass
class LakebaseConfig:
    """Lakebase configuration.
    
    When deployed to Databricks Apps with a Lakebase resource, these standard
    PostgreSQL env vars are auto-injected: PGHOST, PGDATABASE, PGUSER, PGPASSWORD, PGPORT
    
    The LAKEBASE_* vars are fallbacks for local development.
    Priority: LAKEBASE_* env vars override PG* vars (for explicit control).
    """
    # Connection params: LAKEBASE_* takes priority over auto-injected PG* vars
    host:                   str = field(default_factory=lambda: os.environ.get("LAKEBASE_HOST") or os.environ.get("PGHOST", ""))
    port:                   int = field(default_factory=lambda: int(os.environ.get("LAKEBASE_PORT") or os.environ.get("PGPORT", "5432")))
    database:               str = field(default_factory=lambda: os.environ.get("LAKEBASE_DATABASES") or os.environ.get("PGDATABASE", "zerobus_app_psg_db"))
    user:                   str = field(default_factory=lambda: os.environ.get("LAKEBASE_USER") or os.environ.get("PGUSER", ""))
    password:               str = field(default_factory=lambda: os.environ.get("LAKEBASE_PASSWORD") or os.environ.get("PGPASSWORD", ""))
    
    # App-specific config (not auto-injected)
    instance:               str = field(default_factory=lambda: os.environ.get("LAKEBASE_INSTANCE", ""))
    catalog:                str = field(default_factory=lambda: os.environ.get("LAKEBASE_CATALOG", "gsethi"))
    # Synced table schema - matches UC schema for synced tables
    schema:                 str = field(default_factory=lambda: os.environ.get("LAKEBASE_SCHEMA", ""))
    table:                  str = field(default_factory=lambda: os.environ.get("LAKEBASE_TABLE", ""))
    active_window_seconds:  int = field(default_factory=lambda: int(os.environ.get("ACTIVE_WINDOW_SECONDS", "")))  # 5 min for better visibility

    @property
    def dsn(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    @property
    def dsn_safe(self) -> str:
        """DSN without password - safe for logging."""
        return (
            f"postgresql://{self.user}@{self.host}:{self.port}/{self.database}"
        )


@dataclass
class AppConfig:
    mobile_app_name:    str = field(default_factory=lambda: os.environ.get("MOBILE_APP", ""))
    dashboard_app_name: str = field(default_factory=lambda: os.environ.get("DASHBOARD_APP", ""))


# ── Singleton instances ────────────────────────────────────────────────────────
databricks_cfg = DatabricksConfig()
delta_cfg      = DeltaConfig()
zerobus_cfg    = ZeroBusConfig()
lakebase_cfg   = LakebaseConfig()
app_cfg        = AppConfig()


# ── Validation helper ──────────────────────────────────────────────────────────
def validate_config() -> list:
    """
    Returns list of missing required config values.
    Call at startup to catch misconfiguration early.
    """
    missing = []

    required = {
        "DATABRICKS_HOST":          databricks_cfg.host,
        "DATABRICKS_TOKEN":         databricks_cfg.token,
        "DATABRICKS_WAREHOUSE_ID":  databricks_cfg.warehouse_id,
        "CATALOG":                  delta_cfg.catalog,
        "SCHEMA":                   delta_cfg.schema,
        "TABLE_NAME":               delta_cfg.table_name,
        "ZEROBUS_SERVER_ENDPOINT":  zerobus_cfg.server_endpoint,
        "ZEROBUS_CLIENT_ID":        zerobus_cfg.client_id,
        "ZEROBUS_CLIENT_SECRET":    zerobus_cfg.client_secret,
        "LAKEBASE_HOST":            lakebase_cfg.host,
        "LAKEBASE_DATABASES":       lakebase_cfg.database,
    }

    for key, value in required.items():
        if not value:
            missing.append(key)

    return missing


# ── Debug helper ───────────────────────────────────────────────────────────────
def print_config():
    """Print current config (masks secrets). Useful for debugging."""
    def mask(v: str) -> str:
        if not v:
            return "NOT SET"
        if len(v) <= 8:
            return "***"
        return v[:4] + "***" + v[-4:]

    print("ZeroStream Configuration:")
    print(f"  Databricks Host     : {databricks_cfg.host}")
    print(f"  Databricks WH ID    : {databricks_cfg.warehouse_id}")
    print(f"  Databricks Token    : {mask(databricks_cfg.token)}")
    print(f"  Delta Table         : {delta_cfg.full_name}")
    print(f"  ZeroBus Endpoint    : {zerobus_cfg.server_endpoint}")
    print(f"  ZeroBus Client ID   : {zerobus_cfg.client_id}")
    print(f"  ZeroBus Secret      : {mask(zerobus_cfg.client_secret)}")
    print(f"  ZeroBus Topic       : {zerobus_cfg.topic}")
    print(f"  Lakebase Host       : {lakebase_cfg.host}")
    print(f"  Lakebase Database   : {lakebase_cfg.database}")
    print(f"  Lakebase Schema     : {lakebase_cfg.schema}")
    print(f"  Mobile App          : {app_cfg.mobile_app_name}")
    print(f"  Dashboard App       : {app_cfg.dashboard_app_name}")
    print(f"  Stream Interval     : {zerobus_cfg.stream_interval_ms}ms")
    print(f"  Active Window       : {lakebase_cfg.active_window_seconds}s")


if __name__ == "__main__":
    print_config()
    missing = validate_config()
    if missing:
        print(f"\n❌ Missing config: {missing}")
    else:
        print("\n✅ All required config values present")