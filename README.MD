# ZeroStream

Real-time IoT sensor data streaming demo using **Databricks ZeroBus** (gRPC) → **Delta Lake** → **Lakebase** (managed PostgreSQL) with a mobile device simulator and analytics dashboard — both deployed as **Databricks Apps**.

## Architecture

```
┌─────────────────────────┐         ┌──────────────────────────────┐
│   Mobile Simulator      │         │   Dashboard App              │
│   (FastAPI + JS)        │         │   (FastAPI + Leaflet.js)     │
│   • Generate GPS data   │         │   • KPI metrics (WebSocket)  │
│   • Simulate N devices  │         │   • Client sidebar           │
│   • Stream via ZeroBus  │         │   • Movement trail map       │
│                         │         │   • Clickable track points   │
└───────────┬─────────────┘         └──────────┬───────────────────┘
            │                                  │
            │ gRPC (ZeroBus)                   │ asyncpg + OAuth
            ▼                                  ▼
    ┌───────────────────────────────────────────────────────────┐
    │                  Databricks Platform                       │
    │                                                           │
    │  ┌─────────────┐    ┌─────────────┐    ┌───────────────┐ │
    │  │   ZeroBus   │───▶│ Delta Table │───▶│   Lakebase    │ │
    │  │   (gRPC)    │    │ sensor_stream│    │ (Synced Table)│ │
    │  └─────────────┘    └─────────────┘    └───────────────┘ │
    │                                                           │
    │  ┌────────────────────────────────────────────┐           │
    │  │ SQL Warehouse (ZeroBus view page queries)  │           │
    │  └────────────────────────────────────────────┘           │
    └───────────────────────────────────────────────────────────┘
```

### Data Flow

1. **Mobile Simulator** generates GPS sensor data for N simulated devices
2. Data streams via **ZeroBus gRPC** into a **Delta table** (`sensor_stream`)
3. A **Synced Table** continuously replicates Delta → **Lakebase** (`sensor_stream_synced`)
4. **Dashboard App** queries Lakebase via `asyncpg` with OAuth token refresh
5. Dashboard also has a "ZeroBus View" page that queries the Delta table via SQL Warehouse

---

## Components

### Mobile Simulator (`mobile_app/`)
- **FastAPI** web application with JavaScript frontend
- Simulates multiple mobile devices with realistic GPS movement patterns
- Generates sensor data: GPS coordinates, speed, battery, heading, signal strength
- Streams data to Databricks via ZeroBus gRPC protocol
- Features: adjustable device count slider, start/stop streaming, per-device view

### Dashboard App (`dashboard_app/`)
- **FastAPI** application with real-time WebSocket updates (3s interval)
- **Two views**:
  - **Lakebase Dashboard** (main) — reads from Lakebase PostgreSQL
  - **ZeroBus View** — reads raw stream data from Delta via SQL Warehouse
- **Features**:
  - **KPI Bar**: Total events, unique clients, data ingested, access time
  - **Client Sidebar**: All devices with active/inactive badges, sorted by status
  - **Map Visualization**: Leaflet.js with MarkerCluster, satellite view
  - **Movement Trails**: Click any client → blue-to-red gradient track with S/E markers
  - **Clickable Track Points**: Click any point on the trail → detail panel shows that point's timestamp, lat, lon, speed, heading, battery
  - **Split Layout**: Map shrinks when detail panel appears below; map auto-resizes

### Key Files

| File | Purpose |
|------|---------|
| `dashboard_app/lakebase_client.py` | Async Lakebase client with OAuth token refresh, connection pooling |
| `dashboard_app/delta_client.py` | Delta/SQL Warehouse queries (ZeroBus view) |
| `dashboard_app/app.py` | FastAPI routes, WebSocket, lifespan management |
| `dashboard_app/app.yaml` | Databricks Apps config with all env vars |
| `config/settings.py` | Centralized dataclass-based configuration |

### Configuration (`config/`)
- Centralized settings using Python dataclasses (no pydantic dependency)
- Environment variable based: `LAKEBASE_*` vars override `PG*` auto-injected vars
- Key config: `LakebaseConfig`, `DeltaConfig`, `ZeroBusConfig`, `DatabricksConfig`
- `active_window_seconds` controls active/inactive badge threshold (default: 120s)

---

## Lakebase Connection Pattern

The dashboard app connects to **Lakebase Provisioned** using OAuth tokens generated via the Databricks SDK. This is the recommended pattern for Databricks Apps.

### How It Works

```
App starts → SDK generates OAuth token (1hr TTL)
          → asyncpg pool created with token as password
          → Background refresh at 55min before expiry
          → Pool recycled on token refresh
```

### Token Generation (SDK API)

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Lakebase Provisioned — uses w.database API
cred = w.database.generate_database_credential(
    request_id=str(uuid.uuid4()),
    instance_names=["your-lakebase-instance"]
)
token = cred.token  # 1-hour OAuth token

# Lakebase Autoscale (different API) — uses w.postgres API
# cred = w.postgres.generate_database_credential(endpoint="your-endpoint")
```

> **Important**: The app uses `w.database` (not `w.postgres`). These are different SDK APIs with different signatures. `w.database` is for Lakebase Provisioned instances.

### Connection Pool

```python
pool = await asyncpg.create_pool(
    user     = "service-principal-client-id",
    password = oauth_token,          # from SDK
    database = "zerobusstream",
    host     = "instance-xxx.database.cloud.databricks.com",
    port     = 5432,
    ssl      = "require",
    min_size = 1,
    max_size = 4,
)
```

### Environment Variables (app.yaml)

| Variable | Description | Example |
|----------|-------------|---------|
| `LAKEBASE_INSTANCE` | Instance name for SDK token generation | `gsethi-lakebase` |
| `LAKEBASE_HOST` | PostgreSQL host | `instance-xxx.database.cloud.databricks.com` |
| `LAKEBASE_PORT` | PostgreSQL port | `5432` |
| `LAKEBASE_DATABASES` | Database name | `zerobusstream` |
| `LAKEBASE_SCHEMA` | Schema name | `zerobusstream` |
| `LAKEBASE_TABLE` | Synced table name | `sensor_stream_synced` |
| `LAKEBASE_USER` | SP client ID (PostgreSQL user) | `d49c82f8-...` |
| `ACTIVE_WINDOW_SECONDS` | Threshold for active/inactive badge | `120` |

---

## Lakebase Permissions & Role Setup

### Service Principal Role (Critical)

The dashboard app's service principal must have a **properly provisioned role** in Lakebase with an OAuth security label. This is **not** the same as a raw SQL `CREATE ROLE`.

#### Correct way — via Databricks SDK API:

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import (
    DatabaseInstanceRole,
    DatabaseInstanceRoleIdentityType,
)

w = WorkspaceClient()

# Create role with OAuth security label
role = DatabaseInstanceRole(
    name="your-sp-client-id",
    identity_type=DatabaseInstanceRoleIdentityType.SERVICE_PRINCIPAL,
)
w.database.create_database_instance_role(
    instance_name="your-lakebase-instance",
    database_instance_role=role,
)
```

#### Then grant SQL permissions:

```sql
-- Connect as a superuser (your own user)
GRANT USAGE ON SCHEMA zerobusstream TO "your-sp-client-id";
GRANT SELECT ON ALL TABLES IN SCHEMA zerobusstream TO "your-sp-client-id";
```

> **Warning**: If you create a role via raw SQL (`CREATE ROLE "..."`) instead of the SDK API, the role will **not** have the OAuth security label. The app will fail with:
> ```
> An oauth token was supplied but no role security label was configured in postgres
> for role "..." and native postgres login was either not attempted or failed.
> ```
> **Fix**: Drop the raw SQL role (revoke its grants first), then re-create via the SDK API.

#### Dropping a conflicting raw-SQL role:

```sql
-- As superuser, connect via your own creds
GRANT "sp-client-id" TO "your-user@databricks.com";
REASSIGN OWNED BY "sp-client-id" TO "your-user@databricks.com";
DROP OWNED BY "sp-client-id";
REVOKE SELECT ON zerobusstream.sensor_stream_synced
    FROM "sp-client-id" GRANTED BY "your-user@databricks.com";
DROP ROLE IF EXISTS "sp-client-id";
```

Then re-create via SDK and re-grant.

### Listing Existing Roles

```python
for role in w.database.list_database_instance_roles("your-lakebase-instance"):
    print(role.name, role.identity_type)
```

---

## ZeroBus Setup

### ZeroBus Service Principal

The mobile app streams data via ZeroBus using a dedicated service principal with OAuth credentials.

- **SP Name**: `{prefix}-zerobus-service`
- **OAuth**: Client ID + Client Secret (stored as Databricks App secret)
- **Topic**: `{prefix}_sensor_stream_topic`
- **Endpoint**: `{workspace-id}.zerobus.{region}.cloud.databricks.com`

### ZeroBus Credentials

Created by `infra/create_zerobus_credentials.py`:
1. Creates a service principal
2. Generates OAuth client ID + secret
3. Grants `CAN_USE` on the ZeroBus endpoint
4. Stores client ID in `generated_config.env`
5. Client secret must be set as an app secret in Databricks UI

### ZeroBus View (Dashboard)

The "SHOW DATA SERVED BY ZEROBUS" button opens a separate view that:
- Queries the **Delta table** directly via SQL Warehouse (not Lakebase)
- Shows raw streaming data in a table with all sensor fields
- Uses `delta_client.py` for Databricks SQL queries

---

## Quick Start

### 1. Clone and Setup

```bash
cd ZeroStream
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure Environment

Create `.env` with your values:

```bash
INFRA_PREFIX=myprefix
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com/
DATABRICKS_TOKEN=your-personal-access-token

# Unity Catalog
CATALOG=your_catalog
SCHEMA=your_schema
TABLE_NAME=sensor_stream

# ZeroBus
ZEROBUS_SERVER_ENDPOINT=your-workspace-id.zerobus.region.cloud.databricks.com

# Lakebase
LAKEBASE_INSTANCE=${INFRA_PREFIX}-lakebase
LAKEBASE_DATABASES=zerobusstream
LAKEBASE_SCHEMA=zerobusstream
LAKEBASE_CAPACITY=CU_2

# Apps
MOBILE_APP_NAME=${INFRA_PREFIX}-mobile-app
DASHBOARD_APP_NAME=${INFRA_PREFIX}-dashboard-app

# Streaming
STREAM_INTERVAL_MS=5000
ACTIVE_WINDOW_SECONDS=120
```

### 3. Setup Infrastructure (One-time)

```bash
bash infra/setup_infra.sh
```

Creates: SQL Warehouse, Databricks Apps, Delta tables, Lakebase instance, Synced table, ZeroBus SP, permissions, `app.yaml` files. All values saved to `generated_config.env`.

### 4. Create Lakebase SP Role (One-time, post-infra)

After infrastructure setup, create the dashboard app's SP role in Lakebase:

```bash
python3 -c "
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import DatabaseInstanceRole, DatabaseInstanceRoleIdentityType

w = WorkspaceClient()
sp_client_id = 'YOUR_DASHBOARD_SP_CLIENT_ID'  # from generated_config.env

role = DatabaseInstanceRole(
    name=sp_client_id,
    identity_type=DatabaseInstanceRoleIdentityType.SERVICE_PRINCIPAL,
)
w.database.create_database_instance_role(
    instance_name='YOUR_LAKEBASE_INSTANCE',
    database_instance_role=role,
)
print('Role created')
"
```

Then grant SELECT:

```bash
# Connect to Lakebase as superuser and run:
# GRANT USAGE ON SCHEMA zerobusstream TO "sp-client-id";
# GRANT SELECT ON ALL TABLES IN SCHEMA zerobusstream TO "sp-client-id";
```

### 5. Deploy Applications

```bash
bash deployment/deploy_all.sh
```

### 6. Quick Redeploy (Code Changes Only)

For fast iteration without the full deploy pipeline:

```bash
source generated_config.env
REMOTE_DIR="/Workspace/zerostream/$DASHBOARD_APP"
find dashboard_app config -name '.DS_Store' -delete
databricks workspace delete "$REMOTE_DIR" --recursive 2>/dev/null
databricks workspace mkdirs "$REMOTE_DIR"
databricks workspace import-dir dashboard_app "$REMOTE_DIR" --overwrite
databricks workspace mkdirs "$REMOTE_DIR/config"
databricks workspace import-dir config "$REMOTE_DIR/config" --overwrite
databricks apps deploy "$DASHBOARD_APP" --source-code-path "$REMOTE_DIR" --no-wait
```

---

## Deployed Apps

| App | Description | URL Pattern |
|-----|-------------|-------------|
| Mobile Simulator | Generate and stream sensor data | `https://{prefix}-mobile-app-*.databricksapps.com` |
| Dashboard | Lakebase KPIs, map, track points | `https://{prefix}-dashboard-app-*.databricksapps.com` |

---

## Demo Walkthrough

1. **Open Mobile Simulator** → set slider to 5-20 devices
2. **Click "Apply"** → creates simulated mobile devices
3. **Click "Start Streaming"** → devices start sending GPS data via ZeroBus
4. **Open Dashboard** → KPIs update via WebSocket every 3 seconds
5. **Client sidebar** shows active (green) / inactive (amber) devices
6. **Click any client** → map zooms to trail, detail panel appears below map
7. **Click any point on the trail** → detail panel updates with that point's data (timestamp, lat, lon, speed, heading, battery)
8. **Click "SHOW DATA SERVED BY ZEROBUS"** → raw Delta table data view

---

## SDK Version Requirements

| Package | Minimum | Recommended |
|---------|---------|-------------|
| `databricks-sdk` | `>=0.54.0` | `>=0.81.0` |
| `asyncpg` | `0.29.0` | `0.29.0` |
| `fastapi` | `0.111.0` | `0.111.0` |

> The dashboard `requirements.txt` uses `databricks-sdk>=0.54.0`. If you hit missing API errors (`w.database` not found), upgrade to `>=0.81.0`.

---

## Project Structure

```
ZeroStream/
├── .env                    # Base configuration (not in git)
├── generated_config.env    # Generated config from infra setup (not in git)
├── README.MD
├── requirements.txt        # Root dependencies
│
├── config/                 # Shared configuration
│   ├── __init__.py
│   └── settings.py         # Dataclass configs (Lakebase, Delta, ZeroBus, etc.)
│
├── infra/                  # Infrastructure setup (run once)
│   ├── setup_infra.sh
│   ├── create_warehouse.py
│   ├── create_apps.py
│   ├── create_delta_tables.py
│   ├── create_lakebase.py
│   ├── create_synced_table.py
│   ├── create_zerobus_credentials.py
│   ├── grant_permissions.py
│   └── generate_app_yaml.py
│
├── deployment/             # App deployment
│   ├── deploy_all.sh
│   ├── 00_setup_environment.sh
│   ├── 03_deploy_apps.sh
│   ├── install_dependencies.sh
│   └── verify_setup.py
│
├── dashboard_app/          # Dashboard (Lakebase + ZeroBus views)
│   ├── app.py              # FastAPI routes, WebSocket, lifespan
│   ├── app.yaml            # Databricks App config (auto-generated)
│   ├── lakebase_client.py  # asyncpg + OAuth token refresh
│   ├── delta_client.py     # SQL Warehouse queries
│   ├── requirements.txt
│   ├── static/
│   │   ├── css/dashboard.css
│   │   └── js/
│   │       ├── dashboard.js    # Map, track points, detail panel
│   │       └── zerobus_view.js # ZeroBus data table
│   └── templates/
│       ├── dashboard.html      # Main dashboard page
│       └── zerobus_view.html   # ZeroBus view page
│
└── mobile_app/             # Mobile Simulator
    ├── app.py
    ├── app.yaml
    ├── data_generator.py
    ├── zerobus_client.py
    ├── requirements.txt
    ├── static/
    │   ├── css/styles.css
    │   └── js/
    │       ├── app.js
    │       ├── connections.js
    │       ├── map.js
    │       └── sensors.js
    └── templates/
        └── index.html
```

---

## Key Technologies

| Technology | Used For |
|------------|----------|
| **Databricks ZeroBus** | Low-latency gRPC streaming (mobile → Delta) |
| **Delta Lake** | ACID storage, source of truth for sensor data |
| **Lakebase Provisioned** | Managed PostgreSQL with Delta sync for low-latency reads |
| **Synced Tables** | Continuous Delta → Lakebase replication |
| **Databricks SQL Warehouse** | Serverless SQL for ZeroBus view queries |
| **Databricks Apps** | Managed app hosting with SP identity |
| **asyncpg** | Async PostgreSQL driver for Lakebase |
| **FastAPI** | Python web framework (both apps) |
| **Leaflet.js + MarkerCluster** | Interactive map with movement trails |
| **WebSockets** | Real-time dashboard updates |

---

## Useful Commands

```bash
# View app logs
databricks apps logs $MOBILE_APP
databricks apps logs $DASHBOARD_APP

# Check app status
databricks apps get $DASHBOARD_APP

# Check Delta table row count
databricks sql execute \
  --warehouse-id $DATABRICKS_WAREHOUSE_ID \
  --statement "SELECT COUNT(*) FROM $CATALOG.$SCHEMA.$TABLE_NAME"

# Generate fresh Lakebase OAuth token (for local testing)
python3 -c "
from databricks.sdk import WorkspaceClient; import uuid
w = WorkspaceClient()
c = w.database.generate_database_credential(
    request_id=str(uuid.uuid4()),
    instance_names=['your-lakebase-instance'])
print(c.token[:30] + '...')
"

# List Lakebase roles
python3 -c "
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
for r in w.database.list_database_instance_roles('your-lakebase-instance'):
    print(r.name, r.identity_type)
"

# Redeploy dashboard only (fast)
source generated_config.env && \
  REMOTE_DIR="/Workspace/zerostream/$DASHBOARD_APP" && \
  databricks workspace delete "$REMOTE_DIR" --recursive 2>/dev/null; \
  databricks workspace mkdirs "$REMOTE_DIR" && \
  databricks workspace import-dir dashboard_app "$REMOTE_DIR" --overwrite && \
  databricks workspace mkdirs "$REMOTE_DIR/config" && \
  databricks workspace import-dir config "$REMOTE_DIR/config" --overwrite && \
  databricks apps deploy "$DASHBOARD_APP" --source-code-path "$REMOTE_DIR" --no-wait

# Run verification checks
python3 deployment/verify_setup.py
```

---

## Troubleshooting

### Lakebase OAuth Error
```
An oauth token was supplied but no role security label was configured
```
**Cause**: Role created via raw SQL instead of SDK API.
**Fix**: Drop the raw SQL role, recreate via `w.database.create_database_instance_role()` with `identity_type=SERVICE_PRINCIPAL`. See [Lakebase Permissions](#lakebase-permissions--role-setup).

### Role Cannot Be Dropped
```
role "..." cannot be dropped because some objects depend on it
```
**Fix**: Revoke grants with `GRANTED BY` clause:
```sql
GRANT "sp-id" TO "your-user@databricks.com";
REASSIGN OWNED BY "sp-id" TO "your-user@databricks.com";
DROP OWNED BY "sp-id";
REVOKE SELECT ON schema.table FROM "sp-id" GRANTED BY "your-user@databricks.com";
DROP ROLE IF EXISTS "sp-id";
```

### SDK API Mismatch
```
unexpected keyword argument 'request_id'
```
**Cause**: Using `w.postgres` (Autoscale API) instead of `w.database` (Provisioned API). They have different method signatures.
**Fix**: Ensure code uses `w.database.generate_database_credential(instance_names=[...], request_id=...)`.

### Clients Show as Inactive
**Cause**: `ACTIVE_WINDOW_SECONDS` too low. If set to `5` and the mobile app streams every 5s, sync latency makes all clients appear inactive.
**Fix**: Set `ACTIVE_WINDOW_SECONDS=120` (2 minutes) in `app.yaml`.

### Apps Not Showing Data
- Check ZeroBus connection in mobile app logs: `databricks apps logs $MOBILE_APP`
- Verify Delta table has rows: `SELECT COUNT(*) FROM your_table`
- Ensure SQL Warehouse is running (for ZeroBus view)
- Check Lakebase instance is started (not stopped)

### Map Not Showing Track
- Hard refresh browser (`Cmd+Shift+R` / `Ctrl+Shift+R`)
- Check browser console for JS errors
- Verify client has GPS data: click client in sidebar to fetch track

### Deployment Fails
- Run `python3 deployment/verify_setup.py` to diagnose
- Ensure `.env` and `generated_config.env` exist
- Ensure Databricks CLI is authenticated: `databricks auth login`
- Apps must exist first — run `bash infra/setup_infra.sh` before deploying

---

## License

Internal Databricks demo project.
