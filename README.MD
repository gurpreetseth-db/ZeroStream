# ZeroStream

Real-time IoT sensor data streaming demo using **Databricks ZeroBus** (gRPC) and **Lakebase Autoscaling** (managed PostgreSQL) with a mobile device simulator and analytics dashboard — both deployed as **Databricks Apps**.

## Architecture

```
┌─────────────────────────┐         ┌──────────────────────────────┐
│   Mobile Simulator      │         │   Dashboard App              │
│   (FastAPI + JS)        │         │   (FastAPI + Leaflet.js)     │
│   • Generate GPS data   │         │   • KPI metrics (WebSocket)  │
│   • Simulate N devices  │         │   • Client sidebar           │
│   • Direct PG writes    │         │   • Movement trail map       │
│                         │         │   • Satellite / Street view  │
└────────┬────────────────┘         └──────────┬───────────────────┘
         │                                     │
         │ psycopg2 + OAuth                    │ psycopg2 + OAuth
         ▼                                     ▼
    ┌───────────────────────────────────────────────────────────┐
    │                  Databricks Platform                       │
    │                                                           │
    │  ┌─────────────────────────────────────────────────────┐  │
    │  │  Lakebase Autoscaling (PostgreSQL)                   │  │
    │  │  • sensor_stream table (primary data store)          │  │
    │  │  • Scale-to-zero, auto-resume, branch-based          │  │
    │  └─────────────────────────────────────────────────────┘  │
    │                                                           │
    │  ┌─────────────┐    ┌────────────────────────────────┐    │
    │  │   ZeroBus   │    │ SQL Warehouse (test endpoint)  │    │
    │  │   (gRPC)    │    └────────────────────────────────┘    │
    │  └──────┬──────┘                                          │
    │         ▼                                                 │
    │  ┌─────────────┐                                          │
    │  │ Delta Table  │                                         │
    │  │ sensor_stream│                                         │
    │  └─────────────┘                                          │
    └───────────────────────────────────────────────────────────┘
```

### Data Flow

1. **Mobile Simulator** generates realistic GPS sensor data for N simulated devices
2. Data is written **directly to Lakebase Autoscaling** PostgreSQL via `psycopg2` with OAuth
3. ZeroBus gRPC also streams into a **Delta table** (`sensor_stream`) for OLAP access
4. **Dashboard App** queries Lakebase via `psycopg2` with SDK-generated OAuth tokens
5. Dashboard "ZeroBus Stream Data" view also queries **Lakebase** (same table)
6. A SQL Warehouse is available for ad-hoc Delta queries (test endpoint)

---

## Components

### Mobile Simulator (`mobile_app/`)
- **FastAPI** web application with JavaScript frontend
- Simulates 1–100 mobile devices with realistic random-walk GPS physics
- Generates sensor data: GPS coordinates, speed, heading, altitude, acceleration, gyroscope, battery, signal strength
- **Primary write path**: Direct INSERT into Lakebase Autoscaling PostgreSQL
- **Secondary write path**: ZeroBus gRPC → Delta table
- 20 pre-seeded cities (Sydney, London, NYC, Tokyo, etc.)
- Features: adjustable device count slider, start/stop streaming, per-device detail view with map

### Dashboard App (`dashboard_app/`)
- **FastAPI** application with real-time WebSocket updates (3s interval)
- **All data served from Lakebase** — both main dashboard and ZeroBus stream view
- **Features**:
  - **KPI Bar**: Total events, unique clients, data ingested, access time
  - **Client Sidebar**: All devices with active/inactive badges, sorted by status
  - **Map Visualization**: Leaflet.js with MarkerCluster, **satellite view** (Esri World Imagery) with street toggle (CartoDB Voyager)
  - **Movement Trails**: Click any client → blue-to-red gradient track with S/E markers
  - **Clickable Track Points**: Click any point on the trail → detail panel shows timestamp, lat, lon, speed, heading, battery
  - **Split Layout**: Map shrinks when detail panel appears below; map auto-resizes
  - **ZeroBus Stream View**: Raw Lakebase data table — all sensor fields, sortable

### Key Files

| File | Purpose |
|------|---------|
| `dashboard_app/lakebase_client.py` | psycopg2 Lakebase client with SDK OAuth token refresh |
| `dashboard_app/delta_client.py` | Delta/SQL Warehouse queries (test endpoint only) |
| `dashboard_app/app.py` | FastAPI routes, WebSocket, lifespan management |
| `mobile_app/zerobus_client.py` | ZeroBus publisher + Lakebase direct writer |
| `mobile_app/data_generator.py` | Realistic sensor simulation with random-walk physics |
| `config/settings.py` | Centralized dataclass-based configuration |

### Configuration (`config/`)
- Centralized settings using Python dataclasses (no pydantic — avoids Rust build issues)
- Environment variable based: `LAKEBASE_*` vars override `PG*` auto-injected vars
- Key config: `LakebaseConfig`, `DeltaConfig`, `ZeroBusConfig`, `DatabricksConfig`
- `active_window_seconds` controls active/inactive badge threshold (default: 120s)

---

## Lakebase Connection Pattern

Both apps connect to **Lakebase Autoscaling** using OAuth tokens generated via the Databricks SDK `w.postgres` API. This uses `psycopg2` as the PostgreSQL driver.

### How It Works (M2M OAuth)

```
Databricks Apps runtime injects DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET
  → WorkspaceClient() auto-authenticates as the app's service principal
  → w.postgres.generate_database_credential() → short-lived PG token (~1hr)
  → psycopg2 connects using SP client_id as PG username + token as password
  → Auto-refresh 5 min before expiry
  → On auth failure: force token refresh + reconnect (escalating retry)
```

### Token Generation (SDK API)

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()  # auto M2M OAuth in Apps runtime

# Lakebase Autoscaling — uses w.postgres API
cred = w.postgres.generate_database_credential(
    endpoint="projects/{project-id}/branches/production/endpoints/primary"
)
token = cred.token  # 1-hour OAuth token
```

> **Important**: The apps use `w.postgres` (not `w.database`). These are different SDK APIs:
> - `w.postgres` — Lakebase Autoscaling (branch-based endpoints)
> - `w.database` — Lakebase Provisioned (instance-based)

### Connection Pattern

```python
import os, psycopg2

# PG username = SP application_id (auto-injected as DATABRICKS_CLIENT_ID)
pg_user = os.environ.get("DATABRICKS_CLIENT_ID")  # Apps runtime
# Fallback for local dev: workspace user email

conn = psycopg2.connect(
    host     = "ep-xxx.database.region.cloud.databricks.com",
    port     = 5432,
    dbname   = "databricks_postgres",
    user     = pg_user,
    password = oauth_token,   # from generate_database_credential()
    sslmode  = "require",
)
```

### Environment Variables (app.yaml)

| Variable | Description | Example |
|----------|-------------|---------|
| `LAKEBASE_INSTANCE` | Project name for SDK operations | `myprefix-lakebase` |
| `LAKEBASE_HOST` | PostgreSQL host (auto-discovered) | `ep-xxx.database.region.cloud.databricks.com` |
| `LAKEBASE_PORT` | PostgreSQL port | `5432` |
| `LAKEBASE_ENDPOINT` | Full endpoint path for token generation | `projects/myprefix-lakebase/branches/production/endpoints/primary` |
| `LAKEBASE_DATABASES` | Database name | `lakebaseprovisioned` |
| `LAKEBASE_SCHEMA` | Schema name | `lakebaseprovisioned` |
| `LAKEBASE_TABLE` | Table name | `sensor_stream` |
| `ACTIVE_WINDOW_SECONDS` | Threshold for active/inactive badge | `120` |

---

## Lakebase Permissions & Role Setup

### Service Principal Roles (Automated)

Both app service principals need **OAuth-enabled roles** in Lakebase. These are created automatically by `infra/create_sp_roles.py` and granted access by `infra/grant_lakebase_sp_access.py`.

#### How roles are created — via Databricks SDK API:

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

w.postgres.create_role(
    parent="projects/{project-id}/branches/production/endpoints/primary",
    role_id="sp-a4615999-7927-4c5e",     # must start with lowercase letter
    role={
        "display_name": "Mobile App SP",
        "auth_method": "LAKEBASE_OAUTH_V1",
        "identity": {
            "identity_type": "SERVICE_PRINCIPAL",
            "id": "a4615999-7927-4c5e-a89d-bb9fa01ce7be",
        },
    },
)
```

> **Note**: The `role_id` must match pattern `^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$`. If the SP client ID starts with a digit, it's prefixed with `sp-`.

#### SQL grants applied automatically:

```sql
GRANT CONNECT ON DATABASE lakebaseprovisioned TO "sp-a4615999-...";
GRANT USAGE ON SCHEMA lakebaseprovisioned TO "sp-a4615999-...";
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA lakebaseprovisioned TO "sp-a4615999-...";
```

> **Warning**: If you create a role via raw SQL (`CREATE ROLE "..."`) instead of the SDK API, it will **not** have the OAuth security label. The app will fail with:
> ```
> An oauth token was supplied but no role security label was configured in postgres
> for role "..." and native postgres login was either not attempted or failed.
> ```

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

---

## Vendored Wheels

Both apps include pre-compiled wheels in their `wheels/` directories to run in the **air-gapped Databricks Apps runtime** (no PyPI access):

| Wheel | Purpose |
|-------|---------|
| `databricks_sdk-0.97.0-py3-none-any.whl` | Databricks SDK with `w.postgres` API for Lakebase Autoscaling |
| `psycopg2_binary-2.9.11-cp311-cp311-manylinux2014_x86_64.whl` | Pre-compiled PostgreSQL driver (avoids C build in container) |

Referenced in each app's `requirements.txt` as `./wheels/filename.whl`.

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

Copy the sample and fill in your values:

```bash
cp .env-sample .env
```

Edit `.env` — at minimum set these required values:

| Variable | Description |
|----------|-------------|
| `INFRA_PREFIX` | Prefix for all resource names (e.g. `gsethi`) |
| `DATABRICKS_HOST` | Your workspace URL |
| `DATABRICKS_TOKEN` | Personal access token |
| `CATALOG` | Unity Catalog name |
| `SCHEMA` | Schema name |
| `ZEROBUS_SERVER_ENDPOINT` | `<workspace-id>.zerobus.<region>.cloud.databricks.com` |
| `LAKEBASE_USERNAME` | Lakebase admin username |
| `DATABRICKS_ACCOUNT_ID` | Databricks account ID |

Optional storage locations (leave empty for defaults):
```bash
CATALOG_STORAGE_LOCATION="s3://your-bucket/"
SCHEMA_STORAGE_LOCATION="s3://your-bucket/${SCHEMA}/"
```

See [.env-sample](.env-sample) for the full list of variables with defaults for Lakebase capacity, SQL Warehouse sizing, app compute, and streaming behaviour.

> **Note**: After running `infra/setup_infra.sh`, all generated values (warehouse IDs, SP credentials, hosts, etc.) are saved to `generated_config.env` which becomes the single source of truth. Never commit `.env` or `generated_config.env` to version control.

### 3. Setup Infrastructure (One-time)

```bash
bash infra/setup_infra.sh
```

This script runs the following steps:
1. Validate prerequisites (CLI, Python, SDK) and activate venv
2. Initialize `generated_config.env` with base configuration
3. Create SQL Warehouse (PRO, serverless, auto-stop)
4. Create Databricks Apps (mobile + dashboard) with SP identities
5. Create Delta tables (catalog, schema, table)
6. Create Lakebase Autoscaling project (database, schema, table, indexes)
7. Create Lakebase SP OAuth roles (`LAKEBASE_OAUTH_V1`) + grant PG permissions
8. Create ZeroBus service principal + OAuth credentials
9. Grant SQL Warehouse + Unity Catalog permissions to app SPs
10. Generate `app.yaml` files for both apps from `generated_config.env`

### 4. Deploy Applications

```bash
bash deployment/deploy_all.sh
```

### 5. Quick Redeploy (Code Changes Only)

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

1. **Open Mobile Simulator** → set slider to 5–20 devices
2. **Click "Apply"** → creates simulated mobile devices
3. **Click "Start Streaming"** → devices start sending GPS data directly to Lakebase
4. **Open Dashboard** → KPIs update via WebSocket every 3 seconds
5. **Client sidebar** shows active (green) / inactive (amber) devices
6. **Click any client** → map zooms to trail, detail panel appears below map
7. **Click any point on the trail** → detail panel updates with timestamp, lat, lon, speed, heading, battery
8. **Toggle map layer** → switch between Satellite (default) and Street views
9. **Click "SHOW DATA SERVED BY ZEROBUS"** → raw Lakebase stream data view

---

## Project Structure

```
ZeroStream/
├── .env                    # Your configuration (not in git)
├── .env-sample             # Sample configuration template
├── generated_config.env    # Auto-generated by setup_infra.sh (not in git)
├── README.MD
├── requirements.txt        # Root dev dependencies
│
├── config/                 # Shared configuration (deployed with both apps)
│   ├── __init__.py
│   └── settings.py         # Dataclass configs (Lakebase, Delta, ZeroBus, etc.)
│
├── infra/                  # Infrastructure setup (run once via setup_infra.sh)
│   ├── setup_infra.sh              # Orchestrator — runs all steps below in order
│   ├── create_warehouse.py         # Step 3: SQL Warehouse (PRO, serverless)
│   ├── create_apps.py              # Step 4: Databricks Apps + SP identities
│   ├── create_delta_tables.py      # Step 5: Catalog, schema, Delta table
│   ├── create_lakebase.py          # Step 6: Lakebase Autoscaling project + DB + table
│   ├── create_sp_roles.py          # Step 7: SP OAuth roles (LAKEBASE_OAUTH_V1)
│   ├── grant_lakebase_sp_access.py # Step 7: PG grants (CONNECT, USAGE, ALL PRIVILEGES)
│   ├── create_zerobus_credentials.py # Step 8: ZeroBus SP + OAuth secret
│   ├── grant_permissions.py        # Step 9: SQL Warehouse + UC permissions for app SPs
│   └── generate_app_yaml.py        # Step 10: Inject env vars into app.yaml files
│
├── deployment/             # App deployment (run via deploy_all.sh)
│   ├── deploy_all.sh               # Orchestrator — validates, installs, deploys, verifies
│   ├── 00_setup_environment.sh     # Step 1: Pre-deployment validation
│   ├── install_dependencies.sh     # Step 2: Smart dependency installer
│   ├── 03_deploy_apps.sh           # Step 3: Upload code + redeploy apps
│   └── verify_setup.py             # Step 4: End-to-end verification
│
├── dashboard_app/          # Dashboard (reads from Lakebase, M2M OAuth)
│   ├── app.py              # FastAPI routes, WebSocket, lifespan
│   ├── app.yaml            # Databricks App config (auto-generated by infra)
│   ├── lakebase_client.py  # psycopg2 + M2M OAuth token refresh via DATABRICKS_CLIENT_ID
│   ├── delta_client.py     # SQL Warehouse queries (test endpoint)
│   ├── requirements.txt
│   ├── wheels/             # Vendored: databricks-sdk 0.97.0, psycopg2-binary
│   ├── static/
│   │   ├── css/dashboard.css
│   │   └── js/
│   │       ├── dashboard.js    # Map, track points, detail panel
│   │       └── zerobus_view.js # ZeroBus data table
│   └── templates/
│       ├── dashboard.html      # Main dashboard page
│       └── zerobus_view.html   # ZeroBus stream data page
│
└── mobile_app/             # Mobile Simulator (writes to Lakebase + ZeroBus)
    ├── app.py              # FastAPI routes, WebSocket, streaming loop
    ├── app.yaml            # Databricks App config (auto-generated by infra)
    ├── data_generator.py   # Realistic random-walk sensor physics
    ├── zerobus_client.py   # ZeroBus publisher + Lakebase direct writer
    ├── requirements.txt
    ├── wheels/             # Vendored: databricks-sdk 0.97.0, psycopg2-binary
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
| **Lakebase Autoscaling** | Managed PostgreSQL (primary data store) with scale-to-zero |
| **Databricks ZeroBus** | Low-latency gRPC streaming (mobile → Delta) |
| **Delta Lake** | ACID storage for ZeroBus-ingested sensor data |
| **Databricks SQL Warehouse** | Serverless SQL for ad-hoc Delta queries |
| **Databricks Apps** | Managed app hosting with SP identity + OAuth |
| **psycopg2** | PostgreSQL driver for Lakebase (pre-compiled binary wheel) |
| **databricks-sdk 0.97.0** | `w.postgres` API for Lakebase Autoscaling management |
| **FastAPI** | Python web framework (both apps) |
| **Leaflet.js + MarkerCluster** | Interactive map with satellite/street layers |
| **WebSockets** | Real-time dashboard updates (3s interval) |

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
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
cred = w.postgres.generate_database_credential(
    endpoint='projects/myprefix-lakebase/branches/production/endpoints/primary')
print(cred.token[:30] + '...')
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
**Cause**: Role created via raw SQL instead of SDK API, or `auth_method` not set to `LAKEBASE_OAUTH_V1`.
**Fix**: Re-run `python3 infra/create_sp_roles.py` to create roles via `w.postgres.create_role()`, then `python3 infra/grant_lakebase_sp_access.py` for PG grants. See [Lakebase Permissions](#lakebase-permissions--role-setup).

### Dashboard Can't Connect to Lakebase (M2M Auth)
```
Missing PG credentials: user=MISSING, token=MISSING
```
**Cause**: The app's service principal doesn't have a Lakebase OAuth role, or `DATABRICKS_CLIENT_ID` isn't available.
**Fix**: Ensure `infra/setup_infra.sh` Step 7 completed successfully. The dashboard uses `DATABRICKS_CLIENT_ID` (auto-injected by Apps runtime) as the PG username — this must match the Lakebase role created by `create_sp_roles.py`.

### role_id Pattern Violation
```
role_id must match ^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$
```
**Cause**: SP client IDs starting with a digit can't be used directly as `role_id`.
**Fix**: `create_sp_roles.py` automatically prefixes with `sp-` when the ID starts with a digit.

### SDK Too Old for w.postgres
```
AttributeError: 'WorkspaceClient' has no attribute 'postgres'
```
**Cause**: Installed `databricks-sdk` is older than 0.97.0.
**Fix**: The infra scripts automatically bootstrap the vendored 0.97.0 wheel from `dashboard_app/wheels/`. Ensure the wheel file exists.

### Clients Show as Inactive
**Cause**: `ACTIVE_WINDOW_SECONDS` too low relative to `STREAM_INTERVAL_MS`.
**Fix**: Set `ACTIVE_WINDOW_SECONDS=120` (2 minutes) in `app.yaml`.

### Apps Not Showing Data
1. Check mobile app logs: `databricks apps logs $MOBILE_APP`
2. Verify Lakebase table has rows (connect via psql or dashboard)
3. Ensure Lakebase project is awake (not scaled to zero — first request wakes it)
4. Check Lakebase host in `generated_config.env` matches `app.yaml`

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
