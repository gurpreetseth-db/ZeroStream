# ZeroStream

Real-time IoT sensor data streaming demo using **Databricks ZeroBus** (gRPC) with a mobile device simulator and backend analytics dashboard.

## Architecture

```
┌─────────────────────────┐         ┌─────────────────────────┐
│   Mobile Simulator      │         │   Backend Dashboard     │
│   (FastAPI + JS)        │         │   (FastAPI + Leaflet)   │
│   • Generate GPS data   │         │   • KPI metrics         │
│   • Simulate devices    │         │   • Client list         │
│   • Stream via ZeroBus  │         │   • Movement map        │
└───────────┬─────────────┘         └───────────┬─────────────┘
            │                                   │
            │ gRPC                              │ SQL Warehouse
            ▼                                   ▼
    ┌───────────────────────────────────────────────────────────┐
    │                  Databricks Platform                       │
    │  ┌─────────────┐    ┌─────────────┐    ┌───────────────┐  │
    │  │   ZeroBus   │───▶│ Delta Table │───▶│   Lakebase    │  │
    │  │   (gRPC)    │    │ sensor_stream│    │ (Synced Table)│  │
    │  └─────────────┘    └─────────────┘    └───────────────┘  │
    └───────────────────────────────────────────────────────────┘
```

## Components

### Mobile Simulator (`mobile_app/`)
- **FastAPI** web application with JavaScript frontend
- Simulates multiple mobile devices with realistic GPS movement
- Generates sensor data: GPS coordinates, speed, battery, signal strength
- Streams data to Databricks via ZeroBus gRPC protocol
- Features: adjustable device count, start/stop streaming, per-device view

### Backend Dashboard (`dashboard_app/`)
- **FastAPI** application with real-time WebSocket updates
- Queries Delta table via Databricks SQL Warehouse
- Features:
  - **KPIs**: Total events, unique clients, data ingested, active connections
  - **Client List**: All devices with status, events, data volume
  - **Map Visualization**: Click any client to see movement track with blobs for each GPS point
  - **Track Markers**: Green "S" for start position, Red "E" for current position

### Configuration (`config/`)
- Centralized settings using Python dataclasses
- Environment variable based configuration via `.env` and `generated_config.env`
- Works with both local development and Databricks Apps

### Infrastructure Scripts (`infra/`)
- `setup_infra.sh` - Master infrastructure setup script
- `create_warehouse.py` - Creates SQL Warehouse
- `create_apps.py` - Creates Databricks Apps
- `create_delta_tables.py` - Creates Unity Catalog tables
- `create_lakebase.py` - Creates Lakebase PostgreSQL instance
- `create_synced_table.py` - Creates Delta → Lakebase sync
- `create_zerobus_credentials.py` - Creates ZeroBus service principal
- `grant_permissions.py` - Grants permissions to app service principals
- `generate_app_yaml.py` - Generates app.yaml from configuration

### Deployment Scripts (`deployment/`)
- `deploy_all.sh` - Master deployment orchestrator
- `00_setup_environment.sh` - Validates environment and dependencies
- `03_deploy_apps.sh` - Deploys app code to Databricks Apps
- `install_dependencies.sh` - Installs all Python packages
- `verify_setup.py` - Post-deployment verification

## Quick Start

### 1. Clone and Setup

```bash
cd ZeroStream
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure Environment

Create `.env` file with base configuration:

```bash
# Infrastructure prefix (used for naming resources)
INFRA_PREFIX=myprefix

# Databricks
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com/
DATABRICKS_TOKEN=your-personal-access-token

# Unity Catalog
CATALOG=your_catalog
SCHEMA=your_schema
TABLE_NAME=sensor_stream

# ZeroBus
ZEROBUS_SERVER_ENDPOINT=your-workspace-id.zerobus.region.cloud.databricks.com

# Lakebase (optional)
LAKEBASE_INSTANCE=${INFRA_PREFIX}-lakebase
LAKEBASE_DATABASES=zerobusstream
LAKEBASE_SCHEMA=sensordata
LAKEBASE_CAPACITY=CU_2

# SQL Warehouse
WAREHOUSE_NAME=${INFRA_PREFIX}_warehouse
WAREHOUSE_CLUSTER_SIZE=Small

# Databricks Apps
MOBILE_APP_NAME=${INFRA_PREFIX}-mobile-app
DASHBOARD_APP_NAME=${INFRA_PREFIX}-dashboard-app

# ZeroBus Service Principal
ZEROBUS_SP_NAME=${INFRA_PREFIX}-zerobus-service
ZEROBUS_TOPIC=${INFRA_PREFIX}_sensor_stream_topic

# Streaming settings
STREAM_INTERVAL_MS=5000
ACTIVE_WINDOW_SECONDS=5
```

### 3. Setup Infrastructure (One-time)

```bash
bash infra/setup_infra.sh
```

This creates all Databricks resources:
1. SQL Warehouse
2. Databricks Apps (mobile + dashboard)
3. Unity Catalog tables (catalog, schema, Delta table)
4. Lakebase PostgreSQL instance
5. Delta → Lakebase synced table
6. ZeroBus service principal with OAuth credentials
7. Permissions for app service principals
8. Auto-generated `app.yaml` files with correct configuration

All generated values are saved to `generated_config.env`.

### 4. Deploy Applications

```bash
bash deployment/deploy_all.sh
```

This deploys the application code:
1. Validates environment and dependencies
2. Installs Python packages
3. Uploads code and deploys both Databricks Apps
4. Runs verification checks

## Deployed Apps

| App | Description | URL Pattern |
|-----|-------------|-------------|
| Mobile Simulator | Generate and stream sensor data | `https://{prefix}-mobile-app-*.databricksapps.com` |
| Backend Dashboard | View clients, KPIs, and movement tracks | `https://{prefix}-dashboard-app-*.databricksapps.com` |

## Demo Walkthrough

1. **Open Mobile Simulator** → Use slider to set 5-20 devices
2. **Click "Apply"** → Creates simulated mobile devices
3. **Click "Start Streaming"** → Devices begin sending GPS data via ZeroBus
4. **Open Backend Dashboard** → Watch KPIs update in real-time
5. **Click any client** in the list → Map zooms to show movement track
6. **See the trail** → Each GPS point shown as a blob (blue→red gradient)

## Project Structure

```
ZeroStream/
├── .env                    # Base configuration (not in git)
├── generated_config.env    # Generated config from infra setup (not in git)
├── .gitignore
├── README.md
├── requirements.txt        # Root dependencies
│
├── infra/                  # Infrastructure setup (run once)
│   ├── setup_infra.sh      # Master infrastructure script
│   ├── create_warehouse.py
│   ├── create_apps.py
│   ├── create_delta_tables.py
│   ├── create_lakebase.py
│   ├── create_synced_table.py
│   ├── create_zerobus_credentials.py
│   ├── grant_permissions.py
│   ├── generate_app_yaml.py
│   ├── 01_create_delta_tables.sql   # Reference SQL
│   └── 02_create_synced_table.sql   # Reference SQL
│
├── deployment/             # App deployment (run to update apps)
│   ├── deploy_all.sh       # Master deployment script
│   ├── 00_setup_environment.sh
│   ├── 03_deploy_apps.sh
│   ├── install_dependencies.sh
│   └── verify_setup.py
│
├── dashboard_app/          # Backend Dashboard
│   ├── app.py              # FastAPI application
│   ├── app.yaml            # Databricks App config (auto-generated)
│   ├── delta_client.py     # Delta/SQL queries
│   ├── requirements.txt
│   ├── static/
│   │   ├── css/dashboard.css
│   │   └── js/dashboard.js
│   └── templates/
│       ├── dashboard.html
│       └── zerobus_view.html
│
├── mobile_app/             # Mobile Simulator
│   ├── app.py              # FastAPI application
│   ├── app.yaml            # Databricks App config (auto-generated)
│   ├── data_generator.py   # Sensor data generation
│   ├── zerobus_client.py   # ZeroBus gRPC/REST client
│   ├── requirements.txt
│   ├── static/
│   │   ├── css/styles.css
│   │   └── js/
│   │       ├── app.js
│   │       ├── connections.js
│   │       ├── map.js
│   │       └── sensors.js
│   └── templates/
│       └── index.html
│
└── config/                 # Shared configuration
    ├── __init__.py
    └── settings.py         # Dataclass-based settings
```

## Key Technologies

- **Databricks ZeroBus** - Low-latency gRPC streaming
- **Delta Lake** - ACID transactions, CDC, time travel
- **Databricks SQL Warehouse** - Serverless SQL queries
- **Databricks Apps** - Managed application hosting
- **Lakebase** - Managed PostgreSQL with Delta sync
- **FastAPI** - Modern Python web framework
- **Leaflet.js** - Interactive map visualization
- **WebSockets** - Real-time UI updates

## Useful Commands

```bash
# View app logs
databricks apps logs ${MOBILE_APP}
databricks apps logs ${DASHBOARD_APP}

# Check Delta table row count
databricks sql execute \
  --warehouse-id ${DATABRICKS_WAREHOUSE_ID} \
  --statement "SELECT COUNT(*) FROM ${CATALOG}.${SCHEMA}.${TABLE_NAME}"

# Re-run verification
python deployment/verify_setup.py

# Redeploy apps only (no infra changes)
bash deployment/deploy_all.sh

# Re-run infrastructure setup
bash infra/setup_infra.sh
```

## Troubleshooting

**Apps not showing data?**
- Check ZeroBus connection in mobile app logs
- Verify Delta table has rows: `SELECT COUNT(*) FROM your_table`
- Ensure SQL Warehouse is running

**Map not showing track?**
- Hard refresh browser (`Cmd+Shift+R` / `Ctrl+Shift+R`)
- Check browser console for errors
- Verify client has GPS data in Delta table

**Deployment fails?**
- Run `python deployment/verify_setup.py` to diagnose
- Check `.env` file has all required variables
- Ensure `generated_config.env` exists (run `bash infra/setup_infra.sh` first)
- Ensure Databricks CLI is authenticated: `databricks auth login`

**App doesn't exist error?**
- Apps are created by `infra/setup_infra.sh`, not `deployment/deploy_all.sh`
- Run infrastructure setup first: `bash infra/setup_infra.sh`

**Lakebase check fails in verify_setup.py?**
- This is expected if using OAuth M2M verification. The Lakebase permissions API is not publicly available, so service principals must be granted access manually.
- **To fix:** Go to Databricks UI → Compute → Lakebase → Your instance → Permissions → Add the service principals with **CAN_USE**
- **Note:** Deployed Databricks Apps will work anyway because they access Lakebase via app resources configured in `app.yaml`. This check only affects the local verification script.

## License

Internal Databricks demo project.
