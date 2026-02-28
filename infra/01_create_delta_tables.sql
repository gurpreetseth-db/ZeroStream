-- =============================================================================
-- ZeroStream - Delta Table Setup
-- Safe / idempotent:
--   CREATE CATALOG IF NOT EXISTS
--   CREATE SCHEMA  IF NOT EXISTS
--   DROP TABLE IF EXISTS + CREATE TABLE
-- Variables: ${catalog}, ${schema}, ${table_name}
-- Optional: ${catalog_storage_location}, ${schema_storage_location}
-- =============================================================================

-- ── 1. Catalog ────────────────────────────────────────────────────────────────
CREATE CATALOG IF NOT EXISTS ${catalog}
    COMMENT 'ZeroStream unified catalog';

-- ── 2. Schema ─────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS ${catalog}.${schema}
    COMMENT 'ZeroBus sensor streaming schema - managed by ZeroStream';

-- ── 3. Drop existing table ────────────────────────────────────────────────────
DROP TABLE IF EXISTS ${catalog}.${schema}.${table_name};

-- ── 4. Create sensor stream table ─────────────────────────────────────────────
CREATE TABLE ${catalog}.${schema}.${table_name} (

    -- Identity
    event_id          STRING        NOT NULL  COMMENT 'UUID v4 generated per event',
    connection_id     STRING        NOT NULL  COMMENT 'Simulated device connection ID',
    device_name       STRING                  COMMENT 'Human-readable device label',

    -- Timestamps
    event_timestamp   TIMESTAMP     NOT NULL  COMMENT 'Event generation time (device clock)',
    event_date        DATE          NOT NULL  COMMENT 'Event generation date (device clock)',
    ingested_at       TIMESTAMP               COMMENT 'Landing time in Delta Lake',

    -- Location
    latitude          DOUBLE                  COMMENT 'GPS latitude  decimal degrees (-90 to 90)',
    longitude         DOUBLE                  COMMENT 'GPS longitude decimal degrees (-180 to 180)',
    altitude_m        DOUBLE                  COMMENT 'Altitude in metres above sea level',

    -- Orientation
    heading_deg       DOUBLE                  COMMENT 'Compass heading 0-360 degrees',
    pitch_deg         DOUBLE                  COMMENT 'Pitch angle -90 to +90 degrees',
    roll_deg          DOUBLE                  COMMENT 'Roll angle -180 to +180 degrees',

    -- Acceleration (m/s²)
    accel_x           DOUBLE                  COMMENT 'Linear acceleration X axis m/s²',
    accel_y           DOUBLE                  COMMENT 'Linear acceleration Y axis m/s²',
    accel_z           DOUBLE                  COMMENT 'Linear acceleration Z axis m/s²',
    accel_magnitude   DOUBLE                  COMMENT 'Total acceleration magnitude m/s²',

    -- Gyroscope (°/s)
    gyro_x            DOUBLE                  COMMENT 'Angular velocity X axis degrees/s',
    gyro_y            DOUBLE                  COMMENT 'Angular velocity Y axis degrees/s',
    gyro_z            DOUBLE                  COMMENT 'Angular velocity Z axis degrees/s',

    -- Derived
    speed_kmh         DOUBLE                  COMMENT 'Estimated speed km/h',
    battery_pct       INT                     COMMENT 'Simulated battery percentage 0-100',
    signal_strength   INT                     COMMENT 'Simulated RSSI signal strength dBm',

    -- ZeroBus metadata
    zerobus_topic     STRING                  COMMENT 'ZeroBus topic name',
    zerobus_offset    BIGINT                  COMMENT 'ZeroBus message offset',
    payload_bytes     INT                     COMMENT 'Raw payload size in bytes'
)
USING DELTA
PARTITIONED BY (event_date)
TBLPROPERTIES (
    'delta.enableChangeDataFeed'                = 'true',
    'delta.autoOptimize.optimizeWrite'          = 'true',
    'delta.autoOptimize.autoCompact'            = 'true',
    'delta.columnMapping.mode'                  = 'name',
    'delta.minReaderVersion'                    = '2',
    'delta.minWriterVersion'                    = '5',
    'pipelines.autoOptimize.zOrderCols'         = 'connection_id,event_timestamp',
    'delta.targetFileSize'                      = '134217728',
    'delta.checkpointInterval'                  = '10'
)
COMMENT 'ZeroStream sensor events - source of truth ingested via ZeroBus';

-- ── 5. Drop checkConstraints feature ──────────────────────────────────────────
ALTER TABLE ${catalog}.${schema}.${table_name} DROP FEATURE checkConstraints;

-- ── 6. Verify table ───────────────────────────────────────────────────────────
DESCRIBE TABLE EXTENDED ${catalog}.${schema}.${table_name};
