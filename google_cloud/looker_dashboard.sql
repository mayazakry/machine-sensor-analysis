-- =============================================================================
-- Looker / BigQuery Studio Dashboard Queries
-- Machine Sensor Anomaly Detection – Real-Time Visualization
--
-- Replace the placeholders before running:
--   YOUR_PROJECT  →  your GCP project ID
--   sensor_dataset  →  your BigQuery dataset name
--   raw_sensor_data →  your BigQuery table name
--
-- All queries are designed for BigQuery Standard SQL.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. Real-Time Sensor Readings (last 24 h) – main time-series chart
--    Suitable for a line/scatter chart in Looker Studio.
-- -----------------------------------------------------------------------------
SELECT
    TIMESTAMP(timestamp)                                    AS event_time,
    sensor_id,
    temperature,
    vibration,
    pressure,
    value,
    CASE WHEN is_anomaly THEN 'ANOMALY' ELSE 'NORMAL' END  AS status,
    anomaly_reason
FROM
    `YOUR_PROJECT.sensor_dataset.raw_sensor_data`
WHERE
    TIMESTAMP(timestamp) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
ORDER BY
    event_time DESC;


-- -----------------------------------------------------------------------------
-- 2. Anomaly Count by Hour (last 7 days) – bar/column chart
--    Shows how many anomalies were detected in each hour.
-- -----------------------------------------------------------------------------
SELECT
    TIMESTAMP_TRUNC(TIMESTAMP(timestamp), HOUR)            AS hour,
    sensor_id,
    COUNTIF(is_anomaly)                                    AS anomaly_count,
    COUNT(*)                                               AS total_readings,
    SAFE_DIVIDE(COUNTIF(is_anomaly), COUNT(*))             AS anomaly_rate
FROM
    `YOUR_PROJECT.sensor_dataset.raw_sensor_data`
WHERE
    TIMESTAMP(timestamp) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
GROUP BY
    1, 2
ORDER BY
    1 DESC, 3 DESC;


-- -----------------------------------------------------------------------------
-- 3. Latest Status per Sensor – scorecard / table
--    One row per sensor showing the most recent reading and its status.
-- -----------------------------------------------------------------------------
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY sensor_id
            ORDER BY TIMESTAMP(timestamp) DESC
        ) AS rn
    FROM
        `YOUR_PROJECT.sensor_dataset.raw_sensor_data`
    WHERE
        TIMESTAMP(timestamp) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
)
SELECT
    sensor_id,
    TIMESTAMP(timestamp)                                   AS last_seen,
    temperature,
    vibration,
    pressure,
    value,
    is_anomaly,
    anomaly_reason,
    CASE
        WHEN is_anomaly AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP(timestamp), MINUTE) <= 5
            THEN 'CRITICAL'
        WHEN is_anomaly
            THEN 'WARNING'
        ELSE 'OK'
    END                                                    AS alert_level
FROM
    ranked
WHERE
    rn = 1
ORDER BY
    is_anomaly DESC, last_seen DESC;


-- -----------------------------------------------------------------------------
-- 4. Rolling 5-Minute Anomaly Rate per Sensor – gauge chart
--    Used to set alert thresholds and assess sensor health.
-- -----------------------------------------------------------------------------
SELECT
    sensor_id,
    COUNTIF(is_anomaly)                                    AS anomaly_count,
    COUNT(*)                                               AS total_count,
    ROUND(SAFE_DIVIDE(COUNTIF(is_anomaly), COUNT(*)), 4)   AS anomaly_rate_5m
FROM
    `YOUR_PROJECT.sensor_dataset.raw_sensor_data`
WHERE
    TIMESTAMP(timestamp) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)
GROUP BY
    sensor_id
ORDER BY
    anomaly_rate_5m DESC;


-- -----------------------------------------------------------------------------
-- 5. Temperature / Vibration / Pressure Distribution – histogram data
--    Provides bucket counts for histograms on each sensor dimension.
-- -----------------------------------------------------------------------------
SELECT
    ROUND(temperature, 0)                                  AS temp_bucket,
    ROUND(vibration, 2)                                    AS vib_bucket,
    ROUND(pressure, 0)                                     AS pres_bucket,
    COUNT(*)                                               AS reading_count,
    COUNTIF(is_anomaly)                                    AS anomaly_count
FROM
    `YOUR_PROJECT.sensor_dataset.raw_sensor_data`
WHERE
    TIMESTAMP(timestamp) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
GROUP BY
    1, 2, 3
ORDER BY
    reading_count DESC
LIMIT 500;


-- -----------------------------------------------------------------------------
-- 6. Anomaly Timeline – Gantt-style table
--    Lists every anomaly event with duration for root-cause analysis.
-- -----------------------------------------------------------------------------
WITH anomaly_events AS (
    SELECT
        sensor_id,
        TIMESTAMP(timestamp)                               AS event_time,
        anomaly_reason,
        temperature,
        vibration,
        pressure,
        value,
        LAG(TIMESTAMP(timestamp)) OVER (
            PARTITION BY sensor_id
            ORDER BY TIMESTAMP(timestamp)
        )                                                  AS prev_event_time
    FROM
        `YOUR_PROJECT.sensor_dataset.raw_sensor_data`
    WHERE
        is_anomaly = TRUE
        AND TIMESTAMP(timestamp) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
)
SELECT
    sensor_id,
    event_time,
    anomaly_reason,
    temperature,
    vibration,
    pressure,
    value,
    TIMESTAMP_DIFF(event_time, prev_event_time, MINUTE)    AS minutes_since_last_anomaly
FROM
    anomaly_events
ORDER BY
    event_time DESC;


-- -----------------------------------------------------------------------------
-- 7. Daily Summary Statistics – KPI overview table
--    Aggregates per-sensor statistics for a management dashboard.
-- -----------------------------------------------------------------------------
SELECT
    DATE(TIMESTAMP(timestamp))                             AS report_date,
    sensor_id,
    COUNT(*)                                               AS total_readings,
    COUNTIF(is_anomaly)                                    AS total_anomalies,
    ROUND(SAFE_DIVIDE(COUNTIF(is_anomaly), COUNT(*)) * 100, 2)  AS anomaly_pct,
    ROUND(AVG(temperature), 2)                             AS avg_temp,
    ROUND(MAX(temperature), 2)                             AS max_temp,
    ROUND(MIN(temperature), 2)                             AS min_temp,
    ROUND(AVG(vibration), 4)                               AS avg_vibration,
    ROUND(MAX(vibration), 4)                               AS max_vibration,
    ROUND(AVG(pressure), 2)                                AS avg_pressure
FROM
    `YOUR_PROJECT.sensor_dataset.raw_sensor_data`
WHERE
    TIMESTAMP(timestamp) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
GROUP BY
    1, 2
ORDER BY
    1 DESC, total_anomalies DESC;
