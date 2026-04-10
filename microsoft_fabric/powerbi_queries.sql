-- =============================================================================
-- Power BI Dashboard Queries
-- Machine Sensor Anomaly Detection System
-- Microsoft Fabric / Synapse Analytics SQL
-- =============================================================================
-- These queries power real-time Power BI dashboards connected to the
-- Fabric Lakehouse. They include both SQL (for Direct Lake / Import mode)
-- and DAX measures (wrapped in comments for Power BI Desktop).
-- =============================================================================


-- =============================================================================
-- SECTION 1 : Base Views
-- =============================================================================

-- View: Latest processed sensor events (last 24 hours)
CREATE OR REPLACE VIEW vw_sensor_events_24h AS
SELECT
    event_id,
    machine_id,
    CAST(timestamp AS DATETIME2) AS event_time,
    temperature,
    vibration,
    pressure,
    humidity,
    current,
    voltage,
    temperature_zscore,
    vibration_zscore,
    pressure_zscore,
    CAST(event_date AS DATE)     AS event_date,
    hour_of_day,
    day_of_week
FROM processed_sensor_events
WHERE event_date >= CAST(DATEADD(HOUR, -24, GETUTCDATE()) AS DATE);

-- View: Anomaly predictions joined with sensor data
CREATE OR REPLACE VIEW vw_anomaly_predictions AS
SELECT
    p.machine_id,
    p.timestamp                          AS prediction_time,
    p.is_anomaly,
    p.anomaly_score,
    p.confidence,
    e.temperature,
    e.vibration,
    e.pressure,
    e.humidity,
    e.current,
    e.voltage,
    CAST(p.timestamp AS DATE)            AS prediction_date,
    DATEPART(HOUR, p.timestamp)          AS prediction_hour
FROM anomaly_predictions       p
LEFT JOIN processed_sensor_events e
    ON  p.machine_id = e.machine_id
    AND CAST(p.timestamp AS DATETIME2)
        BETWEEN DATEADD(SECOND, -5, CAST(e.timestamp AS DATETIME2))
            AND DATEADD(SECOND,  5, CAST(e.timestamp AS DATETIME2));


-- =============================================================================
-- SECTION 2 : Anomaly Trend Visualization
-- =============================================================================

-- Query: Anomaly count by hour (last 7 days) – Line chart data
SELECT
    DATETRUNC(HOUR, CAST(timestamp AS DATETIME2)) AS hour_bucket,
    machine_id,
    COUNT(*)                                       AS total_events,
    SUM(CASE WHEN is_anomaly = 1 THEN 1 ELSE 0 END) AS anomaly_count,
    ROUND(
        100.0 * SUM(CASE WHEN is_anomaly = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
        2
    )                                              AS anomaly_rate_pct
FROM anomaly_predictions
WHERE timestamp >= DATEADD(DAY, -7, GETUTCDATE())
GROUP BY
    DATETRUNC(HOUR, CAST(timestamp AS DATETIME2)),
    machine_id
ORDER BY hour_bucket DESC, machine_id;


-- Query: Rolling 1-hour anomaly rate – KPI card / gauge
SELECT TOP 1
    COUNT(*)                                                          AS events_last_hour,
    SUM(CASE WHEN is_anomaly = 1 THEN 1 ELSE 0 END)                   AS anomalies_last_hour,
    ROUND(
        100.0 * SUM(CASE WHEN is_anomaly = 1 THEN 1 ELSE 0 END)
              / NULLIF(COUNT(*), 0),
        2
    )                                                                  AS anomaly_rate_pct,
    GETUTCDATE()                                                       AS as_of_utc
FROM anomaly_predictions
WHERE timestamp >= DATEADD(HOUR, -1, GETUTCDATE());


-- Query: Daily anomaly trend (last 30 days) – Area chart
SELECT
    CAST(timestamp AS DATE)                                            AS event_date,
    COUNT(*)                                                           AS total_events,
    SUM(CASE WHEN is_anomaly = 1 THEN 1 ELSE 0 END)                    AS anomaly_count,
    ROUND(
        100.0 * SUM(CASE WHEN is_anomaly = 1 THEN 1 ELSE 0 END)
              / NULLIF(COUNT(*), 0),
        2
    )                                                                   AS anomaly_rate_pct
FROM anomaly_predictions
WHERE timestamp >= DATEADD(DAY, -30, GETUTCDATE())
GROUP BY CAST(timestamp AS DATE)
ORDER BY event_date;


-- =============================================================================
-- SECTION 3 : Distribution Charts
-- =============================================================================

-- Query: Temperature distribution – Histogram bins
SELECT
    FLOOR(temperature / 5.0) * 5          AS temp_bin_start,
    FLOOR(temperature / 5.0) * 5 + 5      AS temp_bin_end,
    COUNT(*)                               AS reading_count,
    SUM(CASE WHEN is_anomaly = 1 THEN 1 ELSE 0 END) AS anomaly_in_bin
FROM vw_anomaly_predictions
WHERE prediction_date >= DATEADD(DAY, -7, CAST(GETUTCDATE() AS DATE))
GROUP BY FLOOR(temperature / 5.0) * 5
ORDER BY temp_bin_start;


-- Query: Vibration vs Temperature scatter (last 1000 events) – Scatter chart
SELECT TOP 1000
    machine_id,
    temperature,
    vibration,
    pressure,
    is_anomaly,
    anomaly_score,
    prediction_time
FROM vw_anomaly_predictions
ORDER BY prediction_time DESC;


-- Query: Sensor value statistics per machine – Table visual
SELECT
    machine_id,
    COUNT(*)                          AS total_readings,
    ROUND(AVG(temperature),  2)       AS avg_temperature,
    ROUND(STDEV(temperature), 2)      AS std_temperature,
    ROUND(MIN(temperature),  2)       AS min_temperature,
    ROUND(MAX(temperature),  2)       AS max_temperature,
    ROUND(AVG(vibration),   3)        AS avg_vibration,
    ROUND(MAX(vibration),   3)        AS max_vibration,
    ROUND(AVG(pressure),    2)        AS avg_pressure,
    ROUND(AVG(current),     2)        AS avg_current,
    SUM(CASE WHEN is_anomaly = 1 THEN 1 ELSE 0 END) AS total_anomalies
FROM vw_anomaly_predictions
WHERE prediction_date >= DATEADD(DAY, -7, CAST(GETUTCDATE() AS DATE))
GROUP BY machine_id
ORDER BY total_anomalies DESC;


-- =============================================================================
-- SECTION 4 : Real-time Alerts Table
-- =============================================================================

-- Query: Most recent anomalies (last 100) – Alert feed table
SELECT TOP 100
    p.machine_id,
    p.prediction_time,
    p.anomaly_score,
    p.confidence,
    p.temperature,
    p.vibration,
    p.pressure,
    p.humidity,
    p.current,
    p.voltage,
    CASE
        WHEN p.anomaly_score >= 0.8 THEN 'CRITICAL'
        WHEN p.anomaly_score >= 0.5 THEN 'WARNING'
        ELSE 'LOW'
    END                               AS alert_severity,
    DATEDIFF(MINUTE, p.prediction_time, GETUTCDATE()) AS minutes_ago
FROM vw_anomaly_predictions p
WHERE p.is_anomaly = 1
ORDER BY p.prediction_time DESC;


-- Query: Active alert counts by severity – Donut chart
SELECT
    CASE
        WHEN anomaly_score >= 0.8 THEN 'CRITICAL'
        WHEN anomaly_score >= 0.5 THEN 'WARNING'
        ELSE 'LOW'
    END                               AS severity,
    COUNT(*)                          AS alert_count
FROM vw_anomaly_predictions
WHERE is_anomaly = 1
  AND prediction_date >= CAST(GETUTCDATE() AS DATE)
GROUP BY
    CASE
        WHEN anomaly_score >= 0.8 THEN 'CRITICAL'
        WHEN anomaly_score >= 0.5 THEN 'WARNING'
        ELSE 'LOW'
    END
ORDER BY alert_count DESC;


-- =============================================================================
-- SECTION 5 : Performance Metrics
-- =============================================================================

-- Query: Model performance over time (requires ground-truth labels)
SELECT
    CAST(timestamp AS DATE)            AS eval_date,
    COUNT(*)                           AS total_predictions,
    SUM(CASE WHEN is_anomaly = 1 AND is_simulated_anomaly = 1 THEN 1 ELSE 0 END)
                                       AS true_positives,
    SUM(CASE WHEN is_anomaly = 0 AND is_simulated_anomaly = 0 THEN 1 ELSE 0 END)
                                       AS true_negatives,
    SUM(CASE WHEN is_anomaly = 1 AND is_simulated_anomaly = 0 THEN 1 ELSE 0 END)
                                       AS false_positives,
    SUM(CASE WHEN is_anomaly = 0 AND is_simulated_anomaly = 1 THEN 1 ELSE 0 END)
                                       AS false_negatives,
    ROUND(
        1.0 * SUM(CASE WHEN is_anomaly = 1 AND is_simulated_anomaly = 1 THEN 1 ELSE 0 END)
            / NULLIF(SUM(CASE WHEN is_anomaly = 1 THEN 1 ELSE 0 END), 0),
        4
    )                                  AS precision_score,
    ROUND(
        1.0 * SUM(CASE WHEN is_anomaly = 1 AND is_simulated_anomaly = 1 THEN 1 ELSE 0 END)
            / NULLIF(SUM(CASE WHEN is_simulated_anomaly = 1 THEN 1 ELSE 0 END), 0),
        4
    )                                  AS recall_score
FROM anomaly_predictions ap
LEFT JOIN processed_sensor_events pe
    ON  ap.machine_id = pe.machine_id
    AND CAST(ap.timestamp AS DATETIME2) BETWEEN
        DATEADD(SECOND, -5, CAST(pe.timestamp AS DATETIME2)) AND
        DATEADD(SECOND,  5, CAST(pe.timestamp AS DATETIME2))
WHERE ap.timestamp >= DATEADD(DAY, -30, GETUTCDATE())
GROUP BY CAST(ap.timestamp AS DATE)
ORDER BY eval_date DESC;


-- Query: System throughput – events per minute
SELECT
    DATETRUNC(MINUTE, CAST(timestamp AS DATETIME2)) AS minute_bucket,
    COUNT(*)                                         AS events_per_minute
FROM anomaly_predictions
WHERE timestamp >= DATEADD(HOUR, -1, GETUTCDATE())
GROUP BY DATETRUNC(MINUTE, CAST(timestamp AS DATETIME2))
ORDER BY minute_bucket DESC;


-- Query: Top anomalous machines (last 24 hours) – Bar chart
SELECT TOP 10
    machine_id,
    COUNT(*)                                                           AS total_readings,
    SUM(CASE WHEN is_anomaly = 1 THEN 1 ELSE 0 END)                    AS anomaly_count,
    ROUND(
        100.0 * SUM(CASE WHEN is_anomaly = 1 THEN 1 ELSE 0 END)
              / NULLIF(COUNT(*), 0),
        1
    )                                                                   AS anomaly_rate_pct,
    ROUND(MAX(anomaly_score), 4)                                        AS max_anomaly_score,
    ROUND(AVG(anomaly_score), 4)                                        AS avg_anomaly_score
FROM anomaly_predictions
WHERE timestamp >= DATEADD(HOUR, -24, GETUTCDATE())
GROUP BY machine_id
ORDER BY anomaly_count DESC;


-- =============================================================================
-- SECTION 6 : DAX Measures (for Power BI Desktop)
-- Copy these into the DAX editor in Power BI Desktop as calculated measures.
-- =============================================================================

/*
-- Total Events
Total Events = COUNTROWS(anomaly_predictions)

-- Anomaly Count
Anomaly Count =
    CALCULATE(
        COUNTROWS(anomaly_predictions),
        anomaly_predictions[is_anomaly] = TRUE()
    )

-- Anomaly Rate %
Anomaly Rate % =
    DIVIDE(
        [Anomaly Count],
        [Total Events],
        0
    ) * 100

-- Anomaly Rate (Last Hour)
Anomaly Rate Last Hour % =
    CALCULATE(
        [Anomaly Rate %],
        DATESINPERIOD(
            anomaly_predictions[timestamp],
            NOW(),
            -1,
            HOUR
        )
    )

-- Average Anomaly Score
Avg Anomaly Score =
    CALCULATE(
        AVERAGEX(
            FILTER(anomaly_predictions, anomaly_predictions[is_anomaly] = TRUE()),
            anomaly_predictions[anomaly_score]
        )
    )

-- Highest Risk Machine (last 24h)
Highest Risk Machine =
    CALCULATE(
        TOPN(
            1,
            SUMMARIZE(
                anomaly_predictions,
                anomaly_predictions[machine_id],
                "AnomalyCount", [Anomaly Count]
            ),
            [AnomalyCount],
            DESC
        ),
        DATESINPERIOD(anomaly_predictions[timestamp], NOW(), -24, HOUR)
    )

-- Alert Status
Alert Status =
    IF(
        [Anomaly Rate Last Hour %] >= 10,
        "🔴 CRITICAL",
        IF(
            [Anomaly Rate Last Hour %] >= 5,
            "🟡 WARNING",
            "🟢 NORMAL"
        )
    )

-- Temperature Status
Temperature Status =
    IF(
        MAX(processed_sensor_events[temperature]) > 100,
        "⚠️ HIGH",
        IF(
            MAX(processed_sensor_events[temperature]) > 85,
            "🟡 ELEVATED",
            "✅ NORMAL"
        )
    )
*/
