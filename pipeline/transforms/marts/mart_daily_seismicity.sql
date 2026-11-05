-- One row per event date for the cleaned earthquake set.

SELECT
    event_date,
    COUNT(*) AS event_count,
    CAST(AVG(magnitude) AS DECIMAL(10, 2)) AS avg_magnitude,
    MAX(magnitude) AS max_magnitude,
    CAST(AVG(depth_km) AS DECIMAL(10, 2)) AS avg_depth_km,
    SUM(CASE WHEN magnitude >= 5 THEN 1 ELSE 0 END) AS significant_event_count
FROM staging.stg_earthquakes
GROUP BY event_date
