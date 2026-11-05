-- Region is the text after the first comma in the USGS place field.
-- Places with no comma stay whole, for example "South of the Fiji Islands".

SELECT
    region,
    COUNT(*) AS event_count,
    CAST(AVG(magnitude) AS DECIMAL(10, 2)) AS avg_magnitude,
    MAX(magnitude) AS max_magnitude,
    CAST(AVG(depth_km) AS DECIMAL(10, 2)) AS avg_depth_km,
    MIN(event_time) AS first_event_time,
    MAX(event_time) AS last_event_time
FROM staging.stg_earthquakes
GROUP BY region
