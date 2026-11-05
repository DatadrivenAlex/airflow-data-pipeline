-- Earthquakes at magnitude 5 and above. This is the analyst-facing event list.

SELECT
    event_id,
    event_time,
    event_date,
    magnitude,
    magnitude_band,
    depth_km,
    latitude,
    longitude,
    place,
    region,
    review_status
FROM staging.stg_earthquakes
WHERE magnitude >= 5
