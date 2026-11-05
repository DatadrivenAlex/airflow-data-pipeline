-- One current row per USGS event id. The latest `updated` revision wins.
-- Non-earthquake types are dropped after that revision is chosen.
-- A row with no event id is kept so the not-null check fails the DAG
-- instead of hiding a bad extract.

WITH typed AS (
    SELECT
        NULLIF(TRIM(event_id), '') AS event_id,
        event_time,
        CAST(event_time AS DATE) AS event_date,
        CAST(NULLIF(TRIM(latitude_text), '') AS DOUBLE PRECISION) AS latitude,
        CAST(NULLIF(TRIM(longitude_text), '') AS DOUBLE PRECISION) AS longitude,
        CAST(NULLIF(TRIM(depth_text), '') AS DOUBLE PRECISION) AS depth_km,
        CAST(NULLIF(TRIM(mag_text), '') AS DOUBLE PRECISION) AS magnitude,
        NULLIF(TRIM(mag_type_text), '') AS magnitude_type,
        NULLIF(TRIM(place_text), '') AS place,
        NULLIF(TRIM(event_type_text), '') AS event_type,
        NULLIF(TRIM(status_text), '') AS review_status,
        NULLIF(TRIM(net_text), '') AS source_network,
        updated_at,
        record_source,
        extracted_at,
        load_id
    FROM raw.earthquakes
),
ranked AS (
    SELECT
        typed.*,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY updated_at DESC NULLS LAST, extracted_at DESC, load_id DESC
        ) AS _row_number
    FROM typed
)
SELECT
    event_id,
    event_time,
    event_date,
    latitude,
    longitude,
    depth_km,
    magnitude,
    magnitude_type,
    place,
    CASE
        WHEN place IS NULL THEN 'Unknown'
        WHEN STRPOS(place, ',') > 0 AND TRIM(SPLIT_PART(place, ',', 2)) <> ''
            THEN TRIM(SPLIT_PART(place, ',', 2))
        ELSE TRIM(place)
    END AS region,
    event_type,
    review_status,
    source_network,
    updated_at,
    CASE
        WHEN magnitude >= 7 THEN 'major'
        WHEN magnitude >= 6 THEN 'strong'
        WHEN magnitude >= 5 THEN 'moderate'
        WHEN magnitude >= 4 THEN 'light'
        WHEN magnitude IS NULL THEN NULL
        ELSE 'minor'
    END AS magnitude_band,
    record_source,
    extracted_at,
    load_id
FROM ranked
WHERE _row_number = 1
  AND (event_type = 'earthquake' OR event_id IS NULL)
