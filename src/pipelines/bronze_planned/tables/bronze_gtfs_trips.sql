-- ======================================================================
-- TABLE: bronze.bronze_gtfs_trips
-- LAYER: Bronze (Raw Ingestion - AS-IS)
-- DOMAIN: Public Transport (SPTrans GTFS)
-- SOURCE: GTFS - trips.txt
-- ======================================================================
--
-- DESCRIPTION:
-- This table stores the raw GTFS trips dataset exactly as ingested from
-- the landing zone. No business rules, validation, cleansing, trimming,
-- or normalization are applied in this layer.
--
-- The Bronze layer exists to preserve source data as-is while adding only
-- technical metadata required for lineage and traceability.
--
-- SOURCE REFERENCE:
-- https://gtfs.org/documentation/schedule/reference/#tripstxt
--
-- ======================================================================

CREATE OR REFRESH STREAMING TABLE bronze.bronze_gtfs_trips
TBLPROPERTIES (
  "quality" = "bronze",
  "layer" = "ingestion",
  "source" = "gtfs",
  "source-owner" = "sptrans",
  "entity" = "trips",
  "documentation_url" = "https://gtfs.org/documentation/schedule/reference/#tripstxt"
)
AS
SELECT
  src.route_id AS route_id,
  src.service_id AS service_id,
  src.trip_id AS trip_id,
  src.trip_headsign AS trip_headsign,
  src.direction_id AS direction_id,
  src.shape_id AS shape_id,

  CAST(
    to_date(
      regexp_extract(
        _metadata.file_path,
        '([0-9]{4}-[0-9]{2}-[0-9]{2})',
        1
      ),
      'yyyy-MM-dd'
    ) AS DATE
  ) AS gtfs_load_date,

  current_timestamp() AS gtfs_loaded_at,

  sha2(
    concat_ws(
      '||',
      coalesce(CAST(src.route_id AS STRING), ''),
      coalesce(CAST(src.service_id AS STRING), ''),
      coalesce(CAST(src.trip_id AS STRING), ''),
      coalesce(CAST(src.trip_headsign AS STRING), ''),
      coalesce(CAST(src.direction_id AS STRING), ''),
      coalesce(CAST(src.shape_id AS STRING), '')
    ),
    256
  ) AS record_hash
FROM STREAM read_files(
  'dbfs:/Volumes/main/landing/raw_sptrans/gtfs/trips/',
  format => 'csv',
  header => true,
  sep => ',',
  inferColumnTypes => false
) src;