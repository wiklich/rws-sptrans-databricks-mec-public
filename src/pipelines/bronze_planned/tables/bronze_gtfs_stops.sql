-- ======================================================================
-- TABLE: bronze.bronze_gtfs_stops
-- LAYER: Bronze (Raw Ingestion - AS-IS)
-- DOMAIN: Public Transport (SPTrans GTFS)
-- SOURCE: GTFS - stops.txt
-- ======================================================================
--
-- DESCRIPTION:
-- This table stores the raw GTFS stops dataset exactly as ingested from
-- the landing zone. No business rules, validation, cleansing, trimming,
-- or normalization are applied in this layer.
--
-- The Bronze layer exists to preserve source data as-is while adding only
-- technical metadata required for lineage and traceability.
--
-- SOURCE REFERENCE:
-- https://gtfs.org/documentation/schedule/reference/#stopstxt
--
-- ======================================================================

CREATE OR REFRESH STREAMING TABLE bronze.bronze_gtfs_stops
TBLPROPERTIES (
  "quality" = "bronze",
  "layer" = "ingestion",
  "source" = "gtfs",
  "source-owner" = "sptrans",
  "entity" = "stops",
  "documentation_url" = "https://gtfs.org/documentation/schedule/reference/#stopstxt"
)
AS
SELECT
  src.stop_id AS stop_id,
  src.stop_name AS stop_name,
  src.stop_desc AS stop_desc,
  src.stop_lat AS stop_lat,
  src.stop_lon AS stop_lon,

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
      coalesce(CAST(src.stop_id AS STRING), ''),
      coalesce(CAST(src.stop_name AS STRING), ''),
      coalesce(CAST(src.stop_desc AS STRING), ''),
      coalesce(CAST(src.stop_lat AS STRING), ''),
      coalesce(CAST(src.stop_lon AS STRING), '')
    ),
    256
  ) AS record_hash
FROM STREAM read_files(
  'dbfs:/Volumes/main/landing/raw_sptrans/gtfs/stops/',
  format => 'csv',
  header => true,
  sep => ',',
  inferColumnTypes => false
) src;