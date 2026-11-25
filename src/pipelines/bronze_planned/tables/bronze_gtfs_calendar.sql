-- ======================================================================
-- TABLE: bronze.bronze_gtfs_calendar
-- LAYER: Bronze (Raw Ingestion - AS-IS)
-- DOMAIN: Public Transport (SPTrans GTFS)
-- SOURCE: GTFS - calendar.txt
-- ======================================================================
--
-- DESCRIPTION:
-- This table stores the raw GTFS calendar dataset exactly as ingested from
-- the landing zone. No business rules, validation, cleansing, trimming,
-- or normalization are applied in this layer.
--
-- The Bronze layer exists to preserve source data as-is while adding only
-- technical metadata required for lineage and traceability.
--
-- SOURCE REFERENCE:
-- https://gtfs.org/documentation/schedule/reference/#calendartxt
--
-- ======================================================================

CREATE OR REFRESH STREAMING TABLE bronze.bronze_gtfs_calendar
TBLPROPERTIES (
  "quality" = "bronze",
  "layer" = "ingestion",
  "source" = "gtfs",
  "source-owner" = "sptrans",
  "entity" = "calendar",
  "documentation_url" = "https://gtfs.org/documentation/schedule/reference/#calendartxt"
)
AS
SELECT
  src.service_id AS service_id,
  src.monday AS monday,
  src.tuesday AS tuesday,
  src.wednesday AS wednesday,
  src.thursday AS thursday,
  src.friday AS friday,
  src.saturday AS saturday,
  src.sunday AS sunday,
  src.start_date AS start_date,
  src.end_date AS end_date,

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
      coalesce(CAST(src.service_id AS STRING), ''),
      coalesce(CAST(src.monday AS STRING), ''),
      coalesce(CAST(src.tuesday AS STRING), ''),
      coalesce(CAST(src.wednesday AS STRING), ''),
      coalesce(CAST(src.thursday AS STRING), ''),
      coalesce(CAST(src.friday AS STRING), ''),
      coalesce(CAST(src.saturday AS STRING), ''),
      coalesce(CAST(src.sunday AS STRING), ''),
      coalesce(CAST(src.start_date AS STRING), ''),
      coalesce(CAST(src.end_date AS STRING), '')
    ),
    256
  ) AS record_hash
FROM STREAM read_files(
  'dbfs:/Volumes/main/landing/raw_sptrans/gtfs/calendar/',
  format => 'csv',
  header => true,
  sep => ',',
  inferColumnTypes => false
) src;