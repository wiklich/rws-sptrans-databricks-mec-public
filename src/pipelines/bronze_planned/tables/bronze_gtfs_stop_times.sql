-- ======================================================================
-- TABLE: bronze.bronze_gtfs_stop_times
-- LAYER: Bronze (Raw Ingestion - AS-IS)
-- DOMAIN: Public Transport (SPTrans GTFS)
-- SOURCE: GTFS - stop_times.txt
-- ======================================================================
--
-- DESCRIPTION:
-- This table stores the raw GTFS stop_times dataset exactly as ingested from
-- the landing zone. No business rules, validation, cleansing, trimming,
-- or normalization are applied in this layer.
--
-- The Bronze layer exists to preserve source data as-is while adding only
-- technical metadata required for lineage and traceability.
--
-- SOURCE REFERENCE:
-- https://gtfs.org/documentation/schedule/reference/#stop_timestxt
--
-- ======================================================================

CREATE OR REFRESH STREAMING TABLE bronze.bronze_gtfs_stop_times
TBLPROPERTIES (
  "quality" = "bronze",
  "layer" = "ingestion",
  "source" = "gtfs",
  "source-owner" = "sptrans",
  "entity" = "stop_times",
  "documentation_url" = "https://gtfs.org/documentation/schedule/reference/#stop_timestxt"
)
AS
SELECT
  src.trip_id AS trip_id,
  src.arrival_time AS arrival_time,
  src.departure_time AS departure_time,
  src.stop_id AS stop_id,
  src.stop_sequence AS stop_sequence,
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
      coalesce(CAST(src.trip_id AS STRING), ''),
      coalesce(CAST(src.arrival_time AS STRING), ''),
      coalesce(CAST(src.departure_time AS STRING), ''),
      coalesce(CAST(src.stop_id AS STRING), ''),
      coalesce(CAST(src.stop_sequence AS STRING), '')
    ),
    256
  ) AS record_hash
FROM STREAM read_files(
  'dbfs:/Volumes/main/landing/raw_sptrans/gtfs/stop_times/',
  format => 'csv',
  header => true,
  sep => ',',
  inferColumnTypes => false
) src;