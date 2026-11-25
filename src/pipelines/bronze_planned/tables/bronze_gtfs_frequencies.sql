-- ======================================================================
-- TABLE: bronze.bronze_gtfs_frequencies
-- LAYER: Bronze (Raw Ingestion - AS-IS)
-- DOMAIN: Public Transport (SPTrans GTFS)
-- SOURCE: GTFS - frequencies.txt
-- ======================================================================
--
-- DESCRIPTION:
-- This table stores the raw GTFS frequencies dataset exactly as ingested from
-- the landing zone. No business rules, validation, cleansing, trimming,
-- or normalization are applied in this layer.
--
-- Only a subset of columns is selected from the source file, along with
-- technical metadata required for lineage and traceability.
--
-- SOURCE REFERENCE:
-- https://gtfs.org/documentation/schedule/reference/#frequenciestxt
--
-- ======================================================================

CREATE OR REFRESH STREAMING TABLE bronze.bronze_gtfs_frequencies
TBLPROPERTIES (
  "quality" = "bronze",
  "layer" = "ingestion",
  "source" = "gtfs",
  "source-owner" = "sptrans",
  "entity" = "frequencies",
  "documentation_url" = "https://gtfs.org/documentation/schedule/reference/#frequenciestxt"
)
AS
SELECT
  src.trip_id AS trip_id,
  src.start_time AS start_time,
  src.end_time AS end_time,
  src.headway_secs AS headway_secs,

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
      coalesce(CAST(src.start_time AS STRING), ''),
      coalesce(CAST(src.end_time AS STRING), ''),
      coalesce(CAST(src.headway_secs AS STRING), '')
    ),
    256
  ) AS record_hash
FROM STREAM read_files(
  'dbfs:/Volumes/main/landing/raw_sptrans/gtfs/frequencies/',
  format => 'csv',
  header => true,
  sep => ',',
  inferColumnTypes => false
) src;