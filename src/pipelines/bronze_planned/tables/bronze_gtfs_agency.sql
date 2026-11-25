-- ======================================================================
-- TABLE: bronze.bronze_gtfs_agency
-- LAYER: Bronze (Raw Ingestion - AS-IS)
-- DOMAIN: Public Transport (SPTrans GTFS)
-- SOURCE: GTFS - agency.txt
-- ======================================================================
--
-- DESCRIPTION:
-- This table stores the raw GTFS agency dataset exactly as ingested from
-- the landing zone. No business rules, validation, cleansing, trimming,
-- or normalization are applied in this layer.
--
-- The Bronze layer exists to preserve source data as-is while adding only
-- technical metadata required for lineage and traceability.
--
-- SOURCE REFERENCE:
-- https://gtfs.org/documentation/schedule/reference/#agencytxt
--
-- ======================================================================

CREATE OR REFRESH STREAMING TABLE bronze.bronze_gtfs_agency
TBLPROPERTIES (
  "quality" = "bronze",
  "layer" = "ingestion",
  "source" = "gtfs",
  "source-owner" = "sptrans",
  "entity" = "agency",
  "documentation_url" = "https://gtfs.org/documentation/schedule/reference/#agencytxt"
)
AS
SELECT
  src.agency_id AS agency_id,
  src.agency_name AS agency_name,
  src.agency_url AS agency_url,
  src.agency_timezone AS agency_timezone,
  src.agency_lang AS agency_lang,

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
  _metadata.file_path AS source_file,
  _metadata.file_name AS source_file_name,

  regexp_extract(
    _metadata.file_path,
    '([^/]+\\.zip)',
    1
  ) AS source_zip_name,

  sha2(
    concat_ws(
      '||',
      coalesce(CAST(src.agency_id AS STRING), ''),
      coalesce(CAST(src.agency_name AS STRING), ''),
      coalesce(CAST(src.agency_url AS STRING), ''),
      coalesce(CAST(src.agency_timezone AS STRING), ''),
      coalesce(CAST(src.agency_lang AS STRING), '')
    ),
    256
  ) AS record_hash
FROM STREAM read_files(
  'dbfs:/Volumes/main/landing/raw_sptrans/gtfs/agency/',
  format => 'csv',
  header => true,
  sep => ',',
  inferColumnTypes => false
) src;