-- ======================================================================
-- TABLE: bronze.bronze_gtfs_routes
-- LAYER: Bronze (Raw Ingestion - AS-IS)
-- DOMAIN: Public Transport (SPTrans GTFS)
-- SOURCE: GTFS - routes.txt
-- ======================================================================
--
-- DESCRIPTION:
-- This table represents the raw ingestion of the GTFS "routes" dataset.
-- Data is loaded in an AS-IS manner directly from the landing zone without
-- any transformation, cleansing, validation, or normalization.
--
-- The purpose of this table is to provide a reliable and immutable copy
-- of the original source data, enriched only with technical metadata
-- required for traceability and lineage.
--
-- No business rules should be applied at this stage.
--
-- ======================================================================
-- DATA SOURCE DETAILS:
--
-- - Source System: SPTrans (São Paulo Public Transport)
-- - Dataset: GTFS (General Transit Feed Specification)
-- - File: routes.txt
-- - Ingestion Mode: Streaming (Auto Loader / read_files)
-- - File Format: CSV
-- - Header: Yes
--
-- Landing Path:
-- dbfs:/Volumes/main/landing/raw_sptrans/gtfs/routes/
--
-- ======================================================================
-- METADATA STRATEGY:
--
-- The following metadata fields are added for observability and lineage:
--
-- - gtfs_load_date:
--     Extracted from file path (e.g., load_date=YYYY-MM-DD)
--
-- - gtfs_loaded_at:
--     Timestamp when the data was processed in Bronze
--
-- - source_file:
--     Full path of the ingested file
--
-- - source_file_name:
--     Name of the ingested file
--
-- - source_zip_name:
--     Name of the original GTFS ZIP file (if present in path)
--
-- - record_hash:
--     SHA-256 hash generated from all relevant columns + file path
--     Used for traceability and deduplication in downstream layers
--
-- ======================================================================
-- IMPORTANT ASSUMPTIONS:
--
-- - Data is ingested exactly as received (AS-IS)
-- - No trimming, casting normalization, null handling, or validation
-- - Schema is inferred automatically (inferColumnTypes = true)
-- - Header row is present in the source file
--
-- ======================================================================
-- DOWNSTREAM USAGE:
--
-- - Silver Layer:
--     Data cleansing, standardization, validation, and deduplication
--
-- - Gold Layer:
--     Business logic, aggregations, KPIs, and analytics
--
-- ======================================================================
-- CHANGE LOG:
--
-- - 2026-03-18: Initial version created
--
-- ======================================================================

CREATE OR REFRESH STREAMING TABLE bronze.bronze_gtfs_routes
TBLPROPERTIES (
  "quality" = "bronze",
  "layer" = "ingestion",
  "source" = "gtfs",
  "source-owner" = "sptrans",
  "entity" = "routes",
  "documentation_url" = "https://gtfs.org/documentation/schedule/reference/#routestxt"
)
AS
SELECT
  src.route_id AS route_id,
  src.agency_id AS agency_id,
  src.route_short_name AS route_short_name,
  src.route_long_name AS route_long_name,
  src.route_type AS route_type,
  src.route_color AS route_color,
  src.route_text_color AS route_text_color,

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
      coalesce(CAST(src.route_id AS STRING), ''),
      coalesce(CAST(src.agency_id AS STRING), ''),
      coalesce(CAST(src.route_short_name AS STRING), ''),
      coalesce(CAST(src.route_long_name AS STRING), ''),
      coalesce(CAST(src.route_type AS STRING), ''),
      coalesce(CAST(src.route_color AS STRING), ''),
      coalesce(CAST(src.route_text_color AS STRING), ''),
      coalesce(CAST(_metadata.file_path AS STRING), '')
    ),
    256
  ) AS record_hash
FROM STREAM read_files(
  'dbfs:/Volumes/main/landing/raw_sptrans/gtfs/routes/',
  format => 'csv',
  header => true,
  sep => ',',
  inferColumnTypes => true
) src;