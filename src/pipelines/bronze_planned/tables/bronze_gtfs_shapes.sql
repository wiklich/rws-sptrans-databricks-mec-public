-- ======================================================================
-- TABLE: bronze.bronze_gtfs_shapes
-- LAYER: Bronze (Raw Ingestion - AS-IS)
-- DOMAIN: Public Transport (SPTrans GTFS)
-- SOURCE: GTFS - shapes.txt
-- ======================================================================
--
-- DESCRIPTION:
-- This table stores the raw GTFS shapes dataset exactly as ingested from
-- the landing zone. No business rules, validation, cleansing, trimming,
-- or normalization are applied in this layer.
--
-- Only a subset of columns is selected from the source file, along with
-- technical metadata required for lineage and traceability.
--
-- SOURCE REFERENCE:
-- https://gtfs.org/documentation/schedule/reference/#shapestxt
--
-- ======================================================================

CREATE OR REFRESH STREAMING TABLE bronze.bronze_gtfs_shapes
TBLPROPERTIES (
  "quality" = "bronze",
  "layer" = "ingestion",
  "source" = "gtfs",
  "source-owner" = "sptrans",
  "entity" = "shapes",
  "documentation_url" = "https://gtfs.org/documentation/schedule/reference/#shapestxt"
)
AS
SELECT
  src.shape_id AS shape_id,
  src.shape_pt_lat AS shape_pt_lat,
  src.shape_pt_lon AS shape_pt_lon,
  src.shape_pt_sequence AS shape_pt_sequence,
  src.shape_dist_traveled AS shape_dist_traveled,

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
      coalesce(CAST(src.shape_id AS STRING), ''),
      coalesce(CAST(src.shape_pt_lat AS STRING), ''),
      coalesce(CAST(src.shape_pt_lon AS STRING), ''),
      coalesce(CAST(src.shape_pt_sequence AS STRING), ''),
      coalesce(CAST(src.shape_dist_traveled AS STRING), '')
    ),
    256
  ) AS record_hash

FROM STREAM read_files(
  'dbfs:/Volumes/main/landing/raw_sptrans/gtfs/shapes/',
  format => 'csv',
  header => true,
  sep => ',',
  inferColumnTypes => false
) src;