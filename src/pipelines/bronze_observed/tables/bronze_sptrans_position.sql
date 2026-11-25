CREATE OR REFRESH STREAMING TABLE bronze.bronze_sptrans_positions
TBLPROPERTIES (
  "quality" = "bronze",
  "layer" = "ingestion",
  "source" = "api",
  "source-owner" = "sptrans",
  "endpoint" = "Posicao"
)
AS
SELECT
  src.payload.hr AS reference_hour,

  current_timestamp() AS ingestion_ts,
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS source_file_modification_time,

  src.metadata.ingested_at AS source_ingested_at,
  src.metadata.ingestion_status AS ingestion_status,
  src.metadata.total_elapsed_seconds AS total_elapsed_seconds,
  src.auth.elapsed_seconds AS auth_elapsed_seconds,
  src.auth.http_status AS auth_http_status,
  src.request.elapsed_seconds AS request_elapsed_seconds,
  src.request.http_status AS request_http_status,

  src.metadata AS landing_metadata,
  src.auth AS auth_metadata,
  src.request AS request_metadata,

  linha.c   AS line_sign,
  linha.cl  AS line_id,
  linha.sl  AS direction,
  linha.lt0 AS destination_label,
  linha.lt1 AS origin_label,
  linha.qv  AS vehicles_count,

  veiculo.p  AS vehicle_prefix,
  veiculo.a  AS is_accessible,
  veiculo.ta AS captured_at_utc,
  veiculo.py AS latitude,
  veiculo.px AS longitude
FROM STREAM read_files(
  'dbfs:/Volumes/main/landing/raw_sptrans/posicao_full/',
  format => 'json',
  inferColumnTypes => true
) src
LATERAL VIEW explode(src.payload.l) linhas AS linha
LATERAL VIEW explode(linha.vs) veiculos AS veiculo;