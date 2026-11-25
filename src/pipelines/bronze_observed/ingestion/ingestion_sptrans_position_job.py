"""
SPTrans Positions Ingestion Pipeline (Raw Layer - Bronze Landing)

This module is responsible for ingesting real-time bus position data from the
SPTrans "Olho Vivo" API (v2.2) and persisting it in the raw (landing) layer.

Key characteristics:
- No transformation or validation is applied to the payload (AS-IS ingestion).
- The raw response is wrapped in an envelope containing metadata for traceability.
- Authentication is required before fetching data.
- Data is stored as JSON files in a mounted volume (Databricks Volumes).
- Metadata includes timing, payload size, and request details.

Pipeline steps:
1. Authenticate against SPTrans API.
2. Fetch vehicle positions.
3. Build envelope with metadata.
4. Persist raw JSON file.

This pipeline is intended to feed the Bronze layer, where data will later be
converted to structured formats (e.g., Parquet) without altering original content.

Author: Data Engineering Team
Environment: Databricks (PySpark compatible)
"""

import json
import logging
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

logger = logging.getLogger("sptrans_ingestion")

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

logger.setLevel(logging.INFO)

SECRET_SCOPE = "sptrans"
TOKEN_NAME = "token"
TOKEN = dbutils.secrets.get(SECRET_SCOPE, TOKEN_NAME)

BASE = "https://api.olhovivo.sptrans.com.br/v2.2"
AUTH_ENDPOINT = f"{BASE}/Login/Autenticar"
POSITIONS_ENDPOINT = f"{BASE}/Posicao"

RAW_DIR = "/Volumes/main/landing/raw_sptrans/posicao_full"
TZ = ZoneInfo("America/Sao_Paulo")


def now_sp() -> datetime:
    """
    Returns the current datetime in São Paulo timezone.

    Returns:
        datetime: Current timestamp localized to America/Sao_Paulo.
    """
    return datetime.now(TZ)


def to_epoch_ms(dt: datetime) -> int:
    """
    Converts a datetime object to epoch time in milliseconds.

    Args:
        dt (datetime): Datetime object to convert.

    Returns:
        int: Epoch time in milliseconds.
    """
    return int(dt.timestamp() * 1000)


def authenticate(session: requests.Session) -> dict:
    """
    Authenticates against the SPTrans API using a token.

    Args:
        session (requests.Session): HTTP session used for API communication.

    Returns:
        dict: Metadata about the authentication request.

    Raises:
        RuntimeError: If authentication fails.
        HTTPError: If HTTP request fails.
    """
    auth_requested_at = now_sp()
    started_at = time.time()

    logger.info("Authenticating in SPTrans API")

    resp = session.post(
        AUTH_ENDPOINT,
        params={"token": TOKEN},
        timeout=30,
    )
    resp.raise_for_status()

    response_text = resp.text.strip()
    auth_elapsed_seconds = round(time.time() - started_at, 3)

    if response_text.lower() != "true":
        raise RuntimeError(f"Authentication failed: {resp.text}")

    logger.info("Authentication succeeded")

    return {
        "endpoint": AUTH_ENDPOINT,
        "requested_at": auth_requested_at.isoformat(),
        "requested_at_epoch_ms": to_epoch_ms(auth_requested_at),
        "elapsed_seconds": auth_elapsed_seconds,
        "http_status": resp.status_code,
        "response_text": response_text,
    }


def fetch_positions(session: requests.Session) -> tuple[dict, dict]:
    """
    Fetches real-time bus position data from SPTrans API.

    Args:
        session (requests.Session): Authenticated HTTP session.

    Returns:
        tuple:
            - dict: API payload (positions data).
            - dict: Request metadata (timing, size, headers).

    Raises:
        HTTPError: If API request fails.
    """
    request_requested_at = now_sp()
    started_at = time.time()

    logger.info("Fetching positions from SPTrans API")

    resp = session.get(POSITIONS_ENDPOINT, timeout=60)
    resp.raise_for_status()

    data = resp.json()
    request_elapsed_seconds = round(time.time() - started_at, 3)

    payload_str = json.dumps(data, ensure_ascii=False)
    payload_size_chars = len(payload_str)
    payload_size_bytes = len(payload_str.encode("utf-8"))

    logger.info("Positions fetched successfully")

    request_metadata = {
        "endpoint": POSITIONS_ENDPOINT,
        "requested_at": request_requested_at.isoformat(),
        "requested_at_epoch_ms": to_epoch_ms(request_requested_at),
        "elapsed_seconds": request_elapsed_seconds,
        "http_status": resp.status_code,
        "content_type": resp.headers.get("Content-Type", ""),
        "payload_size_chars": payload_size_chars,
        "payload_size_bytes": payload_size_bytes,
    }

    return data, request_metadata


def build_raw_path(reference_dt: datetime) -> tuple[str, str]:
    """
    Builds the raw file path and file name based on a reference timestamp.

    Args:
        reference_dt (datetime): Reference datetime used for file naming.

    Returns:
        tuple:
            - str: Full raw file path.
            - str: File name.
    """
    file_name = f"posicao_{reference_dt:%Y%m%d_%H%M%S}.json"
    raw_path = f"{RAW_DIR}/{file_name}"
    return raw_path, file_name


def build_envelope(
    payload: dict,
    raw_path: str,
    file_name: str,
    auth_metadata: dict,
    request_metadata: dict,
    total_elapsed_seconds: float,
) -> dict:
    """
    Builds the final envelope structure for raw ingestion.

    The envelope includes:
    - Pipeline metadata
    - Authentication metadata
    - Request metadata
    - Raw payload

    Args:
        payload (dict): API response payload.
        raw_path (str): Destination file path.
        file_name (str): File name.
        auth_metadata (dict): Authentication metadata.
        request_metadata (dict): Request metadata.
        total_elapsed_seconds (float): Total pipeline execution time.

    Returns:
        dict: Structured envelope ready to be persisted.
    """
    ingested_at = now_sp()

    return {
        "metadata": {
            "source_system": "sptrans",
            "pipeline_name": "sptrans_posicao_raw",
            "api_version": "v2.2",
            "base_url": BASE,
            "secret_scope": SECRET_SCOPE,
            "token_name": TOKEN_NAME,
            "ingestion_status": "success",
            "ingested_at": ingested_at.isoformat(),
            "ingested_at_epoch_ms": to_epoch_ms(ingested_at),
            "raw_path": raw_path,
            "file_name": file_name,
            "total_elapsed_seconds": total_elapsed_seconds,
        },
        "auth": auth_metadata,
        "request": request_metadata,
        "payload": payload,
    }


def save_raw_json(envelope: dict, raw_path: str) -> None:
    """
    Persists the envelope as a JSON file in the raw layer.

    Args:
        envelope (dict): Structured data to be saved.
        raw_path (str): Destination file path.
    """
    logger.info("Saving raw file to %s", raw_path)

    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(envelope, f, ensure_ascii=False)

    logger.info("Raw file saved successfully")


def main() -> dict:
    """
    Executes the full ingestion pipeline.

    Steps:
    1. Authenticate
    2. Fetch data
    3. Build envelope
    4. Save raw file

    Returns:
        dict: Summary of ingestion execution.
    """
    pipeline_started_at = time.time()
    reference_dt = now_sp()

    raw_path, file_name = build_raw_path(reference_dt)
    session = requests.Session()

    auth_metadata = authenticate(session)
    payload, request_metadata = fetch_positions(session)

    total_elapsed_seconds = round(time.time() - pipeline_started_at, 3)

    envelope = build_envelope(
        payload=payload,
        raw_path=raw_path,
        file_name=file_name,
        auth_metadata=auth_metadata,
        request_metadata=request_metadata,
        total_elapsed_seconds=total_elapsed_seconds,
    )

    save_raw_json(envelope, raw_path)

    result = {
        "status": "success",
        "raw_path": raw_path,
        "file_name": file_name,
        "total_elapsed_seconds": total_elapsed_seconds,
        "payload_size_chars": request_metadata["payload_size_chars"],
        "payload_size_bytes": request_metadata["payload_size_bytes"],
        "ingested_at": envelope["metadata"]["ingested_at"],
    }

    logger.info("Ingestion finished: %s", result)
    return result


try:
    result = main()
    result
except Exception:
    logger.exception("SPTrans ingestion failed")
    raise
