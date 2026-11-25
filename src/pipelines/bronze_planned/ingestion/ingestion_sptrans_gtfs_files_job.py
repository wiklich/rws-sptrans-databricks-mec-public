import json
import os
import re
import shutil
import zipfile
from datetime import datetime
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup


SPTRANS_BASE = "https://www.sptrans.com.br"
LOGIN_URL = f"{SPTRANS_BASE}/desenvolvedores/login-desenvolvedores/"
PROFILE_URL = f"{SPTRANS_BASE}/desenvolvedores/perfil-desenvolvedor/"

TIMEZONE = ZoneInfo("America/Sao_Paulo")

SECRET_SCOPE = "sptrans-gtfs-login"
SECRET_USERNAME_KEY = "username"
SECRET_PASSWORD_KEY = "password"

DATASET_NAME = "sptrans_gtfs"

STAGE_DIR = "/Volumes/main/landing/raw_sptrans/stage_gtfs"
TMP_UNZIP_DIR = f"{STAGE_DIR}/_tmp"
PUBLISH_ROOT_DIR = "/Volumes/main/landing/raw_sptrans/gtfs"

CONTROL_DIR = f"{STAGE_DIR}/_control"
STATE_FILE = f"{CONTROL_DIR}/sptrans_gtfs_state.json"
HISTORY_DIR = f"{CONTROL_DIR}/history"

USERNAME = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_USERNAME_KEY)
PASSWORD = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_PASSWORD_KEY)

session = requests.Session()
session.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (compatible; Databricks GTFS Ingestion/4.0)"
    }
)


def now_local() -> datetime:
    """Return the current datetime in the configured local timezone."""
    return datetime.now(TIMEZONE)


def ensure_dir(path: str) -> None:
    """Create a directory if it does not already exist."""
    os.makedirs(path, exist_ok=True)


def recreate_dir(path: str) -> None:
    """Delete a directory if it exists and recreate it empty."""
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path, exist_ok=True)


def datetime_to_iso(value: datetime | None) -> str | None:
    """Convert a datetime to an ISO 8601 string in the configured timezone."""
    if value is None:
        return None
    return value.astimezone(TIMEZONE).isoformat()


def iso_to_datetime(value: str | None) -> datetime | None:
    """Convert an ISO 8601 string into a datetime object."""
    if not value:
        return None
    return datetime.fromisoformat(value)


def parse_sptrans_datetime(value: str) -> datetime:
    """
    Parse SPTrans datetime text.

    Example:
        '22/03/2026 12h28'
    """
    value = value.strip()
    match = re.search(r"(\d{2}/\d{2}/\d{4})\s+(\d{2})h(\d{2})", value)

    if not match:
        raise ValueError(f"Could not parse SPTrans datetime value: {value}")

    dt_str = f"{match.group(1)} {match.group(2)}:{match.group(3)}"
    return datetime.strptime(dt_str, "%d/%m/%Y %H:%M").replace(tzinfo=TIMEZONE)


def find_login_form(soup: BeautifulSoup):
    """
    Find the login form without relying on unstable IDs.
    """
    forms = soup.find_all("form")

    for form in forms:
        text = form.get_text(" ", strip=True).lower()
        has_password = form.find("input", {"type": "password"}) is not None

        if has_password or ("usuário" in text and "senha" in text):
            return form

    if forms:
        return forms[0]

    raise RuntimeError("Login form was not found.")


def build_login_payload(form) -> dict:
    """
    Build the login payload while preserving hidden inputs and identifying
    username/password fields heuristically.
    """
    payload = {}
    inputs = form.find_all("input")

    username_input_name = None
    password_input_name = None

    for input_element in inputs:
        name = input_element.get("name")
        if not name:
            continue

        input_type = (input_element.get("type") or "text").lower()
        value = input_element.get("value", "")

        if input_type == "hidden":
            payload[name] = value

        if input_type == "password":
            password_input_name = name

    candidate_inputs = []

    for input_element in inputs:
        name = input_element.get("name")
        if not name:
            continue

        input_type = (input_element.get("type") or "text").lower()
        if input_type in ("text", "email"):
            candidate_inputs.append(input_element)

    preferred_patterns = ["user", "usuario", "usuário", "login", "email", "member"]

    for input_element in candidate_inputs:
        haystack = " ".join(
            [
                input_element.get("name", ""),
                input_element.get("id", ""),
                input_element.get("placeholder", ""),
            ]
        ).lower()

        if any(pattern in haystack for pattern in preferred_patterns):
            username_input_name = input_element.get("name")
            break

    if not username_input_name and candidate_inputs:
        username_input_name = candidate_inputs[0].get("name")

    if not username_input_name or not password_input_name:
        raise RuntimeError(
            "Could not identify login input fields. "
            f"username_input_name={username_input_name}, "
            f"password_input_name={password_input_name}"
        )

    payload[username_input_name] = USERNAME
    payload[password_input_name] = PASSWORD
    return payload


def login() -> None:
    """
    Authenticate into the SPTrans developer portal.
    """
    response = session.get(LOGIN_URL, timeout=60)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    form = find_login_form(soup)

    action = form.get("action") or LOGIN_URL
    method = (form.get("method") or "post").lower()
    submit_url = urljoin(LOGIN_URL, action)
    payload = build_login_payload(form)

    if method == "post":
        login_response = session.post(
            submit_url,
            data=payload,
            timeout=60,
            allow_redirects=True,
        )
    else:
        login_response = session.get(
            submit_url,
            params=payload,
            timeout=60,
            allow_redirects=True,
        )

    login_response.raise_for_status()

    response_text = login_response.text.lower()
    if (
        "login desenvolvedores" in response_text
        and "senha" in response_text
        and "usuário" in response_text
    ):
        raise RuntimeError(
            "Authentication appears to have failed because the response still looks like the login page."
        )


def find_gtfs_block(soup: BeautifulSoup):
    """
    Find the GTFS content block by semantic text instead of fixed IDs.
    """
    for heading in soup.find_all(["h2", "h3"]):
        if heading.get_text(" ", strip=True).upper() == "GTFS":
            node = heading
            for _ in range(4):
                if node.parent is not None:
                    node = node.parent
            return node

    raise RuntimeError("GTFS block was not found on the profile page.")


def extract_last_update_and_download_url(html: str) -> tuple[datetime, str]:
    """
    Extract the source update timestamp and the GTFS download URL from the profile page HTML.
    """
    soup = BeautifulSoup(html, "html.parser")
    gtfs_block = find_gtfs_block(soup)
    gtfs_text = gtfs_block.get_text("\n", strip=True)

    update_match = re.search(
        r"Última atualização:\s*(\d{2}/\d{2}/\d{4}\s+\d{2}h\d{2})",
        gtfs_text,
        flags=re.IGNORECASE,
    )

    if not update_match:
        raise RuntimeError("Could not find the 'Última atualização' text inside the GTFS block.")

    updated_at = parse_sptrans_datetime(update_match.group(1))

    download_candidates = gtfs_block.find_all(["input", "a", "button"])
    download_url = None

    for element in download_candidates:
        visible_text = (
            (element.get("value") or "") + " " + (element.get_text(" ", strip=True) or "")
        ).strip().upper()

        if "DOWNLOAD" not in visible_text:
            continue

        onclick = element.get("onclick", "")
        href = element.get("href", "")

        if onclick:
            onclick_match = re.search(
                r"location\.href\s*=\s*['\"]([^'\"]+)['\"]",
                onclick,
            )
            if onclick_match:
                download_url = urljoin(SPTRANS_BASE, onclick_match.group(1))
                break

        if href:
            download_url = urljoin(SPTRANS_BASE, href)
            break

    if not download_url:
        block_html = str(gtfs_block)
        onclick_match = re.search(
            r"location\.href\s*=\s*['\"]([^'\"]+)['\"]",
            block_html,
        )
        if onclick_match:
            download_url = urljoin(SPTRANS_BASE, onclick_match.group(1))

    if not download_url:
        raise RuntimeError("Could not find the GTFS download URL inside the GTFS block.")

    return updated_at, download_url


def ensure_control_files() -> None:
    """
    Create the control directory and files if they do not already exist.
    """
    ensure_dir(CONTROL_DIR)
    ensure_dir(HISTORY_DIR)

    if not os.path.exists(STATE_FILE):
        initial_state = {
            "dataset_name": DATASET_NAME,
            "last_published_source_updated_at": None,
            "last_download_url": None,
            "last_staged_zip_path": None,
            "last_discovered_at": None,
            "last_downloaded_at": None,
            "last_published_at": None,
            "last_status": None,
            "last_file_count": 0,
            "last_message": None,
        }

        with open(STATE_FILE, "w", encoding="utf-8") as state_file:
            json.dump(initial_state, state_file, ensure_ascii=False, indent=2)


def read_state() -> dict:
    """Read the current control state from disk."""
    ensure_control_files()

    with open(STATE_FILE, "r", encoding="utf-8") as state_file:
        return json.load(state_file)


def write_state(state: dict) -> None:
    """Persist the control state atomically."""
    ensure_control_files()

    temp_state_file = f"{STATE_FILE}.tmp"
    with open(temp_state_file, "w", encoding="utf-8") as state_file:
        json.dump(state, state_file, ensure_ascii=False, indent=2)

    os.replace(temp_state_file, STATE_FILE)


def append_history_record(record: dict) -> str:
    """
    Persist one audit event as an individual JSON file.
    This avoids append mode, which is not supported for Unity Catalog volume files.
    """
    ensure_control_files()

    event_ts = now_local().strftime("%Y%m%d_%H%M%S_%f")
    event_status = (record.get("status") or "UNKNOWN").lower()
    history_path = os.path.join(HISTORY_DIR, f"{event_ts}_{event_status}.json")

    with open(history_path, "w", encoding="utf-8") as history_file:
        json.dump(record, history_file, ensure_ascii=False, indent=2)

    return history_path


def get_last_published_update() -> datetime | None:
    """
    Return the most recent published source update timestamp from the state file.
    """
    state = read_state()
    return iso_to_datetime(state.get("last_published_source_updated_at"))


def write_control_record(
    *,
    source_updated_at: datetime | None,
    source_download_url: str | None,
    staged_zip_path: str | None,
    discovered_at: datetime | None,
    downloaded_at: datetime | None,
    published_at: datetime | None,
    status: str,
    file_count: int | None,
    message: str | None,
) -> None:
    """
    Persist one control event.

    For PUBLISHED, the state file is updated with the latest published source update.
    For any status, an audit event is written as an individual JSON file.
    """
    ensure_control_files()

    history_record = {
        "event_at": datetime_to_iso(now_local()),
        "dataset_name": DATASET_NAME,
        "source_updated_at": datetime_to_iso(source_updated_at),
        "source_download_url": source_download_url,
        "staged_zip_path": staged_zip_path,
        "discovered_at": datetime_to_iso(discovered_at),
        "downloaded_at": datetime_to_iso(downloaded_at),
        "published_at": datetime_to_iso(published_at),
        "status": status,
        "file_count": file_count,
        "message": message,
    }

    append_history_record(history_record)

    state = read_state()
    state["dataset_name"] = DATASET_NAME
    state["last_status"] = status
    state["last_message"] = message
    state["last_discovered_at"] = datetime_to_iso(discovered_at)
    state["last_downloaded_at"] = datetime_to_iso(downloaded_at)

    if status == "PUBLISHED":
        state["last_published_source_updated_at"] = datetime_to_iso(source_updated_at)
        state["last_download_url"] = source_download_url
        state["last_staged_zip_path"] = staged_zip_path
        state["last_published_at"] = datetime_to_iso(published_at)
        state["last_file_count"] = file_count or 0

    write_state(state)


def download_file(url: str, target_path: str) -> str:
    """
    Download the GTFS ZIP file into the staging directory.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; Databricks GTFS Ingestion/4.0)",
        "Referer": PROFILE_URL,
        "Accept": "application/zip, application/octet-stream, */*",
        "Connection": "close",
    }

    print(f"[download] Starting GET request: {url}")

    with session.get(
        url,
        headers=headers,
        timeout=(15, 120),
        allow_redirects=True,
        stream=True,
    ) as response:
        print(f"[download] status={response.status_code}")
        print(f"[download] final_url={response.url}")
        print(f"[download] content_type={response.headers.get('Content-Type')}")
        print(f"[download] content_length={response.headers.get('Content-Length')}")
        print(f"[download] content_disposition={response.headers.get('Content-Disposition')}")

        response.raise_for_status()

        content_type = (response.headers.get("Content-Type") or "").lower()
        content_disposition = (response.headers.get("Content-Disposition") or "").lower()

        is_zip = (
            "application/zip" in content_type
            or "application/octet-stream" in content_type
            or ".zip" in content_disposition
        )

        if not is_zip:
            preview = response.content[:1000]
            print(f"[download] Unexpected response preview: {preview!r}")
            raise RuntimeError("The download response does not look like a valid ZIP file.")

        ensure_dir(os.path.dirname(target_path))

        with open(target_path, "wb") as output_file:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    output_file.write(chunk)

    print(f"[download] File saved to: {target_path}")
    return target_path


def extract_zip_to_temp(zip_path: str, temp_dir: str) -> list[str]:
    """
    Extract the downloaded GTFS ZIP file into the temporary unzip directory.
    Returns the list of extracted file paths.
    """
    recreate_dir(temp_dir)

    print(f"[unzip] Extracting ZIP file: {zip_path}")
    print(f"[unzip] Temporary directory: {temp_dir}")

    with zipfile.ZipFile(zip_path, "r") as zip_file:
        zip_file.extractall(temp_dir)

    extracted_files = []

    for root, _, files in os.walk(temp_dir):
        for file_name in files:
            extracted_files.append(os.path.join(root, file_name))

    extracted_files = sorted(extracted_files)

    print(f"[unzip] Extracted file count: {len(extracted_files)}")
    return extracted_files


def build_versioned_filename(original_filename: str, updated_at: datetime) -> tuple[str, str]:
    """
    Build the final destination directory name and versioned file name.

    Example:
        stop_times.txt -> directory=stop_times, filename=stop_times_2026-03-22.txt
    """
    base_name, extension = os.path.splitext(original_filename)
    version_suffix = updated_at.strftime("%Y-%m-%d")
    final_filename = f"{base_name}_{version_suffix}{extension}"
    return base_name, final_filename


def publish_extracted_files(
    extracted_files: list[str],
    publish_root_dir: str,
    updated_at: datetime,
) -> list[str]:
    """
    Rename extracted GTFS files with the source update date and move them into the final
    directory structure:

        /Volumes/main/landing/raw_sptrans/gtfs/<base_name>/<base_name>_yyyy-MM-dd.txt
    """
    published_paths = []

    for source_path in extracted_files:
        original_filename = os.path.basename(source_path)
        base_name, final_filename = build_versioned_filename(original_filename, updated_at)

        target_dir = os.path.join(publish_root_dir, base_name)
        ensure_dir(target_dir)

        target_path = os.path.join(target_dir, final_filename)

        if os.path.exists(target_path):
            os.remove(target_path)

        shutil.move(source_path, target_path)

        print(f"[publish] Moved file: {source_path} -> {target_path}")
        published_paths.append(target_path)

    return published_paths


def cleanup_temp_dir(temp_dir: str) -> None:
    """
    Remove the temporary unzip directory if it exists.
    """
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
        print(f"[cleanup] Removed temporary directory: {temp_dir}")


# -------------------------------------------------------------------
# Main flow
# -------------------------------------------------------------------

ensure_control_files()

discovered_at = None
updated_at = None
download_url = None
staged_zip_path = None
downloaded_at = None
published_at = None

try:
    login()

    profile_response = session.get(PROFILE_URL, timeout=60, allow_redirects=True)
    profile_response.raise_for_status()

    discovered_at = now_local()
    updated_at, download_url = extract_last_update_and_download_url(profile_response.text)

    print(f"[profile] Source updated_at={updated_at.isoformat()}")
    print(f"[profile] Download URL={download_url}")

    last_published_update = get_last_published_update()

    if last_published_update is not None:
        print(f"[control] Last published source_updated_at={last_published_update.isoformat()}")
    else:
        print("[control] No previously published GTFS update was found in the state file.")

    if last_published_update is not None and updated_at <= last_published_update:
        message = (
            "Skipping download because the source update timestamp is not newer than the last published update."
        )
        print(f"[control] {message}")

        write_control_record(
            source_updated_at=updated_at,
            source_download_url=download_url,
            staged_zip_path=None,
            discovered_at=discovered_at,
            downloaded_at=None,
            published_at=None,
            status="SKIPPED",
            file_count=0,
            message=message,
        )

        dbutils.notebook.exit("SKIPPED_NO_NEW_GTFS")

    ensure_dir(STAGE_DIR)
    ensure_dir(PUBLISH_ROOT_DIR)

    zip_filename = f"gtfs_sptrans_{updated_at.strftime('%Y%m%d_%H%M')}.zip"
    staged_zip_path = os.path.join(STAGE_DIR, zip_filename)

    download_file(download_url, staged_zip_path)
    downloaded_at = now_local()

    extracted_files = extract_zip_to_temp(staged_zip_path, TMP_UNZIP_DIR)

    published_paths = publish_extracted_files(
        extracted_files=extracted_files,
        publish_root_dir=PUBLISH_ROOT_DIR,
        updated_at=updated_at,
    )
    published_at = now_local()

    cleanup_temp_dir(TMP_UNZIP_DIR)

    success_message = (
        f"GTFS package downloaded and published successfully. "
        f"Published file count: {len(published_paths)}."
    )
    print(f"[success] {success_message}")

    write_control_record(
        source_updated_at=updated_at,
        source_download_url=download_url,
        staged_zip_path=staged_zip_path,
        discovered_at=discovered_at,
        downloaded_at=downloaded_at,
        published_at=published_at,
        status="PUBLISHED",
        file_count=len(published_paths),
        message=success_message,
    )

    dbutils.notebook.exit(f"PUBLISHED:{staged_zip_path}")

except Exception as exc:
    error_message = f"{type(exc).__name__}: {str(exc)}"
    print(f"[error] {error_message}")

    try:
        write_control_record(
            source_updated_at=updated_at,
            source_download_url=download_url,
            staged_zip_path=staged_zip_path,
            discovered_at=discovered_at,
            downloaded_at=downloaded_at,
            published_at=published_at,
            status="FAILED",
            file_count=0,
            message=error_message,
        )
    finally:
        cleanup_temp_dir(TMP_UNZIP_DIR)

    raise