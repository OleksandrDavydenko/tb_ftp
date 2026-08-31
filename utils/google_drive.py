"""
Мінімальний клієнт Google Drive поверх requests.

Авторизація — OAuth від власного акаунта: довгоживучий refresh_token міняємо на
короткий access_token, як це зроблено з токеном Power BI (auth.py). Повноцінний
google-api-python-client заради двох GET-запитів не тягнемо.

Змінні оточення:
    client_id, client_secret, refresh_token — з OAuth-клієнта типу Desktop app.
"""

import logging
import os
import time

import requests

TOKEN_URI = "https://oauth2.googleapis.com/token"
FILES_API = "https://www.googleapis.com/drive/v3/files"

# Скільки секунд до фактичного протермінування вважаємо токен уже мертвим
TOKEN_EXPIRY_MARGIN = 60

# Кеш access-токена між викликами: (токен, момент протермінування)
_access_token: str | None = None
_access_token_expires_at: float = 0.0


def _get_access_token() -> str | None:
    """Свіжий access_token; None — якщо немає налаштувань або Google відмовив."""
    global _access_token, _access_token_expires_at

    if _access_token and time.time() < _access_token_expires_at:
        return _access_token

    client_id = os.getenv("client_id")
    client_secret = os.getenv("client_secret")
    refresh_token = os.getenv("refresh_token")
    if not (client_id and client_secret and refresh_token):
        logging.error(
            "❌ Google Drive: не задано client_id / client_secret / refresh_token"
        )
        return None

    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }

    try:
        r = requests.post(TOKEN_URI, data=payload, timeout=30)
    except Exception as e:
        logging.exception(f"❌ Google Drive: виняток при оновленні токена: {e}")
        return None

    if r.status_code != 200:
        # Тіло може містити invalid_grant — найчастіше це відкликаний або
        # протухлий refresh_token (7 днів, якщо застосунок у режимі Testing).
        logging.error(
            f"❌ Google Drive: не вдалося оновити токен, статус {r.status_code}: {r.text[:300]}"
        )
        return None

    try:
        data = r.json()
        _access_token = data["access_token"]
        _access_token_expires_at = time.time() + int(data.get("expires_in", 3600)) - TOKEN_EXPIRY_MARGIN
    except (ValueError, KeyError) as e:
        logging.error(f"❌ Google Drive: неочікувана відповідь token endpoint: {e}")
        return None

    return _access_token


def list_folder_files(folder_id: str, name_contains: str | None = None) -> list[dict] | None:
    """
    Файли з папки (без вкладених).

    Повертає:
      * list[dict] — знайдені файли з полями id/name/size/mimeType (може бути
        порожнім списком, якщо нічого не підійшло);
      * None       — помилка (немає токена, Drive недоступний).
    """
    token = _get_access_token()
    if not token:
        return None

    query = f"'{folder_id}' in parents and trashed = false"
    if name_contains:
        query += f" and name contains '{name_contains}'"

    params = {
        "q": query,
        "fields": "nextPageToken, files(id, name, size, mimeType)",
        "orderBy": "name",
        "pageSize": 100,
        # На випадок, якщо папка живе на Shared Drive
        "supportsAllDrives": "true",
        "includeItemsFromAllDrives": "true",
    }
    headers = {"Authorization": f"Bearer {token}"}

    files: list[dict] = []
    page_token = None

    while True:
        if page_token:
            params["pageToken"] = page_token

        try:
            r = requests.get(FILES_API, headers=headers, params=params, timeout=60)
        except Exception as e:
            logging.exception(f"❌ Google Drive: виняток при пошуку файлів: {e}")
            return None

        if r.status_code != 200:
            logging.error(
                f"❌ Google Drive: пошук повернув статус {r.status_code}: {r.text[:300]}"
            )
            return None

        try:
            data = r.json()
        except ValueError as e:
            logging.error(f"❌ Google Drive: неочікувана відповідь на пошук: {e}")
            return None

        files.extend(data.get("files", []))
        page_token = data.get("nextPageToken")
        if not page_token:
            break

    logging.info(f"📥 Google Drive: знайдено {len(files)} файл(ів) за запитом {name_contains!r}")
    return files


def download_file(file_id: str) -> bytes | None:
    """Вміст файлу; None — якщо не вдалося завантажити."""
    token = _get_access_token()
    if not token:
        return None

    try:
        r = requests.get(
            f"{FILES_API}/{file_id}",
            headers={"Authorization": f"Bearer {token}"},
            params={"alt": "media", "supportsAllDrives": "true"},
            timeout=120,
        )
    except Exception as e:
        logging.exception(f"❌ Google Drive: виняток при завантаженні {file_id}: {e}")
        return None

    if r.status_code != 200:
        logging.error(
            f"❌ Google Drive: завантаження {file_id} повернуло статус {r.status_code}: {r.text[:200]}"
        )
        return None

    return r.content
