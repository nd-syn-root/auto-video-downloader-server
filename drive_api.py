# drive_api.py
# Small wrapper around Google Drive v3 API for uploading + share link creation.
# Expects Google OAuth token file (token.json) and optional credentials file (credentials.json)
# Usage: set DRIVE_TOKEN_FILE and DRIVE_CRED_FILE env vars or put token.json/credentials.json in same folder.

import os
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

SCOPES = ["https://www.googleapis.com/auth/drive.file"]

def _get_creds(token_file=None):
    token_file = token_file or os.environ.get("DRIVE_TOKEN_FILE", "token.json")
    if not os.path.exists(token_file):
        raise FileNotFoundError(f"Drive token file not found: {token_file}")
    creds = Credentials.from_authorized_user_file(token_file, SCOPES)
    return creds

def get_service(token_file=None):
    creds = _get_creds(token_file)
    service = build("drive", "v3", credentials=creds, cache_discovery=False)
    return service

def create_folder(name, parent_id=None, token_file=None):
    svc = get_service(token_file)
    body = {"name": name, "mimeType": "application/vnd.google-apps.folder"}
    if parent_id:
        body["parents"] = [parent_id]
    res = svc.files().create(body=body, fields="id").execute()
    return res.get("id")

def upload_file(path, parent_id=None, token_file=None):
    svc = get_service(token_file)
    metadata = {"name": os.path.basename(path)}
    if parent_id:
        metadata["parents"] = [parent_id]
    media = MediaFileUpload(path, resumable=True)
    f = svc.files().create(body=metadata, media_body=media, fields="id,webViewLink,webContentLink").execute()
    return {
        "id": f.get("id"),
        "webViewLink": f.get("webViewLink"),
        "webContentLink": f.get("webContentLink")
    }

def make_shareable(file_id, token_file=None):
    svc = get_service(token_file)
    # Grant "anyone with link can view"
    try:
        svc.permissions().create(
            fileId=file_id,
            body={"role":"reader", "type":"anyone"},
            fields="id"
        ).execute()
    except Exception:
        # permission might already exist; ignore
        pass
    # return webViewLink
    f = svc.files().get(fileId=file_id, fields="id,webViewLink,webContentLink").execute()
    return {
        "id": f.get("id"),
        "webViewLink": f.get("webViewLink"),
        "webContentLink": f.get("webContentLink")
    }
