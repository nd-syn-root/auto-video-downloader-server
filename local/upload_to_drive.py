from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import os

def upload_file_to_drive(file_path, drive_folder_id=None):
    creds = Credentials.from_authorized_user_file("token.json")
    service = build("drive", "v3", credentials=creds)

    file_metadata = {
        "name": os.path.basename(file_path)
    }

    if drive_folder_id:
        file_metadata["parents"] = [drive_folder_id]

    media = MediaFileUpload(file_path, resumable=True)

    file = service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id"
    ).execute()

    print("Uploaded file ID:", file.get("id"))
    return file.get("id")
