# worker/server.py
# Background worker: pulls jobs -> downloads with yt-dlp -> uploads to Google Drive -> zips -> updates job
# Usage (local): pip install -r worker/requirements.txt ; python worker/server.py

import os, time, json, shutil, subprocess
from pathlib import Path
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

REDIS_URL = os.environ.get('REDIS_URL')
USE_REDIS = bool(REDIS_URL)
if USE_REDIS:
    import redis
    r = redis.from_url(REDIS_URL, decode_responses=True)
else:
    QUEUE_DIR = os.environ.get('QUEUE_DIR', '/tmp/avdq')
    os.makedirs(QUEUE_DIR, exist_ok=True)

DATA_DIR = Path(os.environ.get('PERSISTENT_DIR', '/data'))
DATA_DIR.mkdir(parents=True, exist_ok=True)

YT_DLP = os.environ.get('YTDLP_BIN', 'yt-dlp')
TOKEN_FILE = os.environ.get('GOOGLE_TOKEN_FILE', 'token.json')
SCOPES = ['https://www.googleapis.com/auth/drive.file']

def get_drive_service():
    if not os.path.exists(TOKEN_FILE):
        raise SystemExit(f"Missing {TOKEN_FILE}. Run auth.py locally and upload token.json.")
    creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    service = build('drive', 'v3', credentials=creds, cache_discovery=False)
    return service

def create_drive_folder(name, parent_id=None):
    svc = get_drive_service()
    meta = {'name': name, 'mimeType': 'application/vnd.google-apps.folder'}
    if parent_id:
        meta['parents'] = [parent_id]
    f = svc.files().create(body=meta, fields='id').execute()
    return f['id']

def upload_file_to_drive(path, parent_id=None):
    svc = get_drive_service()
    file_metadata = {'name': os.path.basename(path)}
    if parent_id:
        file_metadata['parents'] = [parent_id]
    media = MediaFileUpload(path, resumable=True)
    inserted = svc.files().create(body=file_metadata, media_body=media, fields='id').execute()
    return inserted.get('id')

def zip_folder(src_folder, out_zip_path):
    base = out_zip_path.replace('.zip','')
    shutil.make_archive(base, 'zip', src_folder)
    return out_zip_path

# Job queue helpers
def blpop_job():
    if USE_REDIS:
        res = r.blpop('job_queue', timeout=5)
        if res:
            return res[1]
        return None
    else:
        files = [f for f in os.listdir(QUEUE_DIR) if f.endswith('.json')]
        if not files:
            time.sleep(2)
            return None
        files.sort()
        # read next file and return job id
        fn = files[0]
        with open(os.path.join(QUEUE_DIR, fn), 'r', encoding='utf-8') as fh:
            job = json.load(fh)
        return job.get('id')

def load_job(job_id):
    if USE_REDIS:
        return r.hgetall(f'job:{job_id}')
    else:
        path = os.path.join(QUEUE_DIR, f'{job_id}.json')
        if not os.path.exists(path):
            return None
        with open(path, 'r', encoding='utf-8') as fh:
            return json.load(fh)

def save_job(job):
    job_id = job['id']
    if USE_REDIS:
        r.hset(f'job:{job_id}', mapping=job)
    else:
        path = os.path.join(QUEUE_DIR, f'{job_id}.json')
        with open(path, 'w', encoding='utf-8') as fh:
            json.dump(job, fh)

# Download function using yt-dlp
def download_urls(job_id, urls):
    folder = DATA_DIR / job_id
    folder.mkdir(parents=True, exist_ok=True)
    for u in urls:
        cmd = [
            YT_DLP,
            '-o', str(folder / '%(playlist_index)s - %(title)s.%(ext)s'),
            '--no-part',
            '--continue',
            '--restrict-filenames',
            u
        ]
        print('RUN:', ' '.join(cmd))
        res = subprocess.run(cmd)
        if res.returncode != 0:
            print('yt-dlp failed for', u, 'code', res.returncode)
    return folder

def process_job(job_id):
    job = load_job(job_id)
    if not job:
        print('job not found', job_id)
        return
    job['status'] = 'running'
    save_job(job)

    urls = job.get('urls') or []
    folder = download_urls(job_id, urls)

    # Create Drive folder for this batch
    try:
        parent_folder_id = create_drive_folder(f'batch_{job_id}')
    except Exception as e:
        job['status'] = 'error'
        job['error'] = f"Drive folder create failed: {e}"
        save_job(job)
        return

    # Upload each downloaded file to Drive
    try:
        for root, dirs, files in os.walk(folder):
            for f in files:
                fp = os.path.join(root, f)
                print('Uploading', fp)
                upload_file_to_drive(fp, parent_folder_id)
    except Exception as e:
        job['status'] = 'error'
        job['error'] = f"Upload failed: {e}"
        save_job(job)
        return

    # Zip local folder and upload zip
    zip_name = f'{job_id}.zip'
    zip_path = str(DATA_DIR / zip_name)
    zip_folder(str(folder), zip_path)
    try:
        zip_id = upload_file_to_drive(zip_path, parent_folder_id)
    except Exception as e:
        job['status'] = 'error'
        job['error'] = f"Zip upload failed: {e}"
        save_job(job)
        return

    job['status'] = 'done'
    job['drive_folder_id'] = parent_folder_id
    job['zip_id'] = zip_id
    save_job(job)

    # cleanup local files
    shutil.rmtree(folder, ignore_errors=True)
    try:
        os.remove(zip_path)
    except:
        pass

def worker_loop():
    print('Worker started, waiting for jobs...')
    while True:
        job_id = blpop_job()
        if not job_id:
            continue
        print('Picked job:', job_id)
        try:
            process_job(job_id)
        except Exception as e:
            print('Job error:', e)
            job = load_job(job_id) or {'id': job_id}
            job['status'] = 'error'
            job['error'] = str(e)
            save_job(job)

if __name__ == '__main__':
    worker_loop()
