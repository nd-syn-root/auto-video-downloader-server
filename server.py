# server.py
# Flask server that accepts job requests, downloads videos with yt-dlp,
# zips and uploads to Google Drive using drive_api.py.
#
# How it works:
# - POST /enqueue  with JSON {"urls": ["url1","url2",...], "name": "optional batch name"}
# - GET  /status/<job_id> to poll job status
#
# Environment:
# - DRIVE_TOKEN_FILE (optional) path to token.json
# - PERSISTENT_DIR (optional) where downloads and zips are stored; default "./downloads"
#
# Run:
#   pip install -r requirements.txt
#   python server.py

import os
import uuid
import threading
import json
import shutil
import subprocess
import time
from pathlib import Path
from flask import Flask, request, jsonify
import drive_api

app = Flask(__name__)

PERSISTENT_DIR = Path(os.environ.get("PERSISTENT_DIR", "./downloads"))
JOBS_DIR = Path(os.environ.get("JOBS_DIR", "./jobs"))
PERSISTENT_DIR.mkdir(parents=True, exist_ok=True)
JOBS_DIR.mkdir(parents=True, exist_ok=True)

YT_DLP = os.environ.get("YTDLP_BIN", "yt-dlp")
DRIVE_TOKEN_FILE = os.environ.get("DRIVE_TOKEN_FILE", "token.json")

# In-memory job store (also saved to disk)
jobs_lock = threading.Lock()
jobs = {}  # job_id -> job dict

def _save_job(job):
    JOBS_DIR.mkdir(parents=True, exist_ok=True)
    path = JOBS_DIR / f"{job['id']}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(job, f, indent=2)

def _load_existing_jobs():
    for f in JOBS_DIR.glob("*.json"):
        try:
            with open(f, "r", encoding="utf-8") as fh:
                job = json.load(fh)
                jobs[job["id"]] = job
        except Exception:
            pass

_load_existing_jobs()

def create_job(urls, name=None):
    jid = str(uuid.uuid4())
    job = {
        "id": jid,
        "urls": urls,
        "name": name or f"batch_{jid[:8]}",
        "state": "queued",   # queued / running / zipping / uploading / done / error
        "progress": {"current": 0, "total": len(urls), "current_url": None},
        "result": None,
        "error": None,
        "created_at": time.time()
    }
    with jobs_lock:
        jobs[jid] = job
        _save_job(job)
    return job

def update_job(job):
    with jobs_lock:
        jobs[job["id"]] = job
        _save_job(job)

def download_and_process(job_id):
    job = jobs.get(job_id)
    if not job:
        return
    job["state"] = "running"
    update_job(job)

    batch_folder = PERSISTENT_DIR / job_id
    if batch_folder.exists():
        shutil.rmtree(batch_folder, ignore_errors=True)
    batch_folder.mkdir(parents=True, exist_ok=True)

    try:
        # Download loop
        urls = job["urls"]
        total = len(urls)
        for idx, url in enumerate(urls, start=1):
            job["progress"]["current"] = idx
            job["progress"]["total"] = total
            job["progress"]["current_url"] = url
            update_job(job)

            # run yt-dlp
            out_template = str(batch_folder / "%(playlist_index)s - %(title)s.%(ext)s")
            cmd = [YT_DLP, "-o", out_template, "--no-part", "--continue", "--restrict-filenames", url]
            print("Running:", " ".join(cmd))
            res = subprocess.run(cmd, capture_output=True, text=True)
            if res.returncode != 0:
                # record error but continue to try others
                job["error"] = f"yt-dlp failed for {url}: {res.returncode} stdout:{res.stdout} stderr:{res.stderr}"
                update_job(job)
                # decide: continue or break — here we continue
        # Zip everything
        job["state"] = "zipping"
        update_job(job)
        zip_name = f"{job_id}.zip"
        zip_path = PERSISTENT_DIR / zip_name
        if zip_path.exists():
            zip_path.unlink()
        shutil.make_archive(str(zip_path).replace(".zip",""), 'zip', batch_folder)

        # Upload zip to Drive
        job["state"] = "uploading"
        update_job(job)
        # create a Drive folder for this batch
        parent_folder_id = drive_api.create_folder(job["name"], token_file=DRIVE_TOKEN_FILE)
        up = drive_api.upload_file(str(zip_path), parent_id=parent_folder_id, token_file=DRIVE_TOKEN_FILE)
        # make shareable
        share = drive_api.make_shareable(up["id"], token_file=DRIVE_TOKEN_FILE)

        job["state"] = "done"
        job["result"] = {
            "drive_folder_id": parent_folder_id,
            "zip_file_id": share["id"],
            "webViewLink": share.get("webViewLink"),
            "webContentLink": share.get("webContentLink")
        }
        update_job(job)

    except Exception as e:
        job["state"] = "error"
        job["error"] = str(e)
        update_job(job)
    finally:
        # cleanup local files (optional) — keep zip if you want
        try:
            shutil.rmtree(batch_folder, ignore_errors=True)
        except Exception:
            pass

@app.route("/enqueue", methods=["POST"])
def enqueue():
    data = request.get_json(force=True)
    urls = data.get("urls")
    name = data.get("name")
    if not urls or not isinstance(urls, list):
        return jsonify({"error": "send JSON with key 'urls' as a list of video/playlist urls"}), 400
    job = create_job(urls, name)
    # start background thread to process
    t = threading.Thread(target=download_and_process, args=(job["id"],), daemon=True)
    t.start()
    return jsonify({"job_id": job["id"]}), 202

@app.route("/status/<job_id>", methods=["GET"])
def status(job_id):
    job = jobs.get(job_id)
    if not job:
        # try load from disk
        path = JOBS_DIR / f"{job_id}.json"
        if path.exists():
            with open(path, "r", encoding="utf-8") as fh:
                job = json.load(fh)
            jobs[job_id] = job
    if not job:
        return jsonify({"error":"job not found"}), 404
    return jsonify(job)

@app.route("/jobs", methods=["GET"])
def list_jobs():
    with jobs_lock:
        return jsonify(list(jobs.values()))

@app.route("/", methods=["GET"])
def home():
    return jsonify({"message":"Auto-downloader server running"}), 200

if __name__ == "__main__":
    # choose port with env PORT if hosting platform requires it
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
