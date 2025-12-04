# client/send_request.py
# Local script to send download requests to your hosted server and poll status.
# Usage:
#   python send_request.py --server https://your-server.example --file urls.txt
#
# file `urls.txt` contains one URL per line (playlist or single video links)

import requests
import time
import argparse
import os
import sys

def read_urls(file):
    with open(file, "r", encoding="utf-8") as f:
        lines = [l.strip() for l in f.readlines()]
    urls = [l for l in lines if l and not l.startswith("#")]
    return urls

def send(server, urls, name=None):
    endpoint = server.rstrip("/") + "/enqueue"
    payload = {"urls": urls}
    if name:
        payload["name"] = name
    r = requests.post(endpoint, json=payload)
    if r.status_code not in (200,201,202):
        print("Failed to enqueue:", r.status_code, r.text)
        return None
    job_id = r.json().get("job_id")
    print("Job queued:", job_id)
    return job_id

def poll(server, job_id, interval=10):
    url = server.rstrip("/") + f"/status/{job_id}"
    while True:
        r = requests.get(url)
        if r.status_code != 200:
            print("Status error:", r.status_code, r.text)
            time.sleep(interval)
            continue
        job = r.json()
        print("state:", job.get("state"), "| progress:", job.get("progress"))
        if job.get("state") in ("done","error"):
            print("final job info:", job.get("result") or job.get("error"))
            return job
        time.sleep(interval)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", required=True, help="Server base URL, e.g. https://my-downloader.onrender.com")
    parser.add_argument("--file", required=True, help="Local text file with one URL per line")
    parser.add_argument("--name", default=None, help="Optional batch name")
    args = parser.parse_args()

    if not os.path.exists(args.file):
        print("URLs file missing:", args.file); sys.exit(1)

    urls = read_urls(args.file)
    if not urls:
        print("No URLs found in file"); sys.exit(1)

    job_id = send(args.server, urls, args.name)
    if job_id:
        poll(args.server, job_id, interval=8)
