# web/app.py
# Simple Flask dashboard to enqueue download jobs
# Usage (local): pip install -r web/requirements.txt ; python web/app.py

from flask import Flask, request, render_template, jsonify, redirect, url_for
import os, uuid, json

app = Flask(__name__, template_folder='templates')

# Choose one queue mode:
# - Redis mode if REDIS_URL is set
# - Filesystem mode fallback if REDIS_URL is not set
REDIS_URL = os.environ.get('REDIS_URL')
USE_REDIS = bool(REDIS_URL)

if USE_REDIS:
    import redis
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
else:
    QUEUE_DIR = os.environ.get('QUEUE_DIR', '/tmp/avdq')
    os.makedirs(QUEUE_DIR, exist_ok=True)

def enqueue_job(urls, meta=None):
    job_id = str(uuid.uuid4())
    job = {
        'id': job_id,
        'urls': urls,
        'status': 'queued',
        'meta': meta or {}
    }
    if USE_REDIS:
        redis_client.hset(f'job:{job_id}', mapping=job)
        redis_client.rpush('job_queue', job_id)
    else:
        path = os.path.join(QUEUE_DIR, f'{job_id}.json')
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(job, f)
    return job_id

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/enqueue', methods=['POST'])
def enqueue():
    data = request.form.get('urls') or (request.json and request.json.get('urls'))
    if not data:
        return 'No URLs provided', 400
    # Accept newline-separated or comma-separated
    urls = [u.strip() for u in data.replace(',', '\n').splitlines() if u.strip()]
    job_id = enqueue_job(urls)
    return redirect(url_for('status', job_id=job_id))

@app.route('/status/<job_id>')
def status(job_id):
    if USE_REDIS:
        job = redis_client.hgetall(f'job:{job_id}')
        if not job:
            return jsonify({'error': 'not found'}), 404
        return jsonify(job)
    else:
        path = os.path.join(QUEUE_DIR, f'{job_id}.json')
        if not os.path.exists(path):
            return jsonify({'error': 'not found'}), 404
        with open(path, 'r', encoding='utf-8') as f:
            return jsonify(json.load(f))

@app.route('/jobs')
def jobs():
    out = []
    if USE_REDIS:
        keys = redis_client.keys('job:*')
        for k in keys:
            out.append(redis_client.hgetall(k))
    else:
        for fn in os.listdir(QUEUE_DIR):
            if fn.endswith('.json'):
                with open(os.path.join(QUEUE_DIR, fn), 'r', encoding='utf-8') as f:
                    out.append(json.load(f))
    return jsonify(out)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
