"""Replay (or record) Prometheus HTTP API responses for the SQL tests.

    python3 test/fixture_server/server.py                      # replay recorded.json
    python3 test/fixture_server/server.py --record http://127.0.0.1:19090

Prints ``PORT:<n>`` on stdout once listening (ci/run-integration.sh waits for
it). Responses are keyed by path plus the sorted query/form parameters, so GET
and POST requests for the same call share an entry. ``recorded.json`` holds
responses captured from the live fixture (test/fixture/run-prometheus.sh);
``handwritten.json`` holds hand-made responses for shapes a stock server will
not produce on demand (errors, Retry-After, mixed native histograms).

Environment:
  FIXTURE_FLAKY=1          answer every key with 429 + Retry-After: 0 once
  FIXTURE_REQUIRE_AUTH=1   demand a tenant and credentials: X-Scope-OrgID:
                           tenant-a plus either "Bearer test-token" or basic
                           auth grafana:secret-key (401/403 otherwise)
"""

import base64
import json
import os
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
RECORDED = os.path.join(HERE, "recorded.json")
HANDWRITTEN = os.path.join(HERE, "handwritten.json")

UPSTREAM = None
if "--record" in sys.argv:
    UPSTREAM = sys.argv[sys.argv.index("--record") + 1].rstrip("/")

LOCK = threading.Lock()


def load(path):
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


FIXTURES = load(RECORDED)
FIXTURES.update(load(HANDWRITTEN))
SEEN = set()
GOOD_AUTH = {"Bearer test-token", "Basic " + base64.b64encode(b"grafana:secret-key").decode()}


def key_of(path, params):
    items = sorted((k, v) for k, vs in params.items() for v in vs)
    return path + ("?" + urllib.parse.urlencode(items) if items else "")


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def reply(self, status, body, headers=None):
        data = body if isinstance(body, bytes) else body.encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def error(self, status, etype, msg):
        self.reply(status, json.dumps({"status": "error", "errorType": etype, "error": msg}))

    def handle_any(self, body=b""):
        url = urllib.parse.urlsplit(self.path)
        params = urllib.parse.parse_qs(url.query, keep_blank_values=True)
        if body:
            for k, vs in urllib.parse.parse_qs(body.decode(), keep_blank_values=True).items():
                params.setdefault(k, []).extend(vs)
        if os.environ.get("FIXTURE_REQUIRE_AUTH") == "1":
            if self.headers.get("Authorization") not in GOOD_AUTH:
                return self.error(401, "unauthorized", "missing or bad credentials")
            if self.headers.get("X-Scope-OrgID") != "tenant-a":
                return self.error(403, "unauthorized", "no org id")
        key = key_of(url.path, params)
        if os.environ.get("FIXTURE_FLAKY") == "1":
            with LOCK:
                first = key not in SEEN
                SEEN.add(key)
            if first:
                return self.reply(429, '{"status":"error","errorType":"too_many_requests","error":"slow down"}',
                                  {"Retry-After": "0"})
        if key in FIXTURES:
            entry = FIXTURES[key]
            return self.reply(entry["status"], entry["body"], entry.get("headers"))
        if UPSTREAM is None:
            return self.error(404, "not_recorded", "no recorded response for " + key)
        target = UPSTREAM + url.path + ("?" + urllib.parse.urlencode(sorted((k, v) for k, vs in params.items() for v in vs), ) if params else "")
        try:
            with urllib.request.urlopen(target, timeout=60) as resp:
                status, data = resp.status, resp.read()
        except urllib.error.HTTPError as e:
            status, data = e.code, e.read()
        with LOCK:
            FIXTURES[key] = {"status": status, "body": data.decode()}
            recorded = load(RECORDED)
            recorded[key] = FIXTURES[key]
            with open(RECORDED, "w") as f:
                json.dump(recorded, f, indent=0, sort_keys=True)
                f.write("\n")
        self.reply(status, data)

    def do_GET(self):
        self.handle_any()

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        self.handle_any(self.rfile.read(n))


def main():
    srv = ThreadingHTTPServer(("127.0.0.1", int(os.environ.get("FIXTURE_PORT", "0"))), Handler)
    print(f"PORT:{srv.server_address[1]}", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
