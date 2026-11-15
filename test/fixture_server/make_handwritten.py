"""Regenerate handwritten.json: hand-made API responses the live fixture can't
produce deterministically (native histograms at a fixed time, warnings,
fractional timestamps, outages, server-side errors).

    python3 test/fixture_server/make_handwritten.py
"""
import json
import os
import urllib.parse

HERE = os.path.dirname(os.path.abspath(__file__))
T = 1767226500  # 2026-01-01T00:15:00Z


def key(path, **params):
    items = sorted(params.items())
    return path + ("?" + urllib.parse.urlencode(items) if items else "")


def ok(data, **extra):
    return {"status": 200, "body": json.dumps({"status": "success", "data": data, **extra})}


H1 = {"count": "10", "sum": "3.5", "buckets": [[0, "0.5", "1", "4"], [3, "1", "2", "6"]]}
H2 = {"count": "12", "sum": "4", "buckets": [[1, "-1", "0", "2"], [0, "0.5", "1", "10"]]}
fx = {
    # A native histogram plus a float series at one instant.
    key("/api/v1/query", query="fixture_native", time=str(T)): ok({"resultType": "vector", "result": [
        {"metric": {"__name__": "fixture_native", "job": "native"}, "histogram": [T, H1]},
        {"metric": {"__name__": "fixture_native", "job": "float"}, "value": [T, "7"]},
    ]}),
    # A range result mixing float and histogram points in one series.
    key("/api/v1/query_range", query="fixture_native", start=str(T), end=str(T + 60), step="30"): ok({"resultType": "matrix", "result": [
        {"metric": {"__name__": "fixture_native", "job": "mixed"},
         "values": [[T + 30, "1.5"]], "histograms": [[T, H1], [T + 60, H2]]},
    ]}),
    # Warnings are logged, rows still returned; fractional-second timestamps keep their milliseconds.
    key("/api/v1/query", query="fixture_warn", time=str(T)): ok(
        {"resultType": "vector", "result": [{"metric": {"job": "w"}, "value": [1767226500.123, "1"]}]},
        warnings=["fixture warning: results may be incomplete"]),
    # A persistent outage: retried, then reported with its status.
    key("/api/v1/query", query="fixture_unavailable", time=str(T)): {
        "status": 503, "body": json.dumps({"status": "error", "errorType": "unavailable", "error": "fixture outage"})},
    # A non-JSON error page (e.g. a proxy) is reported with a snippet.
    key("/api/v1/query", query="fixture_proxy_error", time=str(T)): {"status": 502, "body": "<html>Bad Gateway</html>"},
    # A server-side execution error (422).
    key("/api/v1/query", query="fixture_too_many_samples", time=str(T)): {
        "status": 422, "body": json.dumps({"status": "error", "errorType": "execution",
                                           "error": "query processing would load too many samples into memory in query execution"})},
}
with open(os.path.join(HERE, "handwritten.json"), "w") as f:
    json.dump(fx, f, indent=1, sort_keys=True)
    f.write("\n")
